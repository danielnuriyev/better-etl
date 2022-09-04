import inspect
import json
import os
import sys
import time
import typing
import uuid
import yaml

from boto3 import Session

from dagster import asset_sensor, job, op, repository
from dagster import AssetKey, AssetMaterialization, OpDefinition, OpExecutionContext, Out, Output, RunRequest, SkipReason

from better_etl.caches import S3Cache
from better_etl.sources import MySQLSource

"""
This code is just for playing with various ideas using dagster
"""

@op
def get_aws_secret(context: OpExecutionContext) -> typing.Dict:
    context.log.info("get_aws_secret")
    secret_name = context.solid_config["secret_name"]
    session = Session()
    client = session.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    secret = response["SecretString"]
    return json.loads(secret)


@op
def extract_db_batch(context :OpExecutionContext, secret :dict):

    last_keys = context.solid_config.get("last_keys", None)
    context.log.info(f"last keys: {last_keys}")

    context.log.info(f"bucket: {context.solid_config['cache_bucket']}")
    context.log.info(f"path: {context.solid_config['cache_path']}")

    c = MySQLSource(
        host=context.solid_config["host"],
        user=secret["username"],
        password=secret["password"],
        database=context.solid_config["database"],
        table=context.solid_config["table"],
        limit=context.solid_config["batch"],
        sleep=0,
        stream=False,  # for a small table that will not overfill the local storage, one can use False
        logger=context.log,
        cache=S3Cache(
            bucket=context.solid_config["cache_bucket"],
            path=context.solid_config["cache_path"]
        ),
        start_keys=last_keys
    )
    batch = next(c.next_batch())

    context.log_event(
        AssetMaterialization(
            asset_key=f"{context.job_name}_batch",
            description=f"{context.job_name}_batch",
            metadata={
                "last_keys": batch["metadata"]["last_keys"]
            }
        )
    )

    return batch


@op
def store(context: OpExecutionContext, batch) -> None:
    bucket = context.solid_config["bucket"]
    path = context.solid_config["path"]
    if bucket[-1] == "/": bucket = bucket[:-1]
    if path[0] == "/": path = path[1:]
    if path[-1] == "/": path = path[:-1]
    timestamp = time.strftime("%y%m%d%H%M%S")
    uid = str(uuid.uuid4())
    url = f"s3://{bucket}/{path}/{timestamp}-{uid}.parquet"
    batch["data"].to_parquet(url)


def build_job(op_funcs, job_conf):

    job_name = job_conf["name"]

    ops_list = job_conf["ops"]
    ops_dict = {}
    job_conf = {"ops":{}}
    for op_conf in ops_list:
        if "config" in op_conf:
            job_conf["ops"][op_conf["name"]] = {"config": op_conf["config"]}
        ops_dict[op_conf["name"]] = op_conf

    def dive(op_names, op_returns, depth):
        for op_name in op_names:
            op_conf = ops_dict[op_name]
            action = op_conf["action"]
            if "after" not in op_conf:
                if op_name not in op_returns:
                    op = op_funcs[action]
                    r = op()
                    op_returns[op_name] = r
            else:
                after_list = op_conf["after"]
                dive(after_list, op_returns, depth + 1)
                if op_name not in op_returns:
                    cur_returns = []
                    for prev_name in after_list:
                        prev_return = op_returns[prev_name]
                        cur_returns.append(prev_return)
                    cur_op = op_funcs[action]
                    op_returns[op_name] = cur_op(*cur_returns)


    @job(config=job_conf, name=job_name)
    def j():
        op_returns = {}
        dive(ops_dict.keys(), op_returns, 0)

    return job_conf, j


def build_sensor(job_conf, dagster_job_conf, job_func):

    job_name = job_conf["name"]

    @asset_sensor(name=job_name, asset_key=AssetKey(f"{job_name}_batch"), job=job_func)
    def s(context, asset_event):

        last_keys = asset_event.dagster_event.event_specific_data.materialization.metadata_entries[0].entry_data.data
        print(f"sensor: {last_keys}")

        for op in dagster_job_conf["ops"].values():
            if "type" in op["config"] and op["config"]["type"] == "source":
                op["config"]["last_keys"] = last_keys

        print(dagster_job_conf)

        return RunRequest(
            run_key=str(last_keys),
            run_config=dagster_job_conf,
        )

    return s


@repository
def repo():

    op_funcs = {}
    for m in inspect.getmembers(sys.modules[__name__]):
        if type(m[1]).__name__ == OpDefinition.__name__:
            op_funcs[m[0]] = m[1]

    conf_path = os.path.join(os.getcwd(), "conf", "local.yaml")
    with open(conf_path) as f:
        job_conf = yaml.safe_load(f)

    dagster_job_conf, j = build_job(op_funcs, job_conf)
    s = build_sensor(job_conf, dagster_job_conf, j)

    return [j, s]


def main() -> int:

    # S3Cache(bucket="ss-bi-qa-danieln", path="etl/cache").get("test")
    # return 0

    r = repo()
    j = r[0]
    j.execute_in_process()
    
    return 0 if len(repo()) > 0 else 1


if __name__ == '__main__':

    sys.exit(main())
