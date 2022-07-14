
import json

from boto3 import Session

from dagster import job, op

from better_etl.caches import PickledCache
from better_etl.sources import MySQLSource

@op
def get_aws_secret(context):
    secret_name = context.op_config["secret_name"]

    session = Session()
    client = session.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    secret = response["SecretString"]
    context.log.info(secret)
    return json.loads(secret)


@op
def extract_db_batch(context, secret):
    cache = PickledCache()
    c = MySQLSource(
        host=context.op_config["host"],
        user=secret["username"],
        password=secret["password"],
        database=context.op_config["database"],
        table=context.op_config["table"],
        limit=context.op_config["batch"],
        sleep=0,
        stream=True,  # for a small table that will not overfill the local storage, one can use False
        cache=cache,  # TODO: init cache from conf
    )
    try:
        for batch in c.next_batch():
            context.log.info(batch)
            break
    finally:
        c.close()


@job
def main():
    extract_db_batch(get_aws_secret())
