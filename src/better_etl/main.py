import importlib
import os
import sys
import yaml

from dagster import asset_sensor, job, repository
from dagster import AssetKey, RunRequest


def build_job(job_conf):

    job_name = job_conf["name"]

    ops_list = job_conf["ops"]
    ops_dict = {}
    job_conf = {"ops": {}}
    for op_conf in ops_list:
        if "config" not in op_conf:
            op_conf["config"] = {}

        op_conf["config"]["job_name"] = job_name
        job_conf["ops"][op_conf["name"]] = {"config": op_conf["config"]}
        ops_dict[op_conf["name"]] = op_conf

    op_packages = {}
    op_classes = {}

    def dive(op_names, op_returns, depth):
        for op_name in op_names:
            op_conf = ops_dict[op_name]
            package_name = op_conf["package"]
            if package_name not in op_packages:
                package_obj = importlib.import_module(package_name)
                op_packages[package_name] = package_obj

            class_name = op_conf["class"]
            full_class_name = f"{package_name}.{class_name}"
            if full_class_name not in op_classes:
                class_obj = getattr(op_packages[package_name], class_name)
                class_inst = class_obj()
                op_classes[full_class_name] = class_inst
            else:
                class_inst = op_classes[full_class_name]

            method_name = op_conf["method"]

            if "after" not in op_conf:
                if op_name not in op_returns:
                    op = getattr(class_inst, method_name).alias(op_name)
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
                    cur_op = getattr(class_inst, method_name).alias(op_name)
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

    conf_path = os.path.join(os.getcwd(), "conf", "local.yaml")
    with open(conf_path) as f:
        job_conf = yaml.safe_load(f)

    dagster_job_conf, j = build_job(job_conf)
    s = build_sensor(job_conf, dagster_job_conf, j)

    return [j, s]


def main() -> int:
    r = repo()
    j = r[0]
    j.execute_in_process()
    
    return 0 if len(repo()) > 0 else 1

if __name__ == '__main__':

    sys.exit(main())
