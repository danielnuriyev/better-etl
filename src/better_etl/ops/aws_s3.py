import dagster
import time
import typing
import uuid

from better_etl.ops.op_wrappers import condition

class AWSS3:

    def get_op_metadata(self):
        return {
            "store": {
                "return": {
                    "dynamic": False
                }
            }
        }

    @dagster.op(required_resource_keys={"cache"})
    @condition
    def store(context: dagster.OpExecutionContext, batch: typing.Dict):

        bucket = context.solid_config["bucket"]
        path = context.solid_config["path"]
        if bucket[-1] == "/": bucket = bucket[:-1]
        if path[0] == "/": path = path[1:]
        if path[-1] == "/": path = path[:-1]
        timestamp = time.strftime("%y%m%d%H%M%S")
        uid = str(uuid.uuid4())
        url = f"s3://{bucket}/{path}/{timestamp}-{uid}.parquet"
        batch["data"].to_parquet(url)

        batch.pop("data")

        return batch
