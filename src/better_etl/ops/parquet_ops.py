import dagster
import time
import typing
import uuid

from better_etl.ops.op_wrappers import condition

class Parquet:

    def get_op_metadata(self):
        return {
            "store": {
                "return": {
                    "dynamic": False
                }
            }
        }

    @dagster.op(
        retry_policy=dagster.RetryPolicy(max_retries=2, delay=1, backoff=dagster.Backoff(dagster.Backoff.EXPONENTIAL))
    )
    @condition
    def compact(context: dagster.OpExecutionContext, batch: typing.Dict):

        bucket = context.solid_config["bucket"]
        path = context.solid_config["path"]
        if bucket[-1] == "/": bucket = bucket[:-1]
        if path[0] == "/": path = path[1:]
        if path[-1] == "/": path = path[:-1]

        timestamp = time.strftime("%y%m%d%H%M%S")
        uid = str(uuid.uuid4())
        url = f"s3://{bucket}/{path}/{timestamp}-{uid}.parquet"

        return batch
