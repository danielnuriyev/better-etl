import dagster
import time
import typing
import uuid

class AWSS3:

    @classmethod
    @dagster.op
    def store(context: dagster.OpExecutionContext, batch: typing.Dict) -> None:
        bucket = context.solid_config["bucket"]
        path = context.solid_config["path"]
        if bucket[-1] == "/": bucket = bucket[:-1]
        if path[0] == "/": path = path[1:]
        if path[-1] == "/": path = path[:-1]
        timestamp = time.strftime("%y%m%d%H%M%S")
        uid = str(uuid.uuid4())
        url = f"s3://{bucket}/{path}/{timestamp}-{uid}.parquet"
        batch["data"].to_parquet(url)