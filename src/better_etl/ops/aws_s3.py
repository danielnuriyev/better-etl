import dagster
import logging
import time
import typing
import uuid

from better_etl.ops.op_wrappers import condition

import boto3

class AWSS3:

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
    def store(context: dagster.OpExecutionContext, batch):

        bucket = context.op_config["bucket"]
        path = context.op_config["path"]
        if bucket[-1] == "/": bucket = bucket[:-1]
        if path[0] == "/": path = path[1:]
        if path[-1] == "/": path = path[:-1]
        timestamp = time.strftime("%y%m%d%H%M%S")
        uid = str(uuid.uuid4())
        filename = f"{timestamp}-{uid}.parquet"
        url = f"s3://{bucket}/{path}/{filename}"
        batch["data"].to_parquet(url)

        batch["metadata"]["memory"] = batch["data"].memory_usage(deep=True).sum()

        batch.pop("data")

        response = boto3.client('s3').get_object_attributes(
            Bucket=bucket,
            Key=f"{path}/{filename}",
            ObjectAttributes=[
                "ObjectSize"
            ]
        )
        size = response["ObjectSize"]

        batch["metadata"]["s3"] = {
            "bucket": bucket,
            "path": path,
            "file_name": filename,
            "file_size": size
        }

        context.log.info(batch)

        return batch
