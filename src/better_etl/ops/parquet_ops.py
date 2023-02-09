import dagster
import logging
import time
import typing
import uuid

from better_etl.ops.op_wrappers import condition

import boto3
import pandas as pd

class Parquet:

    def get_op_metadata(self):
        return {
            "compact": {
                "return": {
                    "dynamic": False
                }
            }
        }

    @dagster.op(
        retry_policy=dagster.RetryPolicy(max_retries=2, delay=1, backoff=dagster.Backoff(dagster.Backoff.EXPONENTIAL))
    )
    @condition
    def compact(context: dagster.OpExecutionContext, batch):

        dfs = []
        for file in batch:
            bucket = file["metadata"]["s3"]["bucket"]
            path = file["metadata"]["s3"]["path"]
            filename = file["metadata"]["s3"]["filename"]
            url = f"s3://{bucket}/{path}/{filename}"
            context.log.info(f"Loading {url}")
            df = pd.read_parquet(url)
            dfs.append(df)

        df = pd.concat(dfs) #, ignore_index=True)
        timestamp = time.strftime("%y%m%d%H%M%S")
        uid = str(uuid.uuid4())
        filename = f"compacted-{timestamp}-{uid}.parquet"
        url = f"s3://{bucket}/{path}/{filename}"
        context.log.info(f"Compacting into {url}")
        df.to_parquet(url)

        # if success:

        s3 = boto3.client('s3')
        for file in batch:
            bucket = file["metadata"]["s3"]["bucket"]
            path = file["metadata"]["s3"]["path"]
            filename = file["metadata"]["s3"]["filename"]
            key = f"{path}/{filename}"
            context.log.info(f"Deleting {bucket}/{key}")
            s3.delete_object(
                Bucket=bucket,
                Key=key
            )

        return {
            "bucket": bucket,
            "path": path,
            "filename": filename
        }
