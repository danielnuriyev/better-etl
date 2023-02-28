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

    @classmethod
    def _df_to_s3(cls, context, dfs, bucket, path, files):

        df = pd.concat(dfs, ignore_index=True)
        dfs.clear()

        timestamp = time.strftime("%y%m%d%H%M%S")
        uid = str(uuid.uuid4())
        file_name = f"compacted-{timestamp}-{uid}.parquet"
        url = f"s3://{bucket}/{path}/{file_name}"
        context.log.info(f"Compacting into {url}")
        df.to_parquet(url)
        df = None

        s3 = boto3.client('s3')
        for file in files:
            bucket = file["metadata"]["s3"]["bucket"]
            path = file["metadata"]["s3"]["path"]
            file_name = file["metadata"]["s3"]["file_name"]
            key = f"{path}/{file_name}"
            context.log.info(f"Deleting {bucket}/{key}")
            s3.delete_object(
                Bucket=bucket,
                Key=key
            )

        return True

    @dagster.op(
        retry_policy=dagster.RetryPolicy(max_retries=2, delay=1, backoff=dagster.Backoff(dagster.Backoff.EXPONENTIAL))
    )
    @condition
    def compact(context: dagster.OpExecutionContext, batch):

        max_memory = context.op_config["max_memory"]
        max_file_size = context.op_config["max_file_size"]

        files = []
        dfs = []
        merged_file_size = 0
        merged_memory_size = 0
        file_names = []
        for file in batch:

            bucket = file["metadata"]["s3"]["bucket"]
            path = file["metadata"]["s3"]["path"]
            file_name = file["metadata"]["s3"]["file_name"]
            file_size = file["metadata"]["s3"]["file_size"]

            file_names.append(file_name)

            url = f"s3://{bucket}/{path}/{file_name}"

            context.log.info(f"Loading {url}")
            df = pd.read_parquet(url)

            merged_memory_size += df.memory_usage(index=True, deep=True).sum()
            merged_file_size += file_size

            if merged_memory_size >= max_memory or merged_file_size >= max_file_size:

                Parquet._df_to_s3(context, dfs, bucket, path, files)

                merged_memory_size = 0
                merged_file_size = 0
                dfs.clear()
                files.clear()

            dfs.append(df)
            files.append(file)

        if len(dfs) > 0:
            Parquet._df_to_s3(context, dfs, bucket, path, files)

        return {
            "bucket": bucket,
            "path": path,
            "file_names": file_names
        }






