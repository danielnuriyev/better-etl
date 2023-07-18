import boto3
import time
import uuid

import pandas as pd


def _df_to_s3(dfs, s3, bucket, path, files):
    df = pd.concat(dfs, ignore_index=True)
    dfs.clear()

    timestamp = time.strftime("%y%m%d%H%M%S")
    uid = str(uuid.uuid4())
    file_name = f"compacted-{timestamp}-{uid}.parquet"
    url = f"s3://{bucket}/{path}/{file_name}"

    df.to_parquet(url)
    df = None

    for file in files:

        s3.delete_object(
            Bucket=file["bucket"],
            Key=file["key"]
        )

    return True


def compact(files, max_memory, max_file_size, output_bucket, output_path):

    dfs = []
    merged_memory_size = 0
    merged_file_size = 0
    files_to_delete = []

    s3 = boto3.client('s3')

    for file in files:

        bucket = file["bucket"]
        key = file["key"]

        response = s3.get_object_attributes(
            Bucket=bucket,
            Key=key,
            ObjectAttributes=[
                "ObjectSize"
            ]
        )
        size = response["ObjectSize"]

        url = f"s3://{bucket}/{key}"

        df = pd.read_parquet(url)

        df_memory_size = df.memory_usage(index=True, deep=True).sum()

        if merged_memory_size + df_memory_size >= max_memory or merged_file_size + size >= max_file_size:
            _df_to_s3(dfs, s3, output_bucket, output_path, files_to_delete)

            merged_memory_size = 0
            merged_file_size = 0
            dfs.clear()
            files_to_delete.clear()

        dfs.append(df)
        files_to_delete.append(file)
        merged_memory_size += df_memory_size
        merged_file_size += size

    if len(dfs) > 0:
        _df_to_s3(dfs, s3, output_bucket, output_path, files_to_delete)
