import dagster

import time

import uuid

from better_etl.ops.op_wrappers import condition

class AWSS3:

    @dagster.op(
        retry_policy=dagster.RetryPolicy(max_retries=2, delay=1, backoff=dagster.Backoff(dagster.Backoff.EXPONENTIAL))
    )
    @condition
    def store(context: dagster.OpExecutionContext, batch):

        # context.log.info(batch)

        bucket = context.op_config["bucket"]
        path = context.op_config["path"]
        partition = batch["metadata"].get("partition_column_name", None)

        if bucket[-1] == "/": bucket = bucket[:-1]
        if path[0] == "/": path = path[1:]
        if path[-1] == "/": path = path[:-1]

        timestamp = time.strftime("%y%m%d%H%M%S")
        uid = str(uuid.uuid4().hex)
        filename = f"{timestamp}-{uid}.parquet" # TODO: format should be configurable

        df = batch.pop("data")

        # if partition:
        #    partition = partition.get("column", None)

        partitions = []
        if partition:
            for partition_value, partition_df in df.groupby(partition):
                partition_path = f"s3://{bucket}/{path}/{partition_value}"
                url = f"{partition_path}/{filename}"
                partition_df.to_parquet(url)

                batch.copy()["metadata"]["s3"] = {
                    "bucket": bucket,
                    "path": partition_path,
                    "file_name": filename
                }
                batch["metadata"]["memory"] = partition_df.memory_usage(deep=True).sum()
                # key = uuid.uuid4().hex
                # yield dagster.DynamicOutput(batch, mapping_key=key)
                partitions.append(batch)

        else:
            url = f"s3://{bucket}/{path}/{filename}"
            df.to_parquet(url)

            batch["metadata"]["s3"] = {
                "bucket": bucket,
                "path": path,
                "file_name": filename
            }
            batch["metadata"]["memory"] = df.memory_usage(deep=True).sum()
            # key = uuid.uuid4().hex
            # yield dagster.DynamicOutput(batch, mapping_key=key)
            partitions.append(batch)

        context.log.info(partitions)
        return partitions
