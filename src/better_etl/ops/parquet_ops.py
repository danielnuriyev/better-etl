import boto3
import dagster

from better_etl.ops.op_wrappers import condition
from better_etl.utils.compact import compact

import humanfriendly

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

        max_memory = humanfriendly.parse_size(context.op_config["max_memory"])
        max_file_size = humanfriendly.parse_size(context.op_config["max_file_size"])
        output_bucket = context.op_config["bucket"]
        output_path = context.op_config["path"]

        compact_path = context.op_config.get("compact_path", False)

        if compact_path:

            s3 = boto3.client('s3')

            response = s3.list_objects_v2(
                Bucket=output_bucket
            )

            while True:

                contents = response["Contents"]
                files = []
                for content in contents:
                    size = content["Size"]
                    if size < max_file_size:
                        key = content["Key"]
                        files.append({
                            "bucket": output_bucket,
                            "key": key
                        })

                compact(files, max_memory, max_file_size, output_bucket, output_path)

                token = response.get("ContinuationToken", None)

                if token:
                    response = s3.list_objects_v2(
                        Bucket=output_bucket,
                        ContinuationToken=token
                    )
                else:
                    break

        else:

            files = []
            file_names = []
            for file in batch:

                bucket = file["metadata"]["s3"]["bucket"]
                path = file["metadata"]["s3"]["path"]
                file_name = file["metadata"]["s3"]["file_name"]
                file_names.append(file_name)

                key = f"{path}/{file_name}"

                files.append({
                    "bucket": bucket,
                    "key": key
                })

            compact(files, max_memory, max_file_size, output_bucket, output_path)

        return {
            "bucket": bucket,
            "path": path,
            "file_names": file_names
        }






