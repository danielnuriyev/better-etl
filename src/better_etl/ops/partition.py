from datetime import datetime

import dagster

class Partition:

    def get_op_metadata(self):
        return {
            "timestamp_to_date": {
                "return": {
                    "dynamic": False
                }
            },
            "copy": {
                "return": {
                    "dynamic": False
                }
            }
        }

    @dagster.op(
        #out=dagster.DynamicOut()
    )
    def timestamp_to_date(context: dagster.OpExecutionContext, batch):

        partition_by_column = context.op_config["partition_by_column"]
        partition_column_name = context.op_config["partition_column_name"]

        context.log.info("timestamp_to_date " * 5)
        context.log.info(batch.keys())

        df = batch["data"]
        df[partition_column_name] = df[partition_by_column].apply(lambda x: datetime.fromtimestamp(x).strftime("%Y-%m-%d"))

        batch["metadata"]["partition_column_name"] = partition_column_name

        return batch

    @dagster.op(
        #out=dagster.DynamicOut()
    )
    def copy(context: dagster.OpExecutionContext, batch):
        partition_by_column = context.op_config["partition_by_column"]
        partition_column_name = context.op_config["partition_column_name"]
        df = batch["data"]
        df[partition_column_name] = df[partition_by_column].copy()

        batch["metadata"]["partition_column_name"] = partition_column_name

        return batch