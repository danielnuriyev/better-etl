from datetime import datetime

import dagster

class Partition:

    @dagster.op()
    def timestamp_to_date(context: dagster.OpExecutionContext, batch):

        partition_by_column = context.op_config["partition_by_column"]
        partition_column_name = context.op_config["partition_column_name"]
        batch[partition_column_name] = batch[partition_by_column].apply(lambda x: datetime.fromtimestamp(x).strftime("%Y-%m-%d"))

        return batch