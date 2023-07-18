import dagster

class Partition:

    @dagster.op()
    def timestamp_to_date(context: dagster.OpExecutionContext, batch):
        """
        TODO:
        1. get partition column from conf
        2. add a partition column to df
        """
        return batch