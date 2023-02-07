import dagster
import logging
import time
import typing
import uuid

from better_etl.ops.op_wrappers import condition

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

        context.log.info(batch[0]["metadata"])

        return batch
