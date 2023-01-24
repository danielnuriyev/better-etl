import dagster
import time
import typing
import uuid

from better_etl.ops.op_wrappers import condition

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
    def store(context: dagster.OpExecutionContext, batch: typing.Dict):

        # TODO

        return batch
