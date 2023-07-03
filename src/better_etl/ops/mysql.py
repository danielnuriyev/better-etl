import dagster
import typing

from better_etl.sources import MySQLSource

from better_etl.ops.op_wrappers import condition

class MySQL:

    def get_op_metadata(self):
        return {
            "get_batches": {
                "return": {
                    "dynamic": True
                }
            }
        }

    @dagster.op(
        out=dagster.DynamicOut(),
        required_resource_keys={"cache"},
        retry_policy=dagster.RetryPolicy(max_retries=2, delay=1, backoff=dagster.Backoff(dagster.Backoff.EXPONENTIAL))
    )
    @condition
    def get_batches(context: dagster.OpExecutionContext, secret: typing.Dict):

        context.log.info("IN BATCHES")
        context.log.info(context.resources.cache)

        c = MySQLSource(
            host=context.op_config["host"],
            user=secret["username"],
            password=secret["password"],
            database=context.op_config["database"],
            table=context.op_config["table"],
            limit=context.op_config["batch"],
            stream=False,  # for a small table that will not overfill the local storage, one can use False
            logger=context.log,
            cache=context.resources.cache
        )

        context.log.info(c)
        context.log.info(c.next_batch())

        max_matches = context.op_config.get("batches", None)
        batch_count = 0

        for batch in c.next_batch():
            key = "-".join(str(v) for v in batch["metadata"]["last_keys"].values())
            yield dagster.DynamicOutput(batch, mapping_key=key)
            batch_count += 1
            if max_matches and batch_count >= max_matches:
                break
