import dagster
import importlib
import pandas as pd
import typing

from better_etl.caches import Cache
from better_etl.sources import MySQLSource

from better_etl.ops.op_wrappers import condition
from better_etl.utils.reflect import create_instance

class MySQL:

    def get_op_metadata(self):
        return {
            "get_batches": {
                "return": {
                    "dynamic": True
                }
            }
        }

    @dagster.op(out=dagster.DynamicOut(), required_resource_keys={"cache"})
    @condition
    def get_batches(context: dagster.OpExecutionContext, secret: typing.Dict):

        c = MySQLSource(
            host=context.solid_config["host"],
            user=secret["username"],
            password=secret["password"],
            database=context.solid_config["database"],
            table=context.solid_config["table"],
            limit=context.solid_config["batch"],
            stream=False,  # for a small table that will not overfill the local storage, one can use False
            logger=context.log,
            cache=context.resources.cache
        )

        max_matches = context.solid_config.get("batches", None)
        batch_count = 0

        for batch in c.next_batch():
            key = "-".join(str(v) for v in batch["metadata"]["last_keys"].values())
            yield dagster.DynamicOutput(batch, mapping_key=key)
            batch_count += 1
            if max_matches and batch_count >= max_matches:
                break
