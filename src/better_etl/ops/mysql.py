import dagster
import importlib
import pandas as pd
import typing

from better_etl.caches import Cache
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

    @dagster.op(out=dagster.DynamicOut())
    @condition
    def get_batches(context: dagster.OpExecutionContext, secret: typing.Dict):

        cache = context.solid_config.get("cache", None)
        if cache:
            full_name = cache.pop("class")
            i = full_name.rindex(".")
            module_name = full_name[:i]
            class_name = full_name[i+1:]
            module = importlib.import_module(module_name)
            class_ = getattr(module, class_name)
            cache = class_(**cache)
        else:
            cache = Cache()

        c = MySQLSource(
            host=context.solid_config["host"],
            user=secret["username"],
            password=secret["password"],
            database=context.solid_config["database"],
            table=context.solid_config["table"],
            limit=context.solid_config["batch"],
            stream=False,  # for a small table that will not overfill the local storage, one can use False
            logger=context.log,
            cache=cache
        )
        for batch in c.next_batch():
            key = "-".join(str(v) for v in batch["metadata"]["last_keys"].values())
            yield dagster.DynamicOutput(batch, mapping_key=key)
