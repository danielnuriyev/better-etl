import dagster
import importlib
import typing

from better_etl.caches import S3Cache
from better_etl.sources import MySQLSource

from better_etl.ops.op_wrappers import condition

class MySQL:

    @classmethod
    @dagster.op
    @condition
    def get_batch(context: dagster.OpExecutionContext, secret: typing.Dict):

        last_keys = context.solid_config.get("last_keys", None)
        context.log.info(f"last keys: {last_keys}")

        cache = None
        cache_config = context.solid_config.get("cache", None)
        if cache_config is not None:

            cache_class = cache_config.get("class", None)
            i = cache_class.rindex('.')
            module_name = cache_class[0 : i]
            class_name = cache_class[i+1:]
            module = importlib.import_module(module_name)
            class_ = getattr(module, class_name)

            cache_config.pop("class")
            cache = class_(**cache_config)

        c = MySQLSource(
            host=context.solid_config["host"],
            user=secret["username"],
            password=secret["password"],
            database=context.solid_config["database"],
            table=context.solid_config["table"],
            limit=context.solid_config["batch"],
            sleep=0,
            stream=False,  # for a small table that will not overfill the local storage, one can use False
            logger=context.log,
            cache=cache,
            start_keys=last_keys
        )
        batch = next(c.next_batch())

        context.log_event(
            dagster.AssetMaterialization(
                asset_key=f"{context.job_name}_batch",
                description=f"{context.job_name}_batch",
                metadata={
                    "last_keys": batch["metadata"]["last_keys"]
                }
            )
        )

        return batch