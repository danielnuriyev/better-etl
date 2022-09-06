import dagster
import typing

from better_etl.caches import S3Cache
from better_etl.sources import MySQLSource

class MySQL:

    @classmethod
    @dagster.op
    def get_batch(context: dagster.OpExecutionContext, secret: typing.Dict):
        last_keys = context.solid_config.get("last_keys", None)
        context.log.info(f"last keys: {last_keys}")

        context.log.info(f"bucket: {context.solid_config['cache_bucket']}")
        context.log.info(f"path: {context.solid_config['cache_path']}")

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
            cache=S3Cache(
                bucket=context.solid_config["cache_bucket"],
                path=context.solid_config["cache_path"]
            ),
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