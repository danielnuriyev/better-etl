import dagster

class Cache:

    @dagster.op(required_resource_keys={"cache"})
    def put(context: dagster.OpExecutionContext, metadata):

        context.log.info(metadata)

        context.resources.cache.put(
            metadata["cache_key"],
            metadata["last_keys"]
        )
