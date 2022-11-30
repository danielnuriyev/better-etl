import dagster

class Cache:

    @dagster.op(
        required_resource_keys={"cache"},
        retry_policy=dagster.RetryPolicy(max_retries=2, delay=1, backoff=dagster.Backoff(dagster.Backoff.EXPONENTIAL))
    )
    def put(context: dagster.OpExecutionContext, metadata):

        context.log.info(metadata)

        context.resources.cache.put(
            metadata["cache_key"],
            metadata["last_keys"]
        )
