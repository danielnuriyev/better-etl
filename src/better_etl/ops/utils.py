import dagster

class Utils:

    @dagster.op
    def find_last_keys(context: dagster.OpExecutionContext, collection):

        context.log.info(type(collection))

        max_item = {}
        cache_key = None
        for item in collection:
            metadata = item["metadata"]
            last_keys = metadata["last_keys"]
            for k in last_keys.keys():
                pre_v = max_item.get(k, None)
                cur_v = last_keys[k]
                if pre_v is None:
                    max_item[k] = cur_v
                    cache_key = metadata["cache_key"]
                else:
                    if cur_v > pre_v:
                        max_item[k] = cur_v
                        cache_key = metadata["cache_key"]

        return {
            "last_keys": max_item,
            "cache_key": cache_key
        }