
from dagster import resource

from better_etl.caches.cache import Cache
from better_etl.utils.reflect import create_instance

@resource
def cache(context):

    if context.resource_config:
        class_name = context.resource_config.pop("class", None)
        if class_name:
            return create_instance(class_name, context.resource_config)
        else:
            return None
    else:
        return None
