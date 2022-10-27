import functools

def condition(func):
    @functools.wraps(func)
    def wrapper_decorator(*args, **kwargs):

        for key, val in kwargs.items():
            exec(key + '=val')

        context = args[0]
        condition = context.solid_config.get("condition", None)
        if condition and not eval(condition):
            context.log.info(f"Skipping {condition}")
            return None
        else:
            return func(*args, **kwargs)
    return wrapper_decorator
