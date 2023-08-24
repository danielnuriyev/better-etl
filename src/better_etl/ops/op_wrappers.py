import functools

def condition(func):
    @functools.wraps(func)
    def wrapper_decorator(*args, **kwargs):

        context = args[0]

        for key, val in kwargs.items():
            exec(key + '=val')
            # exec(f'print({key})')

        condition = context.op_config.get("condition", None)
        if condition and not eval(condition):
            context.log.info(f"Skipping {condition}")
            return None
        else:
            return func(*args, **kwargs)
    return wrapper_decorator
