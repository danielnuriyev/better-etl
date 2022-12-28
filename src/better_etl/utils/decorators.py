import functools
import inspect
import logging
import time

_logger = logging.getLogger(__name__)

def retry(_func=None, *, init_sleep=1, max_sleep=900):
    def function_decorator(func):
        @functools.wraps(func)
        def argument_decorator(*args, **kwargs):
            print(func)
            print(inspect.isgeneratorfunction(func))
            current_sleep = 0
            total_sleep = 0
            while True:
                try:

                    if inspect.isgeneratorfunction(func):
                        def g():
                            for i in func(*args, **kwargs):
                                yield i
                        return g()
                    else:
                        return func(*args, **kwargs)

                except Exception as e:
                    current_sleep += init_sleep
                    _logger.warning(f"Sleeping for {current_sleep} seconds before executing {func}")
                    time.sleep(current_sleep)
                    total_sleep += current_sleep
                    if total_sleep > max_sleep:
                        msg = f"Failed to execute {func} after {max_sleep} seconds"
                        _logger.error(msg)
                        raise msg

        return argument_decorator

    if _func is None:
        return function_decorator
    else:
        return function_decorator(_func)