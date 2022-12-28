import functools
import inspect
import logging
import time

_logger = logging.getLogger(__name__)

def retry(_func=None, *, init_sleep=1, max_sleep=900):
    print("IN RETRY")
    def retry_decorator(func):
        print("IN retry_decorator")
        @functools.wraps(func)
        def wrapper_decorator(*args, **kwargs):
            print("IN wrapper_decorator")
            current_sleep = 0
            total_sleep = 0
            while True:
                try:
                    if inspect.isgeneratorfunction(func):
                        for i in func(*args, **kwargs):
                            yield i
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

        return wrapper_decorator

    if _func is None:
        return retry_decorator
    else:
        return retry_decorator(_func)