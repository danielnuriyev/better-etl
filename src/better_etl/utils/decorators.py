import functools
import logging
import time

logger = logging.getLogger(__init__)

def retry(init_sleep=1, max_sleep=900):
    def retry_decorator(func):
        @functools.wraps(func)
        def wrapper_decorator(*args, **kwargs):
            current_sleep = 0
            total_sleep = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    current_sleep += init_sleep
                    logger.warning(f"Sleeping for {current_sleep} seconds before executing {func}")
                    time.sleep(current_sleep)
                    total_sleep += current_sleep
                    if total_sleep > max_sleep:
                        msg = f"Failed to execute {func} after {max_sleep} seconds"
                        logger.error(msg)
                        raise msg

        return wrapper_decorator
    return retry_decorator