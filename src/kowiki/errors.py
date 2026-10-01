import time
from collections.abc import Callable


class TransientError(Exception):
    """잠시 뒤 다시 시도하면 성공할 수 있는 오류."""


def retry[T](fn: Callable[[], T], attempts: int = 5, delay: float = 1.0) -> T:
    for i in range(attempts - 1):
        try:
            return fn()
        except TransientError:
            time.sleep(delay * 2**i)
    return fn()
