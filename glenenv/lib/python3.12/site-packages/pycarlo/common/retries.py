import random
import time
from abc import ABC, abstractmethod
from functools import wraps
from typing import Any, Callable, Generator, Tuple, Type, Union


class Backoff(ABC):
    """
    Backoff is an abstract class that dictates a retry strategy.
    It contains an abstract method `backoff` that returns a calculated delay based on the provided
    attempt.
    """

    def __init__(self, start: float, maximum: float):
        """
        Defines a new backoff retry strategy.

        :param start: the scaling factor for any calculated delays.
        :param maximum: defines a cap on the calculated delays to prevent prohibitively long waits
                        that could time out.
        """
        self.start = start
        self.maximum = maximum

    @abstractmethod
    def backoff(self, attempt: int) -> float:
        pass

    def delays(self) -> Generator[float, None, None]:
        """
        Generates a duration of time to delay for each successive call based on the configured
        backoff strategy.

        :return: a generator that yields the next delay duration.
        """
        duration = 0
        retries = 0
        while duration < self.maximum:
            duration = self.backoff(retries)
            yield duration
            retries += 1


def retry_with_backoff(
    backoff: Backoff, exceptions: Union[Type[Exception], Tuple[Type[Exception], ...]]
) -> Callable:
    """
    A decorator to retry a function based on the :param:`backoff` provided
    if any of the provided :param:`exceptions` are raised.

    :param backoff: the retry strategy to employ.
    :param exceptions: the exceptions that should trigger the retry. Can be further customized by
                       defining a custom attribute `retryable` on the exception class. The retries
                       are abandoned If retryable returns False.
    :return: The same result the decorated function returns.
    """

    def _retry(func: Callable):
        @wraps(func)
        def _impl(*args: Any, **kwargs: Any):
            delays = backoff.delays()
            while True:
                try:
                    result = func(*args, **kwargs)
                except exceptions as exception:
                    if not hasattr(exception, "retryable") or exception.retryable:  # type: ignore
                        pass
                    else:
                        raise exception

                    try:
                        delay = next(delays)
                    except StopIteration:
                        raise exception
                    time.sleep(delay)
                else:
                    return result

        return _impl

    return _retry


class ExponentialBackoff(Backoff):
    """
    A backoff strategy with an exponentially increasing delay in between attempts.
    """

    def exponential(self, attempt: int) -> float:
        return min(self.maximum, pow(2, attempt) * self.start)

    def backoff(self, attempt: int) -> float:
        return self.exponential(attempt)


class ExponentialBackoffJitter(ExponentialBackoff):
    """
    An exponential backoff strategy with an added jitter that randomly spreads out the delays
    uniformly.
    """

    def backoff(self, attempt: int) -> float:
        return random.uniform(0, self.exponential(attempt))
