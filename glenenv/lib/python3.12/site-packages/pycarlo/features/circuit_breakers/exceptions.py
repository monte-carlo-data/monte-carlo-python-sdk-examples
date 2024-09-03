from typing import Any


class CircuitBreakerPipelineException(Exception):
    pass


class CircuitBreakerPollException(Exception):
    def __init__(self, msg: str = "Polling timed out or contains a malformed log.", *args: Any):
        super().__init__(msg, *args)
