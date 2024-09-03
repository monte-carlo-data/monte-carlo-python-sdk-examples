from typing import Dict, Mapping, Optional, Union


class GqlError(Exception):
    def __init__(
        self,
        body: Optional[Union[Dict, str]] = None,
        headers: Optional[Mapping] = None,
        message: str = "",
        status_code: int = 0,
        summary: str = "",
        retryable: Optional[bool] = None,
    ):
        self.body = body
        self.headers = headers
        self.message = message
        self.status_code = status_code
        self.summary = summary
        self.retryable = status_code >= 500 if retryable is None else retryable
        super(GqlError, self).__init__()

    def __str__(self) -> str:
        return self.summary


class InvalidSessionError(Exception):
    pass


class InvalidConfigFileError(Exception):
    pass
