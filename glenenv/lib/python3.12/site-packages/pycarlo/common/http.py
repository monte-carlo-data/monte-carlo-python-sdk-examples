import json
from typing import Dict, Union

import requests

from pycarlo.common.retries import ExponentialBackoffJitter, retry_with_backoff
from pycarlo.common.settings import DEFAULT_RETRY_INITIAL_WAIT_TIME, DEFAULT_RETRY_MAX_WAIT_TIME


@retry_with_backoff(
    backoff=ExponentialBackoffJitter(DEFAULT_RETRY_INITIAL_WAIT_TIME, DEFAULT_RETRY_MAX_WAIT_TIME),
    exceptions=(requests.exceptions.ConnectionError, requests.exceptions.Timeout),
)
def upload(
    url: str,
    content: Union[bytes, str, Dict],
    method: str = "post",
    encoding: str = "utf-8",
):
    """
    Upload a file to a given URL.

    :param url: URL to use for upload
    :param content: file content
    :param method: HTTP method (e.g. 'post' or 'put')
    :param encoding: character encoding to use (unless raw bytes are provided)
    """
    if isinstance(content, str):
        data = content.encode(encoding)
    elif isinstance(content, dict):
        data = json.dumps(content).encode(encoding)
    else:
        data = content

    response = requests.request(method=method, url=url, data=data)
    response.raise_for_status()
