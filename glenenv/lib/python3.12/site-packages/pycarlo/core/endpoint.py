import json
from copy import deepcopy
from typing import Any, Dict, Optional, Union, cast

from requests import Request, Timeout
from requests.exceptions import HTTPError
from sgqlc.endpoint.requests import RequestsEndpoint

from pycarlo.common.errors import GqlError
from pycarlo.common.retries import Backoff, ExponentialBackoffJitter, retry_with_backoff
from pycarlo.common.settings import (
    DEFAULT_IDEMPOTENT_RETRY_INITIAL_WAIT_TIME,
    DEFAULT_IDEMPOTENT_RETRY_MAX_WAIT_TIME,
)
from pycarlo.core.operations import Mutation, Query

X_MCD_IDEMPOTENT_ID = "x-mcd-idempotent-id"
X_MCD_RESPONSE_CONTENT_TYPE = "x-mcd-response-content-type"


class GqlIdempotentRequestRunningError(Exception):
    pass


class Endpoint(RequestsEndpoint):
    def __init__(
        self,
        url: str,
        base_headers: dict,
        timeout: float,
        retry_backoff: Backoff,
        idempotent_retry_backoff: Optional[Backoff] = None,
    ):
        super(Endpoint, self).__init__(url, base_headers=base_headers, timeout=timeout)
        self.retry_backoff = retry_backoff
        self._idempotent_retry_backoff = idempotent_retry_backoff or ExponentialBackoffJitter(
            DEFAULT_IDEMPOTENT_RETRY_INITIAL_WAIT_TIME,
            DEFAULT_IDEMPOTENT_RETRY_MAX_WAIT_TIME,
        )

    def __call__(
        self,
        query: Union[Query, Mutation, str],
        variables: Optional[Dict] = None,
        operation_name: Optional[str] = None,
        extra_headers: Optional[Dict] = None,
        timeout: Optional[int] = None,
        idempotent_request_id: Optional[str] = None,
        response_type: Optional[str] = None,
    ):
        """
        Overloads the inherited `__call__` method that calls the GraphQL endpoint.
        This overload is necessary to wrap the endpoint call with the caller-specified
        retry strategy.

        :param query: the GraphQL query or mutation to execute. Note
          that this is converted using ``bytes()``, thus one may pass
          an object implementing ``__bytes__()`` method to return the
          query, eventually in more compact form (no indentation, etc).
        :type query: :class:`str` or :class:`bytes`.

        :param variables: variables (dict) to use with
          ``query``. This is only useful if the query or
          mutation contains ``$variableName``.
          Must be a **plain JSON-serializeable object**
          (dict with string keys and values being one of dict, list, tuple,
          str, int, float, bool, None... -- :func:`json.dumps` is used)
          and the keys must **match exactly** the variable names (no name
          conversion is done, no dollar-sign prefix ``$`` should be used).
        :type variables: dict

        :param operation_name: if more than one operation is listed in
          ``query``, then it should specify the one to be executed.
        :type operation_name: str

        :param extra_headers: dict with extra HTTP headers to use.
        :type extra_headers: dict

        :param timeout: overrides the default timeout.
        :type timeout: float

        :return: dict with optional fields ``data`` containing the GraphQL
          returned data as nested dict and ``errors`` with an array of
          errors. Note that both ``data`` and ``errors`` may be returned!
        :rtype: dict
        """

        if idempotent_request_id:
            extra_headers = deepcopy(extra_headers) if extra_headers else {}
            extra_headers[X_MCD_IDEMPOTENT_ID] = idempotent_request_id

        if response_type:
            extra_headers = deepcopy(extra_headers) if extra_headers else {}
            extra_headers[X_MCD_RESPONSE_CONTENT_TYPE] = response_type

        @retry_with_backoff(backoff=self.retry_backoff, exceptions=(GqlError, Timeout))
        def action():
            return super(Endpoint, self).__call__(
                query,
                variables=variables,
                operation_name=operation_name,
                extra_headers=extra_headers,
                timeout=timeout,
            )

        if idempotent_request_id:
            # wrap to keep retrying while the idempotent request is still running
            # retry policy is different as we need to wait more time, so we use an additional
            # retry wrapper

            @retry_with_backoff(
                backoff=self._idempotent_retry_backoff, exceptions=GqlIdempotentRequestRunningError
            )
            def idempotent_action():
                self.logger.debug(f"Sending idempotent request with id={idempotent_request_id}")
                return action()

            return idempotent_action()
        else:
            return action()

    def _log_graphql_error(self, query: str, data: Dict):
        """
        Overwrites `_log_graphql_error` from :class:`sgqlc.endpoint.BaseEndpoint` in order to better
        handle errors returned from the GraphQL response.
        This implementation raises a :exc:`pycarlo.common.errors.GqlError` exception that wraps the
        errors returned to allow the caller of the endpoint to decide the level of detail they'd
        like. If there are multiple errors, the GqlError message is newline-delimited to show each
        one. It still keeps the same logging behavior from the parent.

        :param query: the GraphQL query that triggered the result.
        :type query: str

        :param data: the decoded JSON object.
        :type data: dict

        :return: the input ``data``
        :rtype: dict

        :raises: :exc:`pycarlo.common.errors.GqlError`
        """

        if isinstance(query, bytes):  # pragma: no cover
            query = query.decode("utf-8")
        elif not isinstance(query, str):  # pragma: no cover
            # allows sgqlc.operation.Operation to be passed
            # and generate compact representation of the queries
            query = bytes(query).decode("utf-8")

        data = self._fixup_graphql_error(data)
        errors = data["errors"]
        for i, error in enumerate(errors):
            paths = error.get("path")
            if paths:
                paths = " " + "/".join(str(path) for path in paths)
            else:
                paths = ""
            self.logger.info("Error #{}{}:".format(i, paths))
            for line in error.get("message", "").split("\n"):
                self.logger.info("   | {}".format(line))

            locations = self.snippet(query, error.get("locations"))
            if locations:
                self.logger.info("   -")
                self.logger.info("   | Locations:")
                for line in locations:
                    self.logger.info("   | {}".format(line))

        errors = data["errors"]
        if isinstance(errors, list):
            message = "\n".join([str(error["message"]) for error in errors])
        elif isinstance(errors, dict):
            message = str(errors["message"])
        else:
            message = str(errors)
        error_code = self._get_error_code(errors)
        if error_code == "REQUEST_IN_PROGRESS":
            raise GqlIdempotentRequestRunningError(message)
        if error_code == "REQUEST_TIMEOUT":
            self.logger.error("GraphQL request timed out")
            raise GqlError(
                body=errors,  # type: ignore
                headers={},
                message=message,
                status_code=200,
                summary=message,
                retryable=True,
            )

        self.logger.error("GraphQL request failed with %s errors", len(errors))
        raise GqlError(
            body=errors,  # type: ignore
            headers={},
            message=message,
            status_code=200,
            summary=message,
        )

    @staticmethod
    def _get_error_code(body: Any) -> Optional[str]:
        error: Optional[Dict] = None
        if isinstance(body, list):
            error = body[0]
        elif isinstance(body, dict):
            error = body
        if not error:
            return None
        extensions = error.get("extensions")
        if isinstance(extensions, dict):
            return extensions.get("code")
        return error.get("code")

    def _log_http_error(self, query: str, request: Request, exception: HTTPError):
        """
        Overwrites `_log_http_error` from :class:`sgqlc.endpoint.requests.RequestsEndpoint`
        in order to better customize our desired way of handling
        :exc:`requests.exceptions.HTTPError`. This implementation raises a
        :exc:`pycarlo.common.errors.GqlError` exception in each scenario to allow
        the caller of the endpoint to decide the level of detail they'd like of the error.
        It still keeps the same logging behavior from the parent.

        :param query: the GraphQL query that triggered the result.
        :type query: str

        :param request: :class:`requests.Request` instance that was opened.
        :type request: :class:`requests.Request`

        :param exception: :exc:`requests.exceptions.HTTPError` instance
        :type exception: :exc:`requests.exceptions.HTTPError`

        :return: GraphQL-compliant dict with keys ``data`` and ``errors``.
        :rtype: dict

        :raises: :exc:`pycarlo.common.errors.GqlError`
        """
        is_timeout = exception.response.status_code == 504
        is_idempotent = X_MCD_IDEMPOTENT_ID in request.headers
        if not is_timeout or not is_idempotent:
            # don't log the exception for a timeout if we sent an idempotent request, we'll retry
            self.logger.error("log_error - %s: %s", request.url, exception)

        for header in sorted(exception.response.headers):
            self.logger.info("Response header: %s: %s", header, exception.response.headers[header])

        body = cast(str, exception.response.text)
        content_type = exception.response.headers.get("Content-Type", "")
        self.logger.info("Response [%s]:\n%s", content_type, body)
        if not content_type.startswith("application/json"):
            raise GqlError(
                body=body,
                headers=exception.response.headers,
                message=str(body),
                status_code=exception.response.status_code,
                summary=str(exception),
            )
        try:
            data = json.loads(body)
        except json.JSONDecodeError as err:
            raise GqlError(
                body=body,
                headers=exception.response.headers,
                message=str(err),
                status_code=exception.response.status_code,
                summary=str(err),
            )

        if isinstance(data, dict) and data.get("errors"):
            data.update(
                {
                    "exception": exception,
                    "status": exception.response.status_code,
                    "headers": exception.response.headers,
                }
            )
            return self._log_graphql_error(query, data)

        message = cast(
            str,
            data.get("message")
            if isinstance(data, dict) and data.get("message")
            else str(exception),
        )
        raise GqlError(
            body=body,
            headers=exception.response.headers,
            message=message,
            status_code=exception.response.status_code,
            summary=str(exception),
        )
