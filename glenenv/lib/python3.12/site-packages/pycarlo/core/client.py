import uuid
from typing import Dict, Optional, Union, overload
from urllib.parse import urljoin

import requests
from box import Box, BoxList
from requests import HTTPError, Timeout

from pycarlo.common import get_logger
from pycarlo.common.errors import InvalidSessionError
from pycarlo.common.retries import Backoff, ExponentialBackoffJitter, retry_with_backoff
from pycarlo.common.settings import (
    DEFAULT_IGW_TIMEOUT_SECS,
    DEFAULT_MCD_API_ID_HEADER,
    DEFAULT_MCD_API_TOKEN_HEADER,
    DEFAULT_MCD_SESSION_ID,
    DEFAULT_MCD_TRACE_ID,
    DEFAULT_MCD_USER_ID_HEADER,
    DEFAULT_RETRY_INITIAL_WAIT_TIME,
    DEFAULT_RETRY_MAX_WAIT_TIME,
)
from pycarlo.core.endpoint import Endpoint
from pycarlo.core.operations import Mutation, Query
from pycarlo.core.session import Session

logger = get_logger(__name__)


class Client:
    def __init__(self, session: Optional[Session] = None):
        """
        Create a client for making requests to the MCD API.

        :param session: Specify a session. Otherwise, a session is created using the
                        default profile.
        """
        self._session = session or Session()

    @property
    def session_id(self) -> str:
        """
        Retrieves the MCD API ID from the client's current session. For helping to identify
        requester client-side.
        """
        return self._session.id

    @property
    def session_name(self) -> str:
        """
        Retrieves the session name from the client's current session. For helping trace
        requests downstream.
        """
        return self._session.session_name

    @property
    def session_endpoint(self) -> str:
        """
        Retrieves the session MCD endpoint from the client's current session. By default,
        uses MCD_API_ENDPOINT.
        """
        return self._session.endpoint

    @property
    def session_scope(self):
        """
        Retrieves the scope from the client's current session, when a scope is set the client
        can be used only to call REST endpoints and not GraphQL endpoints.
        """
        return self._session.scope

    def _get_headers(self) -> Dict:
        """
        Gets headers from session for using the MCD API.

        Generates a trace ID to help trace (e.g. debug) specific requests downstream.
        Enable verbose logging to echo.
        """
        headers = {
            DEFAULT_MCD_API_ID_HEADER: self.session_id,
            DEFAULT_MCD_API_TOKEN_HEADER: self._session.token,
            DEFAULT_MCD_SESSION_ID: self.session_name,
            DEFAULT_MCD_TRACE_ID: str(uuid.uuid4()),
        }

        if self._session.user_id:
            headers[DEFAULT_MCD_USER_ID_HEADER] = self._session.user_id

        return headers

    @overload
    def __call__(
        self,
        query: Query,
        variables: Optional[Dict] = None,
        operation_name: Optional[str] = None,
        retry_backoff: Backoff = ExponentialBackoffJitter(
            DEFAULT_RETRY_INITIAL_WAIT_TIME,
            DEFAULT_RETRY_MAX_WAIT_TIME,
        ),
        timeout_in_seconds: int = 30,
        idempotent_request_id: Optional[str] = None,
        idempotent_retry_backoff: Optional[Backoff] = None,
        response_type: Optional[str] = None,
    ) -> Query: ...

    @overload
    def __call__(
        self,
        query: Mutation,
        variables: Optional[Dict] = None,
        operation_name: Optional[str] = None,
        retry_backoff: Backoff = ExponentialBackoffJitter(
            DEFAULT_RETRY_INITIAL_WAIT_TIME,
            DEFAULT_RETRY_MAX_WAIT_TIME,
        ),
        timeout_in_seconds: int = 30,
        idempotent_request_id: Optional[str] = None,
        idempotent_retry_backoff: Optional[Backoff] = None,
        response_type: Optional[str] = None,
    ) -> Mutation: ...

    @overload
    def __call__(
        self,
        query: str,
        variables: Optional[Dict] = None,
        operation_name: Optional[str] = None,
        retry_backoff: Backoff = ExponentialBackoffJitter(
            DEFAULT_RETRY_INITIAL_WAIT_TIME,
            DEFAULT_RETRY_MAX_WAIT_TIME,
        ),
        timeout_in_seconds: int = 30,
        idempotent_request_id: Optional[str] = None,
        idempotent_retry_backoff: Optional[Backoff] = None,
        response_type: Optional[str] = None,
    ) -> Union[Query, Mutation, Box, BoxList]: ...

    def __call__(
        self,
        query: Union[Query, Mutation, str],
        variables: Optional[Dict] = None,
        operation_name: Optional[str] = None,
        retry_backoff: Backoff = ExponentialBackoffJitter(
            DEFAULT_RETRY_INITIAL_WAIT_TIME,
            DEFAULT_RETRY_MAX_WAIT_TIME,
        ),
        timeout_in_seconds: int = 30,
        idempotent_request_id: Optional[str] = None,
        idempotent_retry_backoff: Optional[Backoff] = None,
        response_type: Optional[str] = None,
    ) -> Union[Query, Mutation, Box, BoxList]:
        """
        Make a request to the MCD API.

        :param query: GraphQL query or mutation to execute. Can pass a string or
                      Query/Mutation object.
        :param variables: Any variables to use with the query.
        :param operation_name: Name of the operation.
        :param retry_backoff: Set the retry backoff strategy. Defaults to an exponential
                              backoff strategy with jitter.
        :param timeout_in_seconds: Set timeout of request. Requests cannot exceed 30 seconds.

        :return: Returns a Query or Mutation object with the response if the input query was a
            Query or Mutation object. If the input was a string a Box object containing the
            response is returned. Raises GqlError if any errors are found in the response.
            It will continually retry requests with errors using the provided `retry_backoff`
            parameter.

        Box is a transparent replacement for a dictionary - converting CamelCase to snake_case
        and allowing using dot notation in lookups. Can use .to_dict() to get a regular dictionary.
        """
        if self._session.scope:
            raise InvalidSessionError(
                "A session initialized with a scope cannot be used for GraphQL calls"
            )
        headers = self._get_headers()
        request_info = (
            f"idempotent request (id={idempotent_request_id})"
            if idempotent_request_id
            else "request"
        )

        logger.info(
            f"Sending {request_info} to '{self.session_endpoint}' with trace ID "
            f"'{headers[DEFAULT_MCD_TRACE_ID]}' in named "
            f"session '{headers[DEFAULT_MCD_SESSION_ID]}'."
        )

        request = Endpoint(
            url=self.session_endpoint,
            base_headers=headers,
            timeout=timeout_in_seconds,
            retry_backoff=retry_backoff,
            idempotent_retry_backoff=idempotent_retry_backoff,
        )
        response = request(
            query,
            variables=variables,
            operation_name=operation_name,
            idempotent_request_id=idempotent_request_id,
            response_type=response_type,
        )

        if not isinstance(query, str):
            return query + response
        return Box(response, camel_killer_box=True).data

    def make_request(
        self,
        path: str,
        method: str = "POST",
        body: Optional[Dict] = None,
        retry_backoff: Backoff = ExponentialBackoffJitter(
            DEFAULT_RETRY_INITIAL_WAIT_TIME,
            DEFAULT_RETRY_MAX_WAIT_TIME,
        ),
        timeout_in_seconds: int = DEFAULT_IGW_TIMEOUT_SECS,
    ) -> Optional[Dict]:
        """
        Make a request to the REST API exposed by the MCD Gateway, the Session object used to
        initialize this client must be created with a "scope" parameter.

        :param path: the path in the gateway for the endpoint, for example /airflow/callbacks
        :param method: the HTTP method to use, defaults to POST
        :param body: the dictionary to send as the body of the request, defaults to None
        :param retry_backoff: Set the retry backoff strategy. Defaults to an exponential backoff
                              strategy with jitter.
        :param timeout_in_seconds: Set timeout of request, defaults to 10 seconds.

        :return: Returns the JSON dictionary returned by the endpoint or None if the
                 response was empty.
        """
        if not self._session.scope:
            raise InvalidSessionError(
                "A session initialized with a scope is required to call REST endpoints"
            )
        url = urljoin(self._session.endpoint, path)

        @retry_with_backoff(backoff=retry_backoff, exceptions=(HTTPError, Timeout))
        def action() -> Optional[Dict]:
            response = requests.request(
                url=url,
                method=method,
                json=body,
                headers=self._get_headers(),
                timeout=timeout_in_seconds,
            )
            response.raise_for_status()
            return response.json() if response.content else None

        return action()
