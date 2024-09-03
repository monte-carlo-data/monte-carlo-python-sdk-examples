from typing import Optional

import click

from montecarlodata.collector.fields import (
    EXPECTED_TTC_RESPONSE_FIELD,
    EXPECTED_TTOC_RESPONSE_FIELD,
)
from montecarlodata.config import Config
from montecarlodata.errors import manage_errors
from montecarlodata.queries.collector import (
    TEST_TCP_OPEN_CONNECTION,
    TEST_TELNET_CONNECTION,
)
from montecarlodata.utils import GqlWrapper


class CollectorNetworkTestService:
    def __init__(self, config: Config, request_wrapper: Optional[GqlWrapper] = None):
        self._abort_on_error = True

        self._request_wrapper = request_wrapper or GqlWrapper(config)

    @manage_errors
    def echo_telnet_test(self, **kwargs):
        """
        Checks if telnet connection is usable and echos results
        """
        self._echo_network_test_validations(
            query=TEST_TELNET_CONNECTION, operation=EXPECTED_TTC_RESPONSE_FIELD, **kwargs
        )

    @manage_errors
    def echo_tcp_open_test(self, **kwargs):
        """
        Checks if a destination exists and accepts requests and echos results
        """
        self._echo_network_test_validations(
            query=TEST_TCP_OPEN_CONNECTION, operation=EXPECTED_TTOC_RESPONSE_FIELD, **kwargs
        )

    def _echo_network_test_validations(self, query: str, operation: str, **kwargs):
        """
        Do request and echos a validation response
        """
        response = self._request_wrapper.make_request_v2(
            query=query, operation=operation, variables=kwargs
        )
        click.echo(response.data.validations[0].message)  # type: ignore
