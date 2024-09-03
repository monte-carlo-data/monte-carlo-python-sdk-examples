from typing import Dict, Optional

import click

from montecarlodata.common.common import read_as_json_string
from montecarlodata.config import Config
from montecarlodata.errors import complain_and_abort, manage_errors
from montecarlodata.integrations.onboarding.base import BaseOnboardingService
from montecarlodata.integrations.onboarding.fields import (
    EXPECTED_CONFIGURE_METADATA_EVENTS_GQL_RESPONSE_FIELD,
    EXPECTED_CONFIGURE_QUERY_LOG_EVENTS_GQL_RESPONSE_FIELD,
    EXPECTED_DISABLE_METADATA_EVENTS_GQL_RESPONSE_FIELD,
    EXPECTED_DISABLE_QUERY_LOG_EVENTS_GQL_RESPONSE_FIELD,
)
from montecarlodata.queries.onboarding import (
    CONFIGURE_METADATA_EVENTS_MUTATION,
    CONFIGURE_QUERY_LOG_EVENTS_MUTATION,
    DISABLE_METADATA_EVENTS_MUTATION,
    DISABLE_QUERY_LOG_EVENTS_MUTATION,
)


class EventsOnboardingService(BaseOnboardingService):
    def __init__(self, config: Config, **kwargs):
        super().__init__(config, **kwargs)

    @manage_errors
    def configure_metadata_events(self, **kwargs):
        self._configure_events(
            query=CONFIGURE_METADATA_EVENTS_MUTATION,
            operation=EXPECTED_CONFIGURE_METADATA_EVENTS_GQL_RESPONSE_FIELD,
            variables=kwargs,
        )

    @manage_errors
    def configure_query_log_events(self, **kwargs):
        mapping_file = kwargs.pop("mapping_file", None)
        if mapping_file:
            kwargs["mapping"] = read_as_json_string(mapping_file)

        self._configure_events(
            query=CONFIGURE_QUERY_LOG_EVENTS_MUTATION,
            operation=EXPECTED_CONFIGURE_QUERY_LOG_EVENTS_GQL_RESPONSE_FIELD,
            variables=kwargs,
        )

    @manage_errors
    def disable_metadata_events(self, **kwargs):
        self._disable_events(
            query=DISABLE_METADATA_EVENTS_MUTATION,
            operation=EXPECTED_DISABLE_METADATA_EVENTS_GQL_RESPONSE_FIELD,
            variables=kwargs,
        )

    @manage_errors
    def disable_query_log_events(self, **kwargs):
        self._disable_events(
            query=DISABLE_QUERY_LOG_EVENTS_MUTATION,
            operation=EXPECTED_DISABLE_QUERY_LOG_EVENTS_GQL_RESPONSE_FIELD,
            variables=kwargs,
        )

    def _configure_events(self, query: str, operation: str, variables: Optional[Dict] = None):
        self._do_configure_events(
            action="configure", query=query, operation=operation, variables=variables
        )

    def _disable_events(self, query: str, operation: str, variables: Optional[Dict] = None):
        self._do_configure_events(
            action="disable", query=query, operation=operation, variables=variables
        )

    def _do_configure_events(
        self, action: str, query: str, operation: str, variables: Optional[Dict] = None
    ):
        response = self._request_wrapper.make_request_v2(query, operation, variables)
        if response.data.success:  # type: ignore
            return click.echo(f"Successfully {action}d events!")

        complain_and_abort(f"Failed to {action} events!")
