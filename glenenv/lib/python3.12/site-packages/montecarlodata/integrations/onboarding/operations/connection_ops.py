import json
from typing import Dict
from uuid import UUID

import click

from montecarlodata.common.data import MonolithResponse
from montecarlodata.config import Config
from montecarlodata.errors import manage_errors
from montecarlodata.integrations.onboarding.base import BaseOnboardingService
from montecarlodata.integrations.onboarding.fields import (
    EXPECTED_REMOVE_CONNECTION_RESPONSE_FIELD,
    EXPECTED_TEST_EXISTING_RESPONSE_FIELD,
    EXPECTED_UPDATE_CREDENTIALS_RESPONSE_FIELD,
    OPERATION_ERROR_VERBIAGE,
)
from montecarlodata.queries.onboarding import (
    REMOVE_CONNECTION_MUTATION,
    TEST_EXISTING_CONNECTION_QUERY,
    UPDATE_CREDENTIALS_MUTATION,
    ConnectionOperationsQueries,
)


class ConnectionOperationsService(BaseOnboardingService):
    REMOVE_PROMPT = (
        "Are you sure? Deleted connections are not recoverable. Any custom monitors "
        "that have been added to tables from this connection will be deleted. Monte "
        "Carlo will also no longer have access to monitor or observe data from this "
        "source. You will need to re-create the connection and any custom monitors."
    )

    def __init__(self, config: Config, **kwargs):
        super().__init__(config, **kwargs)

    @manage_errors
    def update_credentials(
        self,
        connection_id: UUID,
        changes: Dict,
        should_validate: bool = True,
        should_replace: bool = False,
    ) -> None:
        """
        Update credentials for a connection
        """
        self.echo_operation_status(
            self._request_wrapper.make_request_v2(
                query=UPDATE_CREDENTIALS_MUTATION,
                operation=EXPECTED_UPDATE_CREDENTIALS_RESPONSE_FIELD,
                variables=dict(
                    connection_id=str(connection_id),
                    changes=json.dumps(changes),
                    should_validate=should_validate,
                    should_replace=should_replace,
                ),
            ),
            operation=f"Updated '{connection_id}'.",
        )

    @manage_errors
    def remove_connection(self, connection_id: UUID, no_prompt: bool = False) -> None:
        """
        Remove connection by ID. Deletes any associated jobs
        """
        if no_prompt or click.confirm(self.REMOVE_PROMPT, abort=True):
            self.echo_operation_status(
                self._request_wrapper.make_request_v2(
                    query=REMOVE_CONNECTION_MUTATION,
                    operation=EXPECTED_REMOVE_CONNECTION_RESPONSE_FIELD,
                    variables=dict(connection_id=str(connection_id)),
                ),
                operation=f"Removed '{connection_id}'.",
            )

    @manage_errors
    def echo_test_existing(self, connection_id: UUID) -> None:
        """
        Tests an existing connection and echos results in pretty JSON
        """
        response = self._request_wrapper.make_request_v2(
            query=TEST_EXISTING_CONNECTION_QUERY,
            operation=EXPECTED_TEST_EXISTING_RESPONSE_FIELD,
            variables=dict(connection_id=connection_id),
        )
        click.echo(json.dumps(response.data, indent=4, ensure_ascii=False))

    @staticmethod
    def echo_operation_status(response: MonolithResponse, operation: str) -> None:
        """
        Echos operation status. Expects response contains a 'success' field as per API signature
        """
        if response.data.get("success"):  # type: ignore
            click.echo(f"Success! {operation}")
        else:
            # Any errors thrown by the API are captured and handled in the request
            # lib/caller deco (e.g. validation errors). This should only reachable for
            # invalid inputs for the account
            click.echo(OPERATION_ERROR_VERBIAGE)

    @manage_errors
    def set_warehouse_name(self, current_name: str, new_name: str) -> None:
        """
        Sets the name of a warehouse
        """

        dw_id = self._get_warehouse_id_from_name(name=current_name)
        response = self._request_wrapper.make_request_v2(
            query=ConnectionOperationsQueries.set_warehouse_name.query,
            operation=ConnectionOperationsQueries.set_warehouse_name.operation,
            variables=dict(dw_id=dw_id, name=new_name),
        )

        if response.data.warehouse.name == new_name:  # type: ignore
            click.echo(f"Success! Set Warehouse {current_name} name to {new_name}")
        else:
            click.echo(OPERATION_ERROR_VERBIAGE)

    @manage_errors
    def set_bi_connection_name(self, bi_connection_id: str, new_name: str) -> None:
        """
        Sets the name of a BI Connection
        """
        bi_container_id = self._get_bi_container_id_from_connection_id(
            bi_connection_id=bi_connection_id
        )
        response = self._request_wrapper.make_request_v2(
            query=ConnectionOperationsQueries.set_bi_connection_name.query,
            operation=ConnectionOperationsQueries.set_bi_connection_name.operation,
            variables=dict(resource_id=bi_container_id, name=new_name),
        )

        if response.data.biContainer.name == new_name:  # type: ignore
            click.echo(f"Success! Set BI Connection name to {new_name}")
        else:
            click.echo(OPERATION_ERROR_VERBIAGE)
