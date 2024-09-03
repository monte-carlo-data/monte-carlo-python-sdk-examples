import time
from dataclasses import dataclass
from typing import Dict, List, Optional
from uuid import UUID

import click
from box import Box
from dataclasses_json import DataClassJsonMixin
from pycarlo.common.errors import GqlError
from pycarlo.core import Client
from retry.api import retry_call
from tabulate import tabulate

from montecarlodata.queries.platform import (
    MUTATION_TRIGGER_CAAS_MIGRATION_TEST,
    QUERY_CAAS_MIGRATION_TEST_STATUS,
    QUERY_GET_SERVICES,
)


class CaasMigrationNotReadyError(Exception):
    pass


@dataclass
class MigrationTestedConnection(DataClassJsonMixin):
    success: bool
    name: Optional[str]
    uuid: str
    connection_type: str
    connection_subtype: Optional[str] = None
    error_message: Optional[str] = None
    suggested_action: Optional[str] = None


@dataclass
class MigrationTestedAgent(DataClassJsonMixin):
    success: bool
    agent_type: str
    platform: Optional[str]
    endpoint: str
    uuid: str
    error_message: Optional[str] = None
    suggested_action: Optional[str] = None


@dataclass
class MigrationValidationsResult(DataClassJsonMixin):
    success: bool
    tested_agent: Optional[MigrationTestedAgent] = None
    tested_connections: Optional[List[MigrationTestedConnection]] = None
    error_message: Optional[str] = None


class PlatformService:
    def __init__(
        self,
        mc_client: Client,
    ):
        self._mc_client = mc_client

    def caas_migration_test(self, dc_id: Optional[UUID]):
        dc_id = self._disambiguate_dc(dc_id)
        try:
            operation = self._mc_client(
                MUTATION_TRIGGER_CAAS_MIGRATION_TEST,
                variables={"dcUuid": str(dc_id) if dc_id else None},
            )
        except GqlError as e:
            click.echo(str(e))
            raise click.Abort()

        migration_uuid = operation.trigger_platform_migration_test.migration_uuid  # type: ignore
        click.echo(
            "Migration test triggered, this might take a few minutes, waiting for result ..."
        )
        time.sleep(10)  # wait to start polling
        try:
            output = retry_call(
                self._get_caas_migration_test_status,
                fargs=[dc_id, migration_uuid],
                tries=20,
                delay=18,
                exceptions=CaasMigrationNotReadyError,
            )
        except CaasMigrationNotReadyError:
            click.echo("Migration test timed out.")
            raise click.Abort()
        except GqlError as e:
            click.echo(str(e))
            raise click.Abort()

        self._echo_caas_migration_test_status(output)

    def list_services(self):
        data_collectors = self._get_active_data_collectors()
        table: List[List[str]] = []
        for dc in data_collectors:
            agent = self._get_active_agent(dc.agents)
            table.append(
                [
                    dc.uuid,
                    dc.deployment_type,
                    (
                        agent.endpoint
                        if agent
                        else (dc.stack_arn if dc.deployment_type == "REMOTE_V1" else "N/A")
                    ),
                ]
            )
        if not table:
            click.echo("No services found")
            raise click.Abort()
        headers = [
            "Service ID",
            "Deployment Type",
            "Endpoint",
        ]
        click.echo(tabulate(table, headers=headers, tablefmt="fancy_grid"))

    @staticmethod
    def _get_active_agent(agents: List) -> Optional[Box]:
        return next(filter(lambda a: not a.is_deleted, agents), None)

    @classmethod
    def _echo_caas_migration_test_status(cls, output: Dict):
        result = MigrationValidationsResult.from_dict(output)
        success = result.success
        error_message = result.error_message
        if not success and error_message:
            click.echo(f"Migration test failed: {error_message}")
            raise click.Abort()

        tested_agent = result.tested_agent
        if tested_agent:
            cls._echo_caas_migration_agent_result(tested_agent)
            if tested_agent.agent_type == "REMOTE_AGENT":
                return
        table: List[List[str]] = []
        for connection in result.tested_connections:  # type: ignore
            table.append(
                [
                    connection.name,  # type: ignore
                    connection.uuid,
                    connection.connection_subtype or connection.connection_type,
                    "Success" if connection.success else "Failed",
                    ("None" if connection.success else connection.suggested_action or ""),
                    connection.error_message[:200] if connection.error_message else "",
                ]
            )
        if not table:
            click.echo("No connections tested")
            raise click.Abort()

        headers = [
            "Name",
            "Connection UUID",
            "Connection Type",
            "Result",
            "Required Action",
            "Error Message",
        ]
        widths = [20, 36, 20, 7, 40, 40]
        click.echo(tabulate(table, headers=headers, tablefmt="fancy_grid", maxcolwidths=widths))

    @staticmethod
    def _echo_caas_migration_agent_result(tested_agent: MigrationTestedAgent):
        table = [
            [
                tested_agent.agent_type,
                tested_agent.platform or "",
                tested_agent.endpoint,
                "Success" if tested_agent.success else "Failed",
                ("None" if tested_agent.success else tested_agent.suggested_action or ""),
                tested_agent.error_message[:200] if tested_agent.error_message else "",
            ]
        ]
        headers = [
            "Agent Type",
            "Platform",
            "Endpoint",
            "Result",
            "Required Action",
            "Error Message",
        ]
        widths = [16, 8, 32, 7, 40, 40]
        click.echo(tabulate(table, headers=headers, tablefmt="fancy_grid", maxcolwidths=widths))

    def _get_caas_migration_test_status(self, dc_id: Optional[UUID], migration_uuid: UUID) -> Dict:
        operation = self._mc_client(
            QUERY_CAAS_MIGRATION_TEST_STATUS,
            variables={
                "dcUuid": str(dc_id) if dc_id else None,
                "migrationUuid": str(migration_uuid),
            },
        )
        migration_output = operation.get_platform_migration_status.output  # type: ignore
        if not migration_output or migration_output.get("status") != "dry_run_completed":  # type: ignore
            raise CaasMigrationNotReadyError()
        return migration_output  # type: ignore

    def _get_active_data_collectors(self) -> List[Box]:
        operation = self._mc_client(QUERY_GET_SERVICES)
        data_collectors = operation.get_user.account.data_collectors  # type: ignore
        return list(filter(lambda dc: dc.active, data_collectors))  # type: ignore

    def _disambiguate_dc(self, dc_id: Optional[UUID]) -> UUID:
        active_dcs = self._get_active_data_collectors()
        if dc_id:
            dc = next((dc for dc in active_dcs if dc.uuid == str(dc_id)), None)
            if not dc:
                click.echo(
                    "No active service found with the specified ID, use "
                    "`montecarlo platform list` to list the active services."
                )
                raise click.Abort()
        else:
            active_dc_count = len(active_dcs)
            if active_dc_count == 0:
                click.echo("There are no active services in this account.")
                raise click.Abort()
            elif active_dc_count > 1:
                click.echo(
                    "There are multiple active services, please specify one using --service-id."
                )
                raise click.Abort()
            dc = active_dcs[0]
        if dc.deployment_type in ("CLOUD_V2", "REMOTE_V2"):
            click.echo(f"No need to migrate {dc.deployment_type} services.")
            raise click.Abort()
        return UUID(dc.uuid)
