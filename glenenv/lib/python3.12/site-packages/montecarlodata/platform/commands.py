import uuid
from typing import Optional

import click

from montecarlodata.common import create_mc_client
from montecarlodata.platform.service import PlatformService


@click.group(help="Manage platform settings.")
def platform():
    """
    Group for any platform related subcommands
    """
    pass


@platform.command(help="Tests if all connections can be migrated to the Monte Carlo Platform.")
@click.option(
    "--service-id",
    help="ID for the service as listed by 'platform list' command. "
    "To disambiguate accounts with multiple services.",
    required=False,
)
@click.pass_obj
def test_migration(ctx, service_id: Optional[str] = None):
    PlatformService(
        mc_client=create_mc_client(ctx),
    ).caas_migration_test(dc_id=uuid.UUID(service_id) if service_id else None)


@platform.command(name="list", help="Lists all services in the account.")
@click.pass_obj
def list_services(ctx):
    PlatformService(
        mc_client=create_mc_client(ctx),
    ).list_services()
