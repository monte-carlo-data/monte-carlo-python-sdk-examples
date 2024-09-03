import click

from montecarlodata.common import create_mc_client
from montecarlodata.management.service import ManagementService
from montecarlodata.tools import AdvancedOptions, convert_empty_str_callback


@click.group(help="Manage account settings.")
def management():
    """
    Group for any management related subcommands
    """
    pass


@management.command(help="Get PII filtering preferences.")
@click.pass_obj
def get_pii_preferences(ctx):
    ManagementService(
        config=ctx["config"],
        mc_client=create_mc_client(ctx),
    ).get_pii_preferences()


@management.command(help="Configure PII filtering preferences for the account.")
@click.option(
    "--enable/--disable",
    "enabled",
    required=False,
    type=click.BOOL,
    default=None,
    help="Whether PII filtering should be active for the account.",
)
@click.option(
    "--fail-mode",
    required=False,
    type=click.Choice(["CLOSE", "OPEN"], case_sensitive=False),
    help="Whether PII filter failures will allow (OPEN) or prevent (CLOSE) data "
    "flow for this account.",
)
@click.pass_obj
def configure_pii_filtering(ctx, **kwargs):
    ManagementService(
        config=ctx["config"],
        mc_client=create_mc_client(ctx),
    ).set_pii_filtering(**kwargs)


@management.command(help="List entities blocked from collection on this account.")
@click.option(
    "--resource-name",
    required=False,
    help="Name of a specific resource to filter by. Shows all resources by default.",
)
@click.pass_obj
def get_collection_block_list(ctx, **kwargs):
    ManagementService(
        config=ctx["config"],
        mc_client=create_mc_client(ctx),
    ).get_collection_block_list(**kwargs)


@management.command(help="Update entities for which collection is blocked on this account.")
@click.option(
    "--add/--remove",
    "adding",
    required=True,
    type=click.BOOL,
    default=None,
    help="Whether the entities being specified should be added or removed from the block list.",
)
@click.option(
    "--resource-name",
    "resource_name",
    help="Name of a specific resource to apply collection block to. "
    "Only warehouse names are supported for now.",
    cls=AdvancedOptions,
    mutually_exclusive_options=["filename"],
    required_with_options=["project"],
    at_least_one_set=["resource_name", "filename"],
)
@click.option(
    "--project",
    "project",
    help="Top-level object hierarchy e.g. database, catalog, etc.",
    cls=AdvancedOptions,
    mutually_exclusive_options=["filename"],
    required_with_options=["resource_name"],
)
@click.option(
    "--dataset",
    "dataset",
    default=None,
    callback=convert_empty_str_callback,
    required=False,
    help="Intermediate object hierarchy e.g. schema, database, etc.",
    cls=AdvancedOptions,
    mutually_exclusive_options=["filename"],
    required_with_options=["resource_name", "project"],
)
@click.option(
    "--collection-block-list-filename",
    "filename",
    help="Filename that contains collection block definitions. "
    "This file is expected to be in a CSV format with the headers resource_name, project, "
    "and dataset.",
    cls=AdvancedOptions,
    mutually_exclusive_options=["resource_name", "project", "dataset"],
)
@click.pass_obj
def update_collection_block_list(ctx, **kwargs):
    ManagementService(
        config=ctx["config"],
        mc_client=create_mc_client(ctx),
    ).update_collection_block_list(**kwargs)
