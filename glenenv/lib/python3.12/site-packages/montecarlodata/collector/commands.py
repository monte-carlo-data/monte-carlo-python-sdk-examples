import click
import click_config_file
from click.exceptions import Exit

import montecarlodata.settings as settings
from montecarlodata.collector.fields import DEFAULT_COLLECTION_REGION
from montecarlodata.collector.management import CollectorManagementService
from montecarlodata.collector.network_tests import CollectorNetworkTestService
from montecarlodata.collector.validation import CollectorValidationService
from montecarlodata.common import create_mc_client
from montecarlodata.common.commands import DISAMBIGUATE_DC_OPTIONS
from montecarlodata.tools import (
    add_common_options,
    convert_uuid_callback,
    validate_json_callback,
)

# Shared command verbiage
ADD_DC_VERBIAGE = (
    "Prompts to opens browser to CF console. If declined (or skipped) can use deploy, get-template "
    "or open-link with the generated ID."
)

# Options shared across commands
REGION_OPTIONS = [
    *DISAMBIGUATE_DC_OPTIONS,
    click.option(
        "--aws-region",
        required=False,
        default=DEFAULT_COLLECTION_REGION,
        show_default=True,
        help="AWS region where the collector is deployed or intended to be deployed.",
    ),
]
DEPLOYMENT_OPTIONS = [
    click.option("--aws-profile", required=True, help="AWS profile."),
    click.option(
        "--params",
        required=False,
        default=None,
        callback=validate_json_callback,
        help="""
                  Parameters key,value pairs as JSON. If a key is not specified the existing
                  (or default) value is used.
                  \b
                  \n
                  E.g. --params '{"CreateEventInfra":"True"}'
                  """,
    ),  # \b disables wrapping
    *REGION_OPTIONS,
]

NETWORK_TEST_OPTIONS = [
    *DISAMBIGUATE_DC_OPTIONS,
    click.option("--host", required=True, help="Host to check."),
    click.option("--port", required=True, type=click.INT, help="Port to check."),
    click.option(
        "--timeout",
        required=False,
        default=5,
        type=click.INT,
        show_default=True,
        help="Timeout in seconds.",
    ),
]


@click.group(help="Manage a data collector.")
def collectors():
    """
    Group for any collector related subcommands
    """
    pass


@collectors.command(help="List all collector records.", name="list")
@click.pass_obj
@click.option(
    "--active-only",
    required=False,
    default=False,
    show_default=True,
    is_flag=True,
    help="Only list active collectors.",
)
@click_config_file.configuration_option(settings.OPTION_FILE_FLAG)
def list_collectors(ctx, active_only):
    CollectorManagementService(
        config=ctx["config"],
        mc_client=create_mc_client(ctx),
        aws_wrapper=None,
    ).echo_collectors(active_only=active_only)


@collectors.command(help=f"Add a collector record to the account. {ADD_DC_VERBIAGE}", name="add")
@click.pass_obj
@click.option(
    "--no-prompt",
    required=False,
    default=False,
    show_default=True,
    is_flag=True,
    help="Skip prompt for launching browser (Auto no).",
)
@click_config_file.configuration_option(settings.OPTION_FILE_FLAG)
def add_collector(ctx, no_prompt):
    CollectorManagementService(config=ctx["config"], mc_client=create_mc_client(ctx)).add_collector(
        no_prompt=no_prompt
    )


@collectors.command(
    help="Get link to the latest template. For initial deployment or manually upgrading."
)
@click.pass_obj
@add_common_options(REGION_OPTIONS)
@click.option(
    "--update-infra/--no-update-infra",
    default=False,
    show_default=True,
    help="Update the collector infrastructure. Otherwise, only the lambda code will be updated.",
)
@click_config_file.configuration_option(settings.OPTION_FILE_FLAG)
def get_template(ctx, aws_region, **kwargs):
    """
    Get the latest template for this account
    """
    CollectorManagementService(
        config=ctx["config"],
        mc_client=create_mc_client(ctx),
        aws_region_override=aws_region,
    ).echo_template(**kwargs)


@collectors.command(
    help="Opens browser to CF console with a quick create link. For initial deployment."
)
@click.pass_obj
@click.option(
    "--dry",
    required=False,
    default=False,
    show_default=True,
    is_flag=True,
    help="Echos quick create link.",
)
@add_common_options(REGION_OPTIONS)
@click_config_file.configuration_option(settings.OPTION_FILE_FLAG)
def open_link(ctx, aws_region, **kwargs):
    """
    Open browser with a quick create link for deploying a data collector
    """
    CollectorManagementService(
        config=ctx["config"],
        mc_client=create_mc_client(ctx),
        aws_region_override=aws_region,
    ).launch_quick_create_link(**kwargs)


@collectors.command(help="Deploy a data collector stack.")
@click.pass_obj
@add_common_options(DEPLOYMENT_OPTIONS)
@click.option(
    "--stack-name",
    required=True,
    help="The name that is associated with the CloudFormation stack. "
    "Must be unique in the region.",
)
@click.option(
    "--enable-termination-protection/--no-enable-termination-protection",
    "termination_protection",
    default=False,
    show_default=True,
    help="Enable termination protection for this stack.",
)
@click_config_file.configuration_option(settings.OPTION_FILE_FLAG)
def deploy(ctx, aws_profile, aws_region, params, **kwargs):
    """
    Deploy a collector for this account
    """
    CollectorManagementService(
        config=ctx["config"],
        mc_client=create_mc_client(ctx),
        aws_profile_override=aws_profile,
        aws_region_override=aws_region,
    ).deploy_template(new_params=params, **kwargs)


@collectors.command(help="Upgrade to the latest version.")
@click.pass_obj
@click.option(
    "--update-infra/--no-update-infra",
    default=False,
    show_default=True,
    help="Update the collector infrastructure. Otherwise, only the lambda code will be updated.",
)
@add_common_options(DEPLOYMENT_OPTIONS)
@click_config_file.configuration_option(settings.OPTION_FILE_FLAG)
def upgrade(ctx, aws_profile, aws_region, params, update_infra, **kwargs):
    """
    Upgrade the collector for this account
    """
    CollectorManagementService(
        config=ctx["config"],
        mc_client=create_mc_client(ctx),
        aws_profile_override=aws_profile,
        aws_region_override=aws_region,
    ).upgrade_template(update_infra=update_infra, new_params=params, **kwargs)


@collectors.command(help="Checks if telnet connection is usable from the collector.")
@click.pass_obj
@add_common_options(NETWORK_TEST_OPTIONS)
@click_config_file.configuration_option(settings.OPTION_FILE_FLAG)
def test_telnet(ctx, **kwargs):
    """
    Network debugging utility to test telnet via the collector
    """
    CollectorNetworkTestService(config=ctx["config"]).echo_telnet_test(**kwargs)


@collectors.command(
    help="Tests if a destination exists and accepts requests. "
    "Opens a TCP Socket to a specific port from the collector."
)
@click.pass_obj
@add_common_options(NETWORK_TEST_OPTIONS)
@click_config_file.configuration_option(settings.OPTION_FILE_FLAG)
def test_tcp_open(ctx, **kwargs):
    """
    Network debugging utility to test TCP open via the collector
    """
    CollectorNetworkTestService(config=ctx["config"]).echo_tcp_open_test(**kwargs)


@collectors.command(help="Validate whether a collector is operational.")
@click.pass_obj
@click.option(
    "--trace-id",
    required=False,
    type=click.UUID,
    callback=convert_uuid_callback,
    help=(
        "Optional custom UUID for tracking and correlating the ping response. "
        "By default we generate a random UUID to be sent with the ping request "
        "and validate that the ping response contains the same exact value."
    ),
)
@click.option(
    "--collector-id",
    "dc_id",
    required=True,
    type=click.UUID,
    callback=convert_uuid_callback,
    help="ID for the data collector. To disambiguate accounts with multiple collectors.",
)
@click_config_file.configuration_option(settings.OPTION_FILE_FLAG)
def ping(ctx, **kwargs):
    CollectorManagementService(
        config=ctx["config"],
        mc_client=create_mc_client(ctx),
    ).ping_dc(**kwargs)


@collectors.command(help="Runs all validations for all active integrations in a given collector.")
@click.pass_obj
@click.option(
    "--only-periodic/--all-validations",
    required=False,
    type=click.BOOL,
    default=False,
    help="Whether all validations or only those marked as 'periodic' will be executed. "
    "Running only periodic validations will require less time and run the same validations that "
    "are executed periodically.",
)
@add_common_options(DISAMBIGUATE_DC_OPTIONS)
@click_config_file.configuration_option(settings.OPTION_FILE_FLAG)
def run_validations(ctx, **kwargs):
    failures = CollectorValidationService(
        config=ctx["config"],
        mc_client=create_mc_client(ctx),
    ).run_validations(**kwargs)
    if failures > 0:
        raise Exit(1)
