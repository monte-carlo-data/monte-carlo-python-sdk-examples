import sys

import click

import montecarlodata.settings as settings
from montecarlodata.agents.commands import agents
from montecarlodata.collector.commands import collectors
from montecarlodata.common.user import UserService
from montecarlodata.config import ConfigManager
from montecarlodata.dataimport.commands import import_subcommand
from montecarlodata.discovery.commands import discovery
from montecarlodata.iac.commands import monitors
from montecarlodata.insights.commands import insights
from montecarlodata.integrations.commands import integrations
from montecarlodata.keys.commands import keys
from montecarlodata.management.commands import management
from montecarlodata.platform.commands import platform
from montecarlodata.secrets.commands import secrets
from montecarlodata.tools import dump_help


@click.group(help="Monte Carlo's CLI.")
@click.option(
    "--profile",
    default=settings.DEFAULT_PROFILE_NAME,
    help="Specify an MCD profile name. Uses default otherwise.",
)
@click.option(
    "--config-path",
    default=settings.DEFAULT_CONFIG_PATH,
    type=click.Path(dir_okay=True),
    help=(
        "Specify path where to look for config file. Uses "
        f"{settings.DEFAULT_CONFIG_PATH} otherwise."
    ),
)
@click.version_option()
@click.pass_context
def entry_point(ctx, profile, config_path):
    """
    Entry point for all subcommands and options. Reads configuration and sets as context,
    except when configuring or getting help.
    """
    if (
        ctx.invoked_subcommand != settings.CONFIG_SUB_COMMAND
        and settings.HELP_FLAG not in sys.argv[1:]
    ):
        config = ConfigManager(profile_name=profile, base_path=config_path).read()
        if not config:
            ctx.abort()
        ctx.obj = {"config": config}


@click.command(help="Configure the CLI.")
@click.option(
    "--profile-name",
    required=False,
    help="Specify a profile name for configuration.",
    default=settings.DEFAULT_PROFILE_NAME,
)
@click.option(
    "--config-path",
    required=False,
    help="Specify path where to look for config file.",
    default=settings.DEFAULT_CONFIG_PATH,
    type=click.Path(dir_okay=True),
)
@click.option("--mcd-id", prompt="Key ID", help="Monte Carlo token user ID.")
@click.option("--mcd-token", prompt="Secret", help="Monte Carlo token value.", hide_input=True)
def configure(profile_name, config_path, mcd_id, mcd_token):
    """
    Special subcommand for configuring the CLI
    """
    ConfigManager(profile_name=profile_name, base_path=config_path).write(
        mcd_id=mcd_id,
        mcd_token=mcd_token,
    )


@click.command(help="Validate that the CLI can Connect to Monte Carlo.")
@click.pass_obj
def validate(ctx):
    """
    Special subcommand for validating the CLI was correctly configured
    """
    click.echo(f"Hi, {UserService(config=ctx['config']).user.first_name}! All is well.")


@entry_point.command(help="Echo all help text.", name="help")
def echo_help():
    """
    Special subcommand to echo all help text.
    """
    dump_help(entry_point)


entry_point.add_command(integrations)
entry_point.add_command(configure)
entry_point.add_command(validate)
entry_point.add_command(collectors)
entry_point.add_command(discovery)
entry_point.add_command(monitors)
entry_point.add_command(import_subcommand)
entry_point.add_command(insights)
entry_point.add_command(management)
entry_point.add_command(agents)
entry_point.add_command(keys)
entry_point.add_command(secrets)
entry_point.add_command(platform)

# to allow this to be run as a script within an IDE (for debugging)
if __name__ == "__main__":
    entry_point()
