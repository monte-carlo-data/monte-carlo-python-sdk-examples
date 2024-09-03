from typing import Dict, List, Optional

import click
import click_config_file

from montecarlodata import settings
from montecarlodata.common.commands import DC_RESOURCE_OPTIONS, DISAMBIGUATE_DC_OPTIONS
from montecarlodata.common.resources import CloudResourceService
from montecarlodata.discovery.networking import NetworkDiscoveryService
from montecarlodata.discovery.policy_gen import PolicyDiscoveryService
from montecarlodata.tools import add_common_options

# Shared command verbiage
DISCOVERY_REVIEW_VERBIAGE = (
    "After review, output of this command can be redirected into "
    "`montecarlo integrations create-role` or `montecarlo discovery cf-role-gen` "
    "if you prefer IaC"
)

GLUE_COMMON_OPTIONS = [
    click.option(
        "--database-name",
        "database_names",
        required=True,
        multiple=True,
        help=(
            "Glue/Athena database name to generate a policy from. Enter '\\*' to give "
            "Monte Carlo access to all databases. This option can be passed multiple "
            "times for more than one database."
        ),
    ),
    click.option(
        "--data-bucket-name",
        "bucket_names",
        required=False,
        multiple=True,
        help=(
            "Name of a S3 bucket storing the data for your Glue/Athena tables. If this option is "
            "not specified the bucket names are derived (looked up) from the tables in your "
            "databases. This option can be passed multiple times for more than one bucket. "
            "Enter '\\*' to give Monte Carlo access to all buckets."
        ),
    ),
]


@click.group(help="Display information about resources.")
def discovery():
    """
    Group for any discovery related subcommands
    """
    pass


@discovery.command(help="List details about EMR clusters in a region.")
@click.option("--aws-profile", help="AWS profile.", required=True)
@click.option("--aws-region", help="AWS region.", required=True)
@click.option(
    "--only-log-locations",
    help="Display only unique log locations",
    is_flag=True,
    default=False,
)
@click.option(
    "--created-after",
    help="Display clusters created after date (e.g. 2017-07-04T00:01:30)",
    required=False,
)
@click.option(
    "--state",
    help="Cluster states",
    required=False,
    type=click.Choice(["active", "terminated", "failed"]),
    multiple=True,
)
@click.option(
    "--no-grid",
    help="Do not display as grid and print as results are available, "
    "useful when the cluster list is large",
    is_flag=True,
    default=False,
)
@click_config_file.configuration_option(settings.OPTION_FILE_FLAG)
@click.pass_obj
def list_emr_clusters(
    ctx: Dict,
    aws_profile: Optional[str] = None,
    aws_region: Optional[str] = None,
    only_log_locations: bool = False,
    created_after: Optional[str] = None,
    state: Optional[List] = None,
    no_grid: bool = False,
) -> None:
    CloudResourceService(
        config=ctx["config"],
        aws_profile_override=aws_profile,
        aws_region_override=aws_region,
    ).list_emr_clusters(
        only_log_locations=only_log_locations,
        created_after=created_after,
        states=state,
        no_grid=no_grid,
    )


@discovery.command(
    help="Alpha network recommender. Attempts to analyze and makes recommendations "
    "on how to connect a resource with the Data Collector."
)
@click.pass_obj
@click.option(
    "--resource-identifier",
    required=True,
    help="Identifier for the AWS resource you want to connect the Collector with "
    "(e.g. Redshift cluster ID).",
)
@click.option(
    "--resource-type",
    required=True,
    help="Type of AWS resource.",
    type=click.Choice(list(NetworkDiscoveryService.MCD_NETWORK_REC_RESOURCE_TYPE_MAP.keys())),
)
@click.option(
    "--collector-aws-profile",
    required=True,
    help="AWS profile for the Collector.",
)
@add_common_options(DC_RESOURCE_OPTIONS)
@add_common_options(DISAMBIGUATE_DC_OPTIONS)
@click_config_file.configuration_option(settings.OPTION_FILE_FLAG)
def network_recommender(ctx, **kwargs):
    NetworkDiscoveryService(config=ctx["config"], aws_wrapper=None).recommend_network_dispatcher(
        **kwargs
    )


@discovery.command(
    help="Generate a CloudFormation template to create a resource access IAM role. "
    "After review, this template can be deployed using CloudFormation. "
    "The Role ARN and External ID for onboarding can be found in the stack outputs."
)
@click.pass_obj
@click.option(
    "--policy-file",
    "policy_files",
    required=True,
    multiple=True,
    type=click.File(),
    help="File containing an IAM policy to generate an IAM role from. This option can be passed "
    "multiple times for more than one policy.",
)
@add_common_options(DISAMBIGUATE_DC_OPTIONS)
def cf_role_gen(ctx, **kwargs):
    PolicyDiscoveryService(config=ctx["config"], aws_wrapper=None).generate_cf_role(**kwargs)


@discovery.command(help=f"Generate an IAM policy for Glue. {DISCOVERY_REVIEW_VERBIAGE}.")
@click.pass_obj
@add_common_options(GLUE_COMMON_OPTIONS)
@add_common_options(DC_RESOURCE_OPTIONS)
@add_common_options(DISAMBIGUATE_DC_OPTIONS)
def glue_policy_gen(ctx, **kwargs):
    PolicyDiscoveryService(config=ctx["config"], aws_wrapper=None).generate_glue_policy(**kwargs)


@discovery.command(help=f"Generate an IAM policy for MSK. {DISCOVERY_REVIEW_VERBIAGE}.")
@click.pass_obj
@add_common_options(DC_RESOURCE_OPTIONS)
@add_common_options(DISAMBIGUATE_DC_OPTIONS)
def msk_policy_gen(ctx, **kwargs):
    PolicyDiscoveryService(config=ctx["config"], aws_wrapper=None).generate_msk_policy(**kwargs)


@discovery.command(help=f"Generate an IAM policy for Athena. {DISCOVERY_REVIEW_VERBIAGE}.")
@click.pass_obj
@add_common_options(GLUE_COMMON_OPTIONS)
@click.option(
    "--workgroup-name",
    required=True,
    default="primary",
    show_default=True,
    help="Athena workgroup for Monte Carlo to use when performing queries. "
    'The "primary" workgroup for the region is used if one is not specified.',
)
@add_common_options(DC_RESOURCE_OPTIONS)
@add_common_options(DISAMBIGUATE_DC_OPTIONS)
def athena_policy_gen(ctx, **kwargs):
    PolicyDiscoveryService(config=ctx["config"], aws_wrapper=None).generate_athena_policy(**kwargs)
