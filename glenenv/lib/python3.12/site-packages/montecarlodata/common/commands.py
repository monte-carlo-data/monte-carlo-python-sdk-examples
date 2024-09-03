"""
Common commands
"""

import click

from montecarlodata.tools import (
    AdvancedOptions,
    convert_uuid_callback,
)

DISAMBIGUATE_DC_OPTIONS = [
    click.option(
        "--agent-id",
        "agent_id",
        required=False,
        type=click.UUID,
        callback=convert_uuid_callback,
        help="ID for the agent. To disambiguate accounts with multiple agents.",
        cls=AdvancedOptions,
        mutually_exclusive_options=["dc_id"],
    ),
    click.option(
        "--collector-id",
        "dc_id",
        required=False,
        type=click.UUID,
        callback=convert_uuid_callback,
        help="ID for the data collector. To disambiguate accounts with multiple collectors.",
        cls=AdvancedOptions,
        mutually_exclusive_options=["agent_id"],
    ),
]

DC_RESOURCE_OPTIONS = [
    click.option(
        "--resource-aws-region",
        required=False,
        help="Override the AWS region where the resource is located. "
        "Defaults to the region where the collector is hosted.",
    ),
    click.option(
        "--resource-aws-profile",
        required=True,
        help="AWS profile for the resource.",
    ),
]
