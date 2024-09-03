from typing import Optional

import click

from montecarlodata.common import create_mc_client
from montecarlodata.secrets.service import AccountSecretsService


@click.group(help="Manage account secrets.")
def secrets():
    pass


@secrets.command(name="create", help="Add a new secret to the account.")
@click.pass_obj
@click.option(
    "--name",
    required=True,
    help="Name of the secret, to reference it when using the secret.",
)
@click.option(
    "--scope",
    required=False,
    help="Scope where the secret can be used.",
    default="global",
    show_default=True,
)
@click.option(
    "--value",
    prompt="Secret value",
    required=True,
    hide_input=True,
    help="Secret value",
)
@click.option(
    "--expires-at",
    required=False,
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="A date in the future when the secret should expire and no longer be readable.",
)
@click.option("--description", required=False, help="Description for the secret.")
def create_secret(
    ctx,
    name: str,
    scope: str,
    value: str,
    description: Optional[str] = None,
    expires_at: Optional[click.DateTime] = None,
):
    AccountSecretsService(
        config=ctx["config"],
        pycarlo_client=create_mc_client(ctx),
    ).create_secret(
        name,
        scope=scope,
        value=value,
        description=description,
        expires_at=expires_at,  # type: ignore
    )


@secrets.command(
    name="get", help="Get a secret properties, optionally requesting the secret value."
)
@click.pass_obj
@click.option(
    "--name",
    required=True,
    help="Name of the secret.",
)
@click.option(
    "--reveal",
    required=False,
    help="Show the secret value. Only the owner of the secret or user with special permissions can "
    "get the secret value.",
    default=False,
    show_default=True,
    is_flag=True,
)
def get_secret(ctx, name: str, reveal: bool):
    AccountSecretsService(
        config=ctx["config"],
        pycarlo_client=create_mc_client(ctx),
    ).get_secret(name, reveal=reveal)


@secrets.command(name="list", help="List all the secrets in the account.")
@click.pass_obj
def list_secrets(ctx):
    AccountSecretsService(
        config=ctx["config"],
        pycarlo_client=create_mc_client(ctx),
    ).list_secrets()


@secrets.command(name="delete", help="Delete a secret from the account.")
@click.pass_obj
@click.option(
    "--name",
    required=True,
    help="Name of the secret.",
)
def delete_secret(ctx, name: str):
    AccountSecretsService(
        config=ctx["config"],
        pycarlo_client=create_mc_client(ctx),
    ).delete_secret(name)
