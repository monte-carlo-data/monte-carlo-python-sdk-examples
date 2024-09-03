from datetime import datetime
from typing import Callable, List, Optional

import click
from pycarlo.core import Client
from tabulate import tabulate

from montecarlodata.config import Config
from montecarlodata.errors import manage_errors
from montecarlodata.tools import format_date, format_datetime


class AccountSecretsService:
    def __init__(
        self,
        config: Config,
        pycarlo_client: Client,
        print_func: Optional[Callable] = None,
    ):
        self._pycarlo_client = pycarlo_client
        self._print_func = print_func or click.echo
        self._abort_on_error = True

    @manage_errors
    def create_secret(
        self,
        name: str,
        scope: str,
        value: str,
        description: Optional[str] = None,
        expires_at: Optional[datetime] = None,
    ):
        create_account_secret = """
            mutation createAccountSecret(
                $name: String!
                $value: String!
                $scope: String!
                $description: String
                $expiresAt: DateTime
            ) {
              createAccountSecret(
                name: $name
                value: $value
                scope: $scope
                description: $description
                expiresAt: $expiresAt
              ) {
                secret {
                  name
                }
              }
            }
        """
        response = self._pycarlo_client(
            create_account_secret,
            {
                "name": name,
                "value": value,
                "scope": scope,
                "description": description,
                "expiresAt": expires_at,
            },
        )

        secret = response.create_account_secret.secret  # type: ignore
        self._print_func(f"Created secret: '{secret.name}'.")

    @manage_errors
    def get_secret(self, name: str, reveal: bool = False):
        get_account_secret = """
            query getAccountSecret(
                $name: String!
                $reveal: Boolean!
            ) {
              getAccountSecret(
                name: $name
                reveal: $reveal
              ) {
                name
                scope
                description
                expiresAt
                value
                createdBy
                createdAt
                updatedBy
                lastUpdate
              }
            }
        """
        response = self._pycarlo_client(get_account_secret, {"name": name, "reveal": reveal})

        secret = response.get_account_secret  # type: ignore
        if reveal:
            self._print_func(f"{secret.value}")
        else:
            self._print_func(f"Name: {secret.name}")
            self._print_func(f"Scope: {secret.scope}")
            self._print_func(f"Description: {secret.description or ''}")
            self._print_func(
                f"Expires at: {format_date(secret.expires_at) if secret.expires_at else 'never'}"  # type: ignore
            )
            self._print_func(f"Created by: {secret.created_by}")
            self._print_func(f"Created at: {format_datetime(secret.created_at)}")  # type: ignore
            self._print_func(f"Last updated by: {secret.updated_by}")
            self._print_func(f"Last updated at: {format_datetime(secret.last_update)}")  # type: ignore

    @manage_errors
    def delete_secret(self, name: str):
        delete_secret = """
            mutation deleteAccountSecret(
                $name: String!
            ) {
              deleteAccountSecret(
                name: $name
              ) {
                deleted
              }
            }
        """
        response = self._pycarlo_client(delete_secret, {"name": name})

        deleted = response.delete_account_secret.deleted  # type: ignore
        if deleted:
            self._print_func(f"Deleted secret: '{name}'.")
        else:
            self._print_func(f"Secret '{name}' was not found.")

    @manage_errors
    def list_secrets(self):
        get_account_secrets = """
                query getAccountSecrets {
                  getAccountSecrets {
                    name
                    scope
                    description
                    expiresAt
                    createdBy
                    createdAt
                    updatedBy
                    lastUpdate
                  }
                }
            """
        response = self._pycarlo_client(get_account_secrets)

        secrets = response.get_account_secrets  # type: ignore
        table: List = [
            [
                "Name",
                "Scope",
                "Description",
                "Expires at",
                "Created by",
                "Created at",
                "Last updated by",
                "Last updated at",
            ]
        ]
        for secret in secrets:
            table.append(
                [
                    secret.name,
                    secret.scope,
                    secret.description or "",
                    format_date(secret.expires_at) if secret.expires_at else "never",  # type: ignore
                    secret.created_by,
                    format_datetime(secret.created_at),  # type: ignore
                    secret.updated_by,
                    format_datetime(secret.last_update),  # type: ignore
                ]
            )
        click.echo(tabulate(table, headers="firstrow", tablefmt="fancy_grid", maxcolwidths=100))
