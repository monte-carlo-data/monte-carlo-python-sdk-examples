import traceback
from functools import wraps
from typing import Any, List, Optional

import click

import montecarlodata.settings as settings
from montecarlodata.common.data import MonolithResponse

AMBIGUOUS_COLLECTOR_ERROR_MESSAGE = "Multiple data collectors found. Please specify a collector."
AMBIGUOUS_AGENT_OR_COLLECTOR_MESSAGE = (
    "Multiple options found. Please specify an agent or collector."
)


def manage_errors(func):
    """
    Convenience decorator to abort on any errors after logging based on verbosity settings

    Requires an `_abort_on_error` field to be set in the instance
    """

    @wraps(func)
    def _impl(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except click.Abort:
            raise click.Abort()  # re-raise to prevent swallowing
        except Exception as error:
            if hasattr(self, "_disable_handle_errors") and self._disable_handle_errors:
                raise
            echo_error(error, [traceback.format_exc()])

        if hasattr(self, "_abort_on_error") and self._abort_on_error:
            raise click.Abort()

    return _impl


def echo_error(message: Any, errors: Optional[List[Any]] = None) -> None:
    """
    Convenience utility to echo any error in verbose and quiet mode
    """
    click.echo(f"Error - {message}", err=True)
    if settings.MCD_VERBOSE_ERRORS:
        for error in errors or []:
            click.echo(error, err=True)


def complain_and_abort(message: str) -> None:
    """
    Convenience utility to echo message and exit
    """
    click.echo(f"Error - {message}", err=True)
    raise click.Abort()


def abort_on_gql_errors(response: MonolithResponse):
    """
    Convenience utility to echo any gql errors and exit
    """
    if response.errors:
        for error in response.errors:
            message = error.get("message")
            if message == AMBIGUOUS_COLLECTOR_ERROR_MESSAGE:
                message = AMBIGUOUS_AGENT_OR_COLLECTOR_MESSAGE
            echo_error(message)
        raise click.Abort()


def prompt_connection(message: str, skip_prompt: bool = False) -> None:
    """
    Prompt message and abort if not confirmed
    """
    if not skip_prompt:
        click.confirm(message, abort=True)
