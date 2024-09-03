"""
Command line tools
"""

import copy
import json
from datetime import datetime
from typing import Optional, Set, cast

import click
from click import Context, Option

import montecarlodata.settings as settings
from montecarlodata.common.common import normalize_gql


class AdvancedOptions(Option):
    """
    Set mutually exclusive options, groups of required options and hidden prompts.
    Errors out on conflict or any missing values and appends text to help.

    Usage example -
        click.option('--foo' cls=MutuallyExclusiveOptions, mutually_exclusive_options=['bar']),
        click.option('--bar', cls=MutuallyExclusiveOptions, mutually_exclusive_options=['foo'])
    """

    def __init__(self, *args, **kwargs):
        help_text = kwargs.get("help", "")
        self._friendly_mutual_args = self._friendly_required_args = (
            self._friendly_at_least_one_set
        ) = None

        self.mutually_exclusive_options = set(kwargs.pop("mutually_exclusive_options", []))
        self.required_with_options = set(kwargs.pop("required_with_options", []))
        self.at_least_one_set = set(kwargs.pop("at_least_one_set", []))
        self.prompt_if_requested = kwargs.pop("prompt_if_requested", False)

        self.values_with_required_options = set(kwargs.pop("values_with_required_options", []))
        self.required_options_for_values = set(kwargs.pop("required_options_for_values", []))

        # Update help for options and create friendly args (i.e. human readable).
        if self.mutually_exclusive_options:
            self._friendly_mutual_args = self._create_friendly_args(
                options=self.mutually_exclusive_options
            )
            help_text = f"{help_text} This option cannot be used with {self._friendly_mutual_args}."
        if self.required_with_options:
            self._friendly_required_args = self._create_friendly_args(
                options=self.required_with_options
            )
            help_text = f"{help_text} This option requires setting {self._friendly_required_args}."
        if self.required_options_for_values:
            self._friendly_required_options_for_values = self._create_friendly_args(
                self.required_options_for_values
            )
            help_text = (
                f"{help_text} This option requires setting "
                f"{self._friendly_required_options_for_values} "
                f"when it is set to one of these values: {self.values_with_required_options}."
            )
        self._friendly_at_least_one_set = self._create_friendly_args(self.at_least_one_set)
        kwargs["help"] = help_text
        super(AdvancedOptions, self).__init__(*args, **kwargs)

    def handle_parse_result(self, ctx, opts, args):
        """
        Handle any conflicts, missing values or any required hidden prompts.
        """
        cli_friendly_name = normalize_gql(self.name)  # type: ignore

        if self.name in opts:
            name_opt = cast(str, opts[self.name])  # type: ignore  (should always be set)
            if self.mutually_exclusive_options.intersection(opts):
                raise click.BadParameter(
                    f"Cannot use '{cli_friendly_name}' with {self._friendly_mutual_args}."
                )
            if not self.required_with_options.issubset(opts):
                raise click.BadParameter(
                    f"Cannot use '{cli_friendly_name}' without {self._friendly_required_args}."
                )
            if (
                name_opt in self.values_with_required_options
                and not self.required_options_for_values.issubset(opts)
            ):
                raise click.BadParameter(
                    f"Cannot use '{cli_friendly_name}' with value "
                    f"'{name_opt}' without "
                    f"{self._friendly_required_options_for_values}."
                )
            if self.prompt_if_requested and name_opt == settings.SHOW_PROMPT_VALUE:
                opts[self.name] = click.prompt(  # type: ignore
                    cli_friendly_name,  # type: ignore
                    hide_input=True,
                )
            else:
                self.prompt = None

        if self.at_least_one_set and not any(x in opts for x in self.at_least_one_set):
            raise click.BadParameter(
                f"Missing required options from {self._friendly_at_least_one_set}."
            )

        return super(AdvancedOptions, self).handle_parse_result(ctx, opts, args)

    @staticmethod
    def _create_friendly_args(options: Set[str]) -> str:
        """
        Build a more human readable sentence from options.
        """
        friendly_options = list(copy.deepcopy(options))
        last_element = friendly_options.pop() if len(friendly_options) > 1 else None

        friendly_options = ", ".join([f"'{normalize_gql(option)}'" for option in friendly_options])
        if last_element:
            friendly_options = f"{friendly_options}, and '{last_element}'"
        return friendly_options


def add_common_options(options):
    """
    Convenience decorator for shared options (i.e. options that are common across commands)
    """

    def _add_common_options(function):
        for option in options[::-1]:
            function = option(function)
        return function

    return _add_common_options


def validate_json_callback(ctx_, param_, value):
    """
    Convenience callback to help validate (and load) JSON in option strings
    """
    try:
        return json.loads(value)
    except json.decoder.JSONDecodeError as err:
        raise click.BadParameter(f"Malformed JSON - {err}")
    except TypeError:
        return value


def convert_uuid_callback(ctx_, param_, value):
    """
    Convenience callback to convert UUIDs into strings
    """
    if value:
        return str(value)  # str(None) returns 'None'
    return value


def convert_empty_str_callback(ctx_, param_, value):
    """
    Convenience callback to convert an empty (or empty like) string into None
    """
    if bool(value and not value.isspace()):
        return value


def dump_help(cmd, parent: Optional[Context] = None):
    """
    Recursively echos help text for a cmd
    """
    ctx = click.core.Context(cmd, info_name=cmd.name, parent=parent)
    click.echo(cmd.get_help(ctx), err=True)
    for sub in getattr(cmd, "commands", {}).values():
        dump_help(sub, ctx)


def format_date(dt: str) -> str:
    """Format string from ISO format to YYYY-MM-DD"""
    return datetime.fromisoformat(dt).strftime("%Y-%m-%d")


def format_datetime(dt: str) -> str:
    """Format string from ISO format to YYYY-MM-DD HH:MM:SS"""
    return datetime.fromisoformat(dt).strftime("%Y-%m-%d %H:%M:%S")
