import click

_FG_COLOR_SUCCESS = "green"
_FG_COLOR_ERROR = "red"
_FG_COLOR_WARNING = "yellow"


def echo_success_message(message: str):
    click.echo(click.style(message, fg=_FG_COLOR_SUCCESS))


def echo_error_message(message: str):
    click.echo(click.style("Error: ", fg=_FG_COLOR_ERROR) + message)


def echo_warning_message(message: str):
    click.echo(click.style("Warn: ", fg=_FG_COLOR_WARNING) + message)


def styled_success_icon() -> str:
    return click.style("\u2713", fg=_FG_COLOR_SUCCESS)


def styled_error_icon() -> str:
    return click.style("\u2715", fg=_FG_COLOR_ERROR)


def styled_warning_icon() -> str:
    return click.style("\u26a0", fg=_FG_COLOR_WARNING)
