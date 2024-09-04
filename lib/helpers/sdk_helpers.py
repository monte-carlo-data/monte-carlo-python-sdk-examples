import sys
import time

import pytz
from datetime import datetime, timedelta
from lib.helpers.logs import LOGGER
from rich.progress import Progress
from cronsim import CronSim


def hour_rounder(t):
    # Rounds to nearest hour by adding a timedelta hour if minute >= 30
    return t.replace(second=0, microsecond=0, minute=0, hour=t.hour) + timedelta(hours=t.minute // 30)


def calculate_interval_minutes(cron: str):
    """Return interval in minutes for a crontab string"""

    it = CronSim(cron, datetime.now(pytz.UTC))
    a = next(it)
    b = next(it)
    delta = b - a
    return int(delta.total_seconds() / 60)


def link(uri, label=None):
    """Create clickable link inside terminal"""
    if label is None:
        label = uri
    parameters = ''

    # OSC 8 ; params ; URI ST <name> OSC 8 ;; ST
    escape_mask = '\033]8;{};{}\033\\{}\033]8;;\033\\'

    return escape_mask.format(parameters, uri, label)


def dump_help(parser, func, *args):
    if len(*args) == 0:
        parser.print_help(sys.stderr)
        sys.exit(1)
    elif len(*args) == 1:
        if '-h' not in args[0] and '--help' not in args[0]:
            args[0].append("-h")
            func(*args)
            sys.exit(1)
    elif len(*args) == 2 and args[0][1] in ["-h", "--help"]:
        if parser._subparsers._group_actions[0].choices.get(args[0][0]):
            parser._subparsers._group_actions[0].choices[args[0][0]].print_help(sys.stderr)
            sys.exit(1)


def batch_objects(objects: list, batch_size: int) -> list:
    """Batch Objects into sublists.

    Args:
        objects (list): Objects to be batched.
        batch_size (int): number of elements inside each batch

    Returns:
        list: List of Lists.

    """
    LOGGER.info(f"batching the {len(objects)} into lists of {batch_size}")
    batches = [objects[x:x + batch_size] for x in range(0, len(objects), batch_size)]
    LOGGER.info(f"batching complete. {len(batches)} batch lists created")

    return batches


class PauseProgress:
    def __init__(self, progress: Progress) -> None:
        self._progress = progress

    def _clear_line(self) -> None:
        UP = "\x1b[1A"
        CLEAR = "\x1b[2K"

        for _ in self._progress.tasks:
            print(UP + CLEAR + UP)

    def __enter__(self):
        self._progress.stop()
        self._clear_line()
        return self._progress

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._progress.start()
