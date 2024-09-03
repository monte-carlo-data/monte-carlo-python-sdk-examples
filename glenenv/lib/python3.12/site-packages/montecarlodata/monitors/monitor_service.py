import textwrap
from typing import Callable, Dict, List, Optional

import click
from pycarlo.core import Client, Query
from tabulate import tabulate

from montecarlodata.errors import manage_errors


class MonitorService:
    MONITORS_HEADERS = (
        "Monitor UUID",
        "Type",
        "Namespace",
        "Description",
        "Last Update Time",
    )
    MORE_MONITOR_MESSAGE = "There are more monitors available. Increase the limit to view them."
    PSEUDO_MONITOR_TYPE_CB_COMPATIBLE = "CIRCUIT_BREAKER_COMPATIBLE"
    MONITOR_TYPE_STATS = "STATS"
    MONITOR_TYPE_CATEGORIES = "CATEGORIES"
    MONITOR_TYPE_JSON_SCHEMA = "JSON_SCHEMA"
    MONITOR_TYPE_CUSTOM_SQL = "CUSTOM_SQL"
    MONITOR_TYPE_TABLE_METRIC = "TABLE_METRIC"  # Legacy Volume SLIs
    MONITOR_TYPE_FRESHNESS = "FRESHNESS"
    MONITOR_TYPE_VOLUME = "VOLUME"
    MONITOR_TYPES = [
        PSEUDO_MONITOR_TYPE_CB_COMPATIBLE,
        MONITOR_TYPE_CUSTOM_SQL,
        MONITOR_TYPE_TABLE_METRIC,
        MONITOR_TYPE_FRESHNESS,
        MONITOR_TYPE_VOLUME,
        MONITOR_TYPE_STATS,
        MONITOR_TYPE_CATEGORIES,
        MONITOR_TYPE_JSON_SCHEMA,
    ]

    def __init__(
        self,
        client: Client,
        print_func: Callable = click.echo,
    ):
        self._pycarlo_client = client
        self._print_func = print_func

    @manage_errors
    def list_monitors(
        self,
        limit=100,
        namespaces: Optional[List[str]] = None,
        monitor_types: Optional[List[str]] = None,
    ):
        """
        Get all monitors filter by namespaces and monitor_types
        """
        kwargs: Dict = {"limit": limit + 1}
        if namespaces:
            kwargs["namespaces"] = namespaces
        if monitor_types:
            kwargs["monitor_types"] = monitor_types
        query = Query()
        query.get_monitors(**kwargs).__fields__(
            "uuid",
            "monitor_type",
            "namespace",
            "description",
            "last_update_time",
        )
        response = self._pycarlo_client(query)
        monitors = [
            (
                item.uuid,
                item.monitor_type,
                item.namespace,
                textwrap.fill(
                    item.description,  # type: ignore
                    width=70,
                )
                if item.description
                else "",
                item.last_update_time,
            )
            for item in response.get_monitors
        ]
        more_ns_available = False
        if len(monitors) > limit:
            monitors = monitors[:-1]
            more_ns_available = True
        self._print_func(tabulate(monitors, headers=self.MONITORS_HEADERS, tablefmt="fancy_grid"))

        if more_ns_available:
            self._print_func(self.MORE_MONITOR_MESSAGE)
