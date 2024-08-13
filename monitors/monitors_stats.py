import configparser
import os
import argparse
import subprocess
import csv
import datetime
import logging
from logging.config import dictConfig
from pathlib import Path
from pycarlo.core import Client, Query, Session
from typing import Optional

BATCH = 100
OUTPUT_FILE = "monitors_stats.csv"

# Initialize logger
logging_config = dict(
            version=1,
            formatters={
                'standard': {'format': '%(asctime)s - %(levelname)s - %(message)s'},
                'console': {'format': '%(message)s'}
            },
            handlers={
                'file': {'class': 'logging.FileHandler',
                         'formatter': 'standard',
                         'level': logging.DEBUG,
                         'filename': f"{__file__.split('.')[0]}-{datetime.date.today()}.log",
                         'encoding': "utf-8"},
                'console': {'class': 'logging.StreamHandler',
                            'formatter': 'console',
                            'level': logging.INFO,
                            'stream': 'ext://sys.stdout'}
            },
            root={'handlers': ['file', 'console'],
                  'level': logging.NOTSET},
        )

logging.config.dictConfig(logging_config)
logger = logging.getLogger()


def get_rule_monitors_query(warehouse_id: str, batch_size: Optional[int] = BATCH, after: Optional[str] = None) -> Query:
    """Retrieve custom rule monitors for a particular warehouse.

        Args:
            warehouse_id(str): Warehouse UUID from MC.
            batch_size(int): Limit of results returned by the response.
            after(str): Cursor value for next batch.

        Returns:
            Query: Formed MC Query object.

    """

    query = Query()
    get_custom_rules = query.get_custom_rules(first=batch_size, warehouse_uuid=warehouse_id, **(dict(after=after) if after else {}))
    get_custom_rules.edges.node.__fields__("uuid", "rule_type", "rule_name", "description", "is_deleted",
                                           "prev_execution_time", "next_execution_time")
    get_custom_rules.edges.node.queries(first=batch_size).edges.node.__fields__("uuid", "entities")
    get_custom_rules.page_info.__fields__(end_cursor=True)
    get_custom_rules.page_info.__fields__("has_next_page")
    return query


def get_monitors_query(schema: str = None, batch_size: Optional[int] = BATCH, after: Optional[int] = 0) -> Query:
    """Retrieve all monitors based on search criteria.

            Args:
                schema(str): Schema to apply in search filter.
                batch_size(int): Limit of results returned by the response.
                after(str): Offset to skip for pagination.

            Returns:
                Query: Formed MC Query object.

    """

    query = Query()
    # Add . at the end of the schema to search to ensure delimiter is respected
    if schema:
        get_monitors = query.get_monitors(search=[f"{schema}."], limit=batch_size, offset=after,
                                          search_fields=["ENTITIES"])
    else:
        get_monitors = query.get_monitors(limit=batch_size, offset=after)
    get_monitors.__fields__("resource_id", "uuid", "monitor_type", "name", "description", "prev_execution_time",
                                           "next_execution_time", "monitor_status", "monitor_run_status")
    return query


def validate_cli():
    # Call CLI to export monitors from UI
    logger.info("- Checking montecarlo cli version...")
    proc = subprocess.run(["montecarlo", "--version"], capture_output=True, text=True)
    if proc.returncode != 0:
        logger.info(" [ 𐄂 failure ] montecarlo cli is not installed")
        exit(proc.returncode)
    else:
        logger.info(f" [ ✔ success ] montecarlo cli present")

    logger.info("- Validating montecarlo cli connection...")
    proc = subprocess.run(["montecarlo", "--profile", profile, "validate"], capture_output=True, text=True)
    if proc.returncode != 0:
        logger.info(" [ 𐄂 failure ] an error occurred")
        logger.info(proc.stderr)
        exit(proc.returncode)
    else:
        logger.info(f" [ ✔ success ] validation complete")


if __name__ == '__main__':

    # Capture Command Line Arguments
    parser = argparse.ArgumentParser(description='\n\tMonitor Stats Utility')
    parser._optionals.title = "Options"
    parser._positionals.title = "Commands"
    parser.add_argument('--profile', '-p', required=False, default="default",
                        help='Specify an MCD profile name. Uses default otherwise')
    parser.add_argument('--warehouse', '-w', required=False,
                        help='Warehouse ID')
    args = parser.parse_args()

    # Initialize variables
    profile = args.profile
    warehouses = args.warehouse

    # Read token variables from CLI default's config path ~/.mcd/profiles.ini
    configs = configparser.ConfigParser()
    profile_path = os.path.expanduser("~/.mcd/profiles.ini")
    configs.read(profile_path)
    mcd_id_current = configs[profile]['mcd_id']
    mcd_token_current = configs[profile]['mcd_token']

    client = Client(session=Session(mcd_id=mcd_id_current, mcd_token=mcd_token_current))
    logger.info(f"\nRunning utility using '{args.profile}' profile")
    logger.info("- Validating credentials...")
    validate_cli()
    if warehouses:
        warehouses = list(warehouses.split(" "))
    else:
        query = Query()
        query.get_user().account.warehouses.__fields__("name", "uuid")
        res = client(query).get_user
        warehouses = [warehouse.uuid for warehouse in res.account.warehouses]

    cursor = None
    monitors = {}
    logger.info(f"- Retrieving monitors...")
    logger.debug("Retrieving monitors using [GetCustomRules]")
    for warehouse in warehouses:
        while True:
            response = client(get_rule_monitors_query(warehouse_id=warehouse, after=cursor)).get_custom_rules
            if len(response.edges) > 0:
                for edge in response.edges:
                    node = edge.node
                    if not node.is_deleted and not monitors.get(node.uuid):
                        logger.debug(f"{node.uuid} added to list")
                        monitors[node.uuid] = [warehouse, node.uuid, node.rule_type, node.rule_name, node.description,
                                               node.prev_execution_time, node.next_execution_time, "UNAVAILABLE",
                                               f"https://getmontecarlo.com/monitors/{node.uuid}"]
            if response.page_info.has_next_page:
                cursor = response.page_info.end_cursor
            else:
                break

    logger.debug("Retrieving monitors using [GetMonitors]")
    skip_records = 0
    while True:
        response = client(get_monitors_query(after=skip_records)).get_monitors
        if len(response) > 0:
            for monitor in response:
                if monitor.monitor_status != "PAUSED":
                    if len(warehouses) == 1 and monitor.resource_id != warehouses[0]:
                        continue

                    if not monitors.get(monitor.uuid):
                        logger.debug(f"{monitor.uuid} added to list")
                        monitors[monitor.uuid] = [monitor.resource_id, monitor.uuid, monitor.monitor_type,
                                                  monitor.name, monitor.description, monitor.prev_execution_time,
                                                  monitor.next_execution_time, monitor.monitor_run_status,
                                                  f"https://getmontecarlo.com/alerts/{monitor.uuid}"]

        skip_records += BATCH
        if len(response) < BATCH:
            break

    if len(monitors) > 0:
        logger.info(f"- Retrieving last run status and incidents...")
        for monitor in monitors:
            query = Query()
            query.get_job_execution_history_logs(custom_rule_uuid=monitor).__fields__("status")
            res = client(query).get_job_execution_history_logs
            if len(res) > 0:
                logger.debug(f"Updating last run status for {monitor}")
                monitors[monitor].pop()
                monitors[monitor].append(res[0].status)
            query = Query()
            query.get_incidents(monitor_ids=[monitor], first=1).edges.node.__fields__("uuid", "incident_time")
            res = client(query).get_incidents
            if len(res.edges) > 0:
                edge = res.edges[0]
                monitors[monitor].append(f"https://getmontecarlo.com/alerts/{edge.node.uuid}")
                monitors[monitor].append(edge.node.incident_time)

        logger.info(f"- {len(monitors)} monitors found")
        # Write stats to CSV
        file_path = Path(os.path.abspath(__file__)).parent
        file_path.mkdir(parents=True, exist_ok=True)
        filename = file_path / OUTPUT_FILE
        fields = ['Warehouse UUID', 'Monitor UUID', 'Type', 'Name', 'Description', 'Previous Run', 'Next Run',
                  'Run Status', 'Monitor URL', 'Last Incident URL', 'Last Incident Time']
        with open(filename, 'w') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(fields)
            csvwriter.writerows(list(monitors.values()))
        logger.info(f"- monitor stats generated\n")





