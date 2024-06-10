import subprocess
import configparser
import os
import argparse
from pycarlo.core import Client, Query, Session
from typing import Optional

BATCH = 100


def get_table_query(warehouse_id: str, schema: str, batch_size: Optional[int] = BATCH, after: Optional[str] = None) -> Query:
    """Retrieve table information based on warehouse id and search parameter.

        Args:
            warehouse_id(str): Warehouse UUID from MC.
            schema(str): Schema to apply in search filter.
            batch_size(int): Limit of results returned by the response.
            after(str): Cursor value for next batch.

        Returns:
            Query: Formed MC Query object.

    """

    query = Query()
    # Add . at the end of the schema to search to ensure delimiter is respected
    get_tables = query.get_tables(first=batch_size, dw_id=warehouse_id, search=f"{schema}.", is_deleted=False,
                                  **(dict(after=after) if after else {}))
    get_tables.edges.node.__fields__("full_table_id")
    get_tables.edges.node.monitors(first=batch_size).edges.node.__fields__("uuid")
    get_tables.page_info.__fields__(end_cursor=True)
    get_tables.page_info.__fields__("has_next_page")
    return query


def get_rule_monitors_query(warehouse_id, batch_size: Optional[int] = BATCH, after: Optional[str] = None) -> Query:
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
    get_custom_rules.edges.node.__fields__("uuid", "rule_type")
    get_custom_rules.edges.node.queries(first=batch_size).edges.node.__fields__("uuid", "entities")
    get_custom_rules.page_info.__fields__(end_cursor=True)
    get_custom_rules.page_info.__fields__("has_next_page")
    return query


def get_monitors_query(schema, batch_size: Optional[int] = BATCH, after: Optional[int] = 0) -> Query:
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
    get_monitors = query.get_monitors(search=[f"{schema}."], limit=batch_size, offset=after, search_fields=["ENTITIES"])
    get_monitors.__fields__("uuid", "entities", "monitor_type", "monitor_status", "resource_id", "name", "namespace")
    return query


if __name__ == '__main__':

    # Capture Command Line Arguments
    parser = argparse.ArgumentParser(description='Export UI Based Monitors')
    parser.add_argument('--profile', '-p', required=True, default="default",
                        help='Specify an MCD profile name. Uses default otherwise')
    parser.add_argument('--warehouse', '-w', required=True,
                        help='Warehouse ID')
    parser.add_argument('--schema', '-s', required=True,
                        help='Schema Name. If UI contains database include it i.e. <database>:<schema>')
    parser.add_argument('--namespace', '-n', required=False,
                        help='Namespace for the exported monitors. Defaults to --schema if not set')

    args = parser.parse_args()

    # Initialize variables
    profile = args.profile
    dw_id = args.warehouse
    schema_search = args.schema
    if not args.namespace:
        namespace = args.schema
    else:
        namespace = args.namespace

    # Read token variables from CLI default's config path ~/.mcd/profiles.ini
    configs = configparser.ConfigParser()
    profile_path = os.path.expanduser("~/.mcd/profiles.ini")
    configs.read(profile_path)
    mcd_id_current = configs[profile]['mcd_id']
    mcd_token_current = configs[profile]['mcd_token']

    client = Client(session=Session(mcd_id=mcd_id_current, mcd_token=mcd_token_current))
    cursor = None
    monitors = []
    print(f"- Step 1: Retrieving custom monitors for schema {schema_search} and warehouse {dw_id}...")
    while True:
        response = client(get_rule_monitors_query(warehouse_id=dw_id, after=cursor)).get_custom_rules
        if len(response.edges) > 0:
            for edge in response.edges:
                if len(edge.node.queries.edges) > 0:
                    for node in edge.node.queries.edges:
                        if schema_search in [ent.split('.')[0] for ent in node.node.entities]:
                            print(f"\t• Monitor of type {edge.node.rule_type} found in {node.node.entities}")
                            monitors.append(edge.node.uuid)
        if response.page_info.has_next_page:
            cursor = response.page_info.end_cursor
        else:
            break

    skip_records = 0
    while True:
        response = client(get_monitors_query(schema=schema_search, after=skip_records)).get_monitors
        if len(response) > 0:
            for monitor in response:
                if monitor.monitor_status != "PAUSED" and monitor.namespace == 'ui':
                    if monitor.resource_id == dw_id:
                        if not monitor.uuid in monitors:
                            monitors.append(monitor.uuid)
                            print(f"\t• Monitor of type {monitor.monitor_type} found in {monitor.entities}")
                        else:
                            print(f"\t• {monitor.uuid} already present in monitors to export. Skipping...")

        skip_records += BATCH
        if len(response) < BATCH:
            break

    # using set() to remove duplicated from list
    monitors = list(set(monitors))
    if len(monitors) > 0:
        print(f" [ ✔ success ] {len(monitors)} custom monitors found\n")
        # Write monitor ids to CSV
        filename = "monitors_to_migrate.csv"
        print(f"- Step 2: Write custom monitor ids to {filename}...")
        with open(filename, 'w') as csvfile:
            for mon_id in monitors:
                csvfile.write(f"{mon_id}\n")
        print(f" [ ✔ success ] monitor ids exported\n")

        # Call CLI to export monitors from UI
        print("- Step 3: Checking montecarlo cli version...")
        cmd = subprocess.run(["montecarlo", "--version"], capture_output=True, text=True)
        if cmd.returncode != 0:
            print(" [ 𐄂 failure ] montecarlo cli is not installed")
            exit(cmd.returncode)
        else:
            print(f" [ ✔ success ] montecarlo cli present\n")

        print("- Step 4: Validating montecarlo cli connection...")
        cmd = subprocess.run(["montecarlo", "--profile", profile, "validate"], capture_output=True, text=True)
        if cmd.returncode != 0:
            print(" [ 𐄂 failure ] an error occurred")
            print(cmd.stderr)
            exit(cmd.returncode)
        else:
            print(f" [ ✔ success ] validation complete\n")

        print("- Step 5: Exporting monitors to monitors-as-code...")
        cmd = subprocess.run(["montecarlo", "--profile", profile, "monitors", "convert-to-mac",
                              "--namespace", namespace, "--project-dir", f"output/{schema_search}",
                              "--monitors-file", "monitors_to_migrate.csv", "--dry-run"],
                             capture_output=True, text=True)
        if cmd.returncode != 0:
            print(" [ 𐄂 failure ] an error occurred")
            print(f"{cmd.stderr}")
            exit(cmd.returncode)
        else:
            print(f" [ ✔ success ] export completed")
            print(cmd.stdout)
    else:
        print(f" [ - warning ] {len(monitors)} custom monitors found matching search criteria")
