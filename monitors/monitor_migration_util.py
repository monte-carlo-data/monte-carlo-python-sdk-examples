import subprocess
import configparser
import os
import argparse
from pathlib import Path
from pycarlo.core import Client, Query, Mutation, Session
from typing import Optional

BATCH = 100
OUTPUT_FILE = "monitors_to_migrate.csv"


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
    get_custom_rules.edges.node.__fields__("uuid", "rule_type")
    get_custom_rules.edges.node.queries(first=batch_size).edges.node.__fields__("uuid", "entities")
    get_custom_rules.page_info.__fields__(end_cursor=True)
    get_custom_rules.page_info.__fields__("has_next_page")
    return query


def get_monitors_query(schema: str, batch_size: Optional[int] = BATCH, after: Optional[int] = 0) -> Query:
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


def validate_cli():
    # Call CLI to export monitors from UI
    print("- Checking montecarlo cli version...")
    proc = subprocess.run(["montecarlo", "--version"], capture_output=True, text=True)
    if proc.returncode != 0:
        print(" [ 𐄂 failure ] montecarlo cli is not installed")
        exit(proc.returncode)
    else:
        print(f" [ ✔ success ] montecarlo cli present")

    print("- Validating montecarlo cli connection...")
    proc = subprocess.run(["montecarlo", "--profile", profile, "validate"], capture_output=True, text=True)
    if proc.returncode != 0:
        print(" [ 𐄂 failure ] an error occurred")
        print(proc.stderr)
        exit(proc.returncode)
    else:
        print(f" [ ✔ success ] validation complete\n")


def validate_project_dir(directory: str) -> Path:
    """Retrieve all monitors based on search criteria.

        Args:
            directory(str): Project directory.

        Returns:
            Path: Full path to file containing list of tables.

    """

    project_dir = Path(directory)
    file_path = None

    if project_dir.is_dir():
        file_path = project_dir / OUTPUT_FILE

    return file_path


if __name__ == '__main__':

    # Capture Command Line Arguments
    parser = argparse.ArgumentParser(description='\n\tMonitor Migration Utility')
    subparsers = parser.add_subparsers(dest='commands', required=True, metavar="<command>")
    parser._optionals.title = "Options"
    parser._positionals.title = "Commands"

    export_parser = subparsers.add_parser('export', help='Export monitors from MC UI that match db:schema.')
    export_parser.add_argument('--profile', '-p', required=False, default="default",
                               help='Specify an MCD profile name. Uses default otherwise')
    export_parser.add_argument('--warehouse', '-w', required=True,
                               help='Warehouse ID')
    export_parser.add_argument('--schema', '-s', required=True,
                               help='Schema Name. If UI contains database include it i.e. <database>:<schema>')
    export_parser.add_argument('--namespace', '-n', required=False,
                               help='Namespace for the exported monitors. Defaults to --schema if not set')

    migrate_parser = subparsers.add_parser('migrate', help="Creates monitors as MaC after export.")
    migrate_parser.add_argument('--profile', '-p', required=False, default="default",
                                help='Specify an MCD profile name. Uses default otherwise')
    migrate_parser.add_argument('--namespace', '-n', required=False,
                                help='Namespace for the migrated monitors.')
    migrate_parser.add_argument('--directory', '-d', required=True,
                                help="Project directory where output files from 'export' action were generated.")
    migrate_parser.add_argument('--force', '-f', required=False, action='store_true',
                                help='Run WITHOUT dry-run mode')

    cleanup_parser = subparsers.add_parser('cleanup', help="Removes old monitors.")
    cleanup_parser.add_argument('--profile', '-p', required=False, default="default",
                               help='Specify an MCD profile name. Uses default otherwise')
    cleanup_parser.add_argument('--directory', '-d', required=True,
                                help="Project directory where output files from 'export' action were generated.")

    args = parser.parse_args()

    # Initialize variables
    profile = args.profile
    if 'namespace' not in args:
        if 'schema' in args:
            if args.schema:
                namespace = args.schema
            else:
                namespace = None
        elif 'directory' in args:
            namespace = args.directory.split('/')[-1]
    else:
        namespace = args.namespace

    # Read token variables from CLI default's config path ~/.mcd/profiles.ini
    configs = configparser.ConfigParser()
    profile_path = os.path.expanduser("~/.mcd/profiles.ini")
    configs.read(profile_path)
    mcd_id_current = configs[profile]['mcd_id']
    mcd_token_current = configs[profile]['mcd_token']

    client = Client(session=Session(mcd_id=mcd_id_current, mcd_token=mcd_token_current))
    print(f"Running utility using '{args.profile}' profile")
    if args.commands.lower() == 'cleanup':
        filename = validate_project_dir(args.directory)
        if filename:
            print("- Step 1: Validating montecarlo cli...")
            validate_cli()
            # Remove monitors
            with open(filename, 'r') as to_remove:
                count = 0
                for monitor in to_remove:
                    mutation = Mutation()
                    mutation.delete_monitor(monitor_id=monitor)
                    res = client(mutation).delete_monitor
                    print(f"Monitor [{monitor}] deleted successfully")
                    count += 1
                print(f" [ ✔ success ] cleanup completed. {count} monitors removed")
        else:
            print("Unable to locate output file. Make sure the 'export' and 'migrate' commands were previously run")
    elif args.commands.lower() == 'export':
        dw_id = args.warehouse
        schema_search = args.schema
        cursor = None
        monitors = []
        print(f"- Step 1: Retrieving custom monitors for schema {schema_search} and warehouse {dw_id}...")
        while True:
            response = client(get_rule_monitors_query(warehouse_id=dw_id, after=cursor)).get_custom_rules
            if len(response.edges) > 0:
                for edge in response.edges:
                    if len(edge.node.queries.edges) > 0:
                        for node in edge.node.queries.edges:
                            if node.node.entities:
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

        # using set() to remove duplicated from list, if any
        monitors = list(set(monitors))
        if len(monitors) > 0:
            print(f" [ ✔ success ] {len(monitors)} custom monitors found\n")
            # Write monitor ids to CSV
            file_path = Path(os.path.abspath(__file__)).parent / "output" / schema_search.replace(':', '-')
            file_path.mkdir(parents=True, exist_ok=True)
            print(f"- Step 2: Writing custom monitor ids to output file...")
            filename = file_path / OUTPUT_FILE
            with open(filename, 'w') as csvfile:
                for mon_id in monitors:
                    csvfile.write(f"{mon_id}\n")
            print(f" [ ✔ success ] monitor ids exported\n")

            print("- Step 3: Validating montecarlo cli...")
            validate_cli()

            print("- Step 4: Exporting monitors to monitors-as-code...")
            cmd_args = ["montecarlo", "--profile", profile, "monitors", "convert-to-mac",
                        "--namespace", namespace, "--project-dir", file_path / "cli",
                        "--monitors-file", "monitors_to_migrate.csv", "--dry-run"]
            cmd = subprocess.run(cmd_args,
                                 capture_output=True, text=True)
            if cmd.returncode != 0:
                print(" [ 𐄂 failure ] an error occurred")
                print(f"{cmd.stderr}")
                exit(cmd.returncode)
            else:
                print(f" [ ✔ success ] export completed")
                print(cmd.stdout)
                print(f"Modify the 'monitors.yml' file under output/{schema_search.replace(':', '-')}")
        else:
            print(f" [ - warning ] {len(monitors)} custom monitors found matching search criteria")
    elif args.commands.lower() == 'migrate':

        filename = validate_project_dir(args.directory)
        if filename:
            print("- Step 1: Validating montecarlo cli...")
            validate_cli()

            print("- Step 2: Applying monitor changes...")
            cmd_args = ["montecarlo", "--profile", profile, "monitors", "apply",
                        "--namespace", namespace, "--project-dir", filename.parent / "cli",
                        "--dry-run"]
            if args.force:
                del cmd_args[-1]

            cmd = subprocess.run(cmd_args, capture_output=True, text=True)
            if cmd.returncode != 0:
                print(" [ 𐄂 failure ] an error occurred")
                print(f"{cmd.stdout}")
                print(f"{cmd.stderr}")
                exit(cmd.returncode)
            else:
                print(f" [ ✔ success ] migration completed")
        else:
            print("[ 𐄂 failure ] unable to locate file containing monitors to remove")
