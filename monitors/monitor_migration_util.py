import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import pathlib
from monitors import *
from lib.helpers import sdk_helpers

# Initialize logger
util_name = os.path.basename(__file__).split('.')[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))


class MonitorMigrationUtility(Monitors):

    def __init__(self, profile, config_file: str = None, progress: Progress = None):
        """Creates an instance of MonitorMigrationUtility.

        Args:
            profile(str): Profile to use stored in montecarlo cli.
            config_file (str): Path to the Configuration File.
            progress(Progress): Progress bar.
        """

        super().__init__(profile,  config_file, progress)
        self.OUTPUT_FILE = "monitors_to_migrate.csv"
        self.progress_bar = progress

    def validate_project_dir(self, directory: str) -> Path:
        """Ensure path given exists.

        Args:
            directory(str): Project directory.

        Returns:
            Path: Full path to file containing list of tables.
        """

        project_dir = Path(directory)
        file_path = None

        if project_dir.is_dir():
            file_path = project_dir / self.OUTPUT_FILE

        return file_path

    @staticmethod
    def replace_monitor_names(monitors_dir: str, key: str):
        """Reads YAML file after export and replaces each monitor name to be unique so that there are no conflicts
        when converting back to ui. Unique key in DB is formed by:  Key (account_uuid, namespace, rule_name)

        Args:
            monitors_dir(str): Directory where the cli will output the exported monitors.yml
            key(str): Key used as a prefix to name each monitor.

        """

        LOGGER.info("updating monitor names...")
        with open(f"{monitors_dir}/monitors.yml", 'r') as file:
            yaml_dict = yaml.safe_load(file)
            monitors = yaml_dict.get("montecarlo")

        for monitor_type in monitors:
            count = 0
            for monitor in monitors[monitor_type]:
                if monitor.get('name'):
                    count += 1
                    monitor['name'] = f"{monitor_type}_{key}_{count}"

        with open(f"{monitors_dir}/monitors.yml", 'w') as file:
            yaml.safe_dump(yaml_dict, file, sort_keys=False)

        LOGGER.info(f"monitor names updated successfully")

    def export(self, asset: str, warehouse: str, namespace: str):
        """

        """

        warehouses = self.get_warehouses()
        asset_search = asset
        namespace = namespace.replace(':', '-')

        LOGGER.info(f"retrieving custom monitors for asset {asset_search}...")
        monitors = []
        for dw_id in warehouses:
            monitors.extend(self.get_custom_rules_with_assets(dw_id, asset_search)[0])
            self.progress_bar.update(self.progress_bar.tasks[0].id, advance=50/len(warehouses))
        LOGGER.debug(f"Monitor count = {len(monitors)}")
        dw_id = warehouse
        monitors.extend(self.get_monitors_by_entities(dw_id, asset_search)[0])
        LOGGER.debug(f"Monitor count = {len(monitors)}")

        # using set() to remove duplicated from list, if any
        monitors = list(set(monitors))
        LOGGER.debug(f"Monitor count = {len(monitors)}")
        if len(monitors) > 0:
            LOGGER.info(f"{len(monitors)} custom monitors found")
            # Write monitor ids to CSV
            file_path = Path(self.OUTPUT_DIR) / util_name / namespace
            file_path.mkdir(parents=True, exist_ok=True)
            LOGGER.info(f"writing custom monitor ids to output file...")
            filename = file_path / self.OUTPUT_FILE
            with open(filename, 'w') as csvfile:
                for mon_id in monitors:
                    csvfile.write(f"{mon_id}\n")
            LOGGER.info(f"monitor ids exported")

            LOGGER.info("exporting monitors to monitors-as-code...")
            mc_monitors_path = file_path / "cli"
            cmd_args = ["montecarlo", "--profile", self.profile, "monitors", "convert-to-mac",
                        "--namespace", namespace, "--project-dir", mc_monitors_path,
                        "--monitors-file", file_path / self.OUTPUT_FILE, "--dry-run"]
            cmd = subprocess.run(cmd_args,
                                 capture_output=True, text=True)
            if cmd.returncode != 0:
                LogHelper.split_message(cmd.stdout, logging.ERROR)
                LOGGER.error("an error occurred")
                LogHelper.split_message(cmd.stderr, logging.ERROR)
            else:
                LOGGER.info(f"export completed")
                LogHelper.split_message(cmd.stdout)
                self.replace_monitor_names(mc_monitors_path / "montecarlo", namespace)
                LOGGER.info(f"modify the 'monitors.yml' file under {mc_monitors_path}/montecarlo")

        else:
            LOGGER.warning(f"{len(monitors)} custom monitors found matching search criteria")

    def migrate(self, namespace, directory: str, force: bool):

        filename = self.validate_project_dir(directory)
        if filename:

            LOGGER.info("applying monitor changes...")
            cmd_args = ["montecarlo", "--profile", self.profile, "monitors", "apply",
                        "--namespace", namespace, "--project-dir", filename.parent / "cli",
                        "--dry-run"]
            if force:
                del cmd_args[-1]
                cmd = subprocess.run(cmd_args, capture_output=True, text=True, input="y")
            else:
                cmd = subprocess.run(cmd_args, capture_output=True, text=True)

            if cmd.returncode != 0:
                LogHelper.split_message(cmd.stdout, logging.ERROR)
                LOGGER.error("an error occurred")
                LogHelper.split_message(cmd.stderr, logging.ERROR)
                exit(cmd.returncode)
            else:
                LogHelper.split_message(cmd.stdout)
                self.summarize_apply_results(cmd.stdout)
                LOGGER.info(f"migration completed")
        else:
            LOGGER.error(f"unable to locate {directory}")

    def delete_or_disable(self, directory: str, action: str):
        """

        """

        filename = self.validate_project_dir(directory)
        if filename:
            with open(filename) as to_remove:
                count = 0
                monitors = to_remove.read().splitlines()

                for monitor in monitors:
                    if action == 'cleanup':
                        if self.delete_custom_monitor(monitor):
                            count += 1
                    else:
                        if self.pause_monitor(monitor, True):
                            count += 1

                    self.progress_bar.update(self.progress_bar.tasks[0].id, advance=50 / len(monitors))
                LOGGER.info(f"{action} completed. Applied to {count} monitors")
        else:
            LOGGER.error(
                "unable to locate output file. Make sure the 'export' and 'migrate' commands were previously run")


def main(*args, **kwargs):

    # Capture Command Line Arguments
    parser, subparsers = sdk_helpers.generate_arg_parser(os.path.basename(os.path.dirname(os.path.abspath(__file__)))
                                                         , os.path.basename(__file__))

    if not args:
        args = parser.parse_args(*args, **kwargs)
    else:
        sdk_helpers.dump_help(parser, main, *args)
        args = parser.parse_args(*args, **kwargs)

    # Initialize variables
    profile = args.profile
    namespace = None
    try:
        if not args.namespace:
            if 'asset' in args:
                if args.asset:
                    namespace = args.asset
            elif 'directory' in args:
                namespace = args.directory.split('/')[-1]
        else:
            namespace = args.namespace
    except:
        pass

    # Initialize Util and run in given mode
    try:
        with (Progress() as progress):
            task = progress.add_task("[yellow][RUNNING]...", total=100)
            LogRotater.rotate_logs(retention_period=7)
            progress.update(task, advance=25)

            LOGGER.info(f"running utility using '{args.profile}' profile")
            util = MonitorMigrationUtility(profile, progress=progress)
            if args.commands.lower() in ['cleanup', 'disable']:
                util.delete_or_disable(args.directory, args.commands.lower())
            elif args.commands.lower() == 'export':
                util.export(args.asset, args.warehouse, namespace)
            elif args.commands.lower() == 'migrate':
                util.migrate(namespace, args.directory, args.force)

            progress.update(task, description="[dodger_blue2][COMPLETE]", advance=100)

    except Exception as e:
        LOGGER.error(e, exc_info=False)
        print(f"[red]{traceback.format_exc()}")


if __name__ == '__main__':
    main()
