import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import copy
import uuid
from monitors import *
from lib.helpers import sdk_helpers

# Initialize logger
util_name = os.path.basename(__file__).split('.')[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))


class MonitorMigrationUtility(Monitors):

    def __init__(self, profile, config_file: str = None, progress: Progress = None):
        """Creates an instance of MonitorMigrationUtility.

        Args:
            profile(str): Profile to use stored in montecarlo test.
            config_file (str): Path to the Configuration File.
            progress(Progress): Progress bar.
        """

        super().__init__(profile,  config_file, progress)
        self.OUTPUT_FILE = "monitors_to_promote.csv"
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
            monitors_dir(str): Directory where the test will output the exported monitors.yml
            key(str): Key used as a prefix to name each monitor.

        """

        LOGGER.info("updating monitor names...")
        with open(f"{monitors_dir}/monitors.yml", 'r') as file:
            yaml_dict = yaml.safe_load(file)
            monitors = yaml_dict.get("montecarlo")

        for monitor_type in monitors:
            for monitor in monitors[monitor_type]:
                if monitor.get('name'):
                    monitor['name'] = f"{monitor_type}_{key}_{uuid.uuid4()}"

        with open(f"{monitors_dir}/monitors.yml", 'w') as file:
            yaml.safe_dump(yaml_dict, file, sort_keys=False)

        LOGGER.info(f"monitor names updated successfully")

    @staticmethod
    def merge_monitors_into_target(export_path: Path, target_path: Path):
        """
        Merges monitors from exported file into a target monitors.yml file.

        Args:
            export_path (Path): Path to the source monitors.yml created by export.
            target_path (Path): Path to the destination monitors.yml to merge into.
        """
        LOGGER.info(f"Merging monitors from {export_path} into {target_path}")

        if not export_path.exists():
            LOGGER.error(f"Exported monitors file not found: {export_path}")
            return

        if target_path.exists():
            with open(target_path, "r") as f:
                target_yaml = yaml.safe_load(f) or {}
        else:
            target_yaml = {}

        with open(export_path, "r") as f:
            exported_yaml = yaml.safe_load(f) or {}

        exported_mc = exported_yaml.get("montecarlo", {})
        target_mc = target_yaml.setdefault("montecarlo", {})

        for section, monitors in exported_mc.items():
            if not isinstance(monitors, list):
                continue
            target_section = target_mc.setdefault(section, [])
            existing_names = {m.get("name") for m in target_section}
            for monitor in monitors:
                if monitor.get("name") not in existing_names:
                    target_section.append(copy.deepcopy(monitor))

        with open(target_path, "w") as f:
            yaml.safe_dump(target_yaml, f, sort_keys=False)

        LOGGER.info(f"Merged monitors successfully into {target_path}")

    def export(self, asset: str, warehouse: str, namespace: str, mac_directory: str):
        """

        """

        warehouses, _ = self.get_warehouses()
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
            mc_monitors_path = file_path / "tmp"
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
                # Merge into target file
                exported_file = mc_monitors_path / "montecarlo" / "monitors.yml"
                target_file = Path(
                    os.path.join(mac_directory, "monitors.yml")
                )
                self.merge_monitors_into_target(exported_file, target_file)
        else:
            LOGGER.warning(f"{len(monitors)} custom monitors found matching search criteria")

    def promote(self, namespace, directory: str, force: bool):

        filename = self.validate_project_dir(directory)
        if filename:

            LOGGER.info("applying monitor changes...")
            cmd_args = ["montecarlo", "--profile", self.profile, "monitors", "apply",
                        "--namespace", namespace, "--project-dir", filename.parent,
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
                        self.delete_custom_monitor(monitor)
                        count += 1
                    else:
                        self.pause_monitor(monitor, True)
                        count += 1

                    self.progress_bar.update(self.progress_bar.tasks[0].id, advance=50 / len(monitors))
                LOGGER.info(f"{action} completed. Applied to {count} monitors")
        else:
            LOGGER.error(
                "unable to locate output file. Make sure the 'export' commands were previously run")


def main(*args, **kwargs):

    # Capture Command Line Arguments
    parser, subparsers = sdk_helpers.generate_arg_parser(os.path.basename(os.path.dirname(os.path.abspath(__file__)))
                                                         , os.path.basename(__file__))

    if not args:
        args = parser.parse_args(*args, **kwargs)
    else:
        sdk_helpers.dump_help(parser, main, *args)
        args = parser.parse_args(*args, **kwargs)

    @sdk_helpers.ensure_progress
    def run_utility(progress, util, args):
        util.progress_bar = progress
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

        if args.commands.lower() in ['cleanup', 'disable']:
            util.delete_or_disable(args.directory, args.commands.lower())
        elif args.commands.lower() == 'export':
            util.export(args.asset, args.warehouse, namespace, args.directory)
        elif args.commands.lower() == 'promote':
            util.promote(namespace, args.directory, args.force)

    util = MonitorMigrationUtility(args.profile)
    run_utility(util, args)


if __name__ == '__main__':
    main()
