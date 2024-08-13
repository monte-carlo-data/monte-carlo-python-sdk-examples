import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from monitors import *

# Initialize logger
util_name = __file__.split('/')[-1].split('.')[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))
coloredlogs.install(level='INFO', fmt='%(asctime)s %(levelname)s - %(message)s')


class BulkExportMonitors(Monitors):

    def __init__(self, profile, config_file: str = None):
        """Creates an instance of BulkExportMonitors.

        Args:
            config_file (str): Path to the Configuration File.
        """

        super().__init__(profile,  config_file)
        self.OUTPUT_FILE = "monitors.yaml"

    def bulk_export_yaml(self, export_name):
        """

        """

        monitor_list, _ = self.get_ui_monitors()
        # Split list of monitors in batches of 500
        batches = sdk_helpers.batch_objects(monitor_list, 500)
        file_path = self.OUTPUT_DIR / util_name
        file_path.mkdir(parents=True, exist_ok=True)
        with open(file_path / self.OUTPUT_FILE, "w") as yaml_file:
            yaml_file.write("montecarlo:\n")
            for batch in batches:
                monitor_yaml = self.export_yaml_template(batch, export_name)
                yaml_file.write(textwrap.indent(monitor_yaml["config_template_as_yaml"], prefix="  "))

        LOGGER.info(f"exported ui monitors to yaml templates successfully")


def main(*args, **kwargs):

    # Capture Command Line Arguments
    formatter = lambda prog: argparse.RawTextHelpFormatter(prog, max_help_position=120)
    parser = argparse.ArgumentParser(description="\n[ BULK EXPORT UI MONITORS ]\n\n\t• YML file will be written, which can then"
                                                 " be used when syncing Monitors-as-Code. \n\t• Monitor 'name' is now a "
                                                 "mandatory parameter to apply MaC. Set -e flag to 'y'\n\t  to get monitor names "
                                                 "included in the yaml export.".expandtabs(4), formatter_class=formatter)
    parser._optionals.title = "Options"
    parser._positionals.title = "Commands"
    m = ''

    parser.add_argument('--profile', '-p', required=False, default="default",
                               help='Specify an MCD profile name. Uses default otherwise', metavar=m)
    parser.add_argument('--export-name', '-e', required=False, choices=['y', 'n'], default='n',
                        help='Include the resource name in the export?', metavar=m)

    if not args[0]:
        args = parser.parse_args(*args, **kwargs)
    else:
        sdk_helpers.dump_help(parser, main, *args)
        args = parser.parse_args(*args, **kwargs)

    # Initialize variables
    profile = args.profile
    if args.export_name == 'n':
        export_name = False
    else:
        export_name = True

    # Initialize Util and run actions
    try:
        LOGGER.info(f"running utility using '{args.profile}' profile")
        util = BulkExportMonitors(profile)
        util.bulk_export_yaml(export_name)
    except Exception as e:
        LOGGER.error(e, exc_info=False)
        print(traceback.format_exc())
    finally:
        LOGGER.info('rotating old log files')
        LogRotater.rotate_logs(retention_period=7)


if __name__ == '__main__':
    main()
