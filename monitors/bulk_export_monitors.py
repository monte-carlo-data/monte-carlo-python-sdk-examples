import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from monitors import *

# Initialize logger
util_name = __file__.split('/')[-1].split('.')[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))


class BulkExportMonitors(Monitors):

    def __init__(self, profile, config_file: str = None, progress: Progress = None):
        """Creates an instance of BulkExportMonitors.

        Args:
            config_file (str): Path to the Configuration File.
            progress(Progress): Progress bar.
        """

        super().__init__(profile,  config_file)
        self.OUTPUT_FILE = "monitors.yaml"
        self.progress_bar = progress

    def bulk_export_yaml(self, export_name):

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
    parser = sdk_helpers.generate_arg_parser(os.path.basename(os.path.dirname(os.path.abspath(__file__))),
                                             os.path.basename(__file__))

    if not args:
        args = parser.parse_args(*args, **kwargs)
    else:
        sdk_helpers.dump_help(parser, main, *args)
        args = parser.parse_args(*args, **kwargs)

    @sdk_helpers.ensure_progress
    def run_utility(progress, util, args):
        util.progress_bar = progress
        export_name = False
        if args.export_name == 'y':
            export_name = True
        util.bulk_export_yaml(export_name)

    util = BulkExportMonitors(args.profile)
    run_utility(util, args)


if __name__ == '__main__':
    main()
