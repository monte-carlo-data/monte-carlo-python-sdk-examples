import os
import argparse
import logging.config
import sys
import yaml
import subprocess
import shutil
import textwrap
import traceback
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from pathlib import Path
from lib.util import Util
from lib.helpers.logs import LoggingConfigs, LogHelper, LogRotater
from lib.helpers import sdk_helpers

# Initialize logger
util_name = __file__.split('/')[-1].split('.')[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))
LOGGER = logging.getLogger()


class DeduplicateMonitors(Util):

    def __init__(self, profile, config_file: str = None):
        """Creates an instance of DeduplicateMonitors.

        Args:
            config_file (str): Path to the Configuration File.
        """

        super().__init__(profile,  config_file)
        self.OUTPUT_FILE = "monitors.yaml"

    def deduplicate(self, input_file: str, namespace: str):
        """

        """

        self.OUTPUT_DIR = Path(''.join(input_file.split('/')[:-1]))
        file_path = None

        if self.OUTPUT_DIR.is_dir():
            file_path = self.OUTPUT_DIR / self.OUTPUT_FILE

        if file_path and Path(input_file).is_file():
            shutil.copyfile(input_file, file_path)
        else:
            LOGGER.error(f"unable to locate input file: {input_file}")

        with open(input_file, 'r') as file:
            yaml_dict = yaml.safe_load(file)
            metric_monitors = yaml_dict.get("montecarlo").get("field_health")

        # Initializing compare keys
        comp_keys = ['table', 'timestamp_field', 'lookback_days', 'aggregation_time_interval', 'connection_name',
                     'use_important_fields', 'use_partition_clause', 'metric']

        # Compare each monitor with the rest to find possible duplicates
        duplicate_indexes = []
        for i in range(len(metric_monitors) - 1):
            for j in range(i + 1, len(metric_monitors)):
                if all(metric_monitors[i].get(key) == metric_monitors[j].get(key) for key in comp_keys):
                    print(f"Possible duplicate monitors in {input_file}: {i} - {metric_monitors[i].get('table')} "
                          f"and {j} - {metric_monitors[j].get('table')}")
                    duplicate_indexes.append(i)

        # Remove duplicates
        for index in duplicate_indexes:
            del metric_monitors[index]

        # Save as new file
        with open('monitors.yml', 'w') as outfile:
            yaml.safe_dump(yaml_dict, outfile, sort_keys=False)

        LOGGER.info("executing new configuration dry-run...")
        if not namespace:
            cmd = subprocess.run(["montecarlo", "--profile", self.profile, "monitors", "apply", "--dry-run"],
                                 capture_output=True, text=True)
        else:
            cmd = subprocess.run(["montecarlo", "--profile", self.profile, "monitors", "apply",
                                  "--namespace", namespace, "--dry-run"],
                                 capture_output=True, text=True)
        if cmd.returncode != 0:
            LOGGER.error("an error occurred")
            LogHelper.split_message(cmd.stderr, logging.ERROR)
            exit(cmd.returncode)
        else:
            LOGGER.info(f"export completed")
            LogHelper.split_message(cmd.stdout)


def main(*args, **kwargs):

    # Capture Command Line Arguments
    formatter = lambda prog: argparse.RawTextHelpFormatter(prog, max_help_position=120)
    parser = argparse.ArgumentParser(description='Find and remove duplicate monitors'.expandtabs(4),
                                     formatter_class=formatter)
    parser._optionals.title = "Options"
    parser._positionals.title = "Commands"
    m = ''

    parser.add_argument('--profile', '-p', required=False, default="default",
                        help='Specify an MCD profile name. Uses default otherwise', metavar=m)
    parser.add_argument('--namespace', '-n', required=False,
                        help='Namespace of monitors configuration.', metavar=m)
    parser.add_argument('--input', '-i', required=True,
                        help='Input file path.', metavar=m)

    if not args:
        args = parser.parse_args(*args, **kwargs)
    else:
        sdk_helpers.dump_help(parser, main, *args)
        args = parser.parse_args(*args, **kwargs)

    # Initialize variables
    profile = args.profile
    namespace = args.namespace

    # Initialize Util and run in given mode
    try:
        LOGGER.info(f"running utility using '{args.profile}' profile")
        util = DeduplicateMonitors(profile)
        util.deduplicate(args.input, namespace)
    except Exception as e:
        LOGGER.error(e, exc_info=False)
        print(traceback.format_exc())
    finally:
        LOGGER.info('rotating old log files')
        LogRotater.rotate_logs(retention_period=7)


if __name__ == '__main__':
    main()
