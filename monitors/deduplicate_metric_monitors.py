import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from monitors import *

# Initialize logger
util_name = __file__.split('/')[-1].split('.')[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))
LOGGER = logging.getLogger()


class DeduplicateMetricMonitors(Util):

    def __init__(self, profile, config_file: str = None):
        """Creates an instance of DeduplicateMonitors.

        Args:
            config_file (str): Path to the Configuration File.
        """

        super().__init__(profile,  config_file)
        self.OUTPUT_FILE = None

    def deduplicate(self, input_file: str, namespace: str):
        """

        """

        self.OUTPUT_DIR = Path(''.join(input_file.split('/')[:-1]))
        self.OUTPUT_FILE = ''.join(input_file.split('/')[-1])
        file_path = None

        if self.OUTPUT_DIR.is_dir():
            file_path = self.OUTPUT_DIR / self.OUTPUT_FILE

        if file_path and Path(input_file).is_file():
            LOGGER.info("backing up input file...")
            shutil.copyfile(input_file, f"{file_path}.bkp")
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
                        LOGGER.debug(f"possible duplicate monitors in [{i} "
                                     f"{metric_monitors[i].get('table')}] <=> [{j} - {metric_monitors[j].get('table')}]")
                        duplicate_indexes.append(i)

            # Remove duplicates
            LOGGER.info("removing duplicate metric monitors...")
            for index in duplicate_indexes:
                del metric_monitors[index]

            # Save as new file
            with open(file_path, 'w') as outfile:
                yaml.safe_dump(yaml_dict, outfile, sort_keys=False)

            LOGGER.info("validating updated YML configuration...")
            if not namespace:
                cmd = subprocess.run(["montecarlo", "--profile", self.profile, "monitors", "apply", "--project-dir",
                                      self.OUTPUT_DIR, "--option-file", self.OUTPUT_FILE, "--dry-run"],
                                     capture_output=True, text=True)
            else:
                cmd = subprocess.run(["montecarlo", "--profile", self.profile, "monitors", "apply", "--project-dir",
                                      self.OUTPUT_DIR, "--option-file", self.OUTPUT_FILE, "--namespace", namespace,
                                      "--dry-run"], capture_output=True, text=True)
            if cmd.returncode != 0:
                LogHelper.split_message(cmd.stdout, logging.ERROR)
                LOGGER.error("an error occurred")
                LogHelper.split_message(cmd.stderr, logging.ERROR)
                exit(cmd.returncode)
            else:
                LOGGER.info(f"export completed")
                LogHelper.split_message(cmd.stdout)
        else:
            LOGGER.error(f"unable to locate input file: {input_file}")


def main(*args, **kwargs):

    # Capture Command Line Arguments
    formatter = lambda prog: argparse.RawTextHelpFormatter(prog, max_help_position=120)
    parser = argparse.ArgumentParser(description='\n[ DEDUPLICATE METRIC MONITORS MaC ]\n\n\tFinds and removes duplicate'
                                                 ' metric monitors from an input YML file'.expandtabs(4),
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
        util = DeduplicateMetricMonitors(profile)
        util.deduplicate(args.input, namespace)
    except Exception as e:
        LOGGER.error(e, exc_info=False)
        print(traceback.format_exc())
    finally:
        LOGGER.info('rotating old log files')
        LogRotater.rotate_logs(retention_period=7)


if __name__ == '__main__':
    main()