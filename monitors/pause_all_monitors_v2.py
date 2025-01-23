import os
import sys

from lib.helpers.constants import MonitorTypes

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from monitors import *
from lib.helpers import constants as c

# Initialize logger
util_name = __file__.split('/')[-1].split('.')[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))
LOGGER = logging.getLogger()


class PauseAllMonitorsV2(Monitors):

    def __init__(self, profile, config_file: str = None):
        """Creates an instance of PauseAllMonitorsV2.

        Args:
            operation(str):
        """

        super().__init__(profile, config_file)
        self.enabled = True

    def pause_rules(self, operation):
        """
        """
        counter = 0
        monitors_to_pause = []
        next_token = None
        rules_to_pause = []

        _, rules = self.get_monitors_by_type(types=[MonitorTypes.CUSTOM_SQL, MonitorTypes.COMPARISON, MonitorTypes.VALIDATION, MonitorTypes.FIELD_QUALITY])

        for rule in rules:
            if not rule['is_paused']:
                rules_to_pause.append(rule['uuid'])

        specific_uuid = "10bf3c84-6f96-4037-ae4a-5ec1f2c2d849"
        for uuid in rules_to_pause:
            try:
                if operation == "disable" and uuid == specific_uuid:
                    _ = self.auth.client(self.toggle_rule_state(),
                                         variables={"ruleId": uuid, "pause": True}).pauseRule
                    LOGGER.info(f"monitor [{uuid}] toggled successfully - pauseRule")
            except:
                LOGGER.error(f"Unable to pause monitor - {uuid}")

def main(*args, **kwargs):
    # Capture Command Line Arguments
    formatter = lambda prog: argparse.RawTextHelpFormatter(prog, max_help_position=120)
    parser = argparse.ArgumentParser(description="\n[ PAUSING / DISABLING ALL MONITORS ]\n\n\t"
                                                 " Pauses all monitors including metric and rule-based")
    parser._optionals.title = "Options"
    parser._positionals.title = "Commands"
    m = ''

    parser.add_argument('--profile', '-p', required=False, default="default",
                        help='Specify an MCD profile name. Uses default otherwise')
    parser.add_argument('--operation', '-o', choices=['enable', 'disable'], required=False, default='enable',
                        help='Enable/Disable monitor.')

    if not args:
        args = parser.parse_args(*args, **kwargs)
    else:
        sdk_helpers.dump_help(parser, main, *args)
        args = parser.parse_args(*args, **kwargs)

    # Initialize variables
    profile = args.profile

    # Initialize Util and run actions
    try:
        LOGGER.info(f"running utility using '{args.profile}' profile")
        util = PauseAllMonitorsV2(profile)
        if args.operation == 'disable':
            util.enabled = False
        else:
            util.enabled = True
        util.pause_rules(args.operation)
    except Exception as e:
        LOGGER.error(e, exc_info=False)
        print(traceback.format_exc())
    finally:
        LOGGER.info('rotating old log files')
        LogRotater.rotate_logs(retention_period=7)


if __name__ == '__main__':
    main()
