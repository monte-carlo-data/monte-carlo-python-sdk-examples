import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import re
import pytz
import time
import plotext as plot
from monitors import *
from collections import defaultdict
from prettytable import PrettyTable
from datetime import datetime, timedelta
from rich.prompt import Confirm


# Initialize logger
util_name = __file__.split('/')[-1].split('.')[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))


class OverlappedMonitorSchedules(Monitors):

    def __init__(self, profile, config_file: str = None, progress: Progress = None):
        """Creates an instance of BulkExportMonitors.

        Args:
            config_file (str): Path to the Configuration File.
        """

        super().__init__(profile,  config_file, progress)
        self.progress_bar = progress

    def group_monitors_by_run_time(self, day_range: int, erroring_only: bool):
        LOGGER.info("extracting monitor execution information...")
        groups = defaultdict(lambda: defaultdict(dict))
        _, monitors_raw = self.get_ui_monitors()
        _, mac_monitors_raw = self.get_mac_monitors()
        monitors_raw.extend(mac_monitors_raw)

        cond = "monitor.monitor_run_status == 'ERROR'" if erroring_only else "True"
        for monitor in monitors_raw:
            with sdk_helpers.PauseProgress(self.progress_bar) if self.progress_bar else nullcontext():
                self.progress_bar.update(self.progress_bar.tasks[0].id, advance=40/len(monitors_raw))
                if not monitor.is_paused and eval(cond):
                    next_run = monitor.next_execution_time
                    if monitor.schedule_config.interval_minutes:
                        interval_minutes = monitor.schedule_config.interval_minutes
                    elif monitor.schedule_config.interval_crontab:
                        interval_minutes = sdk_helpers.calculate_interval_minutes(monitor.schedule_config.interval_crontab[0])
                    else:
                        continue

                    # Calculate the next run time in UTC
                    run_time = next_run
                    frequency = 1
                    while datetime.now(pytz.UTC) < run_time < datetime.now(pytz.UTC) + timedelta(days=day_range):
                        run_time += timedelta(minutes=interval_minutes)
                        normalized_run_time = run_time.replace(second=0, microsecond=0) # set minute=0 if only grouping by date & hour
                        resource_id = monitor.resource_id
                        if monitor.uuid not in groups[normalized_run_time][resource_id]:
                            groups[normalized_run_time][resource_id][monitor.uuid] = 1
                        else:
                            groups[normalized_run_time][resource_id][monitor.uuid] += 1
            time.sleep(0.001)

        sorted_groups = {k: dict(v) for k, v in sorted(groups.items())}
        return sorted_groups

    def plot_monitor_data(self, monitor_data: dict, threshold: int):
        LOGGER.info("generating distribution plot...")
        xvals = []
        # Collect all resource IDs and runtimes
        resource_ids = set()
        for resources in monitor_data.values():
            resource_ids.update(resources.keys())

        resource_ids = sorted(resource_ids)  # Sort resource IDs for consistent order

        # Initialize yvals with empty lists for each resource ID
        yvals_dict = {resource_id: [] for resource_id in resource_ids}
        valid_runtimes = []
        table = PrettyTable(['Run Schedule', 'Resource ID', 'Total Runs', 'Monitor UUIDs'])
        table._max_width = {'Resource ID': 1, 'Total Runs': 1, 'Monitor UUIDs': 75}

        for run_time, resources in monitor_data.items():
            self.progress_bar.update(self.progress_bar.tasks[0].id, advance=40/len(monitor_data.items()))
            if any(len(monitors) >= threshold for monitors in resources.values()):
                valid_runtimes.append(str(run_time))
                xvals.append(run_time)  # Add the runtime to xvals
                for resource_id in resource_ids:
                    if resource_id in resources and len(resources[resource_id]) >= threshold:
                        count = len(resources[resource_id])
                        yvals_dict[resource_id].append(count)
                        uuids = "\n".join(["https://getmontecarlo.com/monitors/" + s for s in resources[resource_id].keys()])
                        table.add_row([run_time, resource_id, count, uuids], divider=True)
                    else:
                        yvals_dict[resource_id].append(0)
                time.sleep(0.05)

        # Filter out resource_ids where all counts are 0
        filtered_resource_ids = [resource_id for resource_id in resource_ids if
                                 any(count > 0 for count in yvals_dict[resource_id])]
        yvals = [yvals_dict[resource_id] for resource_id in filtered_resource_ids]

        if len(xvals) > 0:
            plot.simple_stacked_bar(xvals, yvals, width=125, labels=filtered_resource_ids,
                                    title=f"Distinct Monitor Count per Resource ID Over Time (Count >= {threshold})")
            print()
            plot.show()
            print()
            with sdk_helpers.PauseProgress(self.progress_bar) if self.progress_bar else nullcontext():
                log_plot = Confirm.ask(f"would you like to log the plot?", default='y')
                if log_plot:
                    plot_output = plot.build()
                    for row in plot_output.split("\n"):
                        LOGGER.debug(re.sub(r'\[[^\]]*?m', '', row).replace('\x1b', ''))

            with sdk_helpers.PauseProgress(self.progress_bar) if self.progress_bar else nullcontext():
                display_monitors = Confirm.ask(f"would you like to log the monitor ids?", default='y')
                if display_monitors:
                    print()
                    for row in table.get_string().split("\n"):
                        LOGGER.info(row)
        else:
            LOGGER.info("no monitor(s) matched the request")


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
    parser.add_argument('--min', '-m', required=True, type=int,
                        help='Minimum monitor execution count threshold', metavar=m)
    parser.add_argument('--range', '-r', required=False, type=int, default=7,
                        help='Execution range. Defaults to 7 days.', metavar=m)
    parser.add_argument('--error', '-e', required=False, type=bool, default=False,
                        help='Considers only monitors in ERROR state', metavar=m)

    if not args:
        args = parser.parse_args(*args, **kwargs)
    else:
        sdk_helpers.dump_help(parser, main, *args)
        args = parser.parse_args(*args, **kwargs)

    # Initialize variables
    profile = args.profile
    monitor_count_min = args.min
    range = args.range
    error_monitors = args.error

    # Initialize Util and run actions
    with (Progress() as progress):
        try:
            task = progress.add_task("[dark_orange3][RUNNING]...", total=100)
            LogRotater.rotate_logs(retention_period=7)
            progress.update(task, advance=10)
            LOGGER.info(f"running utility using '{args.profile}' profile")
            util = OverlappedMonitorSchedules(profile, progress=progress)
            util.plot_monitor_data(util.group_monitors_by_run_time(range, error_monitors), monitor_count_min)
        except Exception as e:
            LOGGER.error(e, exc_info=False)
            print(traceback.format_exc())
        finally:
            progress.update(task, description="[dodger_blue2][COMPLETE]", advance=100)


if __name__ == '__main__':
    main()
