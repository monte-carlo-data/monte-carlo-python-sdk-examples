import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from monitors import *
import csv

# Initialize LOGGER
util_name = __file__.split('/')[-1].split('.')[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))


class GetMonitorStats(Monitors):

    def __init__(self, profile, config_file: str = None, progress: Progress = None):
        """Creates an instance of BulkExportMonitors.

        Args:
            config_file (str): Path to the Configuration File.
            progress(Progress): Progress bar.
        """

        super().__init__(profile,  config_file)
        self.OUTPUT_FILE = "monitors_stats.csv"
        self.progress_bar = progress

    def generate_stats_file(self, warehouses):

        cursor = None
        monitors = {}
        LOGGER.info(f"- Retrieving monitors...")
        LOGGER.debug("Retrieving monitors using [GetCustomRules]")
        for warehouse in warehouses:
            while True:
                response = self.auth.client(self.get_custom_rules(warehouse_id=warehouse, after=cursor)).get_custom_rules
                if len(response.edges) > 0:
                    for edge in response.edges:
                        node = edge.node
                        if not node.is_deleted and not monitors.get(node.uuid):
                            LOGGER.debug(f"{node.uuid} added to list")
                            monitors[node.uuid] = [warehouse, node.uuid, node.rule_type, node.rule_name,
                                                   node.description,
                                                   node.prev_execution_time, node.next_execution_time, "UNAVAILABLE",
                                                   f"https://getmontecarlo.com/monitors/{node.uuid}"]
                if response.page_info.has_next_page:
                    cursor = response.page_info.end_cursor
                else:
                    break

        LOGGER.debug("Retrieving monitors using [GetMonitors]")
        _, raw = self.auth.client(self.get_monitors_by_entities()).get_monitors
        if len(raw) > 0:
            for monitor in raw:
                if monitor.monitor_status != "PAUSED":
                    if len(warehouses) == 1 and monitor.resource_id != warehouses[0]:
                        continue

                    if not monitors.get(monitor.uuid):
                        LOGGER.debug(f"{monitor.uuid} added to list")
                        monitors[monitor.uuid] = [monitor.resource_id, monitor.uuid, monitor.monitor_type,
                                                  monitor.name, monitor.description, monitor.prev_execution_time,
                                                  monitor.next_execution_time, monitor.monitor_run_status,
                                                  f"https://getmontecarlo.com/alerts/{monitor.uuid}"]

        if len(monitors) > 0:
            LOGGER.info(f"- Retrieving last run status and incidents...")
            for monitor in monitors:
                res = self.auth.client(self.get_job_execution_history_logs).get_job_execution_history_logs
                if len(res) > 0:
                    LOGGER.debug(f"Updating last run status for {monitor}")
                    monitors[monitor].pop()
                    monitors[monitor].append(res[0].status)
                res = self.auth.client(self.get_latest_incident(monitor)).get_incidents
                if len(res.edges) > 0:
                    edge = res.edges[0]
                    monitors[monitor].append(f"https://getmontecarlo.com/alerts/{edge.node.uuid}")
                    monitors[monitor].append(edge.node.incident_time)

            LOGGER.info(f"- {len(monitors)} monitors found")
            # Write stats to CSV
            file_path = Path(os.path.abspath(__file__)).parent
            file_path.mkdir(parents=True, exist_ok=True)
            filename = file_path / self.OUTPUT_FILE
            fields = ['Warehouse UUID', 'Monitor UUID', 'Type', 'Name', 'Description', 'Previous Run', 'Next Run',
                      'Run Status', 'Monitor URL', 'Last Incident URL', 'Last Incident Time']
            with open(filename, 'w') as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(fields)
                csvwriter.writerows(list(monitors.values()))
            LOGGER.info(f"- monitor stats generated\n")
    

def main(*args, **kwargs):
    # Capture Command Line Arguments
    parser = argparse.ArgumentParser(description='\n\tMonitor Stats Utility')
    parser._optionals.title = "Options"
    parser._positionals.title = "Commands"
    parser.add_argument('--profile', '-p', required=False, default="default",
                        help='Specify an MCD profile name. Uses default otherwise')
    parser.add_argument('--warehouse', '-w', required=False,
                        help='Warehouse ID')

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
        if args.warehouse:
            warehouses = list(args.warehouse.split(" "))
        else:
            warehouses, _ = util.get_warehouses
        util.generate_stats_file(warehouses)

    util = GetMonitorStats(args.profile)
    run_utility(util, args)


if __name__ == '__main__':
    main()



