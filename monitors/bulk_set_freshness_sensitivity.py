import os
import sys
import csv
import datetime
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from monitors import *
from cron_validator import CronValidator

# Initialize logger
util_name = __file__.split('/')[-1].split('.')[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))


class SetFreshnessSensitivity(Monitors, Tables):

	def __init__(self, profile, config_file: str = None, progress: Progress = None):
		"""Creates an instance of SetFreshnessSensitivity.

		Args:
			profile(str): Profile to use stored in montecarlo cli.
			config_file (str): Path to the Configuration File.
			progress(Progress): Progress bar.
		"""

		super().__init__(profile, config_file, progress)
		self.progress_bar = progress
		self.rule_operator_type = None

	def validate_input_file(self, input_file: str) -> any:
		"""Ensure contents of input file satisfy requirements.

		Args:
			input_file(str): Path to input file.

		Returns:
			Any: Dictionary with mappings or None.

		"""
		# TODO
		# should fail if input file does not exist

		file_path = Path(input_file)
		LOGGER.info(f"starting input file validation...")
		if file_path.is_file():
			input_tables = None
			auto_required_cols = ['full_table_id', 'sensitivity']
			explicit_required_cols = ['full_table_id', 'updated_in_last_minutes', 'cron']
			try:
				with open(file_path, 'r') as file:
					reader = csv.DictReader(file)
					input_tables = {}
					for index, row in enumerate(reader):
						col_count = len(row)
						if col_count == 3:
							self.rule_operator_type = 'EXPLICIT'
							for col in explicit_required_cols:
								if not row.get(col):
									raise ValueError(f"value for '{col}' is missing: line {index + 1}")
							try:
								int(row["updated_in_last_minutes"])
								try:
									CronValidator.parse(row["cron"])
									input_tables[row["full_table_id"]] = row
								except ValueError:
									raise ValueError(
										f"value under 'cron' is invalid: line {index + 1}")
							except ValueError:
								raise ValueError(
									f"value under 'updated_in_last_minutes' must be an integer: line {index + 1}")
						elif col_count == 2:
							self.rule_operator_type = 'AUTO'
							for col in auto_required_cols:
								if not row.get(col):
									raise ValueError(f"value for '{col}' is missing: line {index + 1}")
							if row["sensitivity"].upper() not in ['LOW', 'MEDIUM', 'HIGH']:
								raise ValueError(f"sensitivity must be LOW, MEDIUM or HIGH: line {index + 1}")
							input_tables[row["full_table_id"]] = row
						else:
							raise ValueError(f"{col_count} columns present in CSV, either {explicit_required_cols} OR "
							                 f"{auto_required_cols} are required")

			except ValueError as e:
				LOGGER.error(f"errors found in file: {e}")

		return input_tables

	def update_freshness_thresholds(self, input_dict: dict, warehouse_id: str):

		if input_dict:
			LOGGER.info(f"updating freshness rules...")
			input_fulltableids = [item['full_table_id'] for item in input_dict.values()]
			input_mcons, _ = self.get_mcons_by_fulltableid(warehouse_id, input_fulltableids)
			monitor_ids, response = self.get_monitors_by_type(warehouse_id, [const.MonitorTypes.FRESHNESS], True, input_mcons)
			for index, full_table_id in enumerate(input_fulltableids):
				try:
					input_mcons[index]
				except IndexError:
					LOGGER.warning(f"skipping {full_table_id} - asset not found")
					continue

				payload = {
					"dw_id": warehouse_id,
					"replaces_ootb": True,
					"event_rollup_until_changed": True,
					"timezone": "UTC",
					"schedule_config": {
						"schedule_type": "FIXED",
						"start_time": datetime.datetime.strftime(sdk_helpers.hour_rounder(datetime.datetime.now()),
						                                        "%Y-%m-%dT%H:%M:%S.%fZ"),
					},
					"comparisons": [
						{
							"full_table_id": input_mcons[index],
							"comparison_type": "FRESHNESS",
						}
					]
				}

				if self.rule_operator_type == 'EXPLICIT':
					payload["schedule_config"]["interval_crontab"] = [input_dict[full_table_id]['cron']]
					payload["comparisons"][0]["operator"] = 'GT'
					payload["comparisons"][0]["threshold"] = float(input_dict[full_table_id]['updated_in_last_minutes'])
				else:
					payload["schedule_config"]["interval_minutes"] = 60
					payload["comparisons"][0]["operator"] = 'AUTO'
					payload["comparisons"][0]["threshold_sensitivity"] = input_dict[full_table_id]['sensitivity'].upper()

				for monitor in response:
					if monitor.rule_comparisons[0].full_table_id == full_table_id:
						payload["description"] = monitor.description
						payload["custom_rule_uuid"] = monitor.uuid
						break

				if not payload.get("description"):
					payload["description"] = f"Freshness rule for {full_table_id}"

				mutation = Mutation()
				mutation.create_or_update_freshness_custom_rule(**payload)
				try:
					self.progress_bar.update(self.progress_bar.tasks[0].id, advance=75 / len(input_fulltableids))
					_ = self.auth.client(mutation).create_or_update_freshness_custom_rule
					LOGGER.info(f"freshness threshold updated successfully for table {full_table_id}")
				except Exception as e:
					LOGGER.error(f"unable to update freshness threshold for table {full_table_id}")
					LOGGER.debug(e)
					continue


def main(*args, **kwargs):
	# Capture Command Line Arguments
	formatter = lambda prog: argparse.RawTextHelpFormatter(prog, max_help_position=120)
	parser = argparse.ArgumentParser(description="\n[ SET FRESHNESS SENSITIVITY ]\n\n\t• If updating to an explicit "
	                                             "threshold, create a CSV with 3 columns: [full_table_id,cron,updated_"
	                                             "in_last_minutes]\n\t• If updating to an automatic threshold, create a "
	                                             "CSV with 2 columns: [full_table_id,sensitivity]. Sensitivity must be "
	                                             "one of LOW, MEDIUM or HIGH.".expandtabs(4), formatter_class=formatter)
	parser._optionals.title = "Options"
	parser._positionals.title = "Commands"
	m = ''

	parser.add_argument('--profile', '-p', required=False, default="default",
	                    help='Specify an MCD profile name. Uses default otherwise', metavar=m)
	parser.add_argument('--input-file', '-i', required=True,
	                    help='Relative or absolute path to csv file containing freshness monitor configuration',
	                    metavar=m)
	parser.add_argument('--warehouse', '-w', required=True, help='Warehouse ID', metavar=m)

	if not args[0]:
		args = parser.parse_args(*args, **kwargs)
	else:
		sdk_helpers.dump_help(parser, main, *args)
		args = parser.parse_args(*args, **kwargs)

	# Initialize variables
	profile = args.profile
	input_file = args.input_file
	dw_id = args.warehouse

	# Initialize Util
	with (Progress() as progress):
		try:
			task = progress.add_task("[dark_orange3][RUNNING]...", total=100)
			LogRotater.rotate_logs(retention_period=7)
			progress.update(task, advance=25)
			LOGGER.info(f"running utility using '{args.profile}' profile")
			util = SetFreshnessSensitivity(profile, progress=progress)
			util.update_freshness_thresholds(util.validate_input_file(input_file), dw_id)
		except Exception as e:
			LOGGER.error(e, exc_info=False)
			print(traceback.format_exc())
		finally:
			progress.update(task, description="[dodger_blue2 bold][COMPLETE]", advance=100)


if __name__ == '__main__':
	main()