import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from monitors import *

# Initialize logger
util_name = __file__.split('/')[-1].split('.')[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))


class UpdateComparisonMonitorDS(Monitors, Tables):

	def __init__(self, profile, config_file: str = None, progress: Progress = None):
		"""Creates an instance of UpdateComparisonMonitorDS.

		Args:
			profile(str): Profile to use stored in montecarlo test.
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

		file_path = Path(input_file)
		LOGGER.info(f"starting input file validation...")
		input_monitors = {}

		if file_path.is_file():
			required_cols = ['monitor_id', 'asset']
			try:
				with open(file_path, 'r', encoding='utf-8-sig') as file:
					reader = csv.DictReader(file)
					for index, row in enumerate(reader):
						col_count = len(row)
						if col_count == 2:
							input_monitors[row["monitor_id"]] = row["asset"]
						else:
							raise ValueError(f"{col_count} columns present in CSV, {required_cols} are required")

			except ValueError as e:
				LOGGER.error(f"errors found in file: {e}")

		return input_monitors

	def update_comparison_monitor_dynamic_schedule(
		self, input_dict: dict
	):

		if input_dict:
			LOGGER.info(f"updating comparison rules schedule...")
			now = datetime.now(timezone.utc).replace(microsecond=0)
			formatted_time = now.isoformat()
			for comp_monitor_uuid, target_table in input_dict.items():
				query = Query()
				get_tables = query.get_tables(full_table_id=target_table, first=1)
				get_tables.edges.node.__fields__("mcon")
				target_mcon = self.auth.client(query).get_tables.edges[0].node.mcon

				query = Query()
				get_custom_rule = query.get_custom_rule(rule_id=comp_monitor_uuid)
				get_custom_rule.__fields__(
					"uuid",
					"rule_type",
					"is_paused",
					"rule_name",
					"description",
					"is_deleted",
					"labels",
					"notes",
					"priority",
					"data_quality_dimension",
					"tags",
					"event_rollup_count",
					"event_rollup_until_changed",
					"failure_audiences",
					"query_result_type"
				)

				get_custom_rule.queries(first=2).edges.node.__fields__("name", "connection_uuid", "warehouse_uuid", "sql_query")
				get_custom_rule.comparisons.__fields__("comparison_type", "operator", "threshold", "is_threshold_relative")

				try:
					source_sql_query, target_sql_query, source_dw_id, target_dw_id, source_conn_id, target_conn_id = (
						"",
						"",
						"",
						"",
						"",
						""
					)
					comp_monitor = self.auth.client(query).get_custom_rule
					for edge in comp_monitor.queries.edges:
						if edge.node.name == "source":
							source_sql_query = edge.node.sql_query
							source_dw_id = edge.node.warehouse_uuid
							source_conn_id = edge.node.connection_uuid
						elif edge.node.name == "target":
							target_sql_query = edge.node.sql_query
							target_dw_id = edge.node.warehouse_uuid
							target_conn_id = edge.node.connection_uuid
					comp_monitor_update_payload = {
						"query_result_type": comp_monitor.query_result_type,
						"custom_rule_uuid": comp_monitor.uuid,
						"source_sql_query": source_sql_query,
						"target_sql_query": target_sql_query,
						"source_dw_id": source_dw_id,
						"target_dw_id": target_dw_id,
						"source_connection_id": source_conn_id,
						"target_connection_id": target_conn_id,
						"comparisons": [
							{
								"comparison_type": comp_monitor.comparisons[
									0
								].comparison_type,
								"operator": comp_monitor.comparisons[0].operator,
								"is_threshold_relative": comp_monitor.comparisons[
									0
								].is_threshold_relative,
								"threshold": comp_monitor.comparisons[0].threshold,
							}
						],
						"description": comp_monitor.description,
						"notes": comp_monitor.notes,
						"labels": comp_monitor.labels,
						"failure_audiences": comp_monitor.failure_audiences,
						"tags": [
							{"name": tag.name, "value": tag.value}
							for tag in comp_monitor.tags
						],
						"priority": comp_monitor.priority,
						"data_quality_dimension": comp_monitor.data_quality_dimension,
						"schedule_config": {
							"dynamic_schedule_mcons": [target_mcon],
							"schedule_type": "DYNAMIC",
							"start_time": formatted_time,
						},
						"event_rollup_until_changed": comp_monitor.event_rollup_until_changed,
						"event_rollup_count": comp_monitor.event_rollup_count,
					}

					filtered_data = {k: v for k, v in comp_monitor_update_payload.items() if v}
					mutation = Mutation()
					mutation.create_or_update_comparison_rule(**filtered_data)
					LOGGER.debug(filtered_data)

					self.progress_bar.update(self.progress_bar.tasks[0].id, advance=75 / len(input_dict))
					_ = self.auth.client(mutation).create_or_update_comparison_rule
					LOGGER.info(f"comparison monitor dynamic schedule updated successfully for monitor: {comp_monitor_uuid} - {comp_monitor.description}")
				except Exception as e:
					LOGGER.error(f"unable to update comparison monitor dynamic schedule for monitor uuid: {comp_monitor_uuid}")
					LOGGER.error(e)
					continue


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
		util.update_comparison_monitor_dynamic_schedule(util.validate_input_file(args.input_file))

	util = UpdateComparisonMonitorDS(args.profile)
	run_utility(util, args)


if __name__ == '__main__':
	main()
