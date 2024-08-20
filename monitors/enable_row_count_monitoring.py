import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from monitors import *

# Initialize logger
util_name = __file__.split('/')[-1].split('.')[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))
LOGGER = logging.getLogger()


class RowCountMonitoring(Monitors, Tables, Admin):

	def __init__(self, profile, config_file: str = None):
		"""Creates an instance of RowCountMonitoring.

		Args:
			config_file (str): Path to the Configuration File.
		"""

		super().__init__(profile,  config_file)
		self.enabled = True

	def create_rules(self, input_file: str) -> dict:
		""" Reads input file and creates rules dictionary

		Args:
			input_file(str): Path of the file containing asset entries.

		Returns:
			dict: Rule dictionary configuration.
		
		"""

		if Path(input_file).is_file():
			mapping = {}
			with open(input_file, 'r') as input_tables:
				for table in input_tables:
					table_filter, table_name = table.split('.')
					content = {'project': table_filter[:table_filter.index(":")],
							   'dataset': table_filter[table_filter.index(":") + 1:],
							   'rules': []}
					if not mapping.get(table_filter):
						mapping[table_filter] = content

					if self.enabled:
						rule = {
							"isExclude": False,
							"ruleType": "wildcard_pattern",
							"tableRuleAttribute": "table_id",
							"tableRuleText": table_name
						}
						mapping[table_filter]['rules'].append(rule)

			return mapping

		else:
			LOGGER.error(f"unable to locate input file: {input_file}")
			sys.exit(1)

	def apply_rules(self, operation: str, warehouse_id: str, rule_configs: dict):
		""" Submits rules for processing only if # of rules is <= 100

		Args:
			operation(str): Enable or Disable.
			warehouse_id(str): UUID of warehouse.
			rule_configs(dict): Dictionary containing rule configuration.
		"""

		for db_schema in rule_configs:
			project = rule_configs[db_schema]['project']
			dataset = rule_configs[db_schema]['dataset']
			rules = rule_configs[db_schema]['rules']
			if len(rules) > 100:
				LOGGER.error("monitor rules allow at most 100 entries. Use a different method to filter out tables i.e."
				             " pattern match")
				exit(0)
			LOGGER.info(f"{operation.title()} usage for database/schema combination "
			            f"[{project}:{dataset}] and warehouse [{warehouse_id}]...")
			response = (self.auth.client(self.enable_schema_usage(dw_id=warehouse_id, project=project,
			                                                            dataset=dataset, rules=rules))
			            .update_monitored_table_rule_list)
			if isinstance(response, list):
				LOGGER.info(f"monitor rule {operation}d\n")
			else:
				LOGGER.error("an error occurred")
				exit(1)

			if self.enabled:
				LOGGER.info(f"retrieving monitored tables matching [{project}:{dataset}] and warehouse "
				            f"[{warehouse_id}]...")
				view_mcons = []
				cursor = None
				while True:
					response = (self.auth.client(self.get_tables(dw_id=warehouse_id,
					                                                    search=f"{project}:{dataset}",
					                                                    after=cursor)).get_tables)
					for table in response.edges:
						if table.node.table_type in ['VIEW', 'EXTERNAL']:
							view_mcons.append(table.node.mcon)
					if response.page_info.has_next_page:
						cursor = response.page_info.end_cursor
					else:
						break
				LOGGER.info(f"assets identified\n")

				LOGGER.info(f"{operation.title()} row count monitoring for views under [{project}:{dataset}] and "
				            f"warehouse [{warehouse_id}]...")
				for view in view_mcons:
					response = self.auth.client(self.enable_row_count(),
					                            variables={"mcon": view, "enabled": self.enabled})
					if response.toggle_size_collection.enabled:
						LOGGER.info(f"row count {operation}d for mcon[{view}]")
					else:
						LOGGER.error(f"unable to apply {operation.lower()} action")
						exit(1)


def main(*args, **kwargs):
	
	# Capture Command Line Arguments
	formatter = lambda prog: argparse.RawTextHelpFormatter(prog, max_help_position=120)
	parser = argparse.ArgumentParser(description="\n[ BULK ENABLE ROW COUNT MONITORING ]\n\n\tAdds or removes tables"
												 " from usage settings. It also enables row count monitoring for views"
												 "/external tables.\n\n\t•NOTE: When 'disable', all rules under the "
	                                             "schema will be removed.".expandtabs(4), formatter_class=formatter)
	parser._optionals.title = "Options"
	parser._positionals.title = "Commands"
	m = ''

	parser.add_argument('--profile', '-p', required=True, default="default",
						help='Specify an MCD profile name. Uses default otherwise')
	parser.add_argument('--warehouse', '-w', required=True,
						help='Warehouse ID')
	parser.add_argument('--input', '-i', required=True,
						help='Path to the txt file containing list of full table ids.')
	parser.add_argument('--operation', '-o', choices=['enable', 'disable'], required=False, default='enable',
						help='Enable/Disable tables under usage.')

	if not args:
		args = parser.parse_args(*args, **kwargs)
	else:
		sdk_helpers.dump_help(parser, main, *args)
		args = parser.parse_args(*args, **kwargs)

	# Initialize variables
	profile = args.profile
	warehouse_id = args.warehouse
	input_file = args.input

	# Initialize Util and run actions
	try:
		LOGGER.info(f"running utility using '{args.profile}' profile")
		util = RowCountMonitoring(profile)
		if args.operation == 'disable':
			util.enabled = False
		else:
			util.enabled = True
		util.apply_rules(args.operation, warehouse_id, util.create_rules(input_file))
	except Exception as e:
		LOGGER.error(e, exc_info=False)
		print(traceback.format_exc())
	finally:
		LOGGER.info('rotating old log files')
		LogRotater.rotate_logs(retention_period=7)


if __name__ == '__main__':
	main()
