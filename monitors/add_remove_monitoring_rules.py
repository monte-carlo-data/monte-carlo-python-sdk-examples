import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from monitors import *

# Initialize logger
util_name = __file__.split('/')[-1].split('.')[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))
LOGGER = logging.getLogger()


class MonitoringRules(Monitors, Tables, Admin):

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


def main(*args, **kwargs):
	
	# Capture Command Line Arguments
	parser, subparsers = sdk_helpers.generate_arg_parser(os.path.basename(os.path.dirname(os.path.abspath(__file__)))
	                                                     , os.path.basename(__file__))

	if not args:
		args = parser.parse_args(*args, **kwargs)
	else:
		sdk_helpers.dump_help(parser, main, *args)
		args = parser.parse_args(*args, **kwargs)

	@sdk_helpers.ensure_progress
	def run_utility(progress, util, args):
		util.progress_bar = progress
		if args.operation == 'disable':
			util.enabled = False
		else:
			util.enabled = True
		util.apply_rules(args.operation, args.warehouse, util.create_rules(args.input))

	util = MonitoringRules(args.profile)
	run_utility(util, args)


if __name__ == '__main__':
	main()
