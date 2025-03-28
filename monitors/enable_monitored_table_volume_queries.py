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
			operation(str): Enable/Disable operation
		"""

		super().__init__(profile,  config_file)
		self.enabled = True

	def enable_monitored_table_volume_queries(self,operation):
		""" Enables query-based volume monitoring for tables that have monitoring enabled."""

		tables_to_enable = []
		next_token=None
		while True:
			tables = self.auth.client(self.get_tables(is_monitored=True, after=next_token)).get_tables
			for table in tables.edges:
				if not table.node.table_capabilities.has_non_metadata_size_collection:
					tables_to_enable.append(table.node.mcon)
			if tables.page_info.has_next_page:
				next_token = tables.page_info.end_cursor
			else:
				break

		for table in tables_to_enable:
			operation = operation
			op = ""
			if operation == "enable":
				op = True
			if operation == "disable":
				op = False
			response = self.auth.client(self.toggle_size_collection(mcon=table,enabled=op)).__fields__("enabled")
			if response.toggle_size_collection.enabled:
				LOGGER.info(f"row count {operation} for mcon[{table}]")
			else:
				LOGGER.error(f"unable to apply {operation.lower()} action on mcon[{table}]")
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
		util.enable_monitored_table_volume_queries(args.operation)

	util = RowCountMonitoring(args.profile)
	run_utility(util, args)


if __name__ == '__main__':
	main()
