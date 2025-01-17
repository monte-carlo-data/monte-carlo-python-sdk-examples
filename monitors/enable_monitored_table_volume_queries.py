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
		""" Enables query-based volume monitoring for tables that have monitoring enabled.
		"""
		counter = 0
		tables_to_enable = []
		next_token=None
		while True:
			tables = self.auth.client(self.get_monitored_tables(after=next_token)).get_tables
			for table in tables.edges:
				# counter += 1
				# print(counter)
				if not table.node.table_capabilities.has_non_metadata_size_collection:
					tables_to_enable.append(table.node.mcon)
			if tables.page_info.has_next_page:
				next_token = tables.page_info.end_cursor
			else:
				break
			
		# print("finished count: " + str(counter))
			
			for table in tables_to_enable:
				operation = operation
				op = ""
				if operation == "enable":
					op = True
				if operation == "disable":
					op = False
				response = self.auth.client(self.toggle_size_collection(mcon=table,enabled=op)).__fields__("enabled")
				if response.toggle_size_collection.enabled:
					LOGGER.info(f"row count {operation} for mcon[{view}]".format(operation=operation, view=table))
				else:
					LOGGER.error(f"unable to apply {operation.lower()} action on mcon[{view}]".format(operation=operation, view=table))
					exit(1)

def main(*args, **kwargs):
	
	# Capture Command Line Arguments
	formatter = lambda prog: argparse.RawTextHelpFormatter(prog, max_help_position=120)
	parser = argparse.ArgumentParser(description="\n[ ENABLE/DISABLE ROW COUNT FOR ALL MONITORED TABLES ]\n\n\t"
												 " Adds all tables that are monitored to row count monitoring")
	parser._optionals.title = "Options"
	parser._positionals.title = "Commands"
	m = ''

	parser.add_argument('--profile', '-p', required=False, default="default",
						help='Specify an MCD profile name. Uses default otherwise')
	parser.add_argument('--operation', '-o', choices=['enable', 'disable'], required=False, default='enable',
						help='Enable/Disable tables under usage.')

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
		util = RowCountMonitoring(profile)
		if args.operation == 'disable':
			util.enabled = False
		else:
			util.enabled = True
		util.enable_monitored_table_volume_queries(args.operation)
	except Exception as e:
		LOGGER.error(e, exc_info=False)
		print(traceback.format_exc())
	finally:
		LOGGER.info('rotating old log files')
		LogRotater.rotate_logs(retention_period=7)

if __name__ == '__main__':
	main()
