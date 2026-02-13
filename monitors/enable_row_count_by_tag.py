import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from monitors import *
from pycarlo.core import Query

# Initialize logger
util_name = os.path.splitext(os.path.basename(__file__))[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))
LOGGER = logging.getLogger()


class RowCountMonitoringByTag(Monitors, Tables, Admin):

	def __init__(self, profile, config_file: str = None):
		"""Creates an instance of RowCountMonitoringByTag.

		Args:
			profile (str): MCD profile name.
			config_file (str, optional): Path to config file.
		"""
		super().__init__(profile, config_file)
		self.enabled = True

	def _get_tables_by_tag(self, tag_key: str, tag_value: str):
		"""Returns MCONs of tables that have the given tag key and value."""
		tag_filters = [{"tag_name": tag_key, "tag_values": [tag_value]}]
		query = Query()
		query.search(query="", tag_filters=tag_filters)
		response = self.auth.client(query).search
		mcons = []
		if response.results:
			for result in response.results:
				mcons.append(result.mcon)
		return mcons

	def enable_row_count_for_tagged_tables(self, tag_key: str, tag_value: str, operation: str):
		"""Enables or disables row count (query-based volume) monitoring for tables with the given tag."""
		mcons = self._get_tables_by_tag(tag_key, tag_value)
		if not mcons:
			LOGGER.warning(f"No tables found with tag {tag_key}={tag_value}")
			return

		LOGGER.info(f"Found {len(mcons)} table(s) with tag {tag_key}={tag_value}")
		op = operation == "enable"

		for mcon in mcons:
			response = self.auth.client(self.toggle_size_collection(mcon=mcon, enabled=op)).__fields__("enabled")
			if response.toggle_size_collection.enabled == op:
				LOGGER.info(f"Row count {operation}d for mcon[{mcon}]")
			else:
				LOGGER.error(f"Unable to apply {operation} action on mcon[{mcon}]")
				exit(1)


def main(*args, **kwargs):
	# Capture Command Line Arguments
	parser = sdk_helpers.generate_arg_parser(
		os.path.basename(os.path.dirname(os.path.abspath(__file__))),
		os.path.basename(__file__)
	)

	if not args:
		args = parser.parse_args(*args, **kwargs)
	else:
		sdk_helpers.dump_help(parser, main, *args)
		args = parser.parse_args(*args, **kwargs)

	@sdk_helpers.ensure_progress
	def run_utility(progress, util, args):
		util.progress_bar = progress
		if args.operation == "disable":
			util.enabled = False
		else:
			util.enabled = True
		util.enable_row_count_for_tagged_tables(args.key, args.value, args.operation)

	util = RowCountMonitoringByTag(args.profile)
	run_utility(util, args)


if __name__ == "__main__":
	main()
