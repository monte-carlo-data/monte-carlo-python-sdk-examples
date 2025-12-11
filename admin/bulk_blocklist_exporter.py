# Instructions:
# 1. Run this script: python admin/bulk_blocklist_exporter.py -p <profile> -o <output_file>
# 2. Profile should be configured in ~/.mcd/profiles.ini
#
# Output CSV format:
#   resource_id,target_object_type,match_type,dataset,project,effect
#   abc123-uuid,TABLE,EXACT,my_dataset,my_project,BLOCK
#
# Note: The output CSV can be used with bulk_blocklist_importer.py to migrate blocklist entries to another workspace.

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import csv
from admin import *
from lib.helpers import sdk_helpers

# Initialize logger
util_name = os.path.basename(__file__).split('.')[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))


class BulkBlocklistExporter(Admin):

	def __init__(self, profile, config_file: str = None, progress: Progress = None):
		"""Creates an instance of BulkBlocklistExporter.

		Args:
			profile(str): Profile to use stored in montecarlo cli.
			config_file (str): Path to the Configuration File.
			progress(Progress): Progress bar.
		"""
		super().__init__(profile, config_file, progress)
		self.progress_bar = progress

	def export_blocklist(self, output_file: str):
		"""Export all blocklist entries to CSV.

		Args:
			output_file (str): Path to output CSV file.
		"""
		LOGGER.info("Fetching blocklist entries...")
		entries = self.get_blocklist_entries()
		LOGGER.info(f"Found {len(entries)} blocklist entries")

		if not entries:
			LOGGER.info("No blocklist entries found.")
			return

		with open(output_file, 'w', newline='') as csvfile:
			writer = csv.writer(csvfile)
			writer.writerow(['resource_id', 'target_object_type', 'match_type', 'dataset', 'project', 'effect'])

			for entry in entries:
				writer.writerow([
					entry['resource_id'],
					entry['target_object_type'],
					entry['match_type'],
					entry['dataset'],
					entry['project'],
					entry['effect']
				])

		LOGGER.info(f"Export complete: {output_file} ({len(entries)} rows)")


def main(*args, **kwargs):

	# Capture Command Line Arguments
	parser = sdk_helpers.generate_arg_parser(os.path.basename(os.path.dirname(os.path.abspath(__file__))),
											 os.path.basename(__file__))

	if not args:
		args = parser.parse_args(*args, **kwargs)
	else:
		sdk_helpers.dump_help(parser, main, *args)

	@sdk_helpers.ensure_progress
	def run_utility(progress, util, args):
		util.progress_bar = progress
		util.export_blocklist(args.output_file)

	util = BulkBlocklistExporter(args.profile)
	run_utility(util, args)


if __name__ == "__main__":
	main()
