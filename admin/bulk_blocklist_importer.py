# Instructions:
# 1. Run this script: python admin/bulk_blocklist_importer.py -p <profile> -i <input_file>
# 2. Profile should be configured in ~/.mcd/profiles.ini
#
# Input CSV format (with header):
#   resource_id,target_object_type,match_type,dataset,project,effect
#   abc123-uuid,dataset,exact_match,my_dataset,my_project,block
#
# Notes:
# - resource_id: The warehouse/connection UUID
# - target_object_type: dataset, project, schema, table (lowercase)
# - match_type: exact_match or wildcard
# - dataset: Dataset/database name
# - project: Project name (required for dataset blocks as parent)
# - effect: block or allow

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import csv
from collections import defaultdict
from admin import *
from lib.helpers import sdk_helpers

# Initialize logger
util_name = os.path.basename(__file__).split('.')[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))


class BulkBlocklistImporter(Admin):

	def __init__(self, profile, config_file: str = None, progress: Progress = None):
		"""Creates an instance of BulkBlocklistImporter.

		Args:
			profile(str): Profile to use stored in montecarlo cli.
			config_file (str): Path to the Configuration File.
			progress(Progress): Progress bar.
		"""
		super().__init__(profile, config_file, progress)
		self.progress_bar = progress

	@staticmethod
	def validate_input_file(input_file: str):
		"""Validate input CSV file and return grouped entries.

		Args:
			input_file (str): Path to input CSV file.

		Returns:
			dict: Entries grouped by (resource_id, target_object_type).
		"""
		file = Path(input_file)

		if not file.is_file():
			LOGGER.error("Invalid input file")
			sys.exit(1)

		entries_by_resource_and_type = defaultdict(list)
		with open(input_file, 'r') as csvfile:
			reader = csv.DictReader(csvfile)
			for row in reader:
				entry = {
					'resource_id': row['resource_id'].strip(),
					'target_object_type': row['target_object_type'].strip(),
					'match_type': row['match_type'].strip(),
					'dataset': row.get('dataset', '').strip() or None,
					'project': row.get('project', '').strip() or None,
					'effect': row.get('effect', '').strip() or None
				}
				# Group by both resource_id and target_object_type
				key = (entry['resource_id'], entry['target_object_type'])
				entries_by_resource_and_type[key].append(entry)

		total_entries = sum(len(entries) for entries in entries_by_resource_and_type.values())
		if total_entries == 0:
			LOGGER.error("No entries found in input file")
			sys.exit(1)

		return entries_by_resource_and_type

	def import_blocklist(self, entries_by_resource_and_type: dict):
		"""Import blocklist entries from validated data.

		Args:
			entries_by_resource_and_type (dict): Entries grouped by (resource_id, target_object_type).
		"""
		# Get existing blocklist entries for reference
		LOGGER.info("Fetching existing blocklist entries...")
		existing_entries = self.get_blocklist_entries()
		LOGGER.info(f"Found {len(existing_entries)} existing blocklist entries")

		# Create a set of existing entries for duplicate detection
		existing_keys = set()
		for entry in existing_entries:
			key = (
				entry['resource_id'],
				entry['target_object_type'],
				entry['match_type'],
				entry['dataset'],
				entry['project']
			)
			existing_keys.add(key)

		total_entries = sum(len(entries) for entries in entries_by_resource_and_type.values())
		LOGGER.info(f"Processing {total_entries} entries from CSV")
		LOGGER.info(f"Grouped into {len(entries_by_resource_and_type)} (resource, type) group(s)")

		success_count = 0
		skip_count = 0
		fail_count = 0

		for (resource_id, target_object_type), entries in entries_by_resource_and_type.items():
			# Filter entries - skip duplicates
			entries_to_process = []
			for entry in entries:
				entry_key = (
					entry['resource_id'],
					entry['target_object_type'],
					entry['match_type'],
					entry['dataset'] or '',
					entry['project'] or ''
				)

				if entry_key in existing_keys:
					LOGGER.info(f"  - SKIP (already exists): {entry['project'] or entry['dataset']} ({entry['target_object_type']})")
					skip_count += 1
					continue

				entries_to_process.append(entry)

			if not entries_to_process:
				continue

			try:
				response = self.modify_blocklist_entries(
					resource_id=resource_id,
					target_object_type=target_object_type,
					entries=entries_to_process
				)

				if response:
					for entry in entries_to_process:
						LOGGER.info(f"  - ADDED: {entry['project'] or entry['dataset']} ({entry['target_object_type']}) - SUCCESS")
						success_count += 1
						entry_key = (
							entry['resource_id'],
							entry['target_object_type'],
							entry['match_type'],
							entry['dataset'] or '',
							entry['project'] or ''
						)
						existing_keys.add(entry_key)
				else:
					for entry in entries_to_process:
						LOGGER.error(f"  - FAILED: {entry['project'] or entry['dataset']} ({entry['target_object_type']}) - no response")
						fail_count += 1

			except Exception as e:
				for entry in entries_to_process:
					LOGGER.error(f"  - FAILED: {entry['project'] or entry['dataset']} ({entry['target_object_type']}) - {e}")
					fail_count += 1

		LOGGER.info(f"Import complete:")
		LOGGER.info(f"  - Successful: {success_count}")
		LOGGER.info(f"  - Skipped: {skip_count}")
		LOGGER.info(f"  - Failed: {fail_count}")


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
		entries = util.validate_input_file(args.input_file)
		util.import_blocklist(entries)

	util = BulkBlocklistImporter(args.profile)
	run_utility(util, args)


if __name__ == "__main__":
	main()
