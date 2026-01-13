# Instructions:
# 1. Run this script: python admin/bulk_exclusion_window_exporter.py -p <profile> -o <output_file>
# 2. Profile should be configured in configs/configs.ini
#
# Output CSV format:
#   id,resource_uuid,scope,database,dataset,full_table_id,start_time,end_time,reason,reason_type
#
# The 'scope' column indicates the level of the exclusion window:
#   - WAREHOUSE: Applies to entire warehouse (database/dataset/full_table_id are empty)
#   - DATABASE: Applies to specific database
#   - SCHEMA: Applies to specific schema (dataset)
#   - TABLE: Applies to specific table
#
# Note: The output CSV can be used with bulk_exclusion_window_importer.py to migrate
#       exclusion windows (data maintenance entries) to another workspace.

"""
Bulk Exclusion Window Exporter - Export all data maintenance entries to CSV.

Uses getDataMaintenanceEntries GraphQL query to fetch all exclusion windows
from the Monte Carlo environment.
"""

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import csv
from admin import *
from lib.helpers import sdk_helpers
from pycarlo.core import Query

# Initialize logger
util_name = os.path.basename(__file__).split('.')[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))


class BulkExclusionWindowExporter(Admin):
	"""Exporter for data maintenance entries (exclusion windows).

	Fetches all exclusion windows from Monte Carlo and exports them to CSV format.
	"""

	def __init__(self, profile, config_file: str = None, progress: Progress = None):
		"""Creates an instance of BulkExclusionWindowExporter.

		Args:
			profile (str): Profile to use stored in configs.ini.
			config_file (str): Path to the Configuration File.
			progress (Progress): Progress bar.
		"""
		super().__init__(profile, config_file, progress)
		self.progress_bar = progress

	def get_all_exclusion_windows(self) -> list:
		"""Fetch all data maintenance entries from Monte Carlo.

		Returns structured data that can be used by other tools (like the migration module)
		without requiring file I/O.

		Note: The getDataMaintenanceEntries API returns a direct list, not a paginated
		connection pattern.

		Returns:
			list[dict]: List of exclusion window dictionaries with keys:
				- id (int): Unique identifier
				- resource_uuid (str): Warehouse/connection UUID
				- scope (str): Level of exclusion (WAREHOUSE, DATABASE, SCHEMA, TABLE)
				- database (str): Database name (optional)
				- dataset (str): Dataset/schema name (optional)
				- full_table_id (str): Full table identifier (optional)
				- start_time (str): ISO timestamp of window start
				- end_time (str): ISO timestamp of window end
				- reason (str): Reason/description for the maintenance window
				- reason_type (str): Type of reason (e.g., CUSTOM)
		"""
		LOGGER.info("Fetching exclusion windows (data maintenance entries)...")
		entries = []

		# getDataMaintenanceEntries returns a list directly, not a connection
		query = Query()
		get_entries = query.get_data_maintenance_entries()
		get_entries.__fields__(
			"id", "resource_uuid", "database", "dataset", "full_table_id",
			"start_time", "end_time", "reason", "reason_type"
		)

		response = self.auth.client(query).get_data_maintenance_entries

		if response:
			for entry in response:
				# Extract field values
				database = getattr(entry, 'database', None) or ''
				dataset = getattr(entry, 'dataset', None) or ''
				full_table_id = getattr(entry, 'full_table_id', None) or ''

				# Derive scope based on which fields are populated
				scope = self._derive_scope(database, dataset, full_table_id)

				entries.append({
					'id': entry.id,
					'resource_uuid': getattr(entry, 'resource_uuid', '') or '',
					'scope': scope,
					'database': database,
					'dataset': dataset,
					'full_table_id': full_table_id,
					'start_time': str(getattr(entry, 'start_time', '') or ''),
					'end_time': str(getattr(entry, 'end_time', '') or ''),
					'reason': getattr(entry, 'reason', '') or '',
					'reason_type': getattr(entry, 'reason_type', '') or ''
				})

		LOGGER.info(f"Found {len(entries)} exclusion windows")
		return entries

	@staticmethod
	def _derive_scope(database: str, dataset: str, full_table_id: str) -> str:
		"""Derive the scope level based on which fields are populated.

		Args:
			database (str): Database name
			dataset (str): Dataset/schema name
			full_table_id (str): Full table identifier

		Returns:
			str: Scope level (WAREHOUSE, DATABASE, SCHEMA, or TABLE)
		"""
		if full_table_id:
			return 'TABLE'
		elif dataset:
			return 'SCHEMA'
		elif database:
			return 'DATABASE'
		else:
			return 'WAREHOUSE'

	def export_exclusion_windows(self, output_file: str):
		"""Export all exclusion windows to CSV.

		Args:
			output_file (str): Path to output CSV file.
		"""
		# Use the shared method to fetch data
		entries = self.get_all_exclusion_windows()

		if not entries:
			LOGGER.info("No exclusion windows found.")
			# Still write header for consistency
			with open(output_file, 'w', newline='') as csvfile:
				writer = csv.writer(csvfile)
				writer.writerow([
					'id', 'resource_uuid', 'scope', 'database', 'dataset', 'full_table_id',
					'start_time', 'end_time', 'reason', 'reason_type'
				])
			return

		with open(output_file, 'w', newline='') as csvfile:
			writer = csv.writer(csvfile)
			writer.writerow([
				'id', 'resource_uuid', 'scope', 'database', 'dataset', 'full_table_id',
				'start_time', 'end_time', 'reason', 'reason_type'
			])

			for entry in entries:
				writer.writerow([
					entry['id'],
					entry['resource_uuid'],
					entry['scope'],
					entry['database'],
					entry['dataset'],
					entry['full_table_id'],
					entry['start_time'],
					entry['end_time'],
					entry['reason'],
					entry['reason_type']
				])

		LOGGER.info(f"Export complete: {output_file} ({len(entries)} rows)")


def main(*args, **kwargs):
	"""Main entry point for the bulk exclusion window exporter."""

	# Capture Command Line Arguments
	parser = sdk_helpers.generate_arg_parser(
		os.path.basename(os.path.dirname(os.path.abspath(__file__))),
		os.path.basename(__file__)
	)

	if not args:
		args = parser.parse_args(*args, **kwargs)
	else:
		sdk_helpers.dump_help(parser, main, *args)

	@sdk_helpers.ensure_progress
	def run_utility(progress, util, args):
		util.progress_bar = progress
		util.export_exclusion_windows(args.output_file)

	util = BulkExclusionWindowExporter(args.profile)
	run_utility(util, args)


if __name__ == "__main__":
	main()

