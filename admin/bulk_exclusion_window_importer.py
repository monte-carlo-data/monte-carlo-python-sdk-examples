# Instructions:
# 1. Run this script: python admin/bulk_exclusion_window_importer.py -p <profile> -i <input_file>
# 2. Profile should be configured in configs/configs.ini
#
# Input CSV format:
#   id,resource_uuid,scope,database,dataset,full_table_id,start_time,end_time,reason,reason_type
#
# Note: This script creates or updates exclusion windows (data maintenance entries)
#       using the createOrUpdateDataMaintenanceEntry mutation.

"""
Bulk Exclusion Window Importer - Import data maintenance entries from CSV.

Uses createOrUpdateDataMaintenanceEntry GraphQL mutation to create exclusion windows.
For table-level entries, it looks up the MCON from the full_table_id.
"""

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import csv
from datetime import datetime
from admin import *
from lib.helpers import sdk_helpers
from pycarlo.core import Query, Mutation

# Initialize logger
util_name = os.path.basename(__file__).split('.')[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))


class BulkExclusionWindowImporter(Admin):
	"""Importer for data maintenance entries (exclusion windows).

	Creates or updates exclusion windows from CSV data using the
	createOrUpdateDataMaintenanceEntry mutation.
	"""

	def __init__(self, profile, config_file: str = None, progress: Progress = None):
		"""Creates an instance of BulkExclusionWindowImporter.

		Args:
			profile (str): Profile to use stored in configs.ini.
			config_file (str): Path to the Configuration File.
			progress (Progress): Progress bar.
		"""
		super().__init__(profile, config_file, progress)
		self.progress_bar = progress
		self._mcon_cache = {}  # Cache for full_table_id -> mcon lookups

	def get_existing_exclusion_windows_map(self) -> dict:
		"""Get a map of existing exclusion windows for duplicate detection.

		Returns:
			dict: Map keyed by (resource_uuid, full_table_id, start_time, end_time)
				  with values being the entry dict.
		"""
		from admin.bulk_exclusion_window_exporter import BulkExclusionWindowExporter
		exporter = BulkExclusionWindowExporter(self.profile, progress=self.progress_bar)
		entries = exporter.get_all_exclusion_windows()

		# Create map keyed by unique combination
		entry_map = {}
		for entry in entries:
			key = (
				entry['resource_uuid'],
				entry['full_table_id'],
				entry['start_time'],
				entry['end_time']
			)
			entry_map[key] = entry

		return entry_map

	def parse_exclusion_window_csv(self, input_file: str) -> dict:
		"""Parse exclusion window CSV file.

		Args:
			input_file (str): Path to CSV file.

		Returns:
			dict: Parsing result with keys:
				- success (bool)
				- windows (list): Parsed window entries
				- count (int): Number of entries
				- errors (list): Any parsing errors
		"""
		windows = []
		errors = []

		try:
			with open(input_file, 'r', newline='') as csvfile:
				reader = csv.DictReader(csvfile)

				# Validate header - required columns
				required = {'resource_uuid', 'start_time', 'end_time'}
				if not required.issubset(set(reader.fieldnames or [])):
					missing = required - set(reader.fieldnames or [])
					errors.append(f"Missing required columns: {missing}")
					return {'success': False, 'windows': [], 'count': 0, 'errors': errors}

				for row_num, row in enumerate(reader, start=2):
					if not row.get('resource_uuid'):
						errors.append(f"Row {row_num}: Missing resource_uuid")
						continue
					if not row.get('start_time'):
						errors.append(f"Row {row_num}: Missing start_time")
						continue
					if not row.get('end_time'):
						errors.append(f"Row {row_num}: Missing end_time")
						continue

					windows.append({
						'id': row.get('id', ''),
						'resource_uuid': row['resource_uuid'],
						'scope': row.get('scope', ''),
						'database': row.get('database', ''),
						'dataset': row.get('dataset', ''),
						'full_table_id': row.get('full_table_id', ''),
						'start_time': row['start_time'],
						'end_time': row['end_time'],
						'reason': row.get('reason', ''),
						'reason_type': row.get('reason_type', 'CUSTOM')
					})

			return {
				'success': len(errors) == 0,
				'windows': windows,
				'count': len(windows),
				'errors': errors
			}

		except Exception as e:
			errors.append(f"Failed to parse CSV: {e}")
			return {'success': False, 'windows': [], 'count': 0, 'errors': errors}

	def lookup_mcon(self, warehouse_id: str, full_table_id: str) -> str:
		"""Look up the MCON for a table given its full_table_id.

		Args:
			warehouse_id (str): The warehouse UUID.
			full_table_id (str): The full table identifier (e.g., 'db:schema.table').

		Returns:
			str: The MCON if found, empty string otherwise.
		"""
		# Check cache first
		cache_key = (warehouse_id, full_table_id)
		if cache_key in self._mcon_cache:
			return self._mcon_cache[cache_key]

		try:
			# Parse full_table_id to extract search terms
			# Format: database:schema.table or database:schema.table
			query = Query()
			get_tables = query.get_tables(
				dw_id=warehouse_id,
				search=full_table_id,
				first=10,
				is_deleted=False
			)
			get_tables.edges.node.__fields__("full_table_id", "mcon")

			response = self.auth.client(query).get_tables

			if response and response.edges:
				for edge in response.edges:
					if edge.node.full_table_id == full_table_id:
						mcon = edge.node.mcon
						self._mcon_cache[cache_key] = mcon
						return mcon

			LOGGER.warning(f"Could not find MCON for {full_table_id}")
			self._mcon_cache[cache_key] = ''
			return ''

		except Exception as e:
			LOGGER.error(f"Error looking up MCON for {full_table_id}: {e}")
			return ''

	def _convert_to_iso8601(self, datetime_str: str) -> str:
		"""Convert datetime string to ISO 8601 format.

		The API requires format: YYYY-MM-DDTHH:MM:SS+00:00
		But exported format may be: YYYY-MM-DD HH:MM:SS+00:00 (with space)

		Args:
			datetime_str (str): Datetime string to convert.

		Returns:
			str: ISO 8601 formatted datetime string.
		"""
		if not datetime_str:
			return datetime_str
		# Replace space with 'T' for ISO 8601 compliance
		return datetime_str.replace(' ', 'T')

	def create_exclusion_window(self, window: dict) -> dict:
		"""Create a single exclusion window.

		Args:
			window (dict): Window data with resource_uuid, start_time, end_time, etc.

		Returns:
			dict: Result with success, id, error keys.
		"""
		try:
			# Build mutation parameters - convert datetime format
			params = {
				'dw_id': window['resource_uuid'],
				'start_time': self._convert_to_iso8601(window['start_time']),
				'end_time': self._convert_to_iso8601(window['end_time'])
			}

			# Add reason if provided
			if window.get('reason'):
				params['reason'] = window['reason']

			# For table-level entries, look up the MCON
			if window.get('full_table_id'):
				mcon = self.lookup_mcon(window['resource_uuid'], window['full_table_id'])
				if mcon:
					params['mcon'] = mcon
				else:
					return {
						'success': False,
						'id': None,
						'error': f"Could not find MCON for table {window['full_table_id']}"
					}

			# Execute mutation
			mutation = Mutation()
			create_entry = mutation.create_or_update_data_maintenance_entry(**params)
			create_entry.entry.__fields__('id', 'resource_uuid')

			response = self.auth.client(mutation).create_or_update_data_maintenance_entry

			if response and response.entry:
				return {
					'success': True,
					'id': response.entry.id,
					'error': None
				}
			else:
				return {
					'success': False,
					'id': None,
					'error': 'No response from mutation'
				}

		except Exception as e:
			return {
				'success': False,
				'id': None,
				'error': str(e)
			}

	def import_exclusion_windows(self, input_file: str, dry_run: bool = False):
		"""Import exclusion windows from CSV file.

		Args:
			input_file (str): Path to input CSV file.
			dry_run (bool): If True, only validate without creating entries.
		"""
		mode = "DRY-RUN" if dry_run else "COMMIT"
		LOGGER.info(f"Starting import ({mode}) from {input_file}")

		# Parse CSV
		parse_result = self.parse_exclusion_window_csv(input_file)
		if not parse_result['success']:
			for error in parse_result['errors']:
				LOGGER.error(f"Parse error: {error}")
			return

		windows = parse_result['windows']
		LOGGER.info(f"Parsed {len(windows)} exclusion windows from CSV")

		if not windows:
			LOGGER.info("No windows to import")
			return

		# Get existing windows for duplicate detection
		LOGGER.info("Fetching existing exclusion windows...")
		existing_map = self.get_existing_exclusion_windows_map()
		LOGGER.info(f"Found {len(existing_map)} existing windows")

		# Process windows
		created = 0
		skipped = 0
		failed = 0

		for window in windows:
			# Check for duplicates
			key = (
				window['resource_uuid'],
				window['full_table_id'],
				window['start_time'],
				window['end_time']
			)

			if key in existing_map:
				LOGGER.info(f"SKIP (exists): {window['scope']} window on {window['resource_uuid'][:8]}... ({window['start_time']} - {window['end_time']})")
				skipped += 1
				continue

			scope_desc = window['scope'] or 'WAREHOUSE'
			target = window['full_table_id'] or window['resource_uuid'][:8] + '...'

			if dry_run:
				LOGGER.info(f"WOULD CREATE: {scope_desc} window on {target} ({window['start_time']} - {window['end_time']})")
				created += 1
			else:
				result = self.create_exclusion_window(window)
				if result['success']:
					LOGGER.info(f"CREATED: {scope_desc} window (id={result['id']}) on {target}")
					created += 1
					# Add to existing map to prevent duplicates in same run
					existing_map[key] = window
				else:
					LOGGER.error(f"FAILED: {scope_desc} window on {target} - {result['error']}")
					failed += 1

		# Summary
		LOGGER.info("=" * 50)
		LOGGER.info(f"Import {mode} complete:")
		LOGGER.info(f"  - Created: {created}")
		LOGGER.info(f"  - Skipped: {skipped}")
		LOGGER.info(f"  - Failed: {failed}")

		if dry_run and created > 0:
			LOGGER.info("")
			LOGGER.info("This was a DRY-RUN. No changes were made.")


def main(*args, **kwargs):
	"""Main entry point for the bulk exclusion window importer."""

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
		# Execute directly (no dry-run for admin scripts - dry-run is handled at migration level)
		util.import_exclusion_windows(args.input_file, dry_run=False)

	util = BulkExclusionWindowImporter(args.profile)
	run_utility(util, args)


if __name__ == "__main__":
	main()

