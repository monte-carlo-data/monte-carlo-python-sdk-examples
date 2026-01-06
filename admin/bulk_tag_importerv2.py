# Instructions:
# 1. Run this script: python admin/bulk_tag_importerv2.py -p <profile> -i <input_file>
# 2. Profile should be configured in ~/.mcd/profiles.ini
#
# Input CSV format (with header):
#   warehouse_id,full_table_id,tag_key,tag_value
#   abc-123-uuid,database:schema.table,owner,team_data
#
# Notes:
# - warehouse_id: The source warehouse UUID (used for reference/logging)
# - full_table_id: Table identifier in format database:schema.table (must be lowercase)
# - tag_key: Tag property name
# - tag_value: Tag property value
# - Tags are applied to tables that exist in the target environment

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import csv
from pathlib import Path
from typing import Optional
from admin import *
from lib.helpers import sdk_helpers
from pycarlo.core import Query

# Initialize logger
util_name = os.path.basename(__file__).split('.')[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))


class BulkTagImporterV2(Tables):
	"""Class-based tag importer for use by migration module and CLI.

	Imports object tags (properties) to Monte Carlo tables. Supports importing
	from CSV files exported by bulk_tag_exporterv2.py.
	"""

	BATCH_SIZE = 99  # MC API batch limit for tag operations

	def __init__(self, profile, config_file: str = None, progress: Progress = None):
		"""Creates an instance of BulkTagImporterV2.

		Args:
			profile(str): Profile to use stored in montecarlo cli.
			config_file (str): Path to the Configuration File.
			progress(Progress): Progress bar.
		"""
		super().__init__(profile, config_file, progress)
		self.progress_bar = progress
		self._mcon_cache = {}  # Cache for full_table_id -> mcon lookups
		self._existing_tags = {}  # Cache for (mcon, tag_key) -> tag_value

	def parse_tag_csv(self, input_file: str) -> dict:
		"""Parse a tag CSV file and return organized data.

		This method returns data instead of calling sys.exit(), making it
		suitable for use by other modules like the migration tool.

		Args:
			input_file (str): Path to CSV file.

		Returns:
			dict: Result with keys:
				- success (bool): Whether parsing succeeded
				- tags (list): List of tag dictionaries
				- count (int): Number of tags found
				- errors (list): Any errors encountered
				- warnings (list): Any warnings (e.g., duplicates)
				- duplicates (int): Number of duplicate tags found
		"""
		file_path = Path(input_file)
		errors = []
		warnings = []
		tags = []

		if not file_path.is_file():
			return {
				'success': False,
				'tags': [],
				'count': 0,
				'errors': [f"File not found: {input_file}"],
				'warnings': [],
				'duplicates': 0
			}

		try:
			with open(input_file, 'r') as csvfile:
				reader = csv.DictReader(csvfile)

				# Check for required columns
				if reader.fieldnames is None:
					return {
						'success': False,
						'tags': [],
						'count': 0,
						'errors': ["CSV file is empty or has no headers"],
						'warnings': [],
						'duplicates': 0
					}

				required = {'full_table_id', 'tag_key', 'tag_value'}
				missing = required - set(reader.fieldnames)
				if missing:
					return {
						'success': False,
						'tags': [],
						'count': 0,
						'errors': [f"Missing required columns: {missing}"],
						'warnings': [],
						'duplicates': 0
					}

				row_num = 1  # Header is row 1
				for row in reader:
					row_num += 1
					full_table_id = row.get('full_table_id', '').strip()
					tag_key = row.get('tag_key', '').strip()
					tag_value = row.get('tag_value', '').strip()
					warehouse_id = row.get('warehouse_id', '').strip()

					if not full_table_id:
						errors.append(f"Row {row_num}: Empty full_table_id")
						continue

					if not tag_key:
						errors.append(f"Row {row_num}: Empty tag_key")
						continue

					tags.append({
						'warehouse_id': warehouse_id,
						'full_table_id': full_table_id.lower(),  # Normalize to lowercase
						'tag_key': tag_key,
						'tag_value': tag_value
					})

		except Exception as e:
			errors.append(f"Error reading file: {str(e)}")

		if not tags and not errors:
			errors.append("No tags found in input file")

		# Check for duplicate tags
		seen = set()
		duplicates = 0
		for tag in tags:
			key = (tag['full_table_id'], tag['tag_key'], tag['tag_value'])
			if key in seen:
				duplicates += 1
			else:
				seen.add(key)

		if duplicates > 0:
			warnings.append(f"Found {duplicates} duplicate tags in CSV (will be de-duplicated on import)")

		return {
			'success': len(errors) == 0 and len(tags) > 0,
			'tags': tags,
			'count': len(tags),
			'errors': errors,
			'warnings': warnings,
			'duplicates': duplicates
		}

	def lookup_mcon(self, warehouse_id: str, full_table_id: str) -> str:
		"""Look up the MCON for a table given its full_table_id.

		Uses the search parameter for targeted lookup instead of fetching all tables.
		Results are cached to avoid duplicate API calls. Also fetches and caches
		existing tags for the table to support skip/update detection during import.

		Args:
			warehouse_id (str): The warehouse UUID.
			full_table_id (str): The full table identifier (e.g., 'db:schema.table').

		Returns:
			str: The MCON if found, empty string otherwise.
		"""
		# Normalize to lowercase for consistent cache keys
		full_table_id_lower = full_table_id.lower()
		cache_key = (warehouse_id, full_table_id_lower)

		# Check cache first
		if cache_key in self._mcon_cache:
			return self._mcon_cache[cache_key]

		try:
			# Use search parameter for targeted lookup
			query = Query()
			get_tables = query.get_tables(
				dw_id=warehouse_id,
				search=full_table_id,
				first=10,
				is_deleted=False
			)
			get_tables.edges.node.__fields__("full_table_id", "mcon")
			# Also fetch existing tags for skip/update detection
			get_tables.edges.node.object_properties.__fields__("property_name", "property_value")

			response = self.auth.client(query).get_tables

			if response and response.edges:
				for edge in response.edges:
					# Exact match check (search may return partial matches)
					if edge.node.full_table_id.lower() == full_table_id_lower:
						mcon = edge.node.mcon
						self._mcon_cache[cache_key] = mcon

						# Cache existing tags for this table
						if edge.node.object_properties:
							for prop in edge.node.object_properties:
								tag_key = (mcon, prop['property_name'])
								self._existing_tags[tag_key] = prop['property_value']

						return mcon

			LOGGER.debug(f"Could not find MCON for {full_table_id}")
			self._mcon_cache[cache_key] = ''
			return ''

		except Exception as e:
			LOGGER.error(f"Error looking up MCON for {full_table_id}: {e}")
			return ''

	def get_mcon_mapping_for_warehouse(self, warehouse_id: str) -> dict:
		"""Get full_table_id -> mcon mapping for a warehouse.

		Args:
			warehouse_id (str): UUID of the warehouse.

		Returns:
			dict: full_table_id (lowercase) -> mcon mapping
		"""
		mcon_map = {}
		cursor = None

		while True:
			response = self.auth.client(
				self.get_tables(dw_id=warehouse_id, after=cursor)
			).get_tables

			for table in response.edges:
				# Store with lowercase key for case-insensitive matching
				mcon_map[table.node.full_table_id.lower()] = table.node.mcon

			if response.page_info.has_next_page:
				cursor = response.page_info.end_cursor
			else:
				break

		return mcon_map

	def get_mcon_mapping_all_warehouses(self) -> dict:
		"""Get full_table_id -> mcon mapping for all warehouses.

		Returns:
			dict: full_table_id (lowercase) -> mcon mapping
		"""
		all_warehouses, _ = self.get_warehouses()
		LOGGER.info(f"Building MCON mapping for {len(all_warehouses)} warehouse(s)...")

		mcon_map = {}
		for wh_id in all_warehouses:
			LOGGER.debug(f"  Processing warehouse: {wh_id}")
			warehouse_map = self.get_mcon_mapping_for_warehouse(wh_id)
			mcon_map.update(warehouse_map)

		LOGGER.info(f"Built mapping for {len(mcon_map)} tables")
		return mcon_map

	def import_tag_batch(self, tags: list) -> dict:
		"""Import a batch of tags (max 99 at a time).

		Args:
			tags (list): List of dicts with mconId, propertyName, propertyValue

		Returns:
			dict: Result with success, count, error
		"""
		if not tags:
			return {'success': True, 'count': 0, 'error': None}

		try:
			mutation = self.bulk_create_or_update_object_properties(tags)
			response = self.auth.client(mutation)

			if response:
				return {
					'success': True,
					'count': len(tags),
					'error': None
				}
			else:
				return {
					'success': False,
					'count': 0,
					'error': "No response from API"
				}

		except Exception as e:
			return {
				'success': False,
				'count': 0,
				'error': str(e)
			}

	def import_tags(self, input_file: str = None, dry_run: bool = True) -> dict:
		"""Import tags from CSV file with batching.

		Args:
			input_file (str): Path to CSV file.
			dry_run (bool): If True, preview changes without committing.

		Returns:
			dict: Result with created, skipped, failed counts
		"""
		mode = "DRY-RUN" if dry_run else "COMMIT"
		LOGGER.info(f"Starting tag import ({mode})...")

		# Parse input file
		parse_result = self.parse_tag_csv(input_file)
		if not parse_result['success']:
			return {
				'success': False,
				'dry_run': dry_run,
				'created': 0,
				'skipped': 0,
				'failed': 0,
				'errors': parse_result['errors']
			}

		tags_data = parse_result['tags']
		LOGGER.info(f"Found {len(tags_data)} tags in input file")

		# Get unique (warehouse_id, full_table_id) pairs for targeted lookups
		unique_tables = set()
		for tag in tags_data:
			if tag.get('warehouse_id') and tag.get('full_table_id'):
				unique_tables.add((tag['warehouse_id'], tag['full_table_id'].lower()))

		LOGGER.info(f"Looking up MCONs for {len(unique_tables)} unique tables...")

		# Targeted lookup: query only the tables we need
		tables_found = 0
		tables_not_found = 0
		for warehouse_id, full_table_id in unique_tables:
			mcon = self.lookup_mcon(warehouse_id, full_table_id)
			if mcon:
				tables_found += 1
			else:
				tables_not_found += 1

		LOGGER.info(f"Resolved {tables_found} tables ({tables_not_found} not found)")

		# Process tags
		created = 0
		skipped = 0
		failed = 0
		errors = []

		# Prepare tags for import (resolve full_table_id -> mcon using cache)
		tags_to_import = []
		for tag in tags_data:
			full_table_id = tag['full_table_id'].lower()
			warehouse_id = tag.get('warehouse_id', '')

			# Look up from cache (already populated by targeted lookups above)
			cache_key = (warehouse_id, full_table_id)
			mcon = self._mcon_cache.get(cache_key, '')

			if not mcon:
				LOGGER.warning(f"Table not found: {tag['full_table_id']}")
				skipped += 1
				continue

			tags_to_import.append({
				'mcon_id': mcon,
				'property_name': tag['tag_key'],
				'property_value': tag['tag_value'],
				'full_table_id': tag['full_table_id']  # Keep for logging
			})

		LOGGER.info(f"Resolved {len(tags_to_import)} tags to import ({skipped} tables not found)")

		# De-duplicate tags: keep last occurrence of each (mcon_id, property_name) pair
		unique_tags = {}
		duplicates_removed = 0
		for tag in tags_to_import:
			key = (tag['mcon_id'], tag['property_name'])
			if key in unique_tags:
				duplicates_removed += 1
			unique_tags[key] = tag
		
		tags_to_import = list(unique_tags.values())
		if duplicates_removed > 0:
			LOGGER.info(f"Removed {duplicates_removed} duplicate tags, {len(tags_to_import)} unique tags to import")

		# Compare against existing tags to categorize as create/update/skip
		tags_to_create = []
		tags_to_update = []
		updated = 0

		for tag in tags_to_import:
			existing_key = (tag['mcon_id'], tag['property_name'])

			if existing_key not in self._existing_tags:
				# Tag doesn't exist -> CREATE
				tags_to_create.append(tag)
			else:
				existing_value = self._existing_tags[existing_key]
				# Normalize None to empty string for comparison
				existing_normalized = existing_value if existing_value is not None else ''
				new_normalized = tag['property_value'] if tag['property_value'] is not None else ''

				if existing_normalized != new_normalized:
					# Tag exists but value changed -> UPDATE
					tags_to_update.append(tag)
				else:
					# Tag exists with same value -> SKIP
					skipped += 1

		# Combine create and update tags for API call
		tags_to_send = tags_to_create + tags_to_update

		LOGGER.info(f"Tags to create: {len(tags_to_create)}, to update: {len(tags_to_update)}, unchanged (skip): {skipped}")

		if dry_run:
			# In dry-run mode, just log what would be done
			for tag in tags_to_create:
				LOGGER.info(f"WOULD CREATE: {tag['full_table_id']} - {tag['property_name']}={tag['property_value']}")
				created += 1
			for tag in tags_to_update:
				existing_value = self._existing_tags.get((tag['mcon_id'], tag['property_name']), '')
				LOGGER.info(f"WOULD UPDATE: {tag['full_table_id']} - {tag['property_name']}={existing_value} -> {tag['property_value']}")
				updated += 1
		else:
			# Process in batches
			batch = []
			batch_creates = 0
			batch_updates = 0

			for tag in tags_to_create:
				batch.append({
					'mcon_id': tag['mcon_id'],
					'property_name': tag['property_name'],
					'property_value': tag['property_value']
				})
				batch_creates += 1

				if len(batch) >= self.BATCH_SIZE:
					result = self.import_tag_batch(batch)
					if result['success']:
						created += batch_creates
						LOGGER.info(f"Imported batch of {result['count']} tags ({batch_creates} creates)")
					else:
						failed += len(batch)
						errors.append(result['error'])
						LOGGER.error(f"Batch import failed: {result['error']}")
					batch = []
					batch_creates = 0

			for tag in tags_to_update:
				batch.append({
					'mcon_id': tag['mcon_id'],
					'property_name': tag['property_name'],
					'property_value': tag['property_value']
				})
				batch_updates += 1

				if len(batch) >= self.BATCH_SIZE:
					result = self.import_tag_batch(batch)
					if result['success']:
						created += batch_creates
						updated += batch_updates
						LOGGER.info(f"Imported batch of {result['count']} tags ({batch_creates} creates, {batch_updates} updates)")
					else:
						failed += len(batch)
						errors.append(result['error'])
						LOGGER.error(f"Batch import failed: {result['error']}")
					batch = []
					batch_creates = 0
					batch_updates = 0

			# Import remaining batch
			if batch:
				result = self.import_tag_batch(batch)
				if result['success']:
					created += batch_creates
					updated += batch_updates
					LOGGER.info(f"Imported final batch of {result['count']} tags ({batch_creates} creates, {batch_updates} updates)")
				else:
					failed += len(batch)
					errors.append(result['error'])
					LOGGER.error(f"Final batch import failed: {result['error']}")

		LOGGER.info(f"Import complete: {created} created, {updated} updated, {skipped} skipped, {failed} failed")

		return {
			'success': failed == 0 and len(errors) == 0,
			'dry_run': dry_run,
			'created': created,
			'updated': updated,
			'skipped': skipped,
			'failed': failed,
			'errors': errors
		}


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
		result = util.import_tags(args.input_file, dry_run=False)
		if not result['success']:
			for error in result['errors']:
				LOGGER.error(error)

	util = BulkTagImporterV2(args.profile)
	run_utility(util, args)


if __name__ == "__main__":
	main()

