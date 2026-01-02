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

	def get_existing_tags_for_tables(self, mcons: list) -> dict:
		"""Get existing tags for specific tables by their MCONs.

		Used to detect what tags already exist (for skip/update logic).

		Args:
			mcons (list): List of MCONs to check.

		Returns:
			dict: mcon -> list of {tag_key, tag_value}
		"""
		existing_tags = {}

		# Query tables with their object_properties
		# We need to query by MCON, but the API doesn't support direct MCON filtering
		# So we'll need to get all tables and filter
		# For efficiency, we batch this per warehouse

		all_warehouses, _ = self.get_warehouses()

		for wh_id in all_warehouses:
			cursor = None
			while True:
				query = Query()
				get_tables = query.get_tables(
					first=self.BATCH,
					dw_id=wh_id,
					is_deleted=False,
					**(dict(after=cursor) if cursor else {})
				)
				get_tables.edges.node.__fields__("mcon")
				get_tables.edges.node.object_properties.__fields__("property_name", "property_value")
				get_tables.page_info.__fields__(end_cursor=True)
				get_tables.page_info.__fields__("has_next_page")

				response = self.auth.client(query).get_tables

				for table in response.edges:
					if table.node.mcon in mcons:
						tags = []
						if table.node.object_properties:
							for prop in table.node.object_properties:
								tags.append({
									'tag_key': prop['property_name'],
									'tag_value': prop['property_value']
								})
						existing_tags[table.node.mcon] = tags

				if response.page_info.has_next_page:
					cursor = response.page_info.end_cursor
				else:
					break

		return existing_tags

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

		# Extract unique warehouse IDs from the CSV to optimize queries
		csv_warehouse_ids = set(tag['warehouse_id'] for tag in tags_data if tag.get('warehouse_id'))
		
		# Build MCON mapping only for relevant warehouses
		if csv_warehouse_ids:
			LOGGER.info(f"Building MCON mapping for {len(csv_warehouse_ids)} warehouse(s) from CSV...")
			mcon_mapping = {}
			for wh_id in csv_warehouse_ids:
				LOGGER.info(f"  Processing warehouse: {wh_id}")
				warehouse_map = self.get_mcon_mapping_for_warehouse(wh_id)
				LOGGER.info(f"    Found {len(warehouse_map)} tables")
				mcon_mapping.update(warehouse_map)
			LOGGER.info(f"Built mapping for {len(mcon_mapping)} tables total")
		else:
			# Fallback: if no warehouse_id in CSV, query all warehouses
			LOGGER.info("No warehouse_id in CSV, querying all warehouses...")
			mcon_mapping = self.get_mcon_mapping_all_warehouses()

		# Process tags
		created = 0
		skipped = 0
		failed = 0
		errors = []

		# Prepare tags for import (resolve full_table_id -> mcon)
		tags_to_import = []
		for tag in tags_data:
			full_table_id = tag['full_table_id']
			mcon = mcon_mapping.get(full_table_id)

			if not mcon:
				LOGGER.warning(f"Table not found: {full_table_id}")
				skipped += 1
				continue

			tags_to_import.append({
				'mcon_id': mcon,
				'property_name': tag['tag_key'],
				'property_value': tag['tag_value'],
				'full_table_id': full_table_id  # Keep for logging
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

		if dry_run:
			# In dry-run mode, just log what would be done
			for tag in tags_to_import:
				LOGGER.info(f"WOULD CREATE/UPDATE: {tag['full_table_id']} - {tag['property_name']}={tag['property_value']}")
				created += 1
		else:
			# Process in batches
			batch = []
			for tag in tags_to_import:
				batch.append({
					'mcon_id': tag['mcon_id'],
					'property_name': tag['property_name'],
					'property_value': tag['property_value']
				})

				if len(batch) >= self.BATCH_SIZE:
					result = self.import_tag_batch(batch)
					if result['success']:
						created += result['count']
						LOGGER.info(f"Imported batch of {result['count']} tags")
					else:
						failed += len(batch)
						errors.append(result['error'])
						LOGGER.error(f"Batch import failed: {result['error']}")
					batch = []

			# Import remaining batch
			if batch:
				result = self.import_tag_batch(batch)
				if result['success']:
					created += result['count']
					LOGGER.info(f"Imported final batch of {result['count']} tags")
				else:
					failed += len(batch)
					errors.append(result['error'])
					LOGGER.error(f"Final batch import failed: {result['error']}")

		LOGGER.info(f"Import complete: {created} created/updated, {skipped} skipped, {failed} failed")

		return {
			'success': failed == 0 and len(errors) == 0,
			'dry_run': dry_run,
			'created': created,
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

