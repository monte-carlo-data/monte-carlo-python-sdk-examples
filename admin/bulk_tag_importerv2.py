# Instructions:
# 1. Run this script: python admin/bulk_tag_importerv2.py -p <profile> -i <input_file>
# 2. Profile should be configured in ~/.mcd/profiles.ini
#
# Input CSV format (with header):
#   warehouse_id,warehouse_name,full_table_id,asset_type,tag_key,tag_value
#   abc-123-uuid,my_warehouse,database:schema.table,table,owner,team_data
#
# Notes:
# - warehouse_id: The source warehouse UUID (used for same-environment matching)
# - warehouse_name: The warehouse name (used for cross-environment MCON construction)
# - full_table_id: Table identifier in format database:schema.table (must be lowercase)
# - asset_type: Asset type ('table' or 'view') - enables MCON construction without API lookups
# - tag_key: Tag property name
# - tag_value: Tag property value
# - When asset_type and warehouse_name are provided, MCONs are constructed by matching warehouse names
# - Fallback to API lookup if asset_type is missing (backwards compatible)

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
		# Caches for MCON construction optimization
		self._dest_account_uuid = None
		self._dest_warehouse_by_name = {}  # warehouse_name -> dest_warehouse_uuid
		self._source_warehouse_names = {}  # source_warehouse_uuid -> warehouse_name

	def _get_destination_account_uuid(self) -> str:
		"""Get the destination account UUID (cached, 1 API call).

		Returns:
			str: The destination account UUID.
		"""
		if self._dest_account_uuid is None:
			_, raw = self.get_warehouses()
			for acct in raw:
				self._dest_account_uuid = raw[acct].uuid
				break
		return self._dest_account_uuid

	def _build_warehouse_name_mapping(self, source_warehouses: dict) -> dict:
		"""Build mapping from source warehouse UUIDs to destination warehouse UUIDs by name.

		Fetches all destination warehouses and matches by name to source warehouses.
		Uses warehouse_name from CSV for cross-environment mapping, or falls back to
		matching by UUID for same-environment cases.

		Args:
			source_warehouses (dict): Dict of source_warehouse_id -> warehouse_name from CSV.

		Returns:
			dict: source_warehouse_uuid -> dest_warehouse_uuid mapping
		"""
		# Get destination warehouses (this populates _dest_account_uuid as side effect)
		_, raw = self.get_warehouses()

		# Build name -> dest_uuid lookup for destination warehouses
		for acct in raw:
			self._dest_account_uuid = raw[acct].uuid
			for wh in raw[acct].warehouses:
				wh_name = wh.name.lower().strip()
				self._dest_warehouse_by_name[wh_name] = wh.uuid

		# Map source warehouses to destination warehouses by name
		mapping = {}
		unmapped = []

		for src_wh_id, src_wh_name in source_warehouses.items():
			if src_wh_name:
				# Use warehouse_name from CSV for cross-environment matching
				wh_name_lower = src_wh_name.lower().strip()
				if wh_name_lower in self._dest_warehouse_by_name:
					self._source_warehouse_names[src_wh_id] = wh_name_lower
					mapping[src_wh_id] = self._dest_warehouse_by_name[wh_name_lower]
				else:
					LOGGER.debug(f"Warehouse name '{src_wh_name}' not found in destination")
					unmapped.append(src_wh_id)
			else:
				# No warehouse_name in CSV - try matching by UUID (same environment)
				# Check if this UUID exists in destination
				if src_wh_id in self._dest_warehouse_by_name.values():
					# Find the name for this UUID
					for name, uuid in self._dest_warehouse_by_name.items():
						if uuid == src_wh_id:
							self._source_warehouse_names[src_wh_id] = name
							mapping[src_wh_id] = uuid
							break
				else:
					unmapped.append(src_wh_id)

		if unmapped:
			LOGGER.warning(
				f"Could not map {len(unmapped)} source warehouse(s) to destination. "
				f"Tags from these warehouses will fall back to API lookup."
			)

		return mapping

	def _get_destination_warehouse_uuid(self, source_warehouse_id: str) -> str:
		"""Get the destination warehouse UUID for a source warehouse.

		Args:
			source_warehouse_id (str): The source warehouse UUID.

		Returns:
			str: The destination warehouse UUID, or empty string if not found.
		"""
		# Check if we have a mapping
		if source_warehouse_id in self._source_warehouse_names:
			wh_name = self._source_warehouse_names[source_warehouse_id]
			return self._dest_warehouse_by_name.get(wh_name, '')
		return ''

	def construct_mcon(self, source_warehouse_id: str, asset_type: str, full_table_id: str) -> str:
		"""Construct a destination MCON from components.

		Args:
			source_warehouse_id (str): The source warehouse UUID (for mapping).
			asset_type (str): The asset type ('table' or 'view').
			full_table_id (str): The full table identifier.

		Returns:
			str: The constructed MCON, or empty string if warehouse mapping failed.
		"""
		account = self._get_destination_account_uuid()
		warehouse = self._get_destination_warehouse_uuid(source_warehouse_id)

		if not account or not warehouse:
			return ''

		return f"MCON++{account}++{warehouse}++{asset_type}++{full_table_id}"

	def fetch_existing_tags_for_tables(self, tables: list) -> int:
		"""Fetch existing tags for a list of tables to enable skip/update detection.

		This is needed when MCONs are constructed (not looked up) because the
		lookup_mcon() method also fetches existing tags, but construct_mcon() doesn't.

		Args:
			tables (list): List of dicts with 'warehouse_id', 'full_table_id', 'mcon' keys.

		Returns:
			int: Number of tables for which tags were fetched.
		"""
		fetched = 0
		for table in tables:
			warehouse_id = table.get('warehouse_id', '')
			full_table_id = table.get('full_table_id', '')
			mcon = table.get('mcon', '')

			if not warehouse_id or not full_table_id or not mcon:
				continue

			# Check if we already have tags cached for this MCON
			# (from a previous lookup_mcon call)
			has_cached_tags = any(k[0] == mcon for k in self._existing_tags.keys())
			if has_cached_tags:
				continue

			try:
				# Query the table to get existing tags
				query = Query()
				get_tables = query.get_tables(
					dw_id=warehouse_id,
					search=full_table_id,
					first=10,
					is_deleted=False
				)
				get_tables.edges.node.__fields__("full_table_id", "mcon")
				get_tables.edges.node.object_properties.__fields__("property_name", "property_value")

				response = self.auth.client(query).get_tables

				if response and response.edges:
					for edge in response.edges:
						if edge.node.full_table_id.lower() == full_table_id.lower():
							# Cache existing tags for this table
							if edge.node.object_properties:
								for prop in edge.node.object_properties:
									tag_key = (mcon, prop['property_name'])
									self._existing_tags[tag_key] = prop['property_value']
							fetched += 1
							break

			except Exception as e:
				LOGGER.debug(f"Error fetching existing tags for {full_table_id}: {e}")

		return fetched

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
					warehouse_name = row.get('warehouse_name', '').strip()
					asset_type = row.get('asset_type', '').strip().lower()

					if not full_table_id:
						errors.append(f"Row {row_num}: Empty full_table_id")
						continue

					if not tag_key:
						errors.append(f"Row {row_num}: Empty tag_key")
						continue

					tags.append({
						'warehouse_id': warehouse_id,
						'warehouse_name': warehouse_name,  # For cross-environment MCON construction
						'full_table_id': full_table_id.lower(),  # Normalize to lowercase
						'asset_type': asset_type,  # 'table' or 'view', or empty for legacy CSVs
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

		# Check if we can use MCON construction (when asset_type is available)
		tags_with_asset_type = [t for t in tags_data if t.get('asset_type')]
		tags_without_asset_type = [t for t in tags_data if not t.get('asset_type')]

		if tags_with_asset_type:
			LOGGER.info(f"Found {len(tags_with_asset_type)} tags with asset_type (can construct MCONs)")

			# Get unique source warehouses with their names for mapping
			source_warehouses = {}
			for t in tags_with_asset_type:
				if t.get('warehouse_id'):
					# Use the first warehouse_name we find for each warehouse_id
					if t['warehouse_id'] not in source_warehouses:
						source_warehouses[t['warehouse_id']] = t.get('warehouse_name', '')

			# Build warehouse name mapping (1 API call to get_warehouses)
			LOGGER.info(f"Building warehouse name mapping for {len(source_warehouses)} source warehouse(s)...")
			self._build_warehouse_name_mapping(source_warehouses)

			# Construct MCONs for tags with asset_type
			mcons_constructed = 0
			mcons_failed = 0
			for tag in tags_with_asset_type:
				cache_key = (tag['warehouse_id'], tag['full_table_id'])
				mcon = self.construct_mcon(tag['warehouse_id'], tag['asset_type'], tag['full_table_id'])
				if mcon:
					self._mcon_cache[cache_key] = mcon
					mcons_constructed += 1
				else:
					# Fallback: add to lookup list
					tags_without_asset_type.append(tag)
					mcons_failed += 1

			LOGGER.info(f"Constructed {mcons_constructed} MCONs ({mcons_failed} will fall back to lookup)")

			# Fetch existing tags for constructed MCONs (needed for skip/update detection)
			# Build list of unique tables with constructed MCONs
			unique_tables_with_mcons = []
			seen_tables = set()
			for tag in tags_with_asset_type:
				cache_key = (tag['warehouse_id'], tag['full_table_id'])
				if cache_key in self._mcon_cache and cache_key not in seen_tables:
					seen_tables.add(cache_key)
					unique_tables_with_mcons.append({
						'warehouse_id': tag['warehouse_id'],
						'full_table_id': tag['full_table_id'],
						'mcon': self._mcon_cache[cache_key]
					})

			if unique_tables_with_mcons:
				LOGGER.info(f"Fetching existing tags for {len(unique_tables_with_mcons)} tables...")
				fetched = self.fetch_existing_tags_for_tables(unique_tables_with_mcons)
				LOGGER.info(f"Fetched existing tags for {fetched} tables")

		# Fallback: API lookup for tags without asset_type or failed construction
		if tags_without_asset_type:
			unique_tables_to_lookup = set()
			for tag in tags_without_asset_type:
				if tag.get('warehouse_id') and tag.get('full_table_id'):
					cache_key = (tag['warehouse_id'], tag['full_table_id'])
					if cache_key not in self._mcon_cache:
						unique_tables_to_lookup.add(cache_key)

			if unique_tables_to_lookup:
				LOGGER.info(f"Looking up MCONs for {len(unique_tables_to_lookup)} tables via API...")

				tables_found = 0
				tables_not_found = 0
				for warehouse_id, full_table_id in unique_tables_to_lookup:
					mcon = self.lookup_mcon(warehouse_id, full_table_id)
					if mcon:
						tables_found += 1
					else:
						tables_not_found += 1

				LOGGER.info(f"Resolved {tables_found} tables ({tables_not_found} not found)")
		else:
			LOGGER.info("All MCONs constructed from CSV data, no API lookups needed")

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
		# Default to dry-run mode (safe), require --force yes to apply changes
		force = getattr(args, 'force', None)
		dry_run = force != 'yes'
		result = util.import_tags(args.input_file, dry_run=dry_run)
		if not result['success']:
			for error in result['errors']:
				LOGGER.error(error)
		elif dry_run:
			LOGGER.info("")
			LOGGER.info("This was a DRY-RUN. No changes were made. Use --force yes to apply changes.")

	util = BulkTagImporterV2(args.profile)
	run_utility(util, args)


if __name__ == "__main__":
	main()

