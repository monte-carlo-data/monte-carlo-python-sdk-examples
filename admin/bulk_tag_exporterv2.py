# Instructions:
# 1. Run this script: python admin/bulk_tag_exporterv2.py -p <profile> -o <output_file>
# 2. Profile should be configured in ~/.mcd/profiles.ini
# 3. Optionally specify a warehouse ID to export tags from a specific warehouse only
#
# Output CSV format:
#   full_table_id,tag_key,tag_value
#   database:schema.table,owner,team_data
#   database:schema.table,priority,high
#
# Note: The output CSV can be used with bulk_tag_importerv2.py to migrate tags to another workspace.

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import csv
from typing import Optional
from admin import *
from lib.helpers import sdk_helpers
from pycarlo.core import Query

# Initialize logger
util_name = os.path.basename(__file__).split('.')[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))


class BulkTagExporterV2(Tables):
	"""Class-based tag exporter for use by migration module and CLI.

	Exports object tags (properties) from Monte Carlo tables. Supports exporting
	from all warehouses or a specific warehouse.
	"""

	def __init__(self, profile, config_file: str = None, progress: Progress = None):
		"""Creates an instance of BulkTagExporterV2.

		Args:
			profile(str): Profile to use stored in montecarlo cli.
			config_file (str): Path to the Configuration File.
			progress(Progress): Progress bar.
		"""
		super().__init__(profile, config_file, progress)
		self.progress_bar = progress

	def _get_tables_with_tags_query(self, dw_id: str, batch_size: int = 1000,
									after: str = None) -> Query:
		"""Build query to fetch tables with their object_properties (tags).

		Args:
			dw_id (str): Warehouse UUID.
			batch_size (int): Number of tables to fetch per request.
			after (str): Cursor for pagination.

		Returns:
			Query: Formed MC Query object.
		"""
		query = Query()
		get_tables = query.get_tables(
			first=batch_size,
			dw_id=dw_id,
			is_deleted=False,
			**(dict(after=after) if after else {})
		)
		get_tables.edges.node.__fields__("full_table_id", "mcon")
		get_tables.edges.node.object_properties.__fields__("property_name", "property_value")
		get_tables.page_info.__fields__(end_cursor=True)
		get_tables.page_info.__fields__("has_next_page")

		return query

	def get_all_tags_for_warehouse(self, warehouse_id: str) -> list:
		"""Fetch all tags for a specific warehouse.

		Args:
			warehouse_id (str): UUID of the warehouse.

		Returns:
			list[dict]: List of tag dictionaries with keys:
				- warehouse_id (str): UUID of the warehouse
				- full_table_id (str): Full table identifier (database:schema.table)
				- mcon (str): Monte Carlo Object Name
				- tag_key (str): Tag property name
				- tag_value (str): Tag property value
		"""
		tags = []
		cursor = None

		while True:
			response = self.auth.client(
				self._get_tables_with_tags_query(dw_id=warehouse_id, after=cursor)
			).get_tables

			for table in response.edges:
				if table.node.object_properties and len(table.node.object_properties) > 0:
					for prop in table.node.object_properties:
						tags.append({
							'warehouse_id': warehouse_id,
							'full_table_id': table.node.full_table_id,
							'mcon': table.node.mcon,
							'tag_key': prop['property_name'],
							'tag_value': prop['property_value']
						})

			if response.page_info.has_next_page:
				cursor = response.page_info.end_cursor
			else:
				break

		return tags

	def get_all_tags(self, warehouse_ids: list = None) -> list:
		"""Fetch all tags from all warehouses (or specified warehouses).

		This is the main method used by the migration module.

		Args:
			warehouse_ids (list): Optional list of warehouse UUIDs to filter.
								  If None, fetches from all warehouses.

		Returns:
			list[dict]: Combined list of all unique tags across warehouses.
		"""
		# Get all warehouses if not specified
		if warehouse_ids is None:
			all_warehouses, _ = self.get_warehouses()
			warehouse_ids = all_warehouses

		LOGGER.info(f"Fetching tags from {len(warehouse_ids)} warehouse(s)...")

		all_tags = []
		for wh_id in warehouse_ids:
			LOGGER.info(f"  Processing warehouse: {wh_id}")
			warehouse_tags = self.get_all_tags_for_warehouse(wh_id)
			LOGGER.info(f"    Found {len(warehouse_tags)} tags")
			all_tags.extend(warehouse_tags)

		# De-duplicate tags: the API sometimes returns duplicate properties
		# Key is (full_table_id, tag_key, tag_value) - keep first occurrence
		seen = set()
		unique_tags = []
		duplicates = 0
		for tag in all_tags:
			key = (tag['full_table_id'], tag['tag_key'], tag['tag_value'])
			if key not in seen:
				seen.add(key)
				unique_tags.append(tag)
			else:
				duplicates += 1

		if duplicates > 0:
			LOGGER.warning(f"Removed {duplicates} duplicate tags from API response")

		LOGGER.info(f"Total unique tags: {len(unique_tags)}")
		return unique_tags

	def export_tags(self, output_file: str, warehouse_id: str = None):
		"""Export tags to CSV file.

		Args:
			output_file (str): Path to output CSV file.
			warehouse_id (str): Optional warehouse UUID to filter. If None, exports all.
		"""
		# Fetch tags
		warehouse_ids = [warehouse_id] if warehouse_id else None
		tags = self.get_all_tags(warehouse_ids)

		if not tags:
			LOGGER.info("No tags found to export.")
			# Still write header for consistency
			with open(output_file, 'w', newline='') as csvfile:
				writer = csv.writer(csvfile)
				writer.writerow(['warehouse_id', 'full_table_id', 'tag_key', 'tag_value'])
			return

		# Write to CSV
		with open(output_file, 'w', newline='') as csvfile:
			writer = csv.writer(csvfile)
			writer.writerow(['warehouse_id', 'full_table_id', 'tag_key', 'tag_value'])

			for tag in tags:
				writer.writerow([
					tag['warehouse_id'],
					tag['full_table_id'],
					tag['tag_key'],
					tag['tag_value']
				])

		LOGGER.info(f"Export complete: {output_file} ({len(tags)} rows)")


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
		warehouse_id = getattr(args, 'warehouse_id', None)
		util.export_tags(args.output_file, warehouse_id)

	util = BulkTagExporterV2(args.profile)
	run_utility(util, args)


if __name__ == "__main__":
	main()

