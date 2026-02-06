"""
Bulk Export Monitors V2 - Migration-compatible monitor exporter.

Inherits from Monitors base class and provides migration-specific methods:
- Structured result dictionaries for integration with migration framework
- Warehouse mapping extraction for cross-environment migrations
- YAML export with progress tracking

Usage:
    # As a standalone script
    python monitors/bulk_export_monitors_v2.py -p <profile> -o <output_file>

    # As a module (used by MonitorMigrator)
    from monitors.bulk_export_monitors_v2 import BulkExportMonitorsV2
    exporter = BulkExportMonitorsV2(profile)
    result = exporter.export_monitors(output_file)
"""

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import logging.config
import textwrap
from pathlib import Path
from monitors import *
from lib.helpers import sdk_helpers

# Initialize logger
util_name = os.path.splitext(os.path.basename(__file__))[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))


class BulkExportMonitorsV2(Monitors):
	"""Migration-compatible monitor exporter.

	Inherits from Monitors base class to access:
	- get_ui_monitors(): Fetches UI monitors with pagination
	- export_yaml_template(): Exports monitor config as YAML
	- get_warehouses(): Gets warehouse UUID -> name mapping

	Provides migration-specific methods:
	- get_warehouse_mapping(): Returns UUID -> name mapping
	- get_ui_monitors_for_export(): Returns UI monitors with warehouse enrichment
	- export_monitors(): Exports to file with structured result dict
	"""

	def __init__(self, profile: str, config_file: str = None, progress: Progress = None):
		"""Creates an instance of BulkExportMonitorsV2.

		Args:
			profile (str): MC profile name from configs.ini
			config_file (str): Path to the Configuration File.
			progress (Progress): Progress bar.
		"""
		super().__init__(profile, config_file, progress)
		self.OUTPUT_FILE = "monitors.yaml"
		self.progress_bar = progress

	def get_warehouse_mapping(self) -> dict:
		"""Get mapping of warehouse UUID to warehouse name.

		Returns:
			dict: {warehouse_uuid: warehouse_name}
		"""
		warehouses, response = self.get_warehouses()
		mapping = {}
		for warehouse in response.account.warehouses:
			mapping[warehouse.uuid] = warehouse.name
		return mapping

	def get_ui_monitors_for_export(self) -> dict:
		"""Fetch UI monitors with warehouse information for export.

		This is the main method used by the migration module.
		Only fetches UI monitors (namespace='ui'), not MaC-managed monitors.

		Returns:
			dict: Result with keys:
				- monitors (list): List of monitor UUIDs
				- raw_monitors (list): Raw monitor objects with metadata
				- warehouse_mapping (dict): UUID -> name mapping for warehouses in export
				- count (int): Number of monitors found
		"""
		LOGGER.info("Fetching UI monitors...")
		monitor_list, raw_monitors = self.get_ui_monitors()
		monitor_count = len(monitor_list)

		if monitor_count == 0:
			LOGGER.info("No UI monitors found")
			return {
				'monitors': [],
				'raw_monitors': [],
				'warehouse_mapping': {},
				'count': 0
			}

		LOGGER.info(f"Found {monitor_count} UI monitors")

		# Get warehouse mapping for cross-environment support
		LOGGER.info("Fetching warehouse information...")
		full_warehouse_mapping = self.get_warehouse_mapping()

		# Collect only warehouses used in exported monitors
		warehouses_in_export = {}
		for monitor in raw_monitors:
			if hasattr(monitor, 'resource_id') and monitor.resource_id:
				wh_id = monitor.resource_id
				if wh_id in full_warehouse_mapping:
					warehouses_in_export[wh_id] = full_warehouse_mapping[wh_id]

		return {
			'monitors': monitor_list,
			'raw_monitors': raw_monitors,
			'warehouse_mapping': warehouses_in_export,
			'count': monitor_count
		}

	def export_monitors(self, output_file: str, export_name: bool = False) -> dict:
		"""Export monitors to YAML file.

		Args:
			output_file (str): Path to output YAML file.
			export_name (bool): Whether to include monitor names in export.

		Returns:
			dict: Export result with keys:
				- success (bool): Whether export succeeded
				- count (int): Number of monitors exported
				- file (str): Path to exported file
				- warehouse_mapping (dict): Warehouses used in export (for mapping template)
				- errors (list): Any errors encountered
		"""
		errors = []

		try:
			# Ensure output directory exists
			output_path = Path(output_file)
			output_path.parent.mkdir(parents=True, exist_ok=True)

			# Get monitors data
			data = self.get_ui_monitors_for_export()
			monitor_list = data['monitors']
			monitor_count = data['count']
			warehouse_mapping = data['warehouse_mapping']

			if monitor_count == 0:
				# Write empty file for consistency
				with open(output_file, "w") as yaml_file:
					yaml_file.write("montecarlo:\n")
				return {
					'success': True,
					'count': 0,
					'file': str(output_file),
					'warehouse_mapping': {},
					'errors': []
				}

			# Split list of monitors in batches of 500
			batches = sdk_helpers.batch_objects(monitor_list, 500)

			with open(output_file, "w") as yaml_file:
				yaml_file.write("montecarlo:\n")
				for batch in batches:
					monitor_yaml = self.export_yaml_template(batch, export_name)
					yaml_file.write(textwrap.indent(monitor_yaml["config_template_as_yaml"], prefix="  "))

					if self.progress_bar and self.progress_bar.tasks:
						progress_per_batch = 40 / max(len(batches), 1)
						self.progress_bar.update(self.progress_bar.tasks[0].id, advance=progress_per_batch)

			LOGGER.info(f"Exported {monitor_count} monitors to {output_file}")

			return {
				'success': True,
				'count': monitor_count,
				'file': str(output_file),
				'warehouse_mapping': warehouse_mapping,
				'errors': []
			}

		except Exception as e:
			LOGGER.error(f"Export failed: {e}")
			errors.append(str(e))
			return {
				'success': False,
				'count': 0,
				'file': None,
				'warehouse_mapping': {},
				'errors': errors
			}


def main(*args, **kwargs):
	"""Main entry point for the bulk export monitors v2 utility."""

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
		# Default to 'y' (export names) for migration compatibility
		# Preserving names enables UPDATE instead of DELETE+CREATE on re-import
		export_name = getattr(args, 'export_name', 'y') == 'y'
		output_file = getattr(args, 'output_file', None) or str(util.OUTPUT_DIR / util_name / util.OUTPUT_FILE)
		result = util.export_monitors(output_file=output_file, export_name=export_name)

		if result['success']:
			LOGGER.info(f"Export completed: {result['count']} monitors exported to {result['file']}")
		else:
			LOGGER.error(f"Export failed: {result['errors']}")

	util = BulkExportMonitorsV2(args.profile)
	run_utility(util, args)


if __name__ == '__main__':
	main()

