"""
Monitor Migrator - Export and import monitors between MC environments.

Monitors are the core observability rules in Monte Carlo, including:
- Metric monitors (aggregations, thresholds)
- Custom SQL monitors
- Table monitors (freshness, volume, schema)

This migrator uses composition to delegate core operations to monitors bulk scripts,
supporting cross-environment migrations with warehouse mapping and namespace-based
organization.

Key features:
- Export UI monitors to MaC YAML format
- Import monitors with warehouse name mapping
- Namespace-based organization (for tracking and rollback)
- Delete monitors by namespace (for rollback/cleanup)
"""

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from pathlib import Path
from rich.progress import Progress
from lib.helpers.logs import LOGGER
from migration.base_migrator import BaseMigrator
from monitors.bulk_export_monitors_v2 import BulkExportMonitorsV2
from monitors.bulk_import_monitors import BulkImportMonitors


class MonitorMigrator(BaseMigrator):
	"""Migrator for monitors.

	Handles export and import of monitors between MC environments.
	Delegates core operations to monitors bulk scripts.

	Features:
	- Export: Fetches UI monitors and exports to MaC YAML format
	- Import: Imports monitors with warehouse mapping and namespace organization
	- Delete: Removes all monitors in a namespace (for rollback)
	- Validate: Validates YAML structure before import

	YAML Format (MaC):
		montecarlo:
		  metric:
		    - name: ...
		      warehouse: ...
		  custom_sql:
		    - name: ...
		      sql: ...
		  table:
		    - name: ...
		      asset_selection: ...

	Note on namespaces:
		Namespaces are labels attached to monitors, not pre-created entities.
		When importing with --namespace X, monitors are tagged with "X".
		The namespace exists as long as monitors with that label exist.
	"""

	def __init__(
		self,
		profile: str,
		config_file: str = None,
		progress: Progress = None,
		namespace: str = "migration"
	):
		"""Initialize the MonitorMigrator.

		Args:
			profile (str): MC profile name from configs.ini
			config_file (str): Path to configuration file (optional)
			progress (Progress): Rich progress bar instance (optional)
			namespace (str): Namespace for imported monitors (default: "migration")
		"""
		# Initialize BaseMigrator
		BaseMigrator.__init__(self, profile, config_file, progress)

		# Composition: use monitors classes for core operations
		self._exporter = BulkExportMonitorsV2(profile, config_file, progress)
		self._importer = BulkImportMonitors(profile, config_file, progress)

		# Monitor-specific settings
		self.namespace = namespace
		self._warehouse_mapping = {}

	@property
	def entity_name(self) -> str:
		return "monitors"

	@property
	def output_filename(self) -> str:
		return "monitors.yaml"

	@property
	def warehouse_mapping(self) -> dict:
		"""Get the warehouse name mapping for cross-environment migrations.

		Returns:
			dict: Source warehouse name -> destination warehouse name
		"""
		return self._warehouse_mapping

	@warehouse_mapping.setter
	def warehouse_mapping(self, mapping: dict):
		"""Set the warehouse name mapping.

		Args:
			mapping (dict): Source warehouse name -> destination warehouse name
		"""
		self._warehouse_mapping = mapping or {}

	def export(self, output_file: str = None, export_name: bool = True) -> dict:
		"""Export all UI monitors to MaC YAML format.

		Args:
			output_file (str): Path to output file. Uses default if not provided.
			export_name (bool): Include monitor names in export (recommended for migrations).

		Returns:
			dict: Export result with:
				- success (bool): Whether export succeeded
				- count (int): Number of monitors exported
				- file (str): Path to exported file
				- warehouse_mapping (dict): UUID -> name mapping for reference
				- errors (list): Any errors encountered
		"""
		LOGGER.info(f"[{self.entity_name}] Starting export...")

		try:
			# Ensure output directory exists
			self.ensure_output_dir()

			# Get output path
			output_path = Path(output_file) if output_file else self.get_output_path()

			# Delegate to monitors exporter
			result = self._exporter.export_monitors(
				output_file=str(output_path),
				export_name=export_name
			)

			if result['success']:
				# Store warehouse mapping for potential cross-env migrations
				if result.get('warehouse_mapping'):
					LOGGER.info(f"[{self.entity_name}] Warehouse mapping available for cross-environment migrations")
					for uuid, name in result['warehouse_mapping'].items():
						LOGGER.debug(f"  {name} ({uuid})")

				result['file'] = str(output_path)
				self.log_result('export', result)

			return result

		except Exception as e:
			LOGGER.error(f"[{self.entity_name}] Export failed: {e}")
			return self.create_result(
				success=False,
				count=0,
				errors=[str(e)]
			)

	def import_data(
		self,
		input_file: str = None,
		dry_run: bool = True,
		warehouse_mapping: dict = None
	) -> dict:
		"""Import monitors from MaC YAML file.

		Monitors are imported under the configured namespace, enabling:
		- Tracking of migrated monitors
		- Easy rollback via delete_by_namespace()
		- Isolation from manually-created monitors

		Args:
			input_file (str): Path to input file. Uses default if not provided.
			dry_run (bool): If True, preview changes without committing.
			warehouse_mapping (dict): Source -> destination warehouse name mapping.
				Used for cross-environment migrations where warehouse names differ.

		Returns:
			dict: Import result with:
				- success (bool): Whether import succeeded
				- dry_run (bool): Whether this was a dry run
				- created (int): Number of monitors created
				- updated (int): Number of monitors updated
				- skipped (int): Number of monitors skipped
				- failed (int): Number of monitors that failed
				- errors (list): Any errors encountered
		"""
		mode = "DRY-RUN" if dry_run else "COMMIT"
		LOGGER.info(f"[{self.entity_name}] Starting import ({mode}) to namespace '{self.namespace}'...")

		try:
			# Get input path
			input_path = Path(input_file) if input_file else self.get_output_path()

			# Validate first
			validation = self.validate(str(input_path))
			if not validation['valid']:
				return self.create_result(
					success=False,
					dry_run=dry_run,
					created=0, updated=0, skipped=0, failed=0,
					errors=validation['errors']
				)

			# Use provided mapping or instance mapping
			mapping = warehouse_mapping or self._warehouse_mapping

			# Delegate to monitors importer
			result = self._importer.import_monitors(
				input_file=str(input_path),
				namespace=self.namespace,
				dry_run=dry_run,
				warehouse_mapping=mapping
			)

			# Normalize result keys for BaseMigrator interface
			normalized_result = self.create_result(
				success=result.get('success', False),
				dry_run=dry_run,
				created=result.get('created', 0),
				updated=result.get('updated', 0),
				skipped=result.get('skipped', 0),
				failed=result.get('failed', 0),
				errors=result.get('errors', [])
			)

			self.log_result('import', normalized_result)
			return normalized_result

		except Exception as e:
			LOGGER.error(f"[{self.entity_name}] Import failed: {e}")
			return self.create_result(
				success=False,
				dry_run=dry_run,
				created=0, updated=0, skipped=0, failed=0,
				errors=[str(e)]
			)

	def validate(self, input_file: str = None) -> dict:
		"""Validate a monitor YAML file.

		Checks:
		- File exists and is readable
		- Valid YAML syntax
		- Has 'montecarlo' root key
		- Contains at least one monitor type

		Args:
			input_file (str): Path to input file. Uses default if not provided.

		Returns:
			dict: Validation result with:
				- valid (bool): Whether file is valid
				- count (int): Number of monitors in file
				- errors (list): Validation errors found
				- warnings (list): Validation warnings found
		"""
		LOGGER.info(f"[{self.entity_name}] Validating input file...")

		errors = []
		warnings = []

		try:
			input_path = Path(input_file) if input_file else self.get_output_path()

			# Check file exists
			if not input_path.is_file():
				errors.append(f"File not found: {input_path}")
				return self.create_result(valid=False, count=0, errors=errors, warnings=warnings)

			# Delegate parsing to importer for validation
			parse_result = self._importer.parse_monitor_yaml(str(input_path))

			if not parse_result['success']:
				errors.extend(parse_result.get('errors', ['Failed to parse YAML']))
				return self.create_result(valid=False, count=0, errors=errors, warnings=warnings)

			monitor_count = parse_result.get('count', 0)

			if monitor_count == 0:
				warnings.append("File contains no monitors")

			# Check for warehouse references (useful for cross-env migrations)
			yaml_content = parse_result.get('yaml_content', {})
			warehouses_found = set()
			montecarlo = yaml_content.get('montecarlo', {})
			for monitor_type, monitors in montecarlo.items():
				if isinstance(monitors, list):
					for monitor in monitors:
						if 'warehouse' in monitor:
							warehouses_found.add(monitor['warehouse'])

			if warehouses_found:
				LOGGER.info(f"[{self.entity_name}] Warehouses referenced: {', '.join(warehouses_found)}")
				if not self._warehouse_mapping:
					warnings.append(
						f"File references warehouses: {', '.join(warehouses_found)}. "
						"Ensure these exist in target environment or provide warehouse_mapping."
					)

			result = self.create_result(
				valid=(len(errors) == 0),
				count=monitor_count,
				errors=errors,
				warnings=warnings
			)
			self.log_result('validate', result)
			return result

		except Exception as e:
			errors.append(f"Validation error: {e}")
			return self.create_result(valid=False, count=0, errors=errors, warnings=warnings)

	def delete_by_namespace(self, namespace: str = None, dry_run: bool = True) -> dict:
		"""Delete all monitors in a namespace.

		Useful for:
		- Rolling back a failed migration
		- Cleaning up test imports
		- Removing monitors before re-import

		Args:
			namespace (str): Namespace to delete from. Uses instance namespace if not provided.
			dry_run (bool): If True, preview what would be deleted.

		Returns:
			dict: Delete result with:
				- success (bool): Whether delete succeeded
				- dry_run (bool): Whether this was a dry run
				- deleted (int): Number of monitors deleted
				- failed (int): Number of monitors that failed to delete
				- errors (list): Any errors encountered
		"""
		target_namespace = namespace or self.namespace
		mode = "DRY-RUN" if dry_run else "COMMIT"
		LOGGER.info(f"[{self.entity_name}] Starting delete ({mode}) for namespace '{target_namespace}'...")

		try:
			result = self._importer.delete_monitors_by_namespace(
				namespace=target_namespace,
				dry_run=dry_run
			)

			if result['success']:
				if dry_run:
					LOGGER.info(f"[{self.entity_name}] Would delete {result['deleted']} monitors")
				else:
					LOGGER.info(f"[{self.entity_name}] Deleted {result['deleted']} monitors")
			else:
				LOGGER.error(f"[{self.entity_name}] Delete failed: {result.get('errors', [])}")

			return result

		except Exception as e:
			LOGGER.error(f"[{self.entity_name}] Delete failed: {e}")
			return self.create_result(
				success=False,
				dry_run=dry_run,
				deleted=0,
				failed=0,
				errors=[str(e)]
			)

	def get_warehouse_mapping_from_export(self, export_result: dict = None) -> dict:
		"""Get warehouse UUID -> name mapping from an export result.

		Useful for setting up warehouse_mapping for cross-environment migrations.

		Args:
			export_result (dict): Result from export(). If None, fetches fresh data.

		Returns:
			dict: Warehouse UUID -> name mapping
		"""
		if export_result and 'warehouse_mapping' in export_result:
			return export_result['warehouse_mapping']

		# Fetch fresh warehouse data
		return self._exporter.get_warehouse_mapping()

