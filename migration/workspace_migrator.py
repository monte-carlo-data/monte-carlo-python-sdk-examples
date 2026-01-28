"""
Workspace Migrator - Unified migration utility for Monte Carlo configurations.

This utility exports and imports MC data observability configurations between
environments, including domains, data products, blocklists, and tags.

Usage:
    # Export all entities
    python migration/workspace_migrator.py export --profile source_env

    # Export specific entities
    python migration/workspace_migrator.py export --profile source_env --entities domains,blocklists

    # Import (dry-run by default)
    python migration/workspace_migrator.py import --profile target_env

    # Import with force (commit changes)
    python migration/workspace_migrator.py import --profile target_env --force yes

    # Validate migration files
    python migration/workspace_migrator.py validate --profile target_env
"""

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import json
import logging
import logging.config
import datetime
from pathlib import Path
from lib.util import Util
from lib.helpers import sdk_helpers
from lib.helpers.logs import LoggingConfigs, LOGGER, LOGS_DIR
from rich.progress import Progress

# Import migrators
from migration.blocklist_migrator import BlocklistMigrator
from migration.domain_migrator import DomainMigrator
from migration.data_product_migrator import DataProductMigrator
from migration.tag_migrator import TagMigrator
from migration.exclusion_window_migrator import ExclusionWindowMigrator
from migration.monitor_migrator import MonitorMigrator
from migration.audience_migrator import AudienceMigrator

# Initialize logger
util_name = os.path.basename(__file__).split('.')[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))

# Store the workspace_migrator log file path
WORKSPACE_MIGRATOR_LOG = LOGS_DIR / f"{util_name}-{datetime.date.today()}.log"

# Available entity types and their import order (dependencies first)
# Note: Monitors are imported last as they may reference other entities (e.g., tags for table monitors)
# Note: Audiences define notification recipients and should be imported before monitors
AVAILABLE_ENTITIES = ['blocklists', 'domains', 'tags', 'exclusion_windows', 'data_products', 'audiences', 'monitors']
IMPLEMENTED_ENTITIES = ['blocklists', 'domains', 'tags', 'exclusion_windows', 'data_products', 'audiences', 'monitors']
IMPORT_ORDER = ['blocklists', 'domains', 'tags', 'exclusion_windows', 'data_products', 'audiences', 'monitors']  # Dependency order


class WorkspaceMigrator(Util):
	"""Unified migration utility for MC configurations.

	Coordinates export, import, and validation of domains, data products,
	and blocklists between Monte Carlo environments.
	"""

	def __init__(self, profile: str, config_file: str = None, progress: Progress = None):
		"""Creates an instance of WorkspaceMigrator.

		Args:
			profile (str): Profile to use from configs.ini.
			config_file (str): Path to the Configuration File.
			progress (Progress): Progress bar.
		"""
		super().__init__(profile, config_file, progress)
		self.progress_bar = progress
		self._migrators = None

	@property
	def migrators(self) -> dict:
		"""Lazy-load migrators to avoid initialization issues."""
		if self._migrators is None:
			self._migrators = {
				'blocklists': BlocklistMigrator(self.profile, progress=self.progress_bar),
				'domains': DomainMigrator(self.profile, progress=self.progress_bar),
				'tags': TagMigrator(self.profile, progress=self.progress_bar),
				'exclusion_windows': ExclusionWindowMigrator(self.profile, progress=self.progress_bar),
				'data_products': DataProductMigrator(self.profile, progress=self.progress_bar),
				'audiences': AudienceMigrator(self.profile, progress=self.progress_bar),
				'monitors': MonitorMigrator(self.profile, progress=self.progress_bar),
			}
		return self._migrators

	def _ensure_workspace_migrator_log_handler(self):
		"""Ensure that workspace_migrator.log file handler is present in root logger."""
		root_logger = logging.getLogger()
		workspace_migrator_log_path = str(WORKSPACE_MIGRATOR_LOG.absolute())

		# Check if workspace_migrator file handler already exists
		handler_exists = False
		for handler in root_logger.handlers:
			if isinstance(handler, logging.FileHandler):
				handler_path = os.path.abspath(handler.baseFilename) if hasattr(handler, 'baseFilename') else None
				if handler_path == workspace_migrator_log_path:
					handler_exists = True
					break

		# Add handler if it doesn't exist
		if not handler_exists:
			file_handler = logging.FileHandler(WORKSPACE_MIGRATOR_LOG, encoding='utf-8')
			file_handler.setLevel(logging.DEBUG)
			formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
			file_handler.setFormatter(formatter)
			root_logger.addHandler(file_handler)
			LOGGER.debug(f"Added workspace_migrator log handler: {workspace_migrator_log_path}")

	def _parse_entities(self, entities_str: str) -> list:
		"""Parse entities string into list of valid entity names.

		Args:
			entities_str (str): Comma-separated entity names or 'all'.

		Returns:
			list: List of valid entity names.
		"""
		if not entities_str or entities_str.lower() == 'all':
			return AVAILABLE_ENTITIES.copy()

		requested = [e.strip().lower() for e in entities_str.split(',')]
		valid = []
		for entity in requested:
			if entity in AVAILABLE_ENTITIES:
				valid.append(entity)
			else:
				LOGGER.warning(f"Unknown entity type '{entity}', skipping. Available: {AVAILABLE_ENTITIES}")

		return valid

	def _set_migrator_directory(self, directory: str):
		"""Set the output/input directory for all migrators.

		Args:
			directory (str): Directory path to use.
		"""
		if directory:
			LOGGER.info(f"Using directory: {directory}")
			for entity, migrator in self.migrators.items():
				if migrator is not None:
					migrator.output_dir = directory

	def run_export(self, entities: list = None, output_dir: str = None):
		"""Run export for specified entities.

		Args:
			entities (list): List of entity types to export. Exports all if None.
			output_dir (str): Directory to write export files. Uses default if None.
		"""
		self._ensure_workspace_migrator_log_handler()

		# Set output directory if provided
		if output_dir:
			self._set_migrator_directory(output_dir)

		entities = entities or AVAILABLE_ENTITIES
		LOGGER.info(f"Starting export for: {', '.join(entities)}")
		LOGGER.info("=" * 50)

		results = {}
		progress_per_entity = 100 / max(len(entities), 1)

		for entity in entities:
			if entity not in self.migrators:
				LOGGER.warning(f"Unknown entity type '{entity}'")
				continue

			migrator = self.migrators[entity]
			if migrator is None:
				LOGGER.info(f"[{entity}] Not yet implemented - skipping")
				self._update_progress(progress_per_entity)
				continue

			LOGGER.info(f"Exporting [{entity}]...")
			try:
				result = migrator.export()
				results[entity] = result

				if result.get('success'):
					LOGGER.info(f"[{entity}] Export successful: {result.get('count', 0)} items")
				else:
					LOGGER.error(f"[{entity}] Export failed")
					for error in result.get('errors', []):
						LOGGER.error(f"  - {error}")

			except Exception as e:
				LOGGER.error(f"[{entity}] Export failed with exception: {e}")
				results[entity] = {'success': False, 'errors': [str(e)]}

			self._update_progress(progress_per_entity)

		# Write manifest
		self._write_manifest(results, 'export')

		LOGGER.info("=" * 50)
		LOGGER.info("Export complete!")
		self._log_summary(results)

	def run_import(
		self,
		entities: list = None,
		input_dir: str = None,
		dry_run: bool = True,
		warehouse_mapping: dict = None
	):
		"""Run import for specified entities.

		Args:
			entities (list): List of entity types to import. Imports all if None.
			input_dir (str): Directory containing import files. Uses default if None.
			dry_run (bool): If True, preview changes without committing.
			warehouse_mapping (dict): Warehouse name mapping for tag migrations.
								    Source name -> destination name.
		"""
		self._ensure_workspace_migrator_log_handler()

		# Set input directory if provided
		if input_dir:
			self._set_migrator_directory(input_dir)

		entities = entities or AVAILABLE_ENTITIES
		mode = "DRY-RUN" if dry_run else "COMMIT"
		LOGGER.info(f"Starting import ({mode}) for: {', '.join(entities)}")
		LOGGER.info("=" * 50)

		# Import in dependency order
		ordered_entities = [e for e in IMPORT_ORDER if e in entities]

		results = {}
		progress_per_entity = 100 / max(len(ordered_entities), 1)

		for entity in ordered_entities:
			if entity not in self.migrators:
				LOGGER.warning(f"Unknown entity type '{entity}'")
				continue

			migrator = self.migrators[entity]
			if migrator is None:
				LOGGER.info(f"[{entity}] Not yet implemented - skipping")
				self._update_progress(progress_per_entity)
				continue

			LOGGER.info(f"Importing [{entity}] ({mode})...")
			try:
				# Pass warehouse_mapping to migrators that support it
				if entity in ('tags', 'monitors') and warehouse_mapping:
					result = migrator.import_data(dry_run=dry_run, warehouse_mapping=warehouse_mapping)
				else:
					result = migrator.import_data(dry_run=dry_run)
				results[entity] = result

				if result.get('success'):
					created = result.get('created', 0)
					updated = result.get('updated', 0)
					skipped = result.get('skipped', 0)
					LOGGER.info(f"[{entity}] Import successful: {created} created, {updated} updated, {skipped} skipped")
				else:
					LOGGER.error(f"[{entity}] Import failed")
					for error in result.get('errors', []):
						LOGGER.error(f"  - {error}")

			except Exception as e:
				LOGGER.error(f"[{entity}] Import failed with exception: {e}")
				results[entity] = {'success': False, 'errors': [str(e)]}

			self._update_progress(progress_per_entity)

		LOGGER.info("=" * 50)
		LOGGER.info(f"Import ({mode}) complete!")
		self._log_summary(results)

		if dry_run:
			LOGGER.info("")
			LOGGER.info("This was a DRY-RUN. To commit changes, run with --force yes")

	def run_validate(self, entities: list = None, input_dir: str = None):
		"""Validate migration files for specified entities.

		Args:
			entities (list): List of entity types to validate. Validates all if None.
			input_dir (str): Directory containing files to validate. Uses default if None.
		"""
		self._ensure_workspace_migrator_log_handler()

		# Set input directory if provided
		if input_dir:
			self._set_migrator_directory(input_dir)

		entities = entities or AVAILABLE_ENTITIES
		LOGGER.info(f"Validating files for: {', '.join(entities)}")
		LOGGER.info("=" * 50)

		results = {}
		all_valid = True
		progress_per_entity = 100 / max(len(entities), 1)

		for entity in entities:
			if entity not in self.migrators:
				LOGGER.warning(f"Unknown entity type '{entity}'")
				continue

			migrator = self.migrators[entity]
			if migrator is None:
				LOGGER.info(f"[{entity}] Not yet implemented - skipping")
				self._update_progress(progress_per_entity)
				continue

			LOGGER.info(f"Validating [{entity}]...")
			try:
				result = migrator.validate()
				results[entity] = result

				if result.get('valid'):
					LOGGER.info(f"[{entity}] Valid: {result.get('count', 0)} items")
					for warning in result.get('warnings', []):
						LOGGER.warning(f"  - {warning}")
				else:
					all_valid = False
					LOGGER.error(f"[{entity}] Invalid")
					for error in result.get('errors', []):
						LOGGER.error(f"  - {error}")

			except Exception as e:
				LOGGER.error(f"[{entity}] Validation failed with exception: {e}")
				results[entity] = {'valid': False, 'errors': [str(e)]}
				all_valid = False

			self._update_progress(progress_per_entity)

		LOGGER.info("=" * 50)
		if all_valid:
			LOGGER.info("Validation complete: All files are valid!")
		else:
			LOGGER.error("Validation complete: Some files have errors")

	def _update_progress(self, advance: float):
		"""Update progress bar if available."""
		if self.progress_bar and self.progress_bar.tasks:
			task_id = self.progress_bar.tasks[0].id
			self.progress_bar.update(task_id, advance=advance)

	def _log_summary(self, results: dict):
		"""Log a summary of operation results."""
		LOGGER.info("")
		LOGGER.info("Summary:")
		for entity, result in results.items():
			status = "✓" if result.get('success', result.get('valid', False)) else "✗"
			LOGGER.info(f"  {status} {entity}")

	def _write_manifest(self, results: dict, operation: str):
		"""Write a manifest file with export metadata.

		Args:
			results (dict): Results from the operation.
			operation (str): The operation performed ('export').
		"""
		if operation != 'export':
			return

		# Get output directory from first available migrator
		output_dir = None
		for migrator in self.migrators.values():
			if migrator is not None:
				output_dir = migrator.output_dir
				break

		if output_dir is None:
			output_dir = Path(__file__).parent / "migration-data-exports"

		manifest_path = Path(output_dir) / "migration_manifest.json"
		manifest_path.parent.mkdir(parents=True, exist_ok=True)

		manifest = {
			'export_timestamp': datetime.datetime.now().isoformat(),
			'source_profile': self.profile,
			'entities': {}
		}

		for entity, result in results.items():
			if result.get('success'):
				manifest['entities'][entity] = {
					'file': result.get('file', f'{entity}.csv'),
					'count': result.get('count', 0),
					'exported_at': datetime.datetime.now().isoformat()
				}

		with open(manifest_path, 'w') as f:
			json.dump(manifest, f, indent=2)

		LOGGER.info(f"Manifest written to: {manifest_path}")


def main(*args, **kwargs):
	"""Main entry point for the workspace migrator."""

	# Capture Command Line Arguments
	parser, subparsers = sdk_helpers.generate_arg_parser(
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

		# Parse entities if provided
		entities_str = getattr(args, 'entities', None)
		entities = util._parse_entities(entities_str) if entities_str else None

		command = args.commands.lower()

		if command == 'export':
			output_dir = getattr(args, 'output_dir', None)
			util.run_export(entities, output_dir=output_dir)

		elif command == 'import':
			input_dir = getattr(args, 'input_dir', None)
			force = getattr(args, 'force', None)
			dry_run = force != 'yes'

			# Parse warehouse mapping from CLI if provided
			warehouse_map_arg = getattr(args, 'warehouse_map', None)
			warehouse_mapping = None
			if warehouse_map_arg:
				from lib.helpers.warehouse_mapping import WarehouseMappingLoader
				warehouse_mapping = WarehouseMappingLoader.parse_cli_mapping(warehouse_map_arg)
				if warehouse_mapping:
					LOGGER.info(f"Using warehouse mapping from CLI: {len(warehouse_mapping)} mapping(s)")

			util.run_import(entities, input_dir=input_dir, dry_run=dry_run, warehouse_mapping=warehouse_mapping)

		elif command == 'validate':
			input_dir = getattr(args, 'input_dir', None)
			util.run_validate(entities, input_dir=input_dir)

	util = WorkspaceMigrator(args.profile)
	run_utility(util, args)


if __name__ == '__main__':
	main()
