"""
Bulk Import Monitors - Migration-compatible monitor importer.

Wraps the montecarlo CLI 'monitors apply' command with:
- Dry-run support
- Warehouse mapping for cross-environment migrations
- Name deduplication to avoid conflicts
- Structured result dictionaries
- Delete by namespace for migration rollback

Usage:
    # As a standalone script
    python monitors/bulk_import_monitors.py -p <profile> import -i <input_file> --namespace <ns>
    python monitors/bulk_import_monitors.py -p <profile> delete --namespace <ns>

    # As a module (used by MonitorMigrator)
    from monitors.bulk_import_monitors import BulkImportMonitors
    importer = BulkImportMonitors(profile)
    result = importer.import_monitors(input_file, namespace="migration")
"""

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import logging.config
import subprocess
import tempfile
import uuid
import yaml
from pathlib import Path
from monitors import *
from lib.helpers import sdk_helpers

# Initialize logger
util_name = os.path.splitext(os.path.basename(__file__))[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))


class BulkImportMonitors(Monitors):
	"""Migration-compatible monitor importer.

	Inherits from Monitors base class to access:
	- summarize_apply_results(): Parse CLI output
	- get_warehouses(): Get warehouse information

	Provides migration-specific methods:
	- parse_monitor_yaml(): Validate YAML structure
	- apply_warehouse_mapping(): Replace warehouse names for cross-env migrations
	- deduplicate_monitor_names(): Ensure unique names
	- import_monitors(): Import via montecarlo CLI
	- get_monitors_by_namespace(): Query monitors by namespace
	- delete_monitors_by_namespace(): Delete monitors via CLI for rollback

	Note on namespaces:
		Namespaces in Monte Carlo MaC are logical groupings for monitors, similar
		to CloudFormation stack names. Monitors from different namespaces are
		isolated from each other. Within a namespace, monitor names must be unique
		- the system uses (account, namespace, name) as the unique identifier.
	"""

	def __init__(self, profile: str, config_file: str = None, progress: Progress = None):
		"""Creates an instance of BulkImportMonitors.

		Args:
			profile (str): MC profile name from configs.ini
			config_file (str): Path to the Configuration File.
			progress (Progress): Progress bar.
		"""
		super().__init__(profile, config_file, progress)
		self.progress_bar = progress

	def parse_monitor_yaml(self, input_file: str) -> dict:
		"""Parse and validate a monitors YAML file.

		Args:
			input_file (str): Path to YAML file.

		Returns:
			dict: Result with keys:
				- success (bool): Whether parsing succeeded
				- yaml_content (dict): Parsed YAML content
				- monitor_count (int): Total number of monitors found
				- monitor_types (dict): Count by type (e.g., {'custom_sql': 5})
				- errors (list): Any errors encountered
				- warnings (list): Any warnings found
		"""
		errors = []
		warnings = []

		file_path = Path(input_file)
		if not file_path.is_file():
			return {
				'success': False,
				'yaml_content': None,
				'monitor_count': 0,
				'monitor_types': {},
				'errors': [f"File not found: {input_file}"],
				'warnings': []
			}

		try:
			with open(input_file, 'r') as f:
				yaml_content = yaml.safe_load(f) or {}

			# Validate structure
			if 'montecarlo' not in yaml_content:
				errors.append("Invalid YAML structure: missing 'montecarlo' root key")
				return {
					'success': False,
					'yaml_content': None,
					'monitor_count': 0,
					'monitor_types': {},
					'errors': errors,
					'warnings': warnings
				}

			montecarlo = yaml_content.get('montecarlo', {})
			if not isinstance(montecarlo, dict):
				montecarlo = {}

			# Count monitors by type
			monitor_types = {}
			total_count = 0
			for monitor_type, monitors in montecarlo.items():
				if isinstance(monitors, list):
					monitor_types[monitor_type] = len(monitors)
					total_count += len(monitors)

			if total_count == 0:
				warnings.append("File contains no monitors")

			return {
				'success': True,
				'yaml_content': yaml_content,
				'monitor_count': total_count,
				'monitor_types': monitor_types,
				'errors': errors,
				'warnings': warnings
			}

		except yaml.YAMLError as e:
			errors.append(f"YAML parse error: {e}")
			return {
				'success': False,
				'yaml_content': None,
				'monitor_count': 0,
				'monitor_types': {},
				'errors': errors,
				'warnings': warnings
			}

		except Exception as e:
			errors.append(f"Error reading file: {e}")
			return {
				'success': False,
				'yaml_content': None,
				'monitor_count': 0,
				'monitor_types': {},
				'errors': errors,
				'warnings': warnings
			}

	def apply_warehouse_mapping(self, yaml_content: str, mapping: dict) -> str:
		"""Replace source warehouse names with destination names in YAML.

		Performs string replacement on the YAML content to swap warehouse names.
		This is necessary because monitor YAML files reference warehouses by name.

		Args:
			yaml_content (str): Raw YAML file content as string.
			mapping (dict): Source warehouse name -> destination warehouse name.

		Returns:
			str: Modified YAML content with warehouse names replaced.
		"""
		if not mapping:
			return yaml_content

		result = yaml_content
		for source_name, dest_name in mapping.items():
			if source_name and dest_name and source_name != dest_name:
				result = result.replace(source_name, dest_name)
				LOGGER.debug(f"Replaced warehouse name: {source_name} -> {dest_name}")

		return result

	def deduplicate_monitor_names(self, yaml_content: dict, namespace: str) -> dict:
		"""Ensure all monitors have names, preserving existing ones.

		Monitor uniqueness is enforced by (account_uuid, namespace, rule_name).
		The 'name' field is REQUIRED by the montecarlo CLI for apply.

		Best practice (per MC docs): Preserve original names from export so that
		re-imports result in UPDATEs rather than DELETE+CREATE cycles.

		This method:
		- Preserves existing names (from export with export_name=True)
		- Only generates names for monitors that don't have one
		- Uses deterministic naming (hash-based) for consistency

		Args:
			yaml_content (dict): Parsed YAML content.
			namespace (str): Namespace for name prefix (used only for new names).

		Returns:
			dict: Modified YAML content with all monitors having names.
		"""
		import hashlib

		if 'montecarlo' not in yaml_content:
			return yaml_content

		montecarlo = yaml_content.get('montecarlo', {})

		for monitor_type, monitors in montecarlo.items():
			if not isinstance(monitors, list):
				continue

			for idx, monitor in enumerate(monitors):
				if monitor.get('name'):
					# Preserve existing name - this enables UPDATE instead of DELETE+CREATE
					LOGGER.debug(f"Preserving existing name: {monitor['name']}")
				else:
					# Generate a deterministic name based on content
					# This ensures same monitor definition = same name across imports
					content_str = f"{monitor.get('description', '')}:{monitor.get('warehouse', '')}:{monitor_type}:{idx}"
					content_hash = hashlib.md5(content_str.encode()).hexdigest()[:8]
					monitor['name'] = f"{monitor_type}_{namespace}_{content_hash}"
					LOGGER.debug(f"Generated name: {monitor['name']}")

		return yaml_content

	def import_monitors(
		self,
		input_file: str,
		namespace: str = "migration",
		dry_run: bool = True,
		warehouse_mapping: dict = None
	) -> dict:
		"""Import monitors from YAML file using montecarlo CLI.

		Monitors are labeled with the specified namespace. The namespace is a
		logical grouping identifier - it doesn't need to be pre-created.
		Monitors are uniquely identified by (account, namespace, name).

		The CLI command `montecarlo monitors apply` will:
		- CREATE monitors that exist locally but not in MC under this namespace
		- UPDATE monitors that have changed
		- DELETE monitors in this namespace that are no longer in local config

		Args:
			input_file (str): Path to YAML file.
			namespace (str): Namespace for monitor grouping (default: "migration").
			dry_run (bool): If True, preview changes without committing.
			warehouse_mapping (dict): Source -> destination warehouse name mapping.

		Returns:
			dict: Result with keys:
				- success (bool): Whether import succeeded
				- dry_run (bool): Whether this was a dry run
				- created (int): Number of monitors created
				- updated (int): Number of monitors updated
				- skipped (int): Number of monitors skipped
				- failed (int): Number of monitors that failed
				- errors (list): Any errors encountered
		"""
		mode = "DRY-RUN" if dry_run else "COMMIT"
		LOGGER.info(f"Starting monitor import ({mode})...")

		# Parse and validate input file
		parse_result = self.parse_monitor_yaml(input_file)
		if not parse_result['success']:
			return {
				'success': False,
				'dry_run': dry_run,
				'created': 0,
				'updated': 0,
				'skipped': 0,
				'failed': 0,
				'errors': parse_result['errors']
			}

		yaml_content = parse_result['yaml_content']
		monitor_count = parse_result['monitor_count']

		if monitor_count == 0:
			LOGGER.info("No monitors to import")
			return {
				'success': True,
				'dry_run': dry_run,
				'created': 0,
				'updated': 0,
				'skipped': 0,
				'failed': 0,
				'errors': []
			}

		LOGGER.info(f"Found {monitor_count} monitors to import")

		# Read raw file content for warehouse mapping (string replacement)
		with open(input_file, 'r') as f:
			raw_yaml = f.read()

		# Apply warehouse mapping if provided
		if warehouse_mapping:
			LOGGER.info(f"Applying {len(warehouse_mapping)} warehouse mapping(s)...")
			raw_yaml = self.apply_warehouse_mapping(raw_yaml, warehouse_mapping)
			# Re-parse after mapping
			yaml_content = yaml.safe_load(raw_yaml)

		# Deduplicate monitor names
		namespace_clean = namespace.replace(':', '-').replace(' ', '_')
		yaml_content = self.deduplicate_monitor_names(yaml_content, namespace_clean)

		# Write modified YAML to temp directory for CLI
		# The montecarlo CLI expects this structure:
		#   project-dir/
		#     montecarlo.yml (or montecarlo.yaml) - root config file
		#     montecarlo/
		#       monitors.yml - monitor definitions
		with tempfile.TemporaryDirectory() as temp_dir:
			temp_project = Path(temp_dir)
			montecarlo_dir = temp_project / "montecarlo"
			montecarlo_dir.mkdir(parents=True, exist_ok=True)

			# Extract default_resource from first monitor's warehouse
			default_resource = self._extract_default_warehouse(yaml_content)

			# Create root montecarlo.yml (required by CLI)
			root_config = temp_project / "montecarlo.yml"
			with open(root_config, 'w') as f:
				f.write("version: 1\n")
				if default_resource:
					f.write(f"default_resource: {default_resource}\n")

			# Create monitors.yml in montecarlo/ subdirectory
			monitors_file = montecarlo_dir / "monitors.yml"
			with open(monitors_file, 'w') as f:
				yaml.safe_dump(yaml_content, f, sort_keys=False)

			# Build CLI command
			cmd_args = [
				"montecarlo", "--profile", self.profile,
				"monitors", "apply",
				"--namespace", namespace_clean,
				"--project-dir", str(temp_project)
			]

			if dry_run:
				cmd_args.append("--dry-run")

			LOGGER.info(f"Running: {' '.join(cmd_args)}")

			# Run CLI
			try:
				if dry_run:
					cmd = subprocess.run(cmd_args, capture_output=True, text=True)
				else:
					# Non-dry-run requires confirmation - pass "y"
					cmd = subprocess.run(
						cmd_args,
						capture_output=True,
						text=True,
						input="y"
					)

				if cmd.returncode != 0:
					LOGGER.error(f"CLI stderr: {cmd.stderr}")
					LOGGER.error(f"CLI stdout: {cmd.stdout}")
					return {
						'success': False,
						'dry_run': dry_run,
						'created': 0,
						'updated': 0,
						'skipped': 0,
						'failed': monitor_count,
						'errors': [cmd.stderr or cmd.stdout or "CLI command failed"]
					}

				# Parse CLI output to extract counts
				created, updated, skipped = self._parse_apply_output(cmd.stdout)

				LOGGER.info(cmd.stdout)
				self.summarize_apply_results(cmd.stdout)

				return {
					'success': True,
					'dry_run': dry_run,
					'created': created,
					'updated': updated,
					'skipped': skipped,
					'failed': 0,
					'errors': []
				}

			except FileNotFoundError:
				error = "montecarlo CLI not found. Install with: pip install montecarlodata"
				LOGGER.error(error)
				return {
					'success': False,
					'dry_run': dry_run,
					'created': 0,
					'updated': 0,
					'skipped': 0,
					'failed': monitor_count,
					'errors': [error]
				}

			except Exception as e:
				LOGGER.error(f"Import failed: {e}")
				return {
					'success': False,
					'dry_run': dry_run,
					'created': 0,
					'updated': 0,
					'skipped': 0,
					'failed': monitor_count,
					'errors': [str(e)]
				}

	def _extract_default_warehouse(self, yaml_content: dict) -> str:
		"""Extract the warehouse name from the first monitor in the YAML.

		Used to set default_resource in montecarlo.yml when multiple warehouses exist.

		Args:
			yaml_content (dict): Parsed YAML content.

		Returns:
			str: Warehouse name, or empty string if not found.
		"""
		montecarlo = yaml_content.get('montecarlo', {})
		for monitor_type, monitors in montecarlo.items():
			if isinstance(monitors, list) and monitors:
				for monitor in monitors:
					warehouse = monitor.get('warehouse')
					if warehouse:
						return warehouse
		return ''

	def _parse_apply_output(self, cli_output: str) -> tuple:
		"""Parse montecarlo CLI apply output to extract counts.

		Args:
			cli_output (str): Raw CLI output.

		Returns:
			tuple: (created, updated, skipped) counts
		"""
		import re

		creates = len(re.findall(r" -.*CREATE", cli_output))
		updates = len(re.findall(r" -.*UPDATE", cli_output))
		# No-op operations are counted as skipped
		skipped = len(re.findall(r" -.*NO.?OP", cli_output, re.IGNORECASE))

		return creates, updates, skipped

	def get_monitors_by_namespace(self, namespace: str) -> tuple:
		"""Get all monitors that belong to a specific namespace.

		Uses the getMonitors API with namespace filter.

		Args:
			namespace (str): The namespace to query.

		Returns:
			tuple: (monitor_uuids, raw_monitors) - List of UUIDs and raw objects
		"""
		from pycarlo.core import Query

		namespace_clean = namespace.replace(':', '-').replace(' ', '_')
		LOGGER.info(f"Fetching monitors in namespace '{namespace_clean}'...")

		monitors = []
		raw_items = []
		skip_records = 0

		while True:
			query = Query()
			get_monitors = query.get_monitors(
				limit=self.BATCH,
				offset=skip_records,
				namespaces=[namespace_clean]
			)
			get_monitors.__fields__(
				"uuid", "monitor_type", "name", "namespace",
				"is_paused", "resource_id"
			)

			response = self.auth.client(query).get_monitors

			if len(response) > 0:
				raw_items.extend(response)
				for monitor in response:
					monitors.append(monitor.uuid)

			skip_records += self.BATCH
			if len(response) < self.BATCH:
				break

		LOGGER.info(f"Found {len(monitors)} monitors in namespace '{namespace_clean}'")
		return monitors, raw_items

	def _parse_delete_output(self, cli_output: str) -> int:
		"""Parse montecarlo monitors delete CLI output to extract count.

		Args:
			cli_output (str): Raw CLI output.

		Returns:
			int: Number of monitors deleted/to be deleted.
		"""
		import re

		# Count standalone DELETE operations (e.g., " - DELETE monitor_name")
		# Use word boundary to avoid matching "DELETED" in other contexts
		delete_count = len(re.findall(r"\bDELETE\b", cli_output))
		if delete_count > 0:
			return delete_count

		# Try to find numeric count in output (e.g., "5 monitors")
		match = re.search(r"(\d+)\s+monitor", cli_output, re.IGNORECASE)
		if match:
			return int(match.group(1))

		return 0

	def delete_monitors_by_namespace(
		self,
		namespace: str,
		dry_run: bool = True
	) -> dict:
		"""Delete all monitors labeled with a specific namespace.

		Uses the built-in Monte Carlo CLI command:
		  montecarlo monitors delete --namespace <namespace>

		This is the recommended way to remove all monitors in a namespace,
		useful for rolling back a migration or cleaning up test imports.

		Note: Deleting all monitors in a namespace removes the entire logical
		grouping. This is a destructive operation - use dry_run first.

		Args:
			namespace (str): The namespace to delete monitors from.
			dry_run (bool): If True, preview what would be deleted.

		Returns:
			dict: Result with keys:
				- success (bool): Whether delete succeeded
				- dry_run (bool): Whether this was a dry run
				- deleted (int): Number of monitors deleted
				- failed (int): Number of monitors that failed to delete
				- errors (list): Any errors encountered
		"""
		mode = "DRY-RUN" if dry_run else "COMMIT"
		namespace_clean = namespace.replace(':', '-').replace(' ', '_')
		LOGGER.info(f"Starting monitor delete ({mode}) for namespace '{namespace_clean}'...")

		# Build CLI command
		cmd_args = [
			"montecarlo", "--profile", self.profile,
			"monitors", "delete",
			"--namespace", namespace_clean
		]

		if dry_run:
			cmd_args.append("--dry-run")

		LOGGER.info(f"Running: {' '.join(cmd_args)}")

		try:
			if dry_run:
				cmd = subprocess.run(cmd_args, capture_output=True, text=True)
			else:
				# Non-dry-run requires confirmation - pass "y"
				cmd = subprocess.run(cmd_args, capture_output=True, text=True, input="y")

			LOGGER.info(cmd.stdout)

			if cmd.returncode != 0:
				LOGGER.error(f"CLI stderr: {cmd.stderr}")
				return {
					'success': False,
					'dry_run': dry_run,
					'deleted': 0,
					'failed': 0,
					'errors': [cmd.stderr or cmd.stdout or "CLI command failed"]
				}

			# Parse CLI output for count
			deleted_count = self._parse_delete_output(cmd.stdout)
			LOGGER.info(f"Delete complete: {deleted_count} monitors {'would be ' if dry_run else ''}deleted")

			return {
				'success': True,
				'dry_run': dry_run,
				'deleted': deleted_count,
				'failed': 0,
				'errors': []
			}

		except FileNotFoundError:
			error = "montecarlo CLI not found. Install with: pip install montecarlodata"
			LOGGER.error(error)
			return {
				'success': False,
				'dry_run': dry_run,
				'deleted': 0,
				'failed': 0,
				'errors': [error]
			}

		except Exception as e:
			LOGGER.error(f"Delete failed: {e}")
			return {
				'success': False,
				'dry_run': dry_run,
				'deleted': 0,
				'failed': 0,
				'errors': [str(e)]
			}

	def _parse_convert_output(self, cli_output: str) -> int:
		"""Parse montecarlo monitors convert-to-ui CLI output to extract count.

		Args:
			cli_output (str): Raw CLI output.

		Returns:
			int: Number of monitors converted/to be converted.
		"""
		import re

		# Count standalone CONVERT operations (e.g., " - CONVERT monitor_name")
		# Use word boundary to avoid matching "CONVERTING" or "CONVERTED"
		convert_count = len(re.findall(r"\bCONVERT\b", cli_output))
		if convert_count > 0:
			return convert_count

		# Try to find numeric count in output (e.g., "5 monitors")
		match = re.search(r"(\d+)\s+monitor", cli_output, re.IGNORECASE)
		if match:
			return int(match.group(1))

		return 0

	def convert_to_ui_by_namespace(
		self,
		namespace: str,
		dry_run: bool = True
	) -> dict:
		"""Convert monitors in a namespace from code-deployed to UI-editable.

		Monitors imported via MaC YAML are deployed "as code" and cannot be
		edited in the Monte Carlo UI. This method converts them to UI monitors,
		enabling users to modify them through the interface.

		Uses the built-in Monte Carlo CLI command:
		  montecarlo monitors convert-to-ui --namespace <namespace>

		Args:
			namespace (str): The namespace containing monitors to convert.
			dry_run (bool): If True, preview what would be converted.

		Returns:
			dict: Result with keys:
				- success (bool): Whether conversion succeeded
				- dry_run (bool): Whether this was a dry run
				- converted (int): Number of monitors converted
				- failed (int): Number of monitors that failed
				- errors (list): Any errors encountered
		"""
		mode = "DRY-RUN" if dry_run else "COMMIT"
		namespace_clean = namespace.replace(':', '-').replace(' ', '_')
		LOGGER.info(f"Starting monitor convert-to-ui ({mode}) for namespace '{namespace_clean}'...")

		# Build CLI command
		cmd_args = [
			"montecarlo", "--profile", self.profile,
			"monitors", "convert-to-ui",
			"--namespace", namespace_clean
		]

		if dry_run:
			cmd_args.append("--dry-run")

		LOGGER.info(f"Running: {' '.join(cmd_args)}")

		try:
			if dry_run:
				cmd = subprocess.run(cmd_args, capture_output=True, text=True)
			else:
				# Non-dry-run requires confirmation - pass "y"
				cmd = subprocess.run(cmd_args, capture_output=True, text=True, input="y")

			LOGGER.info(cmd.stdout)

			if cmd.returncode != 0:
				LOGGER.error(f"CLI stderr: {cmd.stderr}")
				return {
					'success': False,
					'dry_run': dry_run,
					'converted': 0,
					'failed': 0,
					'errors': [cmd.stderr or cmd.stdout or "CLI command failed"]
				}

			# Parse CLI output for count
			converted_count = self._parse_convert_output(cmd.stdout)
			LOGGER.info(f"Convert-to-ui complete: {converted_count} monitors {'would be ' if dry_run else ''}converted")

			return {
				'success': True,
				'dry_run': dry_run,
				'converted': converted_count,
				'failed': 0,
				'errors': []
			}

		except FileNotFoundError:
			error = "montecarlo CLI not found. Install with: pip install montecarlodata"
			LOGGER.error(error)
			return {
				'success': False,
				'dry_run': dry_run,
				'converted': 0,
				'failed': 0,
				'errors': [error]
			}

		except Exception as e:
			LOGGER.error(f"Convert-to-ui failed: {e}")
			return {
				'success': False,
				'dry_run': dry_run,
				'converted': 0,
				'failed': 0,
				'errors': [str(e)]
			}


def main(*args, **kwargs):
	"""Main entry point for the bulk import monitors utility."""

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

		command = args.commands.lower() if hasattr(args, 'commands') else 'import'
		namespace = getattr(args, 'namespace', 'migration')
		force = getattr(args, 'force', None)
		dry_run = force != 'yes'

		if command == 'import':
			input_file = getattr(args, 'input_file', None)
			if not input_file:
				LOGGER.error("Input file required for import. Use -i <file>")
				return

			# Resolve warehouse mapping: CLI > file in input directory
			from lib.helpers.warehouse_mapping import WarehouseMappingLoader
			from pathlib import Path

			warehouse_map_arg = getattr(args, 'warehouse_map', None)
			warehouse_mapping = None

			if warehouse_map_arg:
				# Priority 1: CLI argument
				warehouse_mapping = WarehouseMappingLoader.parse_cli_mapping(warehouse_map_arg)
				if warehouse_mapping:
					LOGGER.info(f"Using warehouse mapping from CLI: {len(warehouse_mapping)} mapping(s)")
			else:
				# Priority 2: warehouse_mapping.json in input directory
				input_dir = str(Path(input_file).parent)
				warehouse_mapping = WarehouseMappingLoader.load_from_file(input_dir)
				if warehouse_mapping:
					LOGGER.info(f"Using shared warehouse_mapping.json: {len(warehouse_mapping)} mapping(s)")

			result = util.import_monitors(
				input_file,
				namespace=namespace,
				dry_run=dry_run,
				warehouse_mapping=warehouse_mapping
			)

			if result['success']:
				LOGGER.info(f"Import completed: {result['created']} created, {result['updated']} updated")
			else:
				LOGGER.error(f"Import failed: {result['errors']}")

			if dry_run:
				LOGGER.info("")
				LOGGER.info("This was a DRY-RUN. No changes were made. Use --force yes to apply.")

		elif command == 'delete':
			result = util.delete_monitors_by_namespace(namespace, dry_run=dry_run)

			if result['success']:
				LOGGER.info(f"Delete completed: {result['deleted']} deleted")
			else:
				LOGGER.error(f"Delete failed: {result['errors']}")

			if dry_run:
				LOGGER.info("")
				LOGGER.info("This was a DRY-RUN. No changes were made. Use --force yes to delete.")

		elif command == 'convert-to-ui':
			result = util.convert_to_ui_by_namespace(namespace, dry_run=dry_run)

			if result['success']:
				LOGGER.info(f"Convert-to-ui completed: {result['converted']} converted")
			else:
				LOGGER.error(f"Convert-to-ui failed: {result['errors']}")

			if dry_run:
				LOGGER.info("")
				LOGGER.info("This was a DRY-RUN. No changes were made. Use --force yes to convert.")

	util = BulkImportMonitors(args.profile)
	run_utility(util, args)


if __name__ == '__main__':
	main()

