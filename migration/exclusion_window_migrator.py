"""
Exclusion Window Migrator - Export and import data maintenance entries between MC environments.

Exclusion windows (data maintenance entries) define time periods when anomaly detection
is suppressed for specific assets or connections.

This migrator uses composition to delegate core operations to admin bulk scripts,
avoiding code duplication while adding migration-specific features like dry-run
support and structured results.
"""

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import csv
from pathlib import Path
from rich.progress import Progress
from lib.helpers.logs import LOGGER
from migration.base_migrator import BaseMigrator
from admin.bulk_exclusion_window_exporter import BulkExclusionWindowExporter
from admin.bulk_exclusion_window_importer import BulkExclusionWindowImporter


class ExclusionWindowMigrator(BaseMigrator):
	"""Migrator for exclusion windows (data maintenance entries).

	Handles export and import of scheduled maintenance windows.
	Delegates core operations to admin bulk scripts.

	CSV Format:
		id,resource_uuid,scope,database,dataset,full_table_id,start_time,end_time,reason,reason_type
	"""

	def __init__(self, profile: str, config_file: str = None, progress: Progress = None):
		"""Initialize the ExclusionWindowMigrator.

		Args:
			profile (str): MC profile name from configs.ini
			config_file (str): Path to configuration file (optional)
			progress (Progress): Rich progress bar instance (optional)
		"""
		# Initialize BaseMigrator
		BaseMigrator.__init__(self, profile, config_file, progress)

		# Composition: use admin classes for core operations
		self._exporter = BulkExclusionWindowExporter(profile, config_file, progress)
		self._importer = BulkExclusionWindowImporter(profile, config_file, progress)

	@property
	def entity_name(self) -> str:
		return "exclusion_windows"

	@property
	def output_filename(self) -> str:
		return "exclusion_windows.csv"

	def export(self, output_file: str = None) -> dict:
		"""Export all exclusion windows to CSV.

		Args:
			output_file (str): Path to output file. Uses default if not provided.

		Returns:
			dict: Export result with success, count, file, and errors.
		"""
		LOGGER.info(f"[{self.entity_name}] Starting export...")

		try:
			# Ensure output directory exists
			self.ensure_output_dir()

			# Get output path
			output_path = Path(output_file) if output_file else self.get_output_path()

			# Delegate to admin exporter to fetch exclusion window data
			LOGGER.info(f"[{self.entity_name}] Fetching exclusion windows...")
			windows_data = self._exporter.get_all_exclusion_windows()
			LOGGER.info(f"[{self.entity_name}] Found {len(windows_data)} exclusion windows")

			if not windows_data:
				LOGGER.info(f"[{self.entity_name}] No exclusion windows to export")
				# Still write header for consistency
				with open(output_path, 'w', newline='') as csvfile:
					writer = csv.writer(csvfile)
					writer.writerow([
						'id', 'resource_uuid', 'scope', 'database', 'dataset', 'full_table_id',
						'start_time', 'end_time', 'reason', 'reason_type'
					])
				return self.create_result(success=True, count=0, file=str(output_path))

			# Write to CSV in migration format (with header)
			with open(output_path, 'w', newline='') as csvfile:
				writer = csv.writer(csvfile)
				writer.writerow([
					'id', 'resource_uuid', 'scope', 'database', 'dataset', 'full_table_id',
					'start_time', 'end_time', 'reason', 'reason_type'
				])

				progress_per_window = 50 / max(len(windows_data), 1)

				for window in windows_data:
					writer.writerow([
						window['id'],
						window['resource_uuid'],
						window['scope'],
						window['database'],
						window['dataset'],
						window['full_table_id'],
						window['start_time'],
						window['end_time'],
						window['reason'],
						window['reason_type']
					])
					self.update_progress(progress_per_window)

			result = self.create_result(
				success=True,
				count=len(windows_data),
				file=str(output_path)
			)
			self.log_result('export', result)
			return result

		except Exception as e:
			LOGGER.error(f"[{self.entity_name}] Export failed: {e}")
			return self.create_result(success=False, count=0, errors=[str(e)])

	def import_data(self, input_file: str = None, dry_run: bool = True) -> dict:
		"""Import exclusion windows from CSV.

		Args:
			input_file (str): Path to input file. Uses default if not provided.
			dry_run (bool): If True, preview changes without committing.

		Returns:
			dict: Import result with success, created, updated, skipped, failed, errors.
		"""
		mode = "DRY-RUN" if dry_run else "COMMIT"
		LOGGER.info(f"[{self.entity_name}] Starting import ({mode})...")

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

			# Delegate parsing to admin importer
			parse_result = self._importer.parse_exclusion_window_csv(str(input_path))
			if not parse_result['success']:
				return self.create_result(
					success=False,
					dry_run=dry_run,
					created=0, updated=0, skipped=0, failed=0,
					errors=parse_result['errors']
				)

			windows = parse_result['windows']
			LOGGER.info(f"[{self.entity_name}] Found {len(windows)} exclusion windows in file")

			# Delegate getting existing windows to admin importer
			LOGGER.info(f"[{self.entity_name}] Fetching existing exclusion windows...")
			existing_map = self._importer.get_existing_exclusion_windows_map()
			LOGGER.info(f"[{self.entity_name}] Found {len(existing_map)} existing windows")

			# Process windows
			created = 0
			skipped = 0
			failed = 0

			progress_per_window = 50 / max(len(windows), 1)

			for window in windows:
				# Check for duplicates using the same key as the importer
				key = (
					window['resource_uuid'],
					window['full_table_id'],
					window['start_time'],
					window['end_time']
				)

				if key in existing_map:
					LOGGER.debug(f"[{self.entity_name}] SKIP (exists): {window['scope']} window")
					skipped += 1
					self.update_progress(progress_per_window)
					continue

				scope_desc = window['scope'] or 'WAREHOUSE'
				target = window['full_table_id'] or window['resource_uuid'][:8] + '...'

				if dry_run:
					LOGGER.info(f"[{self.entity_name}] WOULD CREATE: {scope_desc} window on {target}")
					created += 1
				else:
					# Delegate actual creation to admin importer
					result = self._importer.create_exclusion_window(window)

					if result['success']:
						LOGGER.info(f"[{self.entity_name}] CREATED: {scope_desc} window (id={result['id']}) on {target}")
						created += 1
						# Add to existing map to prevent duplicates in same run
						existing_map[key] = window
					else:
						LOGGER.error(f"[{self.entity_name}] FAILED: {scope_desc} window on {target} - {result['error']}")
						failed += 1

				self.update_progress(progress_per_window)

			result = self.create_result(
				success=(failed == 0),
				dry_run=dry_run,
				created=created,
				updated=0,  # Exclusion windows don't have updates, only create/skip
				skipped=skipped,
				failed=failed
			)
			self.log_result('import', result)
			return result

		except Exception as e:
			LOGGER.error(f"[{self.entity_name}] Import failed: {e}")
			return self.create_result(
				success=False,
				dry_run=dry_run,
				created=0, updated=0, skipped=0, failed=0,
				errors=[str(e)]
			)

	def validate(self, input_file: str = None) -> dict:
		"""Validate an exclusion window CSV file.

		Uses the admin importer's parsing to validate the file structure.

		Args:
			input_file (str): Path to input file. Uses default if not provided.

		Returns:
			dict: Validation result with valid, count, errors, warnings.
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

			# Delegate parsing to admin importer for validation
			parse_result = self._importer.parse_exclusion_window_csv(str(input_path))

			if not parse_result['success']:
				errors.extend(parse_result['errors'])
				return self.create_result(valid=False, count=0, errors=errors, warnings=warnings)

			window_count = parse_result['count']

			if window_count == 0:
				warnings.append("File contains no valid exclusion windows")

			# Count by scope for summary
			scope_counts = {}
			for window in parse_result['windows']:
				scope = window.get('scope', 'WAREHOUSE')
				scope_counts[scope] = scope_counts.get(scope, 0) + 1

			scope_summary = ', '.join(f"{count} {scope}" for scope, count in scope_counts.items())
			LOGGER.info(f"[{self.entity_name}] Found {window_count} windows ({scope_summary})")

			result = self.create_result(
				valid=(len(errors) == 0),
				count=window_count,
				errors=errors,
				warnings=warnings
			)
			self.log_result('validate', result)
			return result

		except Exception as e:
			errors.append(f"Validation error: {e}")
			return self.create_result(valid=False, count=0, errors=errors, warnings=warnings)

