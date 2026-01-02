"""
Tag Migrator - Export and import object tags between MC environments.

Tags (object properties) are key-value pairs attached to tables and fields
that help organize and categorize assets in Monte Carlo.

This migrator uses composition to delegate core operations to admin scripts,
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
from admin.bulk_tag_exporterv2 import BulkTagExporterV2
from admin.bulk_tag_importerv2 import BulkTagImporterV2


class TagMigrator(BaseMigrator):
	"""Migrator for object tags (properties).

	Handles export and import of table/field tags.
	Delegates core operations to admin bulk scripts.

	CSV Format:
		warehouse_id,full_table_id,tag_key,tag_value
	"""

	def __init__(self, profile: str, config_file: str = None, progress: Progress = None):
		"""Initialize the TagMigrator.

		Args:
			profile (str): MC profile name from configs.ini
			config_file (str): Path to configuration file (optional)
			progress (Progress): Rich progress bar instance (optional)
		"""
		# Initialize BaseMigrator
		BaseMigrator.__init__(self, profile, config_file, progress)

		# Composition: use admin classes for core operations
		self._exporter = BulkTagExporterV2(profile, config_file, progress)
		self._importer = BulkTagImporterV2(profile, config_file, progress)

	@property
	def entity_name(self) -> str:
		return "tags"

	@property
	def output_filename(self) -> str:
		return "tags.csv"

	def export(self, output_file: str = None) -> dict:
		"""Export all tags to CSV.

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

			# Delegate to admin exporter to fetch tag data
			LOGGER.info(f"[{self.entity_name}] Fetching tags from all warehouses...")
			tags_data = self._exporter.get_all_tags()
			LOGGER.info(f"[{self.entity_name}] Found {len(tags_data)} tags")

			if not tags_data:
				LOGGER.info(f"[{self.entity_name}] No tags to export")
				# Still write header for consistency
				with open(output_path, 'w', newline='') as csvfile:
					writer = csv.writer(csvfile)
					writer.writerow(['warehouse_id', 'full_table_id', 'tag_key', 'tag_value'])
				return self.create_result(success=True, count=0, file=str(output_path))

			# Write to CSV in migration format (with header)
			with open(output_path, 'w', newline='') as csvfile:
				writer = csv.writer(csvfile)
				writer.writerow(['warehouse_id', 'full_table_id', 'tag_key', 'tag_value'])

				for tag in tags_data:
					writer.writerow([
						tag['warehouse_id'],
						tag['full_table_id'],
						tag['tag_key'],
						tag['tag_value']
					])

			self.update_progress(50)

			# Count unique tables for summary
			unique_tables = len(set(tag['full_table_id'] for tag in tags_data))

			result = self.create_result(
				success=True,
				count=len(tags_data),
				tables=unique_tables,
				file=str(output_path)
			)
			self.log_result('export', result)
			return result

		except Exception as e:
			LOGGER.error(f"[{self.entity_name}] Export failed: {e}")
			return self.create_result(success=False, count=0, errors=[str(e)])

	def import_data(self, input_file: str = None, dry_run: bool = True) -> dict:
		"""Import tags from CSV.

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

			# Delegate to admin importer
			import_result = self._importer.import_tags(str(input_path), dry_run=dry_run)

			self.update_progress(50)

			result = self.create_result(
				success=import_result['success'],
				dry_run=dry_run,
				created=import_result['created'],
				updated=0,  # Tags don't distinguish between create/update
				skipped=import_result['skipped'],
				failed=import_result['failed'],
				errors=import_result.get('errors', [])
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
		"""Validate a tag CSV file.

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
			parse_result = self._importer.parse_tag_csv(str(input_path))

			if not parse_result['success']:
				errors.extend(parse_result['errors'])
				return self.create_result(valid=False, count=0, errors=errors, warnings=warnings)

			tag_count = parse_result['count']

			if tag_count == 0:
				warnings.append("File contains no valid tags")

			# Include any warnings from parsing (e.g., duplicates)
			if parse_result.get('warnings'):
				warnings.extend(parse_result['warnings'])

			# Count unique tables for info
			unique_tables = len(set(tag['full_table_id'] for tag in parse_result['tags']))
			duplicates = parse_result.get('duplicates', 0)
			LOGGER.info(f"[{self.entity_name}] Found {tag_count} tags across {unique_tables} tables ({duplicates} duplicates)")

			result = self.create_result(
				valid=(len(errors) == 0),
				count=tag_count,
				tables=unique_tables,
				errors=errors,
				warnings=warnings
			)
			self.log_result('validate', result)
			return result

		except Exception as e:
			errors.append(f"Validation error: {e}")
			return self.create_result(valid=False, count=0, errors=errors, warnings=warnings)

