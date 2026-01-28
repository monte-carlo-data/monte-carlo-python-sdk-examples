"""
Audience Migrator - Export and import notification audiences between MC environments.

Notification audiences define who receives alerts and how (email, Slack, Teams, etc.).

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
from admin.bulk_audience_exporter import BulkAudienceExporter
from admin.bulk_audience_importer import BulkAudienceImporter


class AudienceMigrator(BaseMigrator):
	"""Migrator for notification audiences.

	Handles export and import of notification audiences and their settings.
	Delegates core operations to admin bulk scripts.

	CSV Format:
		audience_name,notification_type,recipients,recipients_display_names,integration_id
	"""

	def __init__(self, profile: str, config_file: str = None, progress: Progress = None):
		"""Initialize the AudienceMigrator.

		Args:
			profile (str): MC profile name from configs.ini
			config_file (str): Path to configuration file (optional)
			progress (Progress): Rich progress bar instance (optional)
		"""
		# Initialize BaseMigrator
		BaseMigrator.__init__(self, profile, config_file, progress)

		# Composition: use admin classes for core operations
		self._exporter = BulkAudienceExporter(profile, config_file, progress)
		self._importer = BulkAudienceImporter(profile, config_file, progress)

	@property
	def entity_name(self) -> str:
		return "audiences"

	@property
	def output_filename(self) -> str:
		return "audiences.csv"

	def export(self, output_file: str = None) -> dict:
		"""Export all notification audiences to CSV.

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

			# Delegate to admin exporter to fetch audience data
			LOGGER.info(f"[{self.entity_name}] Fetching notification audiences...")
			audiences_data = self._exporter.get_all_audiences()
			LOGGER.info(f"[{self.entity_name}] Found {len(audiences_data)} audiences")

			if not audiences_data:
				LOGGER.info(f"[{self.entity_name}] No audiences to export")
				return self.create_result(success=True, count=0, file=str(output_path))

			# Write to CSV in migration format (with header)
			rows_written = 0
			with open(output_path, 'w', newline='') as csvfile:
				writer = csv.writer(csvfile)
				writer.writerow([
					'audience_name', 'notification_type', 'recipients',
					'recipients_display_names', 'integration_id'
				])

				progress_per_audience = 50 / max(len(audiences_data), 1)

				for audience in audiences_data:
					LOGGER.info(f"[{self.entity_name}] Processing: {audience['name']}")

					if audience['notification_settings']:
						for ns in audience['notification_settings']:
							# Join list fields with semicolons for CSV
							recipients_str = ';'.join(ns['recipients']) if ns['recipients'] else ''
							display_names_str = ';'.join(ns['recipients_display_names']) if ns['recipients_display_names'] else ''

							writer.writerow([
								audience['name'],
								ns['type'],
								recipients_str,
								display_names_str,
								ns['integration_id'] or ''
							])
							rows_written += 1
					else:
						# Include audiences with no notification settings
						writer.writerow([
							audience['name'],
							'', '', '', ''
						])
						rows_written += 1

					self.update_progress(progress_per_audience)

			result = self.create_result(
				success=True,
				count=len(audiences_data),
				rows=rows_written,
				file=str(output_path)
			)
			self.log_result('export', result)
			return result

		except Exception as e:
			LOGGER.error(f"[{self.entity_name}] Export failed: {e}")
			return self.create_result(success=False, count=0, errors=[str(e)])

	def import_data(self, input_file: str = None, dry_run: bool = True) -> dict:
		"""Import notification audiences from CSV.

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
			parse_result = self._importer.parse_audience_csv(str(input_path))
			if not parse_result['success']:
				return self.create_result(
					success=False,
					dry_run=dry_run,
					created=0, updated=0, skipped=0, failed=0,
					errors=parse_result['errors']
				)

			audiences_data = parse_result['audiences']
			LOGGER.info(f"[{self.entity_name}] Found {len(audiences_data)} audiences in file")

			# Delegate getting existing audiences to admin importer
			LOGGER.info(f"[{self.entity_name}] Fetching existing audiences...")
			existing_audiences = self._importer.get_existing_audiences_map()
			LOGGER.info(f"[{self.entity_name}] Found {len(existing_audiences)} existing audiences")

			# Process audiences
			created = 0
			skipped = 0
			failed = 0

			progress_per_audience = 50 / max(len(audiences_data), 1)

			for audience_name, data in audiences_data.items():
				existing = existing_audiences.get(audience_name)
				settings = data.get('notification_settings', [])
				settings_count = len(settings)

				if dry_run:
					if existing:
						LOGGER.info(f"[{self.entity_name}] WOULD SKIP: {audience_name} (already exists)")
						skipped += 1
					else:
						LOGGER.info(f"[{self.entity_name}] WOULD CREATE: {audience_name} with {settings_count} notification setting(s)")
						created += 1
				else:
					if existing:
						# Audience already exists - skip (no update needed for migration)
						LOGGER.info(f"[{self.entity_name}] SKIP: {audience_name} (already exists)")
						skipped += 1
					else:
						# Delegate actual import to admin importer
						result = self._importer.import_single_audience(
							name=audience_name,
							notification_settings=settings,
							uuid=None
						)

						if result['success']:
							LOGGER.info(f"[{self.entity_name}] CREATED: {audience_name} ({result['audience_uuid']})")
							created += 1
						else:
							LOGGER.error(f"[{self.entity_name}] FAILED: {audience_name} - {result['error']}")
							failed += 1

				self.update_progress(progress_per_audience)

			result = self.create_result(
				success=(failed == 0),
				dry_run=dry_run,
				created=created,
				updated=0,  # Audiences are not updated, only created or skipped
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
		"""Validate an audience CSV file.

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
			parse_result = self._importer.parse_audience_csv(str(input_path))

			if not parse_result['success']:
				errors.extend(parse_result['errors'])
				return self.create_result(valid=False, count=0, errors=errors, warnings=warnings)

			audience_count = parse_result['count']

			if audience_count == 0:
				warnings.append("File contains no valid audiences")

			result = self.create_result(
				valid=(len(errors) == 0),
				count=audience_count,
				errors=errors,
				warnings=warnings
			)
			self.log_result('validate', result)
			return result

		except Exception as e:
			errors.append(f"Validation error: {e}")
			return self.create_result(valid=False, count=0, errors=errors, warnings=warnings)
