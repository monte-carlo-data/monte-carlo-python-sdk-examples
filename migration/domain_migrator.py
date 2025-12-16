"""
Domain Migrator - Export and import domains between MC environments.

Domains are logical groupings of tables that help organize assets in Monte Carlo.

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
from admin.bulk_domain_exporter import BulkDomainExporter
from admin.bulk_domain_importerv2 import BulkDomainImporterV2


class DomainMigrator(BaseMigrator):
	"""Migrator for domains.

	Handles export and import of domains and their table assignments.
	Delegates core operations to admin bulk scripts.

	CSV Format:
		domain_name,domain_description,asset_mcon
	"""

	def __init__(self, profile: str, config_file: str = None, progress: Progress = None):
		"""Initialize the DomainMigrator.

		Args:
			profile (str): MC profile name from configs.ini
			config_file (str): Path to configuration file (optional)
			progress (Progress): Rich progress bar instance (optional)
		"""
		# Initialize BaseMigrator
		BaseMigrator.__init__(self, profile, config_file, progress)

		# Composition: use admin classes for core operations
		self._exporter = BulkDomainExporter(profile, config_file, progress)
		self._importer = BulkDomainImporterV2(profile, config_file, progress)

	@property
	def entity_name(self) -> str:
		return "domains"

	@property
	def output_filename(self) -> str:
		return "domains.csv"

	def export(self, output_file: str = None) -> dict:
		"""Export all domains and their assets to CSV.

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

			# Delegate to admin exporter to fetch domain data
			LOGGER.info(f"[{self.entity_name}] Fetching domains...")
			domains_data = self._exporter.get_all_domains_with_assets()
			LOGGER.info(f"[{self.entity_name}] Found {len(domains_data)} domains")

			if not domains_data:
				LOGGER.info(f"[{self.entity_name}] No domains to export")
				return self.create_result(success=True, count=0, file=str(output_path))

			# Write to CSV in migration format (with header)
			rows_written = 0
			with open(output_path, 'w', newline='') as csvfile:
				writer = csv.writer(csvfile)
				writer.writerow(['domain_name', 'domain_description', 'asset_mcon'])

				progress_per_domain = 50 / max(len(domains_data), 1)

				for domain in domains_data:
					LOGGER.info(f"[{self.entity_name}] Processing: {domain['name']}")

					if domain['assets']:
						for mcon in domain['assets']:
							writer.writerow([
								domain['name'],
								domain['description'],
								mcon
							])
							rows_written += 1
					else:
						# Include domains with no assets
						writer.writerow([
							domain['name'],
							domain['description'],
							''
						])
						rows_written += 1

					self.update_progress(progress_per_domain)

			result = self.create_result(
				success=True,
				count=len(domains_data),
				rows=rows_written,
				file=str(output_path)
			)
			self.log_result('export', result)
			return result

		except Exception as e:
			LOGGER.error(f"[{self.entity_name}] Export failed: {e}")
			return self.create_result(success=False, count=0, errors=[str(e)])

	def import_data(self, input_file: str = None, dry_run: bool = True) -> dict:
		"""Import domains from CSV.

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
			parse_result = self._importer.parse_domain_csv(str(input_path))
			if not parse_result['success']:
				return self.create_result(
					success=False,
					dry_run=dry_run,
					created=0, updated=0, skipped=0, failed=0,
					errors=parse_result['errors']
				)

			domains_data = parse_result['domains']
			LOGGER.info(f"[{self.entity_name}] Found {len(domains_data)} domains in file")

			# Delegate getting existing domains to admin importer
			LOGGER.info(f"[{self.entity_name}] Fetching existing domains...")
			existing_domains = self._importer.get_existing_domains_map()
			LOGGER.info(f"[{self.entity_name}] Found {len(existing_domains)} existing domains")

			# Process domains
			created = 0
			updated = 0
			failed = 0

			progress_per_domain = 50 / max(len(domains_data), 1)

			for domain_name, data in domains_data.items():
				existing = existing_domains.get(domain_name)

				if existing:
					# Merge assignments (don't replace)
					merged_assignments = list(set(data['mcons'] + existing['assignments']))
					action = "UPDATE"
					is_new = False
				else:
					merged_assignments = data['mcons']
					action = "CREATE"
					is_new = True

				# Filter out empty MCONs
				merged_assignments = [m for m in merged_assignments if m]

				# Determine description (from file, or existing)
				description = data['description'] or (existing['description'] if existing else None)

				if dry_run:
					LOGGER.info(f"[{self.entity_name}] WOULD {action}: {domain_name} with {len(merged_assignments)} assets")
					if is_new:
						created += 1
					else:
						updated += 1
				else:
					# Delegate actual import to admin importer
					result = self._importer.import_single_domain(
						name=domain_name,
						mcons=merged_assignments,
						description=description,
						uuid=existing['uuid'] if existing else None,
						merge_assignments=False,  # We already merged above
						existing_assignments=None
					)

					if result['success']:
						LOGGER.info(f"[{self.entity_name}] {action}D: {domain_name} ({result['domain_uuid']})")
						if is_new:
							created += 1
						else:
							updated += 1
					else:
						LOGGER.error(f"[{self.entity_name}] FAILED: {domain_name} - {result['error']}")
						failed += 1

				self.update_progress(progress_per_domain)

			result = self.create_result(
				success=(failed == 0),
				dry_run=dry_run,
				created=created,
				updated=updated,
				skipped=0,
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
		"""Validate a domain CSV file.

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
			parse_result = self._importer.parse_domain_csv(str(input_path))

			if not parse_result['success']:
				errors.extend(parse_result['errors'])
				return self.create_result(valid=False, count=0, errors=errors, warnings=warnings)

			domain_count = parse_result['count']

			if domain_count == 0:
				warnings.append("File contains no valid domains")

			result = self.create_result(
				valid=(len(errors) == 0),
				count=domain_count,
				errors=errors,
				warnings=warnings
			)
			self.log_result('validate', result)
			return result

		except Exception as e:
			errors.append(f"Validation error: {e}")
			return self.create_result(valid=False, count=0, errors=errors, warnings=warnings)
