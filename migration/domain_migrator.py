"""
Domain Migrator - Export and import domains between MC environments.

Domains are logical groupings of tables that help organize assets in Monte Carlo.
"""

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import csv
from pathlib import Path
from rich.progress import Progress
from lib.util import Tables
from lib.helpers.logs import LOGGER
from migration.base_migrator import BaseMigrator


class DomainMigrator(BaseMigrator, Tables):
	"""Migrator for domains.

	Handles export and import of domains and their table assignments.

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
		# Initialize Tables (which initializes Util for auth/config)
		Tables.__init__(self, profile, config_file, progress)
		# Initialize BaseMigrator
		BaseMigrator.__init__(self, profile, config_file, progress)

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

			# Fetch domains
			LOGGER.info(f"[{self.entity_name}] Fetching domains...")
			domains = self.get_domains()
			LOGGER.info(f"[{self.entity_name}] Found {len(domains)} domains")

			if not domains:
				LOGGER.info(f"[{self.entity_name}] No domains to export")
				return self.create_result(success=True, count=0, file=str(output_path))

			# Write to CSV
			rows_written = 0
			with open(output_path, 'w', newline='') as csvfile:
				writer = csv.writer(csvfile)
				writer.writerow(['domain_name', 'domain_description', 'asset_mcon'])

				progress_per_domain = 50 / max(len(domains), 1)

				for domain in domains:
					LOGGER.info(f"[{self.entity_name}] Processing: {domain.name}")

					# Get assets for this domain
					assets = self._get_domain_assets(domain.uuid)

					if assets:
						for mcon in assets:
							writer.writerow([
								domain.name,
								domain.description or '',
								mcon
							])
							rows_written += 1
					else:
						# Include domains with no assets
						writer.writerow([
							domain.name,
							domain.description or '',
							''
						])
						rows_written += 1

					self.update_progress(progress_per_domain)

			result = self.create_result(
				success=True,
				count=len(domains),
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

			# Parse input file and group by domain
			domains_data = self._parse_input_file(input_path)
			LOGGER.info(f"[{self.entity_name}] Found {len(domains_data)} domains in file")

			# Get existing domains
			LOGGER.info(f"[{self.entity_name}] Fetching existing domains...")
			existing_domains = self.get_domains()
			domain_mapping = {
				domain.name: {
					'uuid': domain.uuid,
					'description': domain.description,
					'assignments': domain.assignments or []
				}
				for domain in existing_domains
			}
			LOGGER.info(f"[{self.entity_name}] Found {len(domain_mapping)} existing domains")

			# Process domains
			created = 0
			updated = 0
			failed = 0

			progress_per_domain = 50 / max(len(domains_data), 1)

			for domain_name, data in domains_data.items():
				existing = domain_mapping.get(domain_name)

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

				if dry_run:
					LOGGER.info(f"[{self.entity_name}] WOULD {action}: {domain_name} with {len(merged_assignments)} assets")
					if is_new:
						created += 1
					else:
						updated += 1
				else:
					try:
						response = self.create_domain(
							name=domain_name,
							assignments=merged_assignments,
							description=data['description'] or (existing['description'] if existing else None),
							uuid=existing['uuid'] if existing else None
						)
						domain = response.domain
						LOGGER.info(f"[{self.entity_name}] {action}D: {domain.name} ({domain.uuid})")

						if is_new:
							created += 1
						else:
							updated += 1

					except Exception as e:
						LOGGER.error(f"[{self.entity_name}] FAILED: {domain_name} - {e}")
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

			# Parse and validate CSV
			domain_count = 0
			domain_names = set()

			with open(input_path, 'r') as csvfile:
				reader = csv.reader(csvfile)

				# Check for header row
				first_row = next(reader, None)
				if first_row is None:
					errors.append("CSV file is empty")
					return self.create_result(valid=False, count=0, errors=errors, warnings=warnings)

				# Determine if first row is header
				has_header = first_row[0].lower() == 'domain_name'
				if not has_header:
					# First row is data, process it
					if len(first_row) < 2:
						errors.append("Row 1: Expected at least 2 columns (domain_name, asset_mcon)")
					else:
						domain_names.add(first_row[0].strip())

				# Validate remaining rows
				for row_num, row in enumerate(reader, start=2 if has_header else 2):
					if len(row) < 2:
						errors.append(f"Row {row_num}: Expected at least 2 columns")
						continue

					domain_name = row[0].strip()
					if not domain_name:
						errors.append(f"Row {row_num}: Missing domain_name")
					else:
						domain_names.add(domain_name)

			domain_count = len(domain_names)

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

	def _get_domain_assets(self, domain_uuid: str) -> list:
		"""Get all asset MCONs for a domain.

		Args:
			domain_uuid (str): UUID of the domain.

		Returns:
			list: List of asset MCONs.
		"""
		mcons = []
		cursor = None

		while True:
			response = self.auth.client(
				self.get_tables(domain_id=domain_uuid, after=cursor)
			).get_tables

			for table in response.edges:
				mcons.append(table.node.mcon)

			if response.page_info.has_next_page:
				cursor = response.page_info.end_cursor
			else:
				break

		return mcons

	def _parse_input_file(self, input_path: Path) -> dict:
		"""Parse the input CSV file and group by domain.

		Args:
			input_path (Path): Path to input CSV file.

		Returns:
			dict: Domains grouped by name with description and mcons.
		"""
		domains_data = {}

		with open(input_path, 'r') as csvfile:
			reader = csv.reader(csvfile)

			# Check for header row
			first_row = next(reader, None)
			if first_row is None:
				return domains_data

			# Determine if first row is header
			has_header = first_row[0].lower() == 'domain_name'

			# Process first row if it's data
			if not has_header:
				self._process_domain_row(first_row, domains_data)

			# Process remaining rows
			for row in reader:
				self._process_domain_row(row, domains_data)

		return domains_data

	def _process_domain_row(self, row: list, domains_data: dict):
		"""Process a single CSV row and add to domains_data.

		Args:
			row (list): CSV row.
			domains_data (dict): Dictionary to update.
		"""
		if len(row) < 2:
			return

		domain_name = row[0].strip()
		if not domain_name:
			return

		# Handle both 2-column (name, mcon) and 3-column (name, desc, mcon) formats
		if len(row) >= 3:
			description = row[1].strip() if row[1] else None
			mcon = row[2].strip() if row[2] else None
		else:
			description = None
			mcon = row[1].strip() if row[1] else None

		if domain_name not in domains_data:
			domains_data[domain_name] = {
				'description': description,
				'mcons': []
			}

		# Update description if we have one and didn't before
		if description and not domains_data[domain_name]['description']:
			domains_data[domain_name]['description'] = description

		# Add MCON if present
		if mcon:
			domains_data[domain_name]['mcons'].append(mcon)

