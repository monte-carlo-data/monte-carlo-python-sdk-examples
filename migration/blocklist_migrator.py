"""
Blocklist Migrator - Export and import blocklist entries between MC environments.

Blocklists control which tables/datasets/projects are included or excluded from
Monte Carlo's data observability monitoring.
"""

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import csv
from pathlib import Path
from collections import defaultdict
from rich.progress import Progress
from lib.util import Admin
from lib.helpers.logs import LOGGER
from migration.base_migrator import BaseMigrator


class BlocklistMigrator(BaseMigrator, Admin):
	"""Migrator for blocklist entries.

	Handles export and import of blocklist entries (ingestion rules) that control
	which tables, datasets, or projects are monitored by Monte Carlo.

	CSV Format:
		resource_id,target_object_type,match_type,dataset,project,effect
	"""

	def __init__(self, profile: str, config_file: str = None, progress: Progress = None):
		"""Initialize the BlocklistMigrator.

		Args:
			profile (str): MC profile name from configs.ini
			config_file (str): Path to configuration file (optional)
			progress (Progress): Rich progress bar instance (optional)
		"""
		# Initialize Admin (which initializes Util for auth/config)
		Admin.__init__(self, profile, config_file, progress)
		# Initialize BaseMigrator
		BaseMigrator.__init__(self, profile, config_file, progress)

	@property
	def entity_name(self) -> str:
		return "blocklists"

	@property
	def output_filename(self) -> str:
		return "blocklists.csv"

	def export(self, output_file: str = None) -> dict:
		"""Export all blocklist entries to CSV.

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

			# Fetch blocklist entries
			LOGGER.info(f"[{self.entity_name}] Fetching blocklist entries...")
			entries = self.get_blocklist_entries()
			LOGGER.info(f"[{self.entity_name}] Found {len(entries)} blocklist entries")

			if not entries:
				LOGGER.info(f"[{self.entity_name}] No blocklist entries to export")
				return self.create_result(success=True, count=0, file=str(output_path))

			# Write to CSV
			with open(output_path, 'w', newline='') as csvfile:
				writer = csv.writer(csvfile)
				writer.writerow(['resource_id', 'target_object_type', 'match_type', 'dataset', 'project', 'effect'])

				for entry in entries:
					writer.writerow([
						entry['resource_id'],
						entry['target_object_type'],
						entry['match_type'],
						entry['dataset'],
						entry['project'],
						entry['effect']
					])

			self.update_progress(50)

			result = self.create_result(success=True, count=len(entries), file=str(output_path))
			self.log_result('export', result)
			return result

		except Exception as e:
			LOGGER.error(f"[{self.entity_name}] Export failed: {e}")
			return self.create_result(success=False, count=0, errors=[str(e)])

	def import_data(self, input_file: str = None, dry_run: bool = True) -> dict:
		"""Import blocklist entries from CSV.

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

			# Parse input file
			entries_by_resource_and_type = self._parse_input_file(input_path)

			# Get existing entries for duplicate detection
			LOGGER.info(f"[{self.entity_name}] Fetching existing blocklist entries...")
			existing_entries = self.get_blocklist_entries()
			existing_keys = self._build_existing_keys(existing_entries)
			LOGGER.info(f"[{self.entity_name}] Found {len(existing_entries)} existing entries")

			# Process entries
			created = 0
			skipped = 0
			failed = 0

			total_groups = len(entries_by_resource_and_type)
			progress_per_group = 50 / max(total_groups, 1)

			for (resource_id, target_object_type), entries in entries_by_resource_and_type.items():
				# Filter out duplicates
				entries_to_process = []
				for entry in entries:
					entry_key = self._make_entry_key(entry)

					if entry_key in existing_keys:
						LOGGER.debug(f"[{self.entity_name}] SKIP (exists): {entry.get('project') or entry.get('dataset')}")
						skipped += 1
					else:
						entries_to_process.append(entry)

				if not entries_to_process:
					self.update_progress(progress_per_group)
					continue

				# Log what would be created
				for entry in entries_to_process:
					identifier = entry.get('project') or entry.get('dataset') or resource_id
					if dry_run:
						LOGGER.info(f"[{self.entity_name}] WOULD CREATE: {identifier} ({target_object_type})")
						created += 1
					else:
						# Actually create the entry
						try:
							self.modify_blocklist_entries(
								resource_id=resource_id,
								target_object_type=target_object_type,
								entries=[entry]
							)
							LOGGER.info(f"[{self.entity_name}] CREATED: {identifier} ({target_object_type})")
							created += 1
							# Add to existing keys to prevent duplicates in same run
							existing_keys.add(self._make_entry_key(entry))
						except Exception as e:
							LOGGER.error(f"[{self.entity_name}] FAILED: {identifier} - {e}")
							failed += 1

				self.update_progress(progress_per_group)

			result = self.create_result(
				success=(failed == 0),
				dry_run=dry_run,
				created=created,
				updated=0,  # Blocklists don't have updates, only create/skip
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
		"""Validate a blocklist CSV file.

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
			count = 0
			required_columns = {'resource_id', 'target_object_type', 'match_type'}

			with open(input_path, 'r') as csvfile:
				reader = csv.DictReader(csvfile)

				# Check headers
				if reader.fieldnames is None:
					errors.append("CSV file is empty or has no headers")
					return self.create_result(valid=False, count=0, errors=errors, warnings=warnings)

				missing_columns = required_columns - set(reader.fieldnames)
				if missing_columns:
					errors.append(f"Missing required columns: {missing_columns}")
					return self.create_result(valid=False, count=0, errors=errors, warnings=warnings)

				# Validate each row
				for row_num, row in enumerate(reader, start=2):
					count += 1

					# Check required fields have values
					if not row.get('resource_id', '').strip():
						errors.append(f"Row {row_num}: Missing resource_id")
					if not row.get('target_object_type', '').strip():
						errors.append(f"Row {row_num}: Missing target_object_type")
					if not row.get('match_type', '').strip():
						errors.append(f"Row {row_num}: Missing match_type")

					# Validate target_object_type values
					valid_types = {'project', 'dataset', 'schema', 'table'}
					obj_type = row.get('target_object_type', '').strip().lower()
					if obj_type and obj_type not in valid_types:
						warnings.append(f"Row {row_num}: Unknown target_object_type '{obj_type}'")

			if count == 0:
				warnings.append("File contains no data rows")

			result = self.create_result(
				valid=(len(errors) == 0),
				count=count,
				errors=errors,
				warnings=warnings
			)
			self.log_result('validate', result)
			return result

		except Exception as e:
			errors.append(f"Validation error: {e}")
			return self.create_result(valid=False, count=0, errors=errors, warnings=warnings)

	def _parse_input_file(self, input_path: Path) -> dict:
		"""Parse the input CSV file and group entries.

		Args:
			input_path (Path): Path to input CSV file.

		Returns:
			dict: Entries grouped by (resource_id, target_object_type).
		"""
		entries_by_resource_and_type = defaultdict(list)

		with open(input_path, 'r') as csvfile:
			reader = csv.DictReader(csvfile)
			for row in reader:
				entry = {
					'resource_id': row['resource_id'].strip(),
					'target_object_type': row['target_object_type'].strip(),
					'match_type': row['match_type'].strip(),
					'dataset': row.get('dataset', '').strip() or None,
					'project': row.get('project', '').strip() or None,
					'effect': row.get('effect', '').strip() or None
				}
				key = (entry['resource_id'], entry['target_object_type'])
				entries_by_resource_and_type[key].append(entry)

		return entries_by_resource_and_type

	def _build_existing_keys(self, existing_entries: list) -> set:
		"""Build a set of keys for existing entries for duplicate detection.

		Args:
			existing_entries (list): List of existing blocklist entries.

		Returns:
			set: Set of entry keys.
		"""
		existing_keys = set()
		for entry in existing_entries:
			key = (
				entry['resource_id'],
				entry['target_object_type'],
				entry['match_type'],
				entry['dataset'] or '',
				entry['project'] or ''
			)
			existing_keys.add(key)
		return existing_keys

	def _make_entry_key(self, entry: dict) -> tuple:
		"""Create a key tuple for an entry for duplicate detection.

		Args:
			entry (dict): Blocklist entry dictionary.

		Returns:
			tuple: Key tuple for the entry.
		"""
		return (
			entry['resource_id'],
			entry['target_object_type'],
			entry['match_type'],
			entry.get('dataset') or '',
			entry.get('project') or ''
		)

