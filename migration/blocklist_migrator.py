"""
Blocklist Migrator - Export and import blocklist entries between MC environments.

Blocklists control which tables/datasets/projects are included or excluded from
Monte Carlo's data observability monitoring.

This migrator uses composition to delegate core operations to admin bulk scripts,
avoiding code duplication while adding migration-specific features like dry-run
support, scope handling, and structured results.
"""

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import csv
from pathlib import Path
from collections import defaultdict
from rich.progress import Progress
from lib.helpers.logs import LOGGER
from migration.base_migrator import BaseMigrator
from admin.bulk_blocklist_exporter import BulkBlocklistExporter
from admin.bulk_blocklist_importer import BulkBlocklistImporter


class BlocklistMigrator(BaseMigrator):
	"""Migrator for blocklist entries.

	Handles export and import of blocklist entries (ingestion rules) that control
	which tables, datasets, or projects are monitored by Monte Carlo.
	Delegates core operations to admin bulk scripts.

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
		# Initialize BaseMigrator
		BaseMigrator.__init__(self, profile, config_file, progress)

		# Composition: use admin classes for core operations
		self._exporter = BulkBlocklistExporter(profile, config_file, progress)
		self._importer = BulkBlocklistImporter(profile, config_file, progress)

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

			# Delegate to admin exporter to fetch blocklist entries
			entries = self._exporter.get_all_blocklist_entries()

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

			# Delegate parsing to admin importer
			parse_result = self._importer.parse_blocklist_csv(str(input_path))
			if not parse_result['success']:
				return self.create_result(
					success=False,
					dry_run=dry_run,
					created=0, updated=0, skipped=0, failed=0,
					errors=parse_result['errors']
				)

			entries_by_resource_and_type = parse_result['entries_by_resource_and_type']

			# Delegate getting existing keys to admin importer
			LOGGER.info(f"[{self.entity_name}] Fetching existing blocklist entries...")
			existing_keys = self._importer.get_existing_blocklist_keys()
			existing_entries = self._importer.get_blocklist_entries()  # Need full entries for scope handling
			LOGGER.info(f"[{self.entity_name}] Found {len(existing_entries)} existing entries")

			# Process entries
			created = 0
			skipped = 0
			failed = 0

			total_groups = len(entries_by_resource_and_type)
			progress_per_group = 50 / max(total_groups, 1)

			for (resource_id, target_object_type), entries in entries_by_resource_and_type.items():
				# Filter out duplicates and identify new entries
				new_entries = []
				for entry in entries:
					entry_key = self._make_entry_key(entry)

					if entry_key in existing_keys:
						LOGGER.debug(f"[{self.entity_name}] SKIP (exists): {entry.get('project') or entry.get('dataset')}")
						skipped += 1
					else:
						new_entries.append(entry)

				if not new_entries:
					self.update_progress(progress_per_group)
					continue

				# Group new entries by scope (the API replaces all entries within a scope)
				entries_by_scope = self._group_entries_by_scope(new_entries, target_object_type)

				for scope_key, scope_new_entries in entries_by_scope.items():
					# Get existing entries for this scope to include in the API call
					scope_existing = self._get_existing_entries_for_scope(
						existing_entries, resource_id, target_object_type, scope_key
					)

					# Combine existing + new entries for this scope
					all_scope_entries = scope_existing + scope_new_entries

					# Log what would be created
					for entry in scope_new_entries:
						identifier = entry.get('project') or entry.get('dataset') or resource_id
						if dry_run:
							LOGGER.info(f"[{self.entity_name}] WOULD CREATE: {identifier} ({target_object_type})")
							created += 1

					if not dry_run:
						# Delegate API call to admin importer
						result = self._importer.import_blocklist_batch(
							resource_id=resource_id,
							target_object_type=target_object_type,
							entries=all_scope_entries
						)

						if result['success']:
							for entry in scope_new_entries:
								identifier = entry.get('project') or entry.get('dataset') or resource_id
								LOGGER.info(f"[{self.entity_name}] CREATED: {identifier} ({target_object_type})")
								created += 1
								existing_keys.add(self._make_entry_key(entry))
						else:
							for entry in scope_new_entries:
								identifier = entry.get('project') or entry.get('dataset') or resource_id
								LOGGER.error(f"[{self.entity_name}] FAILED: {identifier} - {result['error']}")
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
			parse_result = self._importer.parse_blocklist_csv(str(input_path))

			if not parse_result['success']:
				errors.extend(parse_result['errors'])
				return self.create_result(valid=False, count=0, errors=errors, warnings=warnings)

			count = parse_result['total_count']

			if count == 0:
				warnings.append("File contains no data rows")

			# Additional validation: check for valid target_object_type values
			valid_types = {'project', 'dataset', 'schema', 'table'}
			for (resource_id, target_object_type), entries in parse_result['entries_by_resource_and_type'].items():
				if target_object_type.lower() not in valid_types:
					warnings.append(f"Unknown target_object_type '{target_object_type}'")

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

	def _group_entries_by_scope(self, entries: list, target_object_type: str) -> dict:
		"""Group entries by their API scope.

		The MC API replaces all entries within a scope, so we need to group
		entries that share the same scope and make one API call per scope.

		For datasets: scope is (project,)
		For projects: scope is ()  - all projects share the same scope
		For schemas/tables: scope is (project, dataset)

		Args:
			entries (list): List of entries to group.
			target_object_type (str): The type of blocklist entries.

		Returns:
			dict: Entries grouped by scope key.
		"""
		grouped = defaultdict(list)

		for entry in entries:
			if target_object_type == 'dataset':
				scope_key = (entry.get('project') or '',)
			elif target_object_type == 'project':
				scope_key = ()  # All projects share the same scope
			else:  # schema, table
				scope_key = (entry.get('project') or '', entry.get('dataset') or '')

			grouped[scope_key].append(entry)

		return grouped

	def _get_existing_entries_for_scope(self, existing_entries: list, resource_id: str,
										target_object_type: str, scope_key: tuple) -> list:
		"""Get existing entries that match a given scope.

		Args:
			existing_entries (list): All existing blocklist entries.
			resource_id (str): The warehouse/connection UUID.
			target_object_type (str): The type of blocklist entries.
			scope_key (tuple): The scope key from _group_entries_by_scope.

		Returns:
			list: Existing entries for this scope.
		"""
		matching = []

		for entry in existing_entries:
			# Must match resource_id and target_object_type
			if entry['resource_id'] != resource_id:
				continue
			if entry['target_object_type'] != target_object_type:
				continue

			# Check scope match
			if target_object_type == 'dataset':
				entry_scope = (entry.get('project') or '',)
			elif target_object_type == 'project':
				entry_scope = ()
			else:  # schema, table
				entry_scope = (entry.get('project') or '', entry.get('dataset') or '')

			if entry_scope == scope_key:
				matching.append(entry)

		return matching
