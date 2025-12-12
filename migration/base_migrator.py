"""
Base Migrator - Abstract base class for all entity migrators.

All migrators (blocklist, domain, data_product, monitor) inherit from this class
and implement the export(), import_data(), and validate() methods.
"""

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional
from rich.progress import Progress
from lib.helpers.logs import LOGGER


class BaseMigrator(ABC):
	"""Abstract base class for entity migrators.

	All migrators must implement:
	- entity_name: Name of the entity type (e.g., 'blocklists', 'domains')
	- output_filename: Default filename for export (e.g., 'blocklists.csv')
	- export(): Export entities to file
	- import_data(): Import entities from file (with dry-run support)
	- validate(): Validate import file before importing
	"""

	# Default output directory for exports (within migration folder)
	DEFAULT_OUTPUT_DIR = Path(__file__).parent / "migration-data-exports"

	def __init__(self, profile: str, config_file: str = None, progress: Progress = None):
		"""Initialize the migrator.

		Args:
			profile (str): MC profile name from configs.ini
			config_file (str): Path to configuration file (optional)
			progress (Progress): Rich progress bar instance (optional)
		"""
		self.profile = profile
		self.config_file = config_file
		self.progress_bar = progress
		self._output_dir = self.DEFAULT_OUTPUT_DIR

	@property
	@abstractmethod
	def entity_name(self) -> str:
		"""Return the entity type name (e.g., 'blocklists', 'domains').

		Used for logging and manifest file generation.
		"""
		pass

	@property
	@abstractmethod
	def output_filename(self) -> str:
		"""Return the default output filename (e.g., 'blocklists.csv').

		Used when no custom filename is provided.
		"""
		pass

	@property
	def output_dir(self) -> Path:
		"""Get the output directory for exports."""
		return self._output_dir

	@output_dir.setter
	def output_dir(self, value: str):
		"""Set the output directory for exports."""
		self._output_dir = Path(value)

	def get_output_path(self, filename: str = None) -> Path:
		"""Get the full output path for a file.

		Args:
			filename (str): Filename to use. Defaults to output_filename.

		Returns:
			Path: Full path to the output file.
		"""
		filename = filename or self.output_filename
		return self.output_dir / filename

	def ensure_output_dir(self):
		"""Ensure the output directory exists, creating it if necessary."""
		self.output_dir.mkdir(parents=True, exist_ok=True)

	@abstractmethod
	def export(self, output_file: str = None) -> dict:
		"""Export entities to a file.

		Args:
			output_file (str): Path to output file. Uses default if not provided.

		Returns:
			dict: Export result with keys:
				- success (bool): Whether export succeeded
				- count (int): Number of entities exported
				- file (str): Path to exported file
				- errors (list): Any errors encountered
		"""
		pass

	@abstractmethod
	def import_data(self, input_file: str = None, dry_run: bool = True) -> dict:
		"""Import entities from a file.

		Args:
			input_file (str): Path to input file. Uses default if not provided.
			dry_run (bool): If True, preview changes without committing.

		Returns:
			dict: Import result with keys:
				- success (bool): Whether import succeeded
				- dry_run (bool): Whether this was a dry run
				- created (int): Number of entities created
				- updated (int): Number of entities updated
				- skipped (int): Number of entities skipped
				- failed (int): Number of entities that failed
				- errors (list): Any errors encountered
		"""
		pass

	@abstractmethod
	def validate(self, input_file: str = None) -> dict:
		"""Validate an input file before importing.

		Args:
			input_file (str): Path to input file. Uses default if not provided.

		Returns:
			dict: Validation result with keys:
				- valid (bool): Whether file is valid
				- count (int): Number of entities in file
				- errors (list): Validation errors found
				- warnings (list): Validation warnings found
		"""
		pass

	def update_progress(self, advance: float):
		"""Update the progress bar if available.

		Args:
			advance (float): Amount to advance the progress bar.
		"""
		if self.progress_bar and self.progress_bar.tasks:
			task_id = self.progress_bar.tasks[0].id
			self.progress_bar.update(task_id, advance=advance)

	def log_result(self, operation: str, result: dict):
		"""Log the result of an operation.

		Args:
			operation (str): The operation performed ('export', 'import', 'validate')
			result (dict): The result dictionary from the operation
		"""
		entity = self.entity_name

		if operation == 'export':
			if result.get('success'):
				LOGGER.info(f"[{entity}] Export complete: {result.get('count', 0)} entities to {result.get('file')}")
			else:
				LOGGER.error(f"[{entity}] Export failed")
				for error in result.get('errors', []):
					LOGGER.error(f"  - {error}")

		elif operation == 'import':
			mode = "DRY-RUN" if result.get('dry_run') else "COMMITTED"
			if result.get('success'):
				LOGGER.info(f"[{entity}] Import {mode}:")
				LOGGER.info(f"  - Created: {result.get('created', 0)}")
				LOGGER.info(f"  - Updated: {result.get('updated', 0)}")
				LOGGER.info(f"  - Skipped: {result.get('skipped', 0)}")
				if result.get('failed', 0) > 0:
					LOGGER.warning(f"  - Failed: {result.get('failed', 0)}")
			else:
				LOGGER.error(f"[{entity}] Import failed")
				for error in result.get('errors', []):
					LOGGER.error(f"  - {error}")

		elif operation == 'validate':
			if result.get('valid'):
				LOGGER.info(f"[{entity}] Validation passed: {result.get('count', 0)} entities")
				for warning in result.get('warnings', []):
					LOGGER.warning(f"  - {warning}")
			else:
				LOGGER.error(f"[{entity}] Validation failed")
				for error in result.get('errors', []):
					LOGGER.error(f"  - {error}")

	@staticmethod
	def create_result(success: bool = True, **kwargs) -> dict:
		"""Create a standardized result dictionary.

		Args:
			success (bool): Whether the operation succeeded
			**kwargs: Additional key-value pairs to include

		Returns:
			dict: Result dictionary with success and additional fields
		"""
		result = {'success': success, 'errors': []}
		result.update(kwargs)
		return result

