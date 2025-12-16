"""
Data Product Migrator - Export and import data products between MC environments.

Data Products are business-facing data assets that group related tables for
stakeholders to monitor and understand data quality.

This migrator uses composition to delegate operations to admin scripts:
- BulkDataProductExporter: Fetches data product data from MC
- BulkDataProductImporter: Parses CSV and imports individual data products
"""

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import csv
from pathlib import Path
from rich.progress import Progress
from lib.helpers.logs import LOGGER
from migration.base_migrator import BaseMigrator
from admin.bulk_data_product_exporter import BulkDataProductExporter
from admin.bulk_data_product_importer import BulkDataProductImporter


class DataProductMigrator(BaseMigrator):
	"""Migrator for data products.

	Handles export and import of data products and their asset assignments.
	Uses composition to delegate to admin scripts for core operations.

	CSV Format:
		data_product_name,data_product_description,asset_mcon
	"""

	def __init__(self, profile: str, config_file: str = None, progress: Progress = None):
		"""Initialize the DataProductMigrator.

		Args:
			profile (str): MC profile name from configs.ini
			config_file (str): Path to configuration file (optional)
			progress (Progress): Rich progress bar instance (optional)
		"""
		# Initialize BaseMigrator
		BaseMigrator.__init__(self, profile, config_file, progress)

		# Initialize admin tools via composition
		self._exporter = BulkDataProductExporter(profile, config_file, progress)
		self._importer = BulkDataProductImporter(profile, config_file, progress)

	@property
	def entity_name(self) -> str:
		return "data_products"

	@property
	def output_filename(self) -> str:
		return "data_products.csv"

	def export(self, output_file: str = None) -> dict:
		"""Export all data products and their assets to CSV.

		Delegates to BulkDataProductExporter for fetching data product data,
		then formats and writes to migration CSV.

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

			# Fetch data products using admin exporter
			LOGGER.info(f"[{self.entity_name}] Fetching data products via admin exporter...")
			data_products_data = self._exporter.get_all_data_products_with_assets()
			LOGGER.info(f"[{self.entity_name}] Found {len(data_products_data)} active data products")

			if not data_products_data:
				LOGGER.info(f"[{self.entity_name}] No data products to export")
				# Still write header
				with open(output_path, 'w', newline='') as csvfile:
					writer = csv.writer(csvfile)
					writer.writerow(['data_product_name', 'data_product_description', 'asset_mcon'])
				return self.create_result(success=True, count=0, file=str(output_path))

			# Write to CSV
			rows_written = 0
			with open(output_path, 'w', newline='') as csvfile:
				writer = csv.writer(csvfile)
				writer.writerow(['data_product_name', 'data_product_description', 'asset_mcon'])

				progress_per_dp = 50 / max(len(data_products_data), 1)

				for dp_data in data_products_data:
					name = dp_data['name']
					description = dp_data.get('description', '')
					assets = dp_data.get('assets', [])

					LOGGER.info(f"[{self.entity_name}] Processing: {name}")
					LOGGER.debug(f"[{self.entity_name}]   - {len(assets)} assets")

					if assets:
						for mcon in assets:
							writer.writerow([name, description, mcon])
							rows_written += 1
					else:
						# Include data products with no assets
						writer.writerow([name, description, ''])
						rows_written += 1

					self.update_progress(progress_per_dp)

			result = self.create_result(
				success=True,
				count=len(data_products_data),
				rows=rows_written,
				file=str(output_path)
			)
			self.log_result('export', result)
			return result

		except Exception as e:
			LOGGER.error(f"[{self.entity_name}] Export failed: {e}")
			return self.create_result(success=False, count=0, errors=[str(e)])

	def import_data(self, input_file: str = None, dry_run: bool = True) -> dict:
		"""Import data products from CSV.

		Delegates to BulkDataProductImporter for parsing CSV and importing
		individual data products. Handles dry-run mode and validation.

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

			# Parse input file using admin importer
			LOGGER.info(f"[{self.entity_name}] Parsing CSV via admin importer...")
			parse_result = self._importer.parse_data_product_csv(str(input_path))

			if parse_result['errors']:
				return self.create_result(
					success=False,
					dry_run=dry_run,
					created=0, updated=0, skipped=0, failed=0,
					errors=parse_result['errors']
				)

			data_products_data = parse_result['data_products']
			LOGGER.info(f"[{self.entity_name}] Found {len(data_products_data)} data products in file")

			# Get existing data products using admin importer
			LOGGER.info(f"[{self.entity_name}] Fetching existing data products...")
			dp_mapping = self._importer.get_existing_data_products_map()
			LOGGER.info(f"[{self.entity_name}] Found {len(dp_mapping)} existing data products")

			# Process data products
			created = 0
			updated = 0
			failed = 0

			progress_per_dp = 50 / max(len(data_products_data), 1)

			for dp_name, data in data_products_data.items():
				existing_info = dp_mapping.get(dp_name)
				existing_uuid = existing_info['uuid'] if existing_info else None
				is_new = existing_uuid is None
				action = "CREATE" if is_new else "UPDATE"

				# Filter out empty MCONs
				mcons = [m for m in data['mcons'] if m]

				if dry_run:
					LOGGER.info(f"[{self.entity_name}] WOULD {action}: {dp_name} with {len(mcons)} assets")
					if is_new:
						created += 1
					else:
						updated += 1
				else:
					# Use admin importer for actual import
					result = self._importer.import_single_data_product(
						name=dp_name,
						description=data['description'],
						mcons=mcons,
						uuid=existing_uuid
					)

					if result['success']:
						if result['action'] == 'created':
							created += 1
							# Update mapping for subsequent operations
							if result.get('data_product_uuid'):
								dp_mapping[dp_name] = {'uuid': result['data_product_uuid']}
						else:
							updated += 1
						LOGGER.info(f"[{self.entity_name}] {result['action'].upper()}: {dp_name}")
					else:
						LOGGER.error(f"[{self.entity_name}] FAILED: {dp_name} - {result.get('error', 'Unknown error')}")
						failed += 1

				self.update_progress(progress_per_dp)

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
		"""Validate a data product CSV file.

		Uses the admin importer's parse method for consistency with import.

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

			# Parse using admin importer for consistent validation
			parse_result = self._importer.parse_data_product_csv(str(input_path))

			if parse_result['errors']:
				errors.extend(parse_result['errors'])
				return self.create_result(valid=False, count=0, errors=errors, warnings=warnings)

			dp_count = len(parse_result['data_products'])

			if dp_count == 0:
				warnings.append("File contains no valid data products")

			result = self.create_result(
				valid=(len(errors) == 0),
				count=dp_count,
				errors=errors,
				warnings=warnings
			)
			self.log_result('validate', result)
			return result

		except Exception as e:
			errors.append(f"Validation error: {e}")
			return self.create_result(valid=False, count=0, errors=errors, warnings=warnings)

