"""
Data Product Migrator - Export and import data products between MC environments.

Data Products are business-facing data assets that group related tables for
stakeholders to monitor and understand data quality.
"""

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import csv
from pathlib import Path
from rich.progress import Progress
from lib.util import Util
from lib.helpers.logs import LOGGER
from migration.base_migrator import BaseMigrator


class DataProductMigrator(BaseMigrator, Util):
	"""Migrator for data products.

	Handles export and import of data products and their asset assignments.

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
		# Initialize Util for auth/config
		Util.__init__(self, profile, config_file, progress)
		# Initialize BaseMigrator
		BaseMigrator.__init__(self, profile, config_file, progress)

	@property
	def entity_name(self) -> str:
		return "data_products"

	@property
	def output_filename(self) -> str:
		return "data_products.csv"

	def export(self, output_file: str = None) -> dict:
		"""Export all data products and their assets to CSV.

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

			# Fetch data products
			LOGGER.info(f"[{self.entity_name}] Fetching data products...")
			data_products = self.get_data_products_list()
			active_dps = [dp for dp in data_products if not dp.is_deleted]
			LOGGER.info(f"[{self.entity_name}] Found {len(active_dps)} active data products")

			if not active_dps:
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

				progress_per_dp = 50 / max(len(active_dps), 1)

				for dp in active_dps:
					LOGGER.info(f"[{self.entity_name}] Processing: {dp.name}")

					# Get assets for this data product
					assets = self.get_data_product_assets(dp.uuid)
					LOGGER.debug(f"[{self.entity_name}]   - {len(assets)} assets")

					if assets:
						for mcon in assets:
							writer.writerow([
								dp.name,
								dp.description or '',
								mcon
							])
							rows_written += 1
					else:
						# Include data products with no assets
						writer.writerow([
							dp.name,
							dp.description or '',
							''
						])
						rows_written += 1

					self.update_progress(progress_per_dp)

			result = self.create_result(
				success=True,
				count=len(active_dps),
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

			# Parse input file and group by data product
			data_products_data = self._parse_input_file(input_path)
			LOGGER.info(f"[{self.entity_name}] Found {len(data_products_data)} data products in file")

			# Get existing data products
			LOGGER.info(f"[{self.entity_name}] Fetching existing data products...")
			existing_dps = self.get_data_products_list()
			dp_mapping = {
				dp.name: dp.uuid
				for dp in existing_dps
				if not dp.is_deleted
			}
			LOGGER.info(f"[{self.entity_name}] Found {len(dp_mapping)} existing data products")

			# Process data products
			created = 0
			updated = 0
			failed = 0

			progress_per_dp = 50 / max(len(data_products_data), 1)

			for dp_name, data in data_products_data.items():
				existing_uuid = dp_mapping.get(dp_name)
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
					try:
						# Step 1: Create or update the data product
						response = self.create_or_update_data_product(
							name=dp_name,
							description=data['description'] or None,
							uuid=existing_uuid
						)

						if response and response.data_product:
							dp = response.data_product
							LOGGER.info(f"[{self.entity_name}] {action}D: {dp.name} ({dp.uuid})")

							# Step 2: Set assets for the data product
							if mcons:
								try:
									self.set_data_product_assets(dp.uuid, mcons)
									LOGGER.info(f"[{self.entity_name}]   Assigned {len(mcons)} assets")
								except Exception as e:
									LOGGER.warning(f"[{self.entity_name}]   Failed to assign assets: {e}")

							if is_new:
								created += 1
								dp_mapping[dp_name] = dp.uuid
							else:
								updated += 1
						else:
							LOGGER.error(f"[{self.entity_name}] FAILED: {dp_name} - no response")
							failed += 1

					except Exception as e:
						LOGGER.error(f"[{self.entity_name}] FAILED: {dp_name} - {e}")
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
			dp_count = 0
			dp_names = set()
			required_columns = {'data_product_name'}

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
					dp_name = row.get('data_product_name', '').strip()

					if not dp_name:
						errors.append(f"Row {row_num}: Missing data_product_name")
					else:
						dp_names.add(dp_name)

			dp_count = len(dp_names)

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

	def _parse_input_file(self, input_path: Path) -> dict:
		"""Parse the input CSV file and group by data product.

		Args:
			input_path (Path): Path to input CSV file.

		Returns:
			dict: Data products grouped by name with description and mcons.
		"""
		data_products = {}

		with open(input_path, 'r') as csvfile:
			reader = csv.DictReader(csvfile)

			for row in reader:
				name = row['data_product_name'].strip()
				if not name:
					continue

				desc = row.get('data_product_description', '').strip()
				mcon = row.get('asset_mcon', '').strip()

				if name not in data_products:
					data_products[name] = {
						'description': desc or None,
						'mcons': []
					}

				# Update description if we have one and didn't before
				if desc and not data_products[name]['description']:
					data_products[name]['description'] = desc

				# Add MCON if present
				if mcon:
					data_products[name]['mcons'].append(mcon)

		return data_products

