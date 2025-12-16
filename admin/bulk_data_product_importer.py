# Instructions:
# 1. Run this script: python admin/bulk_data_product_importer.py -p <profile> -i <input_file>
# 2. Profile should be configured in ~/.mcd/profiles.ini
#
# Input CSV format (with header):
#   data_product_name,data_product_description,asset_mcon
#   Customer Analytics,Analytics for customer data,MCON++123++456++table++customers
#   Revenue Dashboard,Revenue KPIs,MCON++123++456++view++orders

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import csv
from admin import *
from lib.helpers import sdk_helpers

# Initialize logger
util_name = os.path.basename(__file__).split('.')[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))


class BulkDataProductImporter(Admin):

	def __init__(self, profile, config_file: str = None, progress: Progress = None):
		"""Creates an instance of BulkDataProductImporter.

		Args:
			profile(str): Profile to use stored in montecarlo cli.
			config_file (str): Path to the Configuration File.
			progress(Progress): Progress bar.
		"""
		super().__init__(profile, config_file, progress)
		self.progress_bar = progress

	def parse_data_product_csv(self, input_file: str) -> dict:
		"""Parse a data product CSV file and return organized data.

		This method returns data instead of calling sys.exit(), making it
		suitable for use by other modules like the migration tool.

		Args:
			input_file (str): Path to CSV file.

		Returns:
			dict: Result with keys:
				- success (bool): Whether parsing succeeded
				- data_products (dict): Name -> {description, mcons} mapping
				- count (int): Number of data products found
				- errors (list): Any errors encountered
		"""
		file_path = Path(input_file)
		errors = []
		data_products = {}

		if not file_path.is_file():
			return {
				'success': False,
				'data_products': {},
				'count': 0,
				'errors': [f"File not found: {input_file}"]
			}

		try:
			with open(input_file, 'r') as csvfile:
				reader = csv.DictReader(csvfile)

				# Check for required column
				if reader.fieldnames is None:
					return {
						'success': False,
						'data_products': {},
						'count': 0,
						'errors': ["CSV file is empty or has no headers"]
					}

				if 'data_product_name' not in reader.fieldnames:
					return {
						'success': False,
						'data_products': {},
						'count': 0,
						'errors': ["Missing required column: data_product_name"]
					}

				row_num = 1  # Header is row 1
				for row in reader:
					row_num += 1
					name = row.get('data_product_name', '').strip()
					desc = row.get('data_product_description', '').strip()
					mcon = row.get('asset_mcon', '').strip()

					if not name:
						errors.append(f"Row {row_num}: Empty data_product_name")
						continue

					if name not in data_products:
						data_products[name] = {'description': desc or None, 'mcons': []}

					# Update description if we have one and didn't before
					if desc and not data_products[name]['description']:
						data_products[name]['description'] = desc

					if mcon:
						data_products[name]['mcons'].append(mcon)

		except Exception as e:
			errors.append(f"Error reading file: {str(e)}")

		if not data_products and not errors:
			errors.append("No data products found in input file")

		return {
			'success': len(errors) == 0 and len(data_products) > 0,
			'data_products': data_products,
			'count': len(data_products),
			'errors': errors
		}

	def get_existing_data_products_map(self) -> dict:
		"""Get a mapping of existing data products in Monte Carlo.

		Returns:
			dict: Data product name -> dict with 'uuid', 'description', 'is_deleted'
		"""
		LOGGER.info("Fetching existing data products...")
		existing_dps = self.get_data_products_list()

		dp_map = {
			dp.name: {
				'uuid': dp.uuid,
				'description': dp.description,
				'is_deleted': dp.is_deleted
			}
			for dp in existing_dps
			if not dp.is_deleted
		}

		LOGGER.info(f"Found {len(dp_map)} existing data products")
		return dp_map

	def import_single_data_product(self, name: str, description: str = None,
								   mcons: list = None, uuid: str = None) -> dict:
		"""Create or update a single data product.

		This method handles one data product at a time, returning a result dict.
		This allows callers (like the migration tool) to implement dry-run
		mode or custom error handling.

		Args:
			name (str): Data product name.
			description (str): Data product description (optional).
			mcons (list): List of asset MCONs to assign (optional).
			uuid (str): Existing data product UUID for updates (optional).

		Returns:
			dict: Result with keys:
				- success (bool): Whether import succeeded
				- data_product_uuid (str): UUID of created/updated data product
				- data_product_name (str): Name of the data product
				- action (str): 'created' or 'updated'
				- assets_assigned (int): Number of assets assigned
				- error (str): Error message if failed
		"""
		mcons = mcons or []

		try:
			# Step 1: Create or update the data product
			response = self.create_or_update_data_product(
				name=name,
				description=description,
				uuid=uuid
			)

			if not response or not response.data_product:
				return {
					'success': False,
					'data_product_uuid': None,
					'data_product_name': name,
					'action': None,
					'assets_assigned': 0,
					'error': "No response from API"
				}

			dp = response.data_product
			action = 'updated' if uuid else 'created'
			LOGGER.info(f"Data product '{dp.name}' {action} ({dp.uuid})")

			# Step 2: Set assets for the data product
			assets_assigned = 0
			asset_error = None
			if mcons:
				try:
					self.set_data_product_assets(dp.uuid, mcons)
					assets_assigned = len(mcons)
					LOGGER.info(f"  Assigned {assets_assigned} assets")
				except Exception as e:
					asset_error = f"Failed to assign assets: {e}"
					LOGGER.error(f"  {asset_error}")

			return {
				'success': True,
				'data_product_uuid': dp.uuid,
				'data_product_name': dp.name,
				'action': action,
				'assets_assigned': assets_assigned,
				'error': asset_error  # May have partial success
			}

		except Exception as e:
			LOGGER.error(f"Failed to create/update data product '{name}': {e}")
			return {
				'success': False,
				'data_product_uuid': None,
				'data_product_name': name,
				'action': None,
				'assets_assigned': 0,
				'error': str(e)
			}

	@staticmethod
	def validate_input_file(input_file: str):
		"""Validate input CSV file and return grouped data products.

		DEPRECATED: Use parse_data_product_csv() instead for better error handling.
		This method is kept for backward compatibility.

		Args:
			input_file (str): Path to input CSV file.

		Returns:
			dict: Data products grouped by name with description and mcons.
		"""
		file = Path(input_file)

		if not file.is_file():
			LOGGER.error("Invalid input file")
			sys.exit(1)

		data_products = {}
		with open(input_file, 'r') as csvfile:
			reader = csv.DictReader(csvfile)
			for row in reader:
				name = row['data_product_name'].strip()
				desc = row.get('data_product_description', '').strip()
				mcon = row['asset_mcon'].strip()

				if name not in data_products:
					data_products[name] = {'description': desc, 'mcons': []}
				if mcon:
					data_products[name]['mcons'].append(mcon)

		if len(data_products) == 0:
			LOGGER.error("No data products found in input file")
			sys.exit(1)

		return data_products

	def import_data_products(self, data_products: dict):
		"""Import data products from validated data.

		Args:
			data_products (dict): Data products grouped by name with description and mcons.
		"""
		# Get existing data products for matching
		LOGGER.info("Fetching existing data products...")
		existing_dps = self.get_data_products_list()
		dp_map = {dp.name: dp.uuid for dp in existing_dps if not dp.is_deleted}
		LOGGER.info(f"Found {len(dp_map)} existing data products")

		LOGGER.info(f"Processing {len(data_products)} data products from CSV")

		# Create/update each data product
		for name, info in data_products.items():
			existing_uuid = dp_map.get(name)

			# Step 1: Create or update the data product
			response = self.create_or_update_data_product(
				name,
				info['description'] or None,
				existing_uuid
			)

			if response and response.data_product:
				dp = response.data_product
				action = 'updated' if existing_uuid else 'created'
				LOGGER.info(f"  - {dp.name}: {action} ({dp.uuid})")

				# Step 2: Set assets for the data product
				if info['mcons']:
					try:
						self.set_data_product_assets(dp.uuid, info['mcons'])
						LOGGER.info(f"    Assigned {len(info['mcons'])} assets")
					except Exception as e:
						LOGGER.error(f"    Failed to assign assets: {e}")
			else:
				LOGGER.error(f"  - {name}: FAILED to create/update")

		LOGGER.info("Import complete")


def main(*args, **kwargs):

	# Capture Command Line Arguments
	parser = sdk_helpers.generate_arg_parser(os.path.basename(os.path.dirname(os.path.abspath(__file__))),
											 os.path.basename(__file__))

	if not args:
		args = parser.parse_args(*args, **kwargs)
	else:
		sdk_helpers.dump_help(parser, main, *args)

	@sdk_helpers.ensure_progress
	def run_utility(progress, util, args):
		util.progress_bar = progress
		data_products = util.validate_input_file(args.input_file)
		util.import_data_products(data_products)

	util = BulkDataProductImporter(args.profile)
	run_utility(util, args)


if __name__ == "__main__":
	main()
