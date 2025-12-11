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

	@staticmethod
	def validate_input_file(input_file: str):
		"""Validate input CSV file and return grouped data products.

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
