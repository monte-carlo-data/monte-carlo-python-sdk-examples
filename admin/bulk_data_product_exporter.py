# Instructions:
# 1. Run this script: python admin/bulk_data_product_exporter.py -p <profile> -o <output_file>
# 2. Profile should be configured in ~/.mcd/profiles.ini
#
# Output CSV format:
#   data_product_name,data_product_description,asset_mcon
#   Customer Analytics,Analytics for customer data,MCON++123++456++table++customers
#   Revenue Dashboard,Revenue KPIs,MCON++123++456++view++orders
#
# Note: The output CSV can be used with bulk_data_product_importer.py to migrate data products to another workspace.

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import csv
from admin import *
from lib.helpers import sdk_helpers

# Initialize logger
util_name = os.path.basename(__file__).split('.')[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))


class BulkDataProductExporter(Admin):

	def __init__(self, profile, config_file: str = None, progress: Progress = None):
		"""Creates an instance of BulkDataProductExporter.

		Args:
			profile(str): Profile to use stored in montecarlo cli.
			config_file (str): Path to the Configuration File.
			progress(Progress): Progress bar.
		"""
		super().__init__(profile, config_file, progress)
		self.progress_bar = progress

	def export_data_products(self, output_file: str):
		"""Export all data products and their assets to CSV.

		Args:
			output_file (str): Path to output CSV file.
		"""
		LOGGER.info("Fetching data products...")
		data_products = self.get_data_products_list()
		active_dps = [dp for dp in data_products if not dp.is_deleted]
		LOGGER.info(f"Found {len(active_dps)} active data products")

		rows_to_write = []

		for dp in active_dps:
			assets = self.get_data_product_assets(dp.uuid)
			LOGGER.info(f"  - {dp.name}: {len(assets)} assets")

			if assets:
				for mcon in assets:
					rows_to_write.append([dp.name, dp.description or '', mcon])
			else:
				rows_to_write.append([dp.name, dp.description or '', ''])

		with open(output_file, 'w', newline='') as csvfile:
			writer = csv.writer(csvfile)
			writer.writerow(['data_product_name', 'data_product_description', 'asset_mcon'])
			for row in rows_to_write:
				writer.writerow(row)

		LOGGER.info(f"Export complete: {output_file} ({len(rows_to_write)} rows)")


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
		util.export_data_products(args.output_file)

	util = BulkDataProductExporter(args.profile)
	run_utility(util, args)


if __name__ == "__main__":
	main()
