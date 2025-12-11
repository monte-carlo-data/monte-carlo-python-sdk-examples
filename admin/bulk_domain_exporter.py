# Instructions:
# 1. Run this script: python admin/bulk_domain_exporter.py -p <profile> -o <output_file>
# 2. Profile should be configured in ~/.mcd/profiles.ini
#
# Output CSV format (no header):
#   Finance,MCON++123++456++table++transactions
#   Analytics,MCON++123++456++view++sessions
#
# Note: The output CSV can be used with bulk_domain_importer.py to migrate domains to another workspace.

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import csv
from tables import *
from lib.helpers import sdk_helpers

# Initialize logger
util_name = os.path.basename(__file__).split('.')[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))


class BulkDomainExporter(Tables):

	def __init__(self, profile, config_file: str = None, progress: Progress = None):
		"""Creates an instance of BulkDomainExporter.

		Args:
			profile(str): Profile to use stored in montecarlo cli.
			config_file (str): Path to the Configuration File.
			progress(Progress): Progress bar.
		"""
		super().__init__(profile, config_file, progress)
		self.progress_bar = progress

	def get_domain_assets(self, domain_uuid: str) -> list:
		"""Get all asset MCONs for a domain with pagination.

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

	def export_domains(self, output_file: str):
		"""Export all domains and their assets to CSV.

		Args:
			output_file (str): Path to output CSV file.
		"""
		LOGGER.info("Fetching domains...")
		domains = self.get_domains()
		LOGGER.info(f"Found {len(domains)} domains")

		with open(output_file, 'w', newline='') as csvfile:
			writer = csv.writer(csvfile)

			for domain in domains:
				LOGGER.info(f"Processing: {domain.name}")
				assets = self.get_domain_assets(domain.uuid)

				if assets:
					for mcon in assets:
						writer.writerow([domain.name, mcon])
				else:
					# Include empty domains
					writer.writerow([domain.name, ''])

		LOGGER.info(f"Export complete: {output_file}")


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
		util.export_domains(args.output_file)

	util = BulkDomainExporter(args.profile)
	run_utility(util, args)


if __name__ == "__main__":
	main()
