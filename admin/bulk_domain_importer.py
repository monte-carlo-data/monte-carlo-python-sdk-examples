import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import csv
from tables import *
from lib.helpers import sdk_helpers

# Initialize logger
util_name = os.path.basename(__file__).split('.')[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))


class BulkDomainImporter(Tables):

	def __init__(self, profile, config_file: str = None, progress: Progress = None):
		"""Creates an instance of BulkDomainImporter.

		Args:
			profile(str): Profile to use stored in montecarlo test.
			config_file (str): Path to the Configuration File.
			progress(Progress): Progress bar.
		"""
		super().__init__(profile, config_file, progress)
		self.progress_bar = progress

	@staticmethod
	def validate_input_file(input_file: str):
		"""
		Validate input CSV file. Expected format:
		domain_name,asset_mcon

		Example:
		Finance,MCON++123++456++table++transactions
		Analytics,MCON++123++456++view++sessions
		"""

		file = Path(input_file)

		if file.is_file():
			rows = []
			with open(input_file, "r") as input_csv:
				reader = csv.reader(input_csv, delimiter=",")
				for row in reader:
					if len(row) < 2:
						LOGGER.error(
							f"Expected at least 2 columns (domain_name, asset_mcon), got {len(row)}"
						)
						sys.exit(1)
					domain_name = row[0].strip()
					asset_mcon = row[1].strip()
					rows.append((domain_name, asset_mcon))
			if len(rows) == 0:
				LOGGER.error("No rows present in input file")
				sys.exit(1)
			return rows
		else:
			LOGGER.error("Invalid input file")
			sys.exit(1)

	def import_domains(self, rows: list):
		"""
		Call MC API to create or update domains and assign assets.

		Args:
			rows(list): List of tuples (domain_name, asset_mcon, description)
		"""

		LOGGER.debug(f"Retrieving existing domains")
		existing_domains = self.get_domains()
		domain_mapping = {domain.name: {'uuid': domain.uuid, 'desc': domain.description} for domain in existing_domains}
		LOGGER.debug(f"Generating payload for {len(rows)} domain assignments")

		domains_payload = {}
		# Group assets by domain
		for domain_name, asset_mcon in rows:
			if domain_name not in domains_payload:
				domains_payload[domain_name] = {
					"name": domain_name,
					"assignments": [],
				}
			domains_payload[domain_name]["assignments"].append(asset_mcon)

		# Batch by domain
		for domain_name, payload in domains_payload.items():
			domain = domain_mapping.get(domain_name, None)
			domain_uuid = domain.get("uuid") if domain else None
			domain_description = domain.get("desc") if domain else None
			LOGGER.info(
				f"Creating/updating domain '{domain_name}' with {len(payload['assignments'])} assets"
			)

			response = self.create_domain(payload["name"], payload["assignments"], domain_description, domain_uuid)

			if not response:
				LOGGER.error(f"Unable to create/update domain {domain_name}")
			else:
				domain = response.domain
				LOGGER.info(
					f"Domain '{domain.name}' ({domain.uuid}) processed successfully"
				)


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
		rows = util.validate_input_file(args.input_file)
		util.import_domains(rows)

	util = BulkDomainImporter(args.profile)
	run_utility(util, args)


if __name__ == "__main__":
	main()
