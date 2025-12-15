# Instructions:
# 1. Run this script: python admin/bulk_domain_importerv2.py -p <profile> -i <input_file>
# 2. Profile should be configured in ~/.mcd/profiles.ini
#
# Input CSV format (with header):
#   domain_name,domain_description,asset_mcon
#   Finance,Financial data domain,MCON++123++456++table++transactions
#   Analytics,Analytics domain,MCON++123++456++view++sessions
#
# This is the v2 version with shareable functions that can be used by other modules
# (such as the migration module) without requiring file I/O or causing program exits.

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import csv
from pathlib import Path
from tables import *
from lib.helpers import sdk_helpers

# Initialize logger
util_name = os.path.basename(__file__).split('.')[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))


class BulkDomainImporterV2(Tables):

	def __init__(self, profile, config_file: str = None, progress: Progress = None, validate: bool = None):
		"""Creates an instance of BulkDomainImporterV2.

		Args:
			profile(str): Profile to use stored in montecarlo cli.
			config_file (str): Path to the Configuration File.
			progress(Progress): Progress bar.
			validate(bool): Whether to validate the MC connection.
		"""
		super().__init__(profile, config_file, progress, validate)
		self.progress_bar = progress

	def parse_domain_csv(self, input_file: str) -> dict:
		"""Parse a domain CSV file and return organized data.

		This method returns data instead of calling sys.exit(), making it
		suitable for use by other modules like the migration tool.

		Expected CSV format (with header):
			domain_name,domain_description,asset_mcon
			Finance,Financial data domain,MCON++123++table++transactions

		Args:
			input_file (str): Path to CSV file.

		Returns:
			dict: Result with keys:
				- success (bool): Whether parsing succeeded
				- domains (dict): Domain name -> {description, mcons} mapping
				- count (int): Number of domains found
				- errors (list): Any errors encountered
		"""
		file_path = Path(input_file)
		errors = []
		domains = {}

		if not file_path.is_file():
			return {
				'success': False,
				'domains': {},
				'count': 0,
				'errors': [f"File not found: {input_file}"]
			}

		try:
			with open(input_file, "r") as input_csv:
				reader = csv.DictReader(input_csv)

				# Check for required column
				if reader.fieldnames is None:
					return {
						'success': False,
						'domains': {},
						'count': 0,
						'errors': ["CSV file is empty or has no headers"]
					}

				if 'domain_name' not in reader.fieldnames:
					return {
						'success': False,
						'domains': {},
						'count': 0,
						'errors': ["Missing required column: domain_name"]
					}

				row_num = 1  # Header is row 1
				for row in reader:
					row_num += 1
					domain_name = row.get('domain_name', '').strip()
					description = row.get('domain_description', '').strip()
					asset_mcon = row.get('asset_mcon', '').strip()

					if not domain_name:
						errors.append(f"Row {row_num}: Empty domain_name")
						continue

					if domain_name not in domains:
						domains[domain_name] = {
							'description': description or None,
							'mcons': []
						}

					# Update description if we have one and didn't before
					if description and not domains[domain_name]['description']:
						domains[domain_name]['description'] = description

					if asset_mcon:  # Only add non-empty MCONs
						domains[domain_name]['mcons'].append(asset_mcon)

			if not domains and not errors:
				errors.append("No valid domains found in file")

		except Exception as e:
			errors.append(f"Error reading file: {str(e)}")

		return {
			'success': len(errors) == 0 and len(domains) > 0,
			'domains': domains,
			'count': len(domains),
			'errors': errors
		}

	def get_existing_domains_map(self) -> dict:
		"""Get a mapping of existing domains in Monte Carlo.

		Returns:
			dict: Domain name -> dict with 'uuid', 'description', 'assignments'
		"""
		LOGGER.debug("Retrieving existing domains")
		existing_domains = self.get_domains()

		domain_mapping = {
			domain.name: {
				'uuid': domain.uuid,
				'description': domain.description,
				'assignments': domain.assignments or []
			}
			for domain in existing_domains
		}

		LOGGER.debug(f"Found {len(domain_mapping)} existing domains")
		return domain_mapping

	def import_single_domain(self, name: str, mcons: list, description: str = None,
							 uuid: str = None, merge_assignments: bool = True,
							 existing_assignments: list = None) -> dict:
		"""Create or update a single domain.

		This method handles one domain at a time, returning a result dict.
		This allows callers (like the migration tool) to implement dry-run
		mode or custom error handling.

		Args:
			name (str): Domain name.
			mcons (list): List of asset MCONs to assign.
			description (str): Domain description (optional).
			uuid (str): Existing domain UUID for updates (optional).
			merge_assignments (bool): If True, merge with existing assignments.
			existing_assignments (list): Current assignments if known (avoids extra API call).

		Returns:
			dict: Result with keys:
				- success (bool): Whether import succeeded
				- domain_uuid (str): UUID of created/updated domain
				- domain_name (str): Name of the domain
				- action (str): 'created' or 'updated'
				- error (str): Error message if failed
		"""
		try:
			# Merge assignments if requested
			if merge_assignments and existing_assignments:
				assignments = list(set(mcons + existing_assignments))
			else:
				assignments = mcons

			# Filter out empty strings
			assignments = [a for a in assignments if a]

			LOGGER.info(f"{'Updating' if uuid else 'Creating'} domain '{name}' with {len(assignments)} assets")

			response = self.create_domain(name, assignments, description, uuid)
			domain = response.domain

			LOGGER.info(f"Domain '{domain.name}' ({domain.uuid}) processed successfully")

			return {
				'success': True,
				'domain_uuid': domain.uuid,
				'domain_name': domain.name,
				'action': 'updated' if uuid else 'created',
				'error': None
			}

		except Exception as e:
			LOGGER.error(f"Unable to create/update domain '{name}': {e}")
			return {
				'success': False,
				'domain_uuid': None,
				'domain_name': name,
				'action': None,
				'error': str(e)
			}

	def import_domains(self, input_file: str) -> dict:
		"""Import domains from a CSV file.

		This is the main entry point that orchestrates parsing and importing.

		Args:
			input_file (str): Path to input CSV file.

		Returns:
			dict: Result with keys:
				- success (bool): Whether all imports succeeded
				- created (int): Number of domains created
				- updated (int): Number of domains updated
				- failed (int): Number of domains that failed
				- errors (list): Any errors encountered
		"""
		# Parse the input file
		parse_result = self.parse_domain_csv(input_file)
		if not parse_result['success']:
			return {
				'success': False,
				'created': 0,
				'updated': 0,
				'failed': 0,
				'errors': parse_result['errors']
			}

		domains_data = parse_result['domains']
		LOGGER.info(f"Parsed {len(domains_data)} domains from file")

		# Get existing domains
		existing_domains = self.get_existing_domains_map()

		# Import each domain
		created = 0
		updated = 0
		failed = 0
		errors = []

		total_domains = len(domains_data)
		for domain_name, data in domains_data.items():
			existing = existing_domains.get(domain_name)

			# Use description from file, fall back to existing description
			description = data['description'] or (existing['description'] if existing else None)

			result = self.import_single_domain(
				name=domain_name,
				mcons=data['mcons'],
				description=description,
				uuid=existing['uuid'] if existing else None,
				merge_assignments=True,
				existing_assignments=existing['assignments'] if existing else None
			)

			if result['success']:
				if result['action'] == 'created':
					created += 1
				else:
					updated += 1
			else:
				failed += 1
				errors.append(f"{domain_name}: {result['error']}")

			# Update progress bar if available
			if self.progress_bar and self.progress_bar.tasks:
				self.progress_bar.update(
					self.progress_bar.tasks[0].id,
					advance=50 / total_domains
				)

		return {
			'success': failed == 0,
			'created': created,
			'updated': updated,
			'failed': failed,
			'errors': errors
		}


def main(*args, **kwargs):
	# Capture Command Line Arguments
	parser = sdk_helpers.generate_arg_parser(
		os.path.basename(os.path.dirname(os.path.abspath(__file__))),
		os.path.basename(__file__)
	)

	if not args:
		args = parser.parse_args(*args, **kwargs)
	else:
		sdk_helpers.dump_help(parser, main, *args)

	@sdk_helpers.ensure_progress
	def run_utility(progress, util, args):
		util.progress_bar = progress
		result = util.import_domains(args.input_file)

		# Log summary
		LOGGER.info("=" * 40)
		LOGGER.info("Import Summary:")
		LOGGER.info(f"  Created: {result['created']}")
		LOGGER.info(f"  Updated: {result['updated']}")
		LOGGER.info(f"  Failed:  {result['failed']}")

		if result['errors']:
			LOGGER.error("Errors:")
			for error in result['errors']:
				LOGGER.error(f"  - {error}")

	util = BulkDomainImporterV2(args.profile)
	run_utility(util, args)


if __name__ == "__main__":
	main()

