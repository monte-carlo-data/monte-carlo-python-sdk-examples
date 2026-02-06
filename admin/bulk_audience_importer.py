# Instructions:
# 1. Run this script: python admin/bulk_audience_importer.py -p <profile> -i <input_file>
# 2. Profile should be configured in configs/configs.ini
#
# Input CSV format (with header):
#   audience_name,notification_type,recipients,recipients_display_names,integration_id
#   Pablo Test,EMAIL,palvarez@company.com,palvarez@company.com,
#   Data Team,SLACK,C12345678,#data-alerts,<integration-uuid>
#
# Notification types: EMAIL, SLACK, SLACK_V2, MSTEAMS, MSTEAMS_V2, PAGERDUTY,
#                     OPSGENIE, WEBHOOK, GOOGLE_CHAT, SERVICENOW, JIRA, etc.
#
# This importer creates or updates notification audiences in Monte Carlo.
# One row per notification_setting.

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import csv
from pathlib import Path
from admin import *
from lib.helpers import sdk_helpers
from pycarlo.core import Query, Mutation

# Initialize logger
util_name = os.path.basename(__file__).split('.')[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))


class BulkAudienceImporter(Admin):
	"""Import notification audiences to Monte Carlo.

	Parses audience CSV files and creates/updates notification audiences
	using the Monte Carlo API.
	"""

	def __init__(self, profile, config_file: str = None, progress: Progress = None, validate: bool = True):
		"""Creates an instance of BulkAudienceImporter.

		Args:
			profile (str): Profile to use from configs.ini.
			config_file (str): Path to the Configuration File.
			progress (Progress): Progress bar.
			validate (bool): Whether to validate the MC connection.
		"""
		super().__init__(profile, config_file, progress, validate)
		self.progress_bar = progress

	def parse_audience_csv(self, input_file: str) -> dict:
		"""Parse an audience CSV file and return organized data.

		Expected CSV format (with header):
			audience_name,notification_type,recipients,recipients_display_names,integration_id
			Pablo Test,EMAIL,user@company.com,user@company.com,

		Args:
			input_file (str): Path to CSV file.

		Returns:
			dict: Result with keys:
				- success (bool): Whether parsing succeeded
				- audiences (dict): Audience name -> {notification_settings} mapping
				- count (int): Number of audiences found
				- errors (list): Any errors encountered
		"""
		file_path = Path(input_file)
		errors = []
		audiences = {}

		if not file_path.is_file():
			return {
				'success': False,
				'audiences': {},
				'count': 0,
				'errors': [f"File not found: {input_file}"]
			}

		try:
			with open(input_file, "r") as input_csv:
				reader = csv.DictReader(input_csv)

				# Check for required columns
				if reader.fieldnames is None:
					return {
						'success': False,
						'audiences': {},
						'count': 0,
						'errors': ["CSV file is empty or has no headers"]
					}

				if 'audience_name' not in reader.fieldnames:
					return {
						'success': False,
						'audiences': {},
						'count': 0,
						'errors': ["Missing required column: audience_name"]
					}

				row_num = 1  # Header is row 1
				for row in reader:
					row_num += 1
					audience_name = row.get('audience_name', '').strip()
					notification_type = row.get('notification_type', '').strip()
					recipients_str = row.get('recipients', '').strip()
					display_names_str = row.get('recipients_display_names', '').strip()
					integration_id = row.get('integration_id', '').strip()

					if not audience_name:
						errors.append(f"Row {row_num}: Empty audience_name")
						continue

					if audience_name not in audiences:
						audiences[audience_name] = {
							'notification_settings': []
						}

					# Add notification setting if type and recipients are specified
					if notification_type and recipients_str:
						# Parse semicolon-separated recipients
						recipients = [r.strip() for r in recipients_str.split(';') if r.strip()]
						display_names = [r.strip() for r in display_names_str.split(';') if r.strip()] if display_names_str else recipients

						notification_setting = {
							'notificationType': notification_type,
							'recipients': recipients,
							'recipientsDisplayNames': display_names
						}

						# Add optional integration ID if present
						if integration_id:
							notification_setting['integrationId'] = integration_id

						audiences[audience_name]['notification_settings'].append(notification_setting)

			if not audiences and not errors:
				errors.append("No valid audiences found in file")

		except Exception as e:
			errors.append(f"Error reading file: {str(e)}")

		return {
			'success': len(errors) == 0 and len(audiences) > 0,
			'audiences': audiences,
			'count': len(audiences),
			'errors': errors
		}

	def _get_existing_audiences_query(self) -> Query:
		"""Build query to fetch existing notification audiences.

		Returns:
			Query: Formed MC Query object.
		"""
		query = Query()
		get_audiences = query.get_notification_audiences()
		get_audiences.__fields__("uuid", "label")
		get_audiences.created_by.__fields__("email")
		get_audiences.notification_settings.__fields__("uuid", "recipients_display_names")

		return query

	def get_existing_audiences_map(self) -> dict:
		"""Get a mapping of existing audiences in Monte Carlo.

		Returns:
			dict: Audience name (label) -> dict with 'uuid', 'notification_settings'
		"""
		LOGGER.debug("Retrieving existing audiences")

		try:
			query = self._get_existing_audiences_query()
			response = self.auth.client(query).get_notification_audiences

			audience_mapping = {}
			for audience in response:
				name = audience.label if hasattr(audience, 'label') else ''
				if name:
					notification_settings = []
					if hasattr(audience, 'notification_settings') and audience.notification_settings:
						for ns in audience.notification_settings:
							recipients = ns.recipients_display_names if hasattr(ns, 'recipients_display_names') else []
							notification_settings.append({
								'uuid': ns.uuid if hasattr(ns, 'uuid') else '',
								'recipients': recipients
							})

					audience_mapping[name] = {
						'uuid': audience.uuid if hasattr(audience, 'uuid') else '',
						'notification_settings': notification_settings
					}

			LOGGER.debug(f"Found {len(audience_mapping)} existing audiences")
			return audience_mapping

		except Exception as e:
			LOGGER.error(f"Failed to fetch existing audiences: {e}")
			return {}

	def import_single_audience(self, name: str, notification_settings: list = None, uuid: str = None) -> dict:
		"""Create or update a single audience with notification settings.

		Args:
			name (str): Audience name (label).
			notification_settings (list): List of notification setting dicts with:
				- notificationType (str): EMAIL, SLACK, PAGERDUTY, etc.
				- recipients (list): List of recipient identifiers
				- recipientsDisplayNames (list): Display names for recipients
				- integrationId (str, optional): UUID of integration for Slack/Teams
			uuid (str): Existing audience UUID for updates (optional).

		Returns:
			dict: Result with keys:
				- success (bool): Whether import succeeded
				- audience_uuid (str): UUID of created/updated audience
				- audience_name (str): Name of the audience
				- action (str): 'created' or 'updated' or 'skipped'
				- error (str): Error message if failed
		"""
		try:
			if uuid:
				# Audience already exists - skip
				LOGGER.info(f"Audience '{name}' already exists (uuid: {uuid}) - skipping")
				return {
					'success': True,
					'audience_uuid': uuid,
					'audience_name': name,
					'action': 'skipped',
					'error': None
				}

			# Build notification settings for the mutation
			settings_count = len(notification_settings) if notification_settings else 0
			LOGGER.info(f"Creating audience '{name}' with {settings_count} notification setting(s)")

			mutation_settings = []
			if notification_settings:
				for ns in notification_settings:
					setting = {
						'notification_type': ns.get('notificationType', 'EMAIL'),
						'recipients': ns.get('recipients', [])
					}
					# Add optional fields
					if ns.get('recipientsDisplayNames'):
						setting['recipients_display_names'] = ns['recipientsDisplayNames']
					if ns.get('integrationId'):
						setting['integration_id'] = ns['integrationId']

					mutation_settings.append(setting)

			# Execute mutation using SDK
			mutation = Mutation()
			create_audience = mutation.create_or_update_audience(
				label=name,
				notification_settings=mutation_settings
			)
			create_audience.audience.__fields__('uuid', 'label')

			response = self.auth.client(mutation).create_or_update_audience

			if response and response.audience:
				audience = response.audience
				LOGGER.info(f"Audience '{audience.label}' ({audience.uuid}) created successfully")
				return {
					'success': True,
					'audience_uuid': audience.uuid,
					'audience_name': audience.label,
					'action': 'created',
					'error': None
				}
			else:
				return {
					'success': False,
					'audience_uuid': None,
					'audience_name': name,
					'action': None,
					'error': 'No response from mutation'
				}

		except Exception as e:
			LOGGER.error(f"Unable to create audience '{name}': {e}")
			return {
				'success': False,
				'audience_uuid': None,
				'audience_name': name,
				'action': None,
				'error': str(e)
			}

	def import_audiences(self, input_file: str, dry_run: bool = True) -> dict:
		"""Import audiences from a CSV file.

		Args:
			input_file (str): Path to input CSV file.
			dry_run (bool): If True, preview changes without committing.

		Returns:
			dict: Result with keys:
				- success (bool): Whether all imports succeeded
				- created (int): Number of audiences created
				- updated (int): Number of audiences updated
				- skipped (int): Number of audiences skipped (already exist)
				- failed (int): Number of audiences that failed
				- errors (list): Any errors encountered
		"""
		# Parse the input file
		parse_result = self.parse_audience_csv(input_file)
		if not parse_result['success']:
			return {
				'success': False,
				'dry_run': dry_run,
				'created': 0,
				'updated': 0,
				'skipped': 0,
				'failed': 0,
				'errors': parse_result['errors']
			}

		audiences_data = parse_result['audiences']
		LOGGER.info(f"Parsed {len(audiences_data)} audiences from file")

		# Get existing audiences
		existing_audiences = self.get_existing_audiences_map()
		LOGGER.info(f"Found {len(existing_audiences)} existing audiences in Monte Carlo")

		# Import each audience
		created = 0
		updated = 0
		skipped = 0
		failed = 0
		errors = []

		total_audiences = len(audiences_data)
		for audience_name, data in audiences_data.items():
			existing = existing_audiences.get(audience_name)
			settings = data.get('notification_settings', [])
			settings_count = len(settings)

			if dry_run:
				if existing:
					LOGGER.info(f"[DRY-RUN] WOULD SKIP: {audience_name} (already exists)")
					skipped += 1
				else:
					LOGGER.info(f"[DRY-RUN] WOULD CREATE: {audience_name} with {settings_count} notification setting(s)")
					created += 1
			else:
				result = self.import_single_audience(
					name=audience_name,
					notification_settings=settings,
					uuid=existing['uuid'] if existing else None
				)

				if result['success']:
					if result['action'] == 'created':
						created += 1
					elif result['action'] == 'updated':
						updated += 1
					elif result['action'] == 'skipped':
						skipped += 1
				else:
					failed += 1
					errors.append(f"{audience_name}: {result['error']}")

			# Update progress bar if available
			if self.progress_bar and self.progress_bar.tasks:
				self.progress_bar.update(
					self.progress_bar.tasks[0].id,
					advance=50 / total_audiences
				)

		return {
			'success': failed == 0,
			'dry_run': dry_run,
			'created': created,
			'updated': updated,
			'skipped': skipped,
			'failed': failed,
			'errors': errors
		}


def main(*args, **kwargs):
	"""Main entry point for CLI usage."""

	# Capture Command Line Arguments
	parser = sdk_helpers.generate_arg_parser(
		os.path.basename(os.path.dirname(os.path.abspath(__file__))),
		os.path.basename(__file__)
	)

	if not args:
		args = parser.parse_args(*args, **kwargs)
	else:
		sdk_helpers.dump_help(parser, main, *args)
		args = parser.parse_args(*args, **kwargs)

	@sdk_helpers.ensure_progress
	def run_utility(progress, util, args):
		util.progress_bar = progress

		# Check for force flag (dry_run by default)
		force = getattr(args, 'force', None)
		dry_run = force != 'yes'

		result = util.import_audiences(args.input_file, dry_run=dry_run)

		# Log summary
		mode = "DRY-RUN" if result['dry_run'] else "COMMITTED"
		LOGGER.info("=" * 40)
		LOGGER.info(f"Import Summary ({mode}):")
		LOGGER.info(f"  Created: {result['created']}")
		LOGGER.info(f"  Updated: {result['updated']}")
		LOGGER.info(f"  Skipped: {result['skipped']}")
		LOGGER.info(f"  Failed:  {result['failed']}")

		if result['errors']:
			LOGGER.error("Errors:")
			for error in result['errors']:
				LOGGER.error(f"  - {error}")

		if result['dry_run']:
			LOGGER.info("")
			LOGGER.info("This was a DRY-RUN. To commit changes, run with --force yes")

	util = BulkAudienceImporter(args.profile)
	run_utility(util, args)


if __name__ == '__main__':
	main()
