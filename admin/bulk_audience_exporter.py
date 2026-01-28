# Instructions:
# 1. Run this script: python admin/bulk_audience_exporter.py -p <profile> -o <output_file>
# 2. Profile should be configured in configs/configs.ini
#
# Output CSV format:
#   audience_name,audience_created_by,recipients_display_names
#   Pablo Test,user@company.com,user@company.com
#   Data Team,admin@company.com,"user1@company.com, user2@company.com"
#
# Note: The output CSV can be used with bulk_audience_importer.py to migrate audiences
# to another workspace. One row per audience-notification_setting combination.

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import csv
from admin import *
from lib.helpers import sdk_helpers
from pycarlo.core import Query

# Initialize logger
util_name = os.path.basename(__file__).split('.')[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))


class BulkAudienceExporter(Admin):
	"""Export notification audiences from Monte Carlo.

	Fetches all notification audiences including their notification channels
	and exports them to CSV format for migration purposes.
	"""

	def __init__(self, profile, config_file: str = None, progress: Progress = None):
		"""Creates an instance of BulkAudienceExporter.

		Args:
			profile (str): Profile to use from configs.ini.
			config_file (str): Path to the Configuration File.
			progress (Progress): Progress bar.
		"""
		super().__init__(profile, config_file, progress)
		self.progress_bar = progress

	def _get_audiences_query(self) -> Query:
		"""Build query to fetch notification audiences with their channels.

		Returns:
			Query: Formed MC Query object for getNotificationAudiences.
		"""
		query = Query()
		get_audiences = query.get_notification_audiences()
		# Note: Field names discovered via API - NotificationAudience uses 'label' not 'name'
		get_audiences.__fields__("uuid", "label")
		# createdBy is a User object - need to select sub-fields
		get_audiences.created_by.__fields__("email")
		# notification_settings contains notification channel info
		get_audiences.notification_settings.__fields__("uuid", "recipients_display_names")

		return query

	def get_all_audiences(self) -> list:
		"""Fetch all notification audiences with their channels.

		Returns:
			list[dict]: List of audiences with:
				- name (str): Audience name (label)
				- created_by (str): Creator email
				- uuid (str): Audience UUID
				- notification_settings (list): List of notification setting dicts
		"""
		LOGGER.info("Fetching notification audiences...")

		try:
			query = self._get_audiences_query()
			response = self.auth.client(query).get_notification_audiences

			results = []
			for audience in response:
				notification_settings = []

				# Process notification_settings if present
				if hasattr(audience, 'notification_settings') and audience.notification_settings:
					for ns in audience.notification_settings:
						# recipients_display_names is a list
						recipients = ns.recipients_display_names if hasattr(ns, 'recipients_display_names') else []
						notification_settings.append({
							'uuid': ns.uuid if hasattr(ns, 'uuid') else '',
							'recipients_display_names': ', '.join(recipients) if recipients else ''
						})

				# created_by is a User object with email field
				created_by_email = ''
				if hasattr(audience, 'created_by') and audience.created_by:
					created_by_email = audience.created_by.email if hasattr(audience.created_by, 'email') else ''

				results.append({
					'name': audience.label if hasattr(audience, 'label') else '',
					'created_by': created_by_email,
					'uuid': audience.uuid if hasattr(audience, 'uuid') else '',
					'notification_settings': notification_settings
				})

			LOGGER.info(f"Found {len(results)} audiences")
			return results

		except Exception as e:
			LOGGER.error(f"Failed to fetch audiences: {e}")
			raise

	def export_audiences(self, output_file: str) -> dict:
		"""Export all audiences to CSV file.

		Args:
			output_file (str): Path to output CSV file.

		Returns:
			dict: Export result with success, count, and file path.
		"""
		LOGGER.info(f"Exporting audiences to {output_file}...")

		try:
			audiences_data = self.get_all_audiences()

			rows_written = 0
			with open(output_file, 'w', newline='') as csvfile:
				writer = csv.writer(csvfile)
				writer.writerow([
					'audience_name', 'audience_created_by', 'recipients_display_names'
				])

				for audience in audiences_data:
					if audience['notification_settings']:
						for ns in audience['notification_settings']:
							writer.writerow([
								audience['name'],
								audience['created_by'],
								ns['recipients_display_names']
							])
							rows_written += 1
					else:
						# Include audiences with no notification settings
						writer.writerow([
							audience['name'],
							audience['created_by'],
							''
						])
						rows_written += 1

			LOGGER.info(f"Export complete: {len(audiences_data)} audiences, {rows_written} rows written to {output_file}")

			return {
				'success': True,
				'count': len(audiences_data),
				'rows': rows_written,
				'file': output_file
			}

		except Exception as e:
			LOGGER.error(f"Export failed: {e}")
			return {
				'success': False,
				'count': 0,
				'errors': [str(e)]
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
		output_file = getattr(args, 'output_file', None) or 'audiences.csv'
		util.export_audiences(output_file)

	util = BulkAudienceExporter(args.profile)
	run_utility(util, args)


if __name__ == '__main__':
	main()
