import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from monitors import *

# Initialize logger
util_name = __file__.split('/')[-1].split('.')[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))


class DeleteMonitorsByAudience(Monitors):
	def __init__(self, profile,config_file: str = None, progress: Progress = None):
		"""Creates an instance of DeleteMonitorsByAudience.

		Args:
			profile(str): Profile to use stored in montecarlo cli.
			config_file (str): Path to the Configuration File.
			progress(Progress): Progress bar.
		"""

		super().__init__(profile, config_file, progress)
		self.progress_bar = progress
		self.rule_operator_type = None

	def delete_custom_monitors(self,audiences):
		_,monitors = self.get_monitors_by_audience(audiences)
		if len(monitors) == 0:
			LOGGER.error("No monitors exist for given audience(s)")
			sys.exit(1)
		LOGGER.info(monitors)
		rules = [const.MonitorTypes.VOLUME,const.MonitorTypes.CUSTOM_SQL,const.MonitorTypes.FRESHNESS,
		         const.MonitorTypes.FIELD_QUALITY, const.MonitorTypes.COMPARISON,const.MonitorTypes.VALIDATION]
		for monitor in monitors:
			self.progress_bar.update(self.progress_bar.tasks[0].id, advance=100 / len(monitors))
			error = False
			if monitor["monitor_type"] in rules:
				response = self.auth.client(self.delete_custom_rule(monitor["uuid"])).delete_custom_rule
				if not response.uuid:
					error = True
			else:
				response = self.auth.client(self.delete_monitor(monitor["uuid"])).delete_monitor
				if not response.success:
					error = True
			if error:
				LOGGER.info(f"Deletion Not Successful for: {monitor.uuid}")
			else:
				LOGGER.info(f"Deletion Successful for: {monitor.uuid}")


def main(*args, **kwargs):

	# Capture Command Line Arguments
	formatter = lambda prog: argparse.RawTextHelpFormatter(prog, max_help_position=120)
	parser = argparse.ArgumentParser(description="\n[ DELETE MONITORS BY AUDIENCE ]\n\n\t• Delete all monitors within "
												 "a given audience. Provide the audience name to delete when prompted"
									 .expandtabs(4), formatter_class=formatter)
	parser._optionals.title = "Options"
	parser._positionals.title = "Commands"
	m = ''

	parser.add_argument('--profile', '-p', required=False, default="default",
						help='Specify an MCD profile name. Uses default otherwise', metavar=m)
	parser.add_argument('--audience', '-a', required=True,
						help='Audience for which to delete all monitors. If multiple Audiences, pass all in comma separated list',
						metavar=m)

	if not args[0]:
		args = parser.parse_args(*args, **kwargs)
	else:
		sdk_helpers.dump_help(parser, main, *args)
		args = parser.parse_args(*args, **kwargs)

	# Initialize variables
	audiences = sdk_helpers.parse_input(args.audience,',')
	profile = args.profile

	try:
		with (Progress() as progress):
			task = progress.add_task("[yellow][RUNNING]...", total=100)
			LogRotater.rotate_logs(retention_period=7)

			LOGGER.info(f"running utility using '{args.profile}' profile")
			util = DeleteMonitorsByAudience(profile,progress=progress)
			util.delete_custom_monitors(audiences)
			progress.update(task, description="[dodger_blue2][COMPLETE]", advance=100)

	except Exception as e:
		LOGGER.error(e,exc_info=False)
		print(traceback.format_exc())


if __name__ == '__main__':
	main()
