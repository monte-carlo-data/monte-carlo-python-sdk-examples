import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from monitors import *

# Initialize logger
util_name = __file__.split('/')[-1].split('.')[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))


class GenerateMonitorsYAMLFromCSV(Monitors, Tables):

    def __init__(self, profile: str = None, config_file: str = None, progress: Progress = None):
        """Creates an instance of GenerateMonitorsYAMLFromCSV.

		Args:
			profile(str): Profile to use stored in montecarlo's CLI configuration file(ini).
			config_file (str): Path to the Configuration File.
			progress(Progress): Progress bar.
		"""

        super().__init__(profile, config_file, progress, False)
        self.progress_bar = progress
        self.rule_operator_type = None

    @staticmethod
    def sanitize_tag(tag):
        if not tag:
            return []

        tags = []
        for t in tag.split(","):
            t = t.strip()
            if not t:
                continue
            if ":" in t:
                name, value = t.split(":", 1)
                tags.append({"name": name.strip(), "value": value.strip()})
            else:
                tags.append({"name": t})
        return tags

    def create_monitor_from_row(self, row):

        rule_name = f"{row['rule_category'].lower()}_{row['rule_subcategory'].replace(' ', '_').lower()}_{row['dqm_id']}"
        schedule = {}
        freq_raw = row.get("frequency", "").strip().lower()

        if freq_raw == "manual":
            schedule = {"type": "manual"}

        elif freq_raw and "t" in freq_raw and "+" in freq_raw:
            # Format: 2025-07-07T19:26:50+00:00 1440
            try:
                start_time_str, interval_str = freq_raw.split()
                schedule = {
					"interval_minutes": int(interval_str),
					"start_time": start_time_str,
				}
            except Exception as e:
                LOGGER.warning(
					f"Failed to parse datetime+interval format: {freq_raw} - {e}"
				)

        elif freq_raw and len(freq_raw.split()) == 5:
            # Format: cron expression
            schedule = {
				"interval_crontab": [freq_raw],
				"interval_crontab_day_operator": "AND",
				"start_time": datetime.now(timezone.utc).isoformat(timespec="seconds"),
			}

        elif freq_raw and "MCON" in freq_raw:
            schedule = {
				"type": "dynamic",
				"dynamic_schedule_tables": [freq_raw],
				"start_time": datetime.now(timezone.utc).isoformat(timespec="seconds"),
			}

        else:
            # Fallback to manual if format is unrecognized
            schedule = {"type": "manual"}

        structure = {
			"name": rule_name,
			"warehouse": row["warehouse"],
			"description": row.get("rule_description", rule_name),
			"notes": row.get("notes", ""),
			"sql": row["rule_query"],
			"alert_conditions": [{"operator": "GT", "threshold_value": 0.0}],
			"schedule": schedule,
			"audiences": [row["audience"]] if row.get("audience") else [],
			"priority": row.get("priority", "").upper(),
			"tags": self.sanitize_tag(row.get("tags", "")),
			"data_quality_dimension": row.get("dimension", "").upper(),
		}

        return {k: v for k, v in structure.items() if v}

    @staticmethod
    def ensure_montecarlo_yml(namespace_dir, namespace):
        path = namespace_dir / "montecarlo.yml"
        if not path.exists():
            with open(path, "w") as f:
                yaml.safe_dump({"version": 1, "namespace": namespace}, f)
            LOGGER.info(f"Created montecarlo.yml for namespace {namespace}")

    def write_monitors_to_file(self, namespace_dir, namespace, new_monitors):
        self.ensure_montecarlo_yml(namespace_dir, namespace)

        monitors_dir = Path(namespace_dir / "montecarlo")
        monitors_dir.mkdir(parents=True, exist_ok=True)
        output_path = monitors_dir / "monitors.yml"

        # Load or initialize monitors.yml
        if output_path.exists():
            with open(output_path, "r") as f:
                current_yaml = yaml.safe_load(f) or {}
        else:
            current_yaml = {}

        mc_section = current_yaml.setdefault("montecarlo", {})
        custom_sql_list = mc_section.setdefault("custom_sql", [])

        custom_sql_list.extend(new_monitors)

        with open(output_path, "w") as f:
            yaml.safe_dump(current_yaml, f, sort_keys=False)

        LOGGER.info(f"Updated monitors.yml for namespace {namespace}")

    def process_csv(self, input_csv, base_output_dir):
        namespace_monitors = {}
        with open(input_csv, newline="", encoding="utf-8-sig") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                namespace = row["namespace"].strip()
                monitor = self.create_monitor_from_row(row)
                namespace_monitors.setdefault(namespace, []).append(monitor)

        for namespace, monitors in namespace_monitors.items():
            namespace_dir = Path(base_output_dir) / namespace
            namespace_dir.mkdir(parents=True, exist_ok=True)
            self.write_monitors_to_file(namespace_dir, namespace, monitors)


def main(*args, **kwargs):
	# Capture Command Line Arguments
	parser = sdk_helpers.generate_arg_parser(os.path.basename(os.path.dirname(os.path.abspath(__file__))),
											 os.path.basename(__file__))

	if not args:
		args = parser.parse_args(*args, **kwargs)
	else:
		sdk_helpers.dump_help(parser, main, *args)
		args = parser.parse_args(*args, **kwargs)

	@sdk_helpers.ensure_progress
	def run_utility(progress, util, args):
		util.progress_bar = progress
		util.process_csv(args.input_csv, args.output_dir)

	util = GenerateMonitorsYAMLFromCSV()
	run_utility(util, args)


if __name__ == '__main__':
	main()
