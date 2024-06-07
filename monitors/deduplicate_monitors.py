import yaml
import subprocess
import argparse

if __name__ == '__main__':

    # Capture Command Line Arguments
    parser = argparse.ArgumentParser(description='Export UI Based Monitors')
    parser.add_argument('--profile', '-p', required=True, default="default",
                        help='Specify an MCD profile name. Uses default otherwise')
    parser.add_argument('--namespace', '-n', required=False,
                        help='Namespace of monitors configuration.')

    args = parser.parse_args()

    # Initialize variables
    profile = args.profile
    namespace = args.namespace

    monitors_file = 'monitors.yaml.bkp'
    with open(monitors_file, 'r') as file:
        yaml_dict = yaml.safe_load(file)
        metric_monitors = yaml_dict.get("montecarlo").get("field_health")

    # Initializing compare keys
    comp_keys = ['table', 'timestamp_field', 'lookback_days', 'aggregation_time_interval', 'connection_name',
                 'use_important_fields', 'use_partition_clause', 'metric']

    # Compare each monitor with the rest to find possible duplicates
    duplicate_indexes = []
    for i in range(len(metric_monitors) - 1):
        for j in range(i + 1, len(metric_monitors)):
            if all(metric_monitors[i].get(key) == metric_monitors[j].get(key) for key in comp_keys):
                print(f"Possible duplicate monitors in {monitors_file}: {i} - {metric_monitors[i].get('table')} "
                      f"and {j} - {metric_monitors[j].get('table')}")
                duplicate_indexes.append(i)

    # Remove duplicates
    for index in duplicate_indexes:
        del metric_monitors[index]

    # Save as new file
    with open('monitors.yml', 'w') as outfile:
        yaml.safe_dump(yaml_dict, outfile, sort_keys=False)

    print("Checking montecarlo cli version...")
    cmd = subprocess.run(["montecarlo", "--version"], capture_output=True, text=True)
    if cmd.returncode != 0:
        print(" [ 𐄂 failure ] montecarlo cli is not installed")
        exit(cmd.returncode)
    else:
        print(f" [ ✔ success ] montecarlo cli present\n")

    print("Validating montecarlo cli connection...")
    cmd = subprocess.run(["montecarlo", "--profile", profile, "validate"], capture_output=True, text=True)
    if cmd.returncode != 0:
        print(" [ 𐄂 failure ] an error occurred")
        print(cmd.stderr)
        exit(cmd.returncode)
    else:
        print(f" [ ✔ success ] validation complete\n")

    print("Executing new configuration dry-run...")
    if not namespace:
        cmd = subprocess.run(["montecarlo", "--profile", profile, "monitors", "apply", "--dry-run"],
                             capture_output=True, text=True)
    else:
        cmd = subprocess.run(["montecarlo", "--profile", profile, "monitors", "apply",
                            "--namespace", namespace, "--dry-run"],
                            capture_output=True, text=True)
    if cmd.returncode != 0:
        print(" [ 𐄂 failure ] an error occurred")
        print(f"{cmd.stderr}")
        exit(cmd.returncode)
    else:
        print(f" [ ✔ success ] export completed")
        print(cmd.stdout)