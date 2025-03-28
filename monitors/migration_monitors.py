import os
import subprocess

from pathlib import Path
from pycarlo.core import Client,Session, Query

OUTPUT_FILE = 'monitors.csv'
MONITORS_FILE_WORKSPACE = '/dbfs/FileStore/temp/montecarlo'

def get_monitors(client, source_tag_value, destination_tag_value, tag_key):

	# Get list of tables that are tagged with tag_name
	print("Getting list of source tables")
	source_table_query = Query()
	filter = [{'tag_name': tag_key,'tag_values': [source_tag_value]}]
	source_tables = {}
	
	source_table_query.search(query='',tag_filters=filter)
	search_results = client(source_table_query).search
	for result in search_results.results:
		print(result)
		source_tables[result.table_id] = {"mcon": result.mcon,
									'monitors': [],
									'object_id': result.object_id}
	if source_tables:
		print("Getting the list of source monitors")
		for table in source_tables:
			source_table_monitors = Query()
			source_table_monitors.get_monitors(mcons=[source_tables[table]["mcon"]]).__fields__('uuid','namespace')
			response = client(source_table_monitors).get_monitors
			for monitor in response:
				if monitor.namespace == 'ui':
					source_tables[table]["monitors"].append(monitor.uuid)
	
	print("Getting destination tables")
	destination_table_query = Query()
	destination_filter = [{'tag_name': tag_key,'tag_values': [destination_tag_value]}]
	destination_table_query.search(query='',tag_filters=destination_filter)
	search_results = client(destination_table_query).search
	for result in search_results.results:
		print(result)
		if result.table_id in source_tables:
			source_tables[result.table_id]['destination'] =  {"mcon": result.mcon,
													 'object_id': result.object_id,
													 'resource_id': result.resource_id}
	return source_tables

def write_csv_file(source_tables):
	print("Writing CSV file")
	monitors_to_write = []
	monitors_file_name = ''
	if len(source_tables) > 0:
		for table in source_tables:
			for monitor in source_tables[table]['monitors']:
				if monitor not in monitors_to_write:
					monitors_to_write.append(monitor)
	if monitors_to_write:
		print("Found monitors to write")
		file_path = Path(os.path.abspath(MONITORS_FILE_WORKSPACE))
		file_path.mkdir(parents=True, exist_ok=True)
		monitors_file_name = file_path / OUTPUT_FILE
		with open(monitors_file_name, 'w') as csvfile:
			for monitor_id in monitors_to_write:
				csvfile.write(f"{monitor_id}\n")
	return monitors_file_name

def export_monitors(monitors_file_path, namespace, warehouse_id):
	print("Exporting monitors")
	mc_monitors_path = MONITORS_FILE_WORKSPACE + "/test"
	cmd_args = ["montecarlo", "monitors", "convert-to-mac",
			 "--namespace", namespace, "--project-dir", mc_monitors_path,
			 "--monitors-file", monitors_file_path, "--dry-run"]
	cmd = subprocess.run(cmd_args,
                                 capture_output=True, text=True)
	print(cmd.stderr)
	print(cmd.stdout)
	print("Adding default_resource")
	with open(mc_monitors_path + '/montecarlo.yml', 'r') as montecarlo_yml:
		file_data = montecarlo_yml.read()
		file_data = file_data + 'default_resource: %s' % warehouse_id
	with open(mc_monitors_path + '/montecarlo.yml', 'w') as new_montecarlo_yml:
		new_montecarlo_yml.write(file_data)
	montecarlo_yml.close()
	new_montecarlo_yml.close()
	print("Wrote the file")
	return mc_monitors_path

def modify_monitors_file_ids(monitor_path, source_tables, source_warehouse_uuid, destination_warehouse_uuid):
	print("Modifying the monitors file")
	monitors_file_yml = monitor_path + '/montecarlo/monitors.yml'

	destination_warehouse_query = Query()
	destination_warehouse_query.get_warehouse(uuid=destination_warehouse_uuid).__fields__('name')
	destination_warehouse = client(destination_warehouse_query).get_warehouse

	source_warehouse_query = Query()
	source_warehouse_query.get_warehouse(uuid=source_warehouse_uuid).__fields__('name')
	source_warehouse = client(source_warehouse_query).get_warehouse
	with open(monitors_file_yml, 'r') as monitors_yml:
		file_data = monitors_yml.read()
		file_data = file_data.replace(source_warehouse.name, destination_warehouse.name)
		for table in source_tables:
			if "destination" in source_tables[table]:
				file_data = file_data.replace(source_tables[table]['object_id'], source_tables[table]['destination']['object_id'])
	with open(monitors_file_yml, 'w') as new_monitors_yml:
		new_monitors_yml.write(file_data)
	monitors_yml.close()
	new_monitors_yml.close()
	print("Completed modifying the monitors file")
	
def move_monitors(namespace, monitors_workspace_dir):
	print("Moving the monitors")
	cmd_args = ["montecarlo", "monitors", "apply", "--namespace", namespace, "--project-dir", monitors_workspace_dir, "--auto-yes"]
	cmd = subprocess.run(cmd_args, capture_output=True, text=True)
	print(cmd.stdout)
	print(cmd.stderr)
	print("Movement complete")

def clean_up_files():
	print("Cleaning up files")
	cmd_args = ['rm', '-rf', MONITORS_FILE_WORKSPACE]
	cmd = subprocess.run(cmd_args, capture_output=True, text=True)

if __name__ == '__main__':
	#-------------------INPUT VARIABLES---------------------
	mcdId = '{Add Key here}'
	mcdToken = '{Add Token Here}'
	tag_key = '{Add tag key here}'
	source_tag_value = '{Add source tage here}'
	destination_tag_value = '{Add destination tag here}'
	# UUID for source Warehouse found via API
	source_warehouse_uuid = '{Source Warehouse UUID}'
	#UUID for destination Warehouse found via API
	destination_warehouse_uuid = '{Destination Warehouse UUID}'
	#-------------------------------------------------------
	print("Preparing to move monitors from '%s' to '%s'" %(source_tag_value, destination_tag_value))
	# Environment setup
	os.environ['MCD_DEFAULT_API_ID'] = mcdId
	os.environ['MCD_DEFAULT_API_TOKEN'] = mcdToken
	client=Client(session=Session(mcd_id=mcdId,mcd_token=mcdToken))
	namespace = destination_tag_value.replace(' ', '_')
	
    #Get monitors to move
	source_tables = get_monitors(client=client, source_tag_value=source_tag_value, destination_tag_value=destination_tag_value, tag_key=tag_key)
	if source_tables:
		# Write UUIDs to csv file
		csv_file_name = write_csv_file(source_tables=source_tables)
		# Export using the csv file from above
		monitors_path = export_monitors(monitors_file_path=csv_file_name, namespace=namespace, warehouse_id=destination_warehouse_uuid)
		# Modify exported files contents to new paths
		modify_monitors_file_ids(monitors_path, source_tables, source_warehouse_uuid=source_warehouse_uuid, destination_warehouse_uuid=destination_warehouse_uuid)
		# Re import the file
		move_monitors(namespace=namespace, monitors_workspace_dir=monitors_path)
	else:
		print("No monitors found to migrate")
	#Clean up files on system
	clean_up_files()
