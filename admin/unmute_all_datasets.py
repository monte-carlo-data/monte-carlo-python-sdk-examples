#####
# About:
#   This script is indended to be used to UNMUTE ALL currently muted datasets within a specified warehouse
# Instructions:
#   1. Run this script, input your API Key ID, Token (generated in Settings -> API within MC UI)
#   2. If applicable, copy/paste the UUID of the warehouse you would like to target to unmute datasets
#      Note: the script must be run for one warehouse at a time, run multiple times for multiple warehouses
#   3. Review the list of datasets to be unmuted in the .csv file provided by the prompt
#      RECOMMENDATION:  Keep this CSV file as a means to audit which datasets were unmuted by this script
#   4. Proceed to unmute the list of datasets
#####

from pycarlo.core import Client, Query, Session
import csv
import json
from typing import Optional
from datetime import datetime

def getWarehouses(mcdId,mcdToken):
	client=Client(session=Session(mcd_id=mcdId,mcd_token=mcdToken))
	warehousesQuery = """
		query getUser {
			getUser {
				account {
					warehouses {
						name
						connectionType
						uuid
					}
				}
			}
			}
		"""

	warehouses=client(warehousesQuery).get_user.account.warehouses

	if len(warehouses) == 1:
		print(f"Found one warehouse - Name: {warehouses[0].name} - UUID: {warehouses[0].uuid}")
		return warehouses[0].uuid
	elif len(warehouses) > 1:
		print("Found multiple warehouses... ")
		for val in warehouses:
			print("Name: " + val.name + ", Connection Type: " + val.connection_type + ", UUID: " + val.uuid)
		dwId = input("Please copy/paste the full UUID of the warehouse you would like to target: ")
		return dwId

def get_dataset_query(dwId,first: Optional[int] = 1000, after: Optional[str] = None) -> Query:
	query = Query()
	get_datasets = query.get_datasets(first=first, dw_id=dwId, **(dict(after=after) if after else {}))
	get_datasets.edges.node.__fields__("dataset","uuid","is_muted")
	get_datasets.page_info.__fields__(end_cursor=True)
	get_datasets.page_info.__fields__("has_next_page")
	return query

def getDatasetUuidDict(mcdId,mcdToken,dwId):
	client=Client(session=Session(mcd_id=mcdId,mcd_token=mcdToken))
	dataset_uuid_dict={}
	next_token=None
	while True:
		response = client(get_dataset_query(dwId=dwId,after=next_token)).get_datasets
		for dataset in response.edges:
			if dataset.node.is_muted:
				dataset_uuid_dict[dataset.node.dataset.lower()] = dataset.node.uuid
		if response.page_info.has_next_page:
			next_token = response.page_info.end_cursor
		else:
			break
	return dataset_uuid_dict

def get_date():
	return datetime.today().strftime('%Y-%m-%d_%H_%M_%S')

def userReview(uuid_dict, dw_id):
	if not uuid_dict:
		print(f"No muted datasets found in selected warehouse id {dw_id}.  Exiting")
		quit()

	fname = f"datasets_to_mute_{get_date()}.csv"
	header = ['dataset', 'uuid']
	with open(fname, 'w') as csvfile:
		writer = csv.writer(csvfile)
		writer.writerow(header)
		for dataset, uuid in uuid_dict.items():
			writer.writerow([dataset, uuid])
	userReview = input(f'Datasets to unmute written to file {fname} for your review. OK to proceed? (y/n) ').lower()

	if userReview == 'y':
		return
	else:
		print("Acknowledged do not proceed. Exiting.")
		quit()

def generateVarsInput(uuid_list):
	vars_input = {
			"input": {
				"datasets": uuid_list,
				"mute": False
				}
			}
	return vars_input

def unmute_datasets(mcdId,dwId,mcdToken,uuidDict):
	client=Client(session=Session(mcd_id=mcdId,mcd_token=mcdToken))
	uuid_list=[]
	unmute_datasets_query = """
		mutation toggleMuteDatasets($input: ToggleMuteDatasetsInput!) {
			toggleMuteDatasets(input: $input) {
				muted {
				uuid
				isMuted
				}
			}
			}
		"""

	unmuted_dataset_counter = 0
	incremental_datasets = 0
	for uuid in uuidDict.values():
		temp_obj=dict(dsId=uuid, dwId=dwId)
		print(temp_obj)
		uuid_list.append(temp_obj)
		unmuted_dataset_counter += 1
		incremental_datasets += 1
		if incremental_datasets == 99:
			vars_input = generateVarsInput(uuid_list)
			print(client(unmute_datasets_query, variables=vars_input))
			uuid_list.clear()
			incremental_datasets = 0
	if incremental_datasets > 0:
		vars_input = generateVarsInput(uuid_list)
		print(client(unmute_datasets_query, variables=vars_input))
	print("Successfully Unmuted " + str(unmuted_dataset_counter) + " Datasets")

if __name__ == '__main__':
	#-------------------INPUT VARIABLES---------------------
	mcd_id = input("MCD ID: ")
	mcd_token = input("MCD Token: ")
	#-------------------------------------------------------
	print("Getting warehouses...")
	dw_id = getWarehouses(mcd_id, mcd_token)
	print("Getting datasets...")
	uuid_dict = getDatasetUuidDict(mcd_id,mcd_token,dw_id)
	userReview(uuid_dict, dw_id)
	unmute_datasets(mcd_id,dw_id,mcd_token,uuid_dict)