#####
# About:
#   This script is indended to be used to UNMUTE all muted tables within a specified warehouse
# Instructions:
#   1. Run this script, input your API Key ID, Token (generated in Settings -> API within MC UI)
#   2. If applicable, copy/paste the UUID of the warehouse you would like to target to unmute tables
#      Note: the script must be run for one warehouse at a time, run multiple times for multiple warehouses
#   3. Review the list of tables to be unmuted in the .csv file provided by the prompt
#      RECOMMENDATION:  Keep this CSV file as a means to audit which tables were unmuted by this script
#   4. Proceed to unmute the list of tables
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

def get_table_query(dwId,first: Optional[int] = 1000, after: Optional[str] = None) -> Query:
	query = Query()
	get_tables = query.get_tables(first=first, dw_id=dwId, is_deleted=False, **(dict(after=after) if after else {}))
	get_tables.edges.node.__fields__("full_table_id","mcon","is_muted")
	get_tables.page_info.__fields__(end_cursor=True)
	get_tables.page_info.__fields__("has_next_page")
	return query

def getMcons(mcdId,mcdToken,dwId):
	client=Client(session=Session(mcd_id=mcdId,mcd_token=mcdToken))
	table_mcon_dict={}
	next_token=None
	while True:
		response = client(get_table_query(dwId=dwId,after=next_token)).get_tables
		for table in response.edges:
			if table.node.is_muted:
				table_mcon_dict[table.node.full_table_id.lower()] = table.node.mcon
		if response.page_info.has_next_page:
			next_token = response.page_info.end_cursor
		else:
			break
	return table_mcon_dict

def get_date():
	return datetime.today().strftime('%Y-%m-%d_%H:%M:%S')

def userReview(mcon_dict, dw_id):
	if not mcon_dict:
		print(f"No muted tables found in selected warehouse id {dw_id}.  Exiting")
		quit()

	fname = f"tables_to_mute_{get_date()}.csv"
	header = ['fullTableName', 'MCON']
	with open(fname, 'w') as csvfile:
		writer = csv.writer(csvfile)
		writer.writerow(header)
		for table, mcon in mcon_dict.items():
			writer.writerow([table, mcon])
	userReview = input(f'Tables to unmute written to file {fname} for your review. OK to proceed? (y/n) ').lower()

	if userReview == 'y':
		return
	else:
		print("Acknowledged do not proceed. Exiting.")
		quit()

def generateVarsInput(mcon_list):
	vars_input = {
			"input": {
				"tables": mcon_list,
				"mute": False
				}
			}
	return vars_input

def unmute_tables(mcdId,mcdToken,mconDict):
	client=Client(session=Session(mcd_id=mcdId,mcd_token=mcdToken))
	mcon_list=[]
	unmute_tables_query = """
		mutation toggleMuteTables($input: ToggleMuteTablesInput!) {
			toggleMuteTables(input: $input) {
				muted {
				mcon
				isMuted
				}
			}
			}
		"""

	unmuted_table_counter = 0
	incremental_tables = 0
	for mcon in mconDict.values():
		temp_obj=dict(mcon=mcon)
		print(temp_obj)
		mcon_list.append(temp_obj)
		unmuted_table_counter += 1
		incremental_tables += 1
		if incremental_tables == 99:
			vars_input = generateVarsInput(mcon_list)
			print(client(unmute_tables_query, variables=vars_input))
			mcon_list.clear()
			incremental_tables = 0
	if incremental_tables > 0:
		vars_input = generateVarsInput(mcon_list)
		print(client(unmute_tables_query, variables=vars_input))
	print("Successfully Unmuted " + str(unmuted_table_counter) + " Tables")

if __name__ == '__main__':
	#-------------------INPUT VARIABLES---------------------
	mcd_id = input("MCD ID: ")
	mcd_token = input("MCD Token: ")
	#-------------------------------------------------------
	print("Getting warehouses...")
	dw_id = getWarehouses(mcd_id, mcd_token)
	print("Getting tables...")
	mcon_dict = getMcons(mcd_id,mcd_token,dw_id)
	userReview(mcon_dict, dw_id)
	unmute_tables(mcd_id,mcd_token,mcon_dict)