#INSTRUCTIONS:
#1.Create a CSV with 2 columns: [full_table_id, minimum sensitivity delay in seconds]
#2. Run this script, providing the mcdId, mcdToken, DWId,and CSV
#Limitation:
#This will make 1 request per table, so 10,000/day request limit via API is still a consideration
#If there are multiple FH monitors on a single table, it will only update for the first one returned by MC APIs

from pycarlo.core import Client, Query, Mutation, Session
import csv
from typing import Optional

def getDefaultWarehouse(mcdId,mcdToken):
	client=Client(session=Session(mcd_id=mcdId,mcd_token=mcdToken))
	query=Query()
	query.get_user().account.warehouses.__fields__("name","connection_type","uuid")
	warehouses=client(query).get_user.account.warehouses
	if len(warehouses) == 1:
		return warehouses[0].uuid
	elif len(warehouses) > 1:
		for val in warehouses:
			print("Name: " + val.name + ", Connection Type: " + val.connection_type + ", UUID: " + val.uuid)
		print("Error: More than one warehouse, please re-run with UUID value")
		quit()

def get_table_query(dwId,first: Optional[int] = 1000, after: Optional[str] = None) -> Query:
    query = Query()
    get_tables = query.get_tables(first=first, dw_id=dwId, is_deleted=False, **(dict(after=after) if after else {}))
    get_tables.edges.node.__fields__("full_table_id","mcon")
    get_tables.page_info.__fields__(end_cursor=True)
    get_tables.page_info.__fields__("has_next_page")
    return query

def getMcons(mcdId,mcdToken,dwId):
	client=Client(session=Session(mcd_id=mcdId,mcd_token=mcdToken))
	table_mcon_dict={}
	next_token=None
	while True:
		response = client(get_table_query(dwId=dwId,after=next_token)).get_tables
		print(response)
		for table in response.edges:
			table_mcon_dict[table.node.full_table_id] = table.node.mcon
		if response.page_info.has_next_page:
			next_token = response.page_info.end_cursor
		else:
			break
	return table_mcon_dict

def getFieldHealthMonitors(mcdId,mcdToken):
	client=Client(session=Session(mcd_id=mcdId,mcd_token=mcdToken))
	get_monitors_query = "query{getMonitors(monitorTypes:[STATS]){monitorType,entities,uuid}}"
	monitor_response = client(get_monitors_query)
	fh_table_dict={}
	for val in monitor_response.get_monitors:
		table_name = val.entities[0]
		fh_table_dict[table_name] = val.uuid
	return fh_table_dict

def bulkSetFieldHealthSensitivity(mcdId,mcdToken,csvFileName,fieldHealthDict):
	client=Client(session=Session(mcd_id=mcdId,mcd_token=mcdToken))
	imported_sensitivity_counter=0
	with open(csvFileName,"r") as sensitivitiesToImport:
		sensitivities=csv.reader(sensitivitiesToImport,delimiter=",")
		for row in sensitivities:
			if row[0] not in fieldHealthDict.keys():
				print("check failed: " +row[0])
				continue
			if fieldHealthDict[row[0]]:
				imported_sensitivity_counter+=1
				print("check succeeded " + row[0])
				print(fieldHealthDict[row[0]])
				mutation=Mutation()
				mutation.set_sensitivity(event_type="metric",monitor_uuid=fieldHealthDict[row[0]],threshold=dict(level=row[1].upper())).__fields__("success")
				print(mutation)
				print(row[0],client(mutation).set_sensitivity,row[1])
	print("Successfully imported freshness for " + str(imported_sensitivity_counter) + " Tables")

if __name__ == '__main__':
	#-------------------INPUT VARIABLES---------------------
	mcd_id = input("MCD ID: ")
	mcd_token = input("MCD Token: ")
	csv_file = input("CSV Filename: ")
	#-------------------------------------------------------
	if csv_file:
		fh_monitors = getFieldHealthMonitors(mcd_id,mcd_token)
		bulkSetFieldHealthSensitivity(mcd_id,mcd_token,csv_file,fh_monitors)
