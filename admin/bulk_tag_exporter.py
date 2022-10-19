#Instructions:
#2. Run this script, input your API Key ID, Token (generated in Settings -> API within MC UI)
#3. Input the Data Warehouse ID in which the tables to import tags exist (will check and ignore tables in other warehouses)
	#Note: If you do not know the Data Warehouse ID, you can skip by pressing enter and the script will give you the options to choose from. You'll need to rerun the script after this.
#4. Input the name of the CSV you would like to create.
#Note: If you would like to get tags for other warehouse connections, run this again and export to a new CSV filename.

from pycarlo.core import Client, Query, Mutation, Session
import csv
import json
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

def getTableQuery(dwId,first: Optional[int] = 1000, after: Optional[str] = None) -> Query:
    query = Query()
    get_tables = query.get_tables(first=first, dw_id=dwId, is_deleted=False, **(dict(after=after) if after else {}))
    get_tables.edges.node.__fields__("full_table_id","mcon")
    get_tables.edges.node.object_properties.__fields__("property_name","property_value")
    get_tables.page_info.__fields__(end_cursor=True)
    get_tables.page_info.__fields__("has_next_page")
    return query

def getMcons(mcdId,mcdToken,dwId):
	client=Client(session=Session(mcd_id=mcdId,mcd_token=mcdToken))
	table_mcon_dict={}
	next_token=None
	while True:
		response = client(getTableQuery(dwId=dwId,after=next_token)).get_tables
		for table in response.edges:
			if len(table.node.object_properties) > 0:
				temp_dict={}
				# temp_dict["table_name"] = table.node.full_table_id
				temp_dict["mcon"] = table.node.mcon
				temp_dict["tags"] = []
				for tag in table.node.object_properties:
					prop_dict={}
					prop_dict["property_name"] = tag["property_name"]
					prop_dict["property_value"] = tag["property_value"]
					temp_dict["tags"].append(prop_dict)
				table_mcon_dict[table.node.full_table_id] = temp_dict
		if response.page_info.has_next_page:
			next_token = response.page_info.end_cursor
		else:
			break
	return table_mcon_dict

def bulkExportTagsToCSV(mcdId,mcdToken,csvFileName,mconDict):
	with open(csvFileName,"w") as tags_to_export:
		writer=csv.writer(tags_to_export)
		writer.writerow(["full_table_id","tag_key","tag_value"])
		for table_name in mconDict:
			for tag in mconDict[table_name]["tags"]:
				writer.writerow([table_name,tag["property_name"],tag["property_value"]])

if __name__ == '__main__':
	#-------------------INPUT VARIABLES---------------------
	mcd_id = input("MCD ID: ")
	mcd_token = input("MCD Token: ")
	dw_id = input("DW ID: ")
	csv_file = input("CSV Export Filename: ")
	#-------------------------------------------------------
	if dw_id and csv_file:
		mcon_dict = getMcons(mcd_id,mcd_token,dw_id)
		bulkExportTagsToCSV(mcd_id,mcd_token,csv_file,mcon_dict)
	elif csv_file and not dw_id:
		warehouse_id = getDefaultWarehouse(mcd_id,mcd_token)
		mcon_dict = getMcons(mcd_id,mcd_token,warehouse_id)
		bulkExportTagsToCSV(mcd_id,mcd_token,csv_file,mcon_dict)
	elif not csv_file:
		print("CSV Export Filename Required.")
