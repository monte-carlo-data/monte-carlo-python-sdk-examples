#Instructions:
#1. Create a CSV with 3 columns in the following order: full_table_id, tag key, tag value
#2. Run this script, input your API Key ID, Token (generated in Settings -> API within MC UI)
#3. Input the Data Warehouse ID in which the tables to import tags exist (will check and ignore tables in other warehouses)
	#Note: If you do not know the Data Warehouse ID, you can skip by pressing enter and the script will give you the options to choose from. You'll need to rerun the script after this.
#4. Input the name of the CSV with the tags
#Note: If you have a list of tags for tables in multiple warehouses, run again for each data warehouse ID

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

def bulkImportTagsFromCSV(mcdId,mcdToken,csvFileName, mconDict):
	client=Client(session=Session(mcd_id=mcdId,mcd_token=mcdToken))
	tags_list=[]
	bulk_tag_query = """
		mutation bulkCreateOrUpdateObjectProperties($inputObjectProperties:[InputObjectProperty]!) {
  			bulkCreateOrUpdateObjectProperties(inputObjectProperties:$inputObjectProperties) {
    			objectProperties {
      				mconId
    			}
  			}
		}
		"""
	with open(csvFileName,"r") as tags_to_import:
		tags=csv.reader(tags_to_import, delimiter=",")
		total_tags=0
		imported_tag_counter = 0
		incremental_tags = 0
		for row in tags:
			total_tags += 1
			if row[0] not in mconDict.keys():
				print("check failed: " + row[0])
				continue
			if mconDict[row[0]]:
				print("check succeeded: " + row[0])
				temp_obj=dict(mconId=mconDict[row[0]],propertyName=row[1],propertyValue=row[2])
				tags_list.append(temp_obj)
				imported_tag_counter += 1
				incremental_tags += 1
			if incremental_tags == 99:
				mutation=Mutation()
				print(client(bulk_tag_query, variables=dict(inputObjectProperties=tags_list)))
				tags_list.clear()
				incremental_tags = 0
		if incremental_tags > 0:
			mutation=Mutation()
			print(client(bulk_tag_query, variables=dict(inputObjectProperties=tags_list)))
	print("Successfully Imported " + str(imported_tag_counter) + " Tags")

if __name__ == '__main__':
	#-------------------INPUT VARIABLES---------------------
	mcd_id = input("MCD ID: ")
	mcd_token = input("MCD Token: ")
	dw_id = input("DW ID: ")
	csv_file = input("CSV Filename: ")
	#-------------------------------------------------------
	if dw_id and csv_file:
		mcon_dict = getMcons(mcd_id,mcd_token,dw_id)
		bulkImportTagsFromCSV(mcd_id,mcd_token,csv_file,mcon_dict)
	elif csv_file and not dw_id:
		warehouse_id = getDefaultWarehouse(mcd_id,mcd_token)
		mcon_dict = getMcons(mcd_id,mcd_token,warehouse_id)
		bulkImportTagsFromCSV(csv_file,mcon_dict)
