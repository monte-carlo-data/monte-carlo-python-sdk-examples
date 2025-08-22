#Instructions:
#1. Backup all tags using bulkTagExporter.py
#2. Run this script, input your API Key ID, Token (generated in Settings -> API within MC UI)

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

def getTableQuery(first: Optional[int] = 1000, after: Optional[str] = None) -> Query:
    query = Query()
    get_tables = query.get_tables(first=first, is_deleted=False, **(dict(after=after) if after else {}))
    get_tables.edges.node.__fields__("full_table_id","mcon")
    get_tables.edges.node.object_properties.__fields__("property_name","property_value")
    get_tables.page_info.__fields__(end_cursor=True)
    get_tables.page_info.__fields__("has_next_page")
    return query

def getMcons(mcdId,mcdToken):
	client=Client(session=Session(mcd_id=mcdId,mcd_token=mcdToken))
	table_mcon_dict={}
	next_token=None
	while True:
		response = client(getTableQuery(after=next_token)).get_tables
		for table in response.edges:
			if len(table.node.object_properties) > 0:
				temp_dict={}
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

def bulkDeleteAllTags(mcdId,mcdToken,mconTagDict):
	client=Client(session=Session(mcd_id=mcdId,mcd_token=mcdToken))
	for table in mconTagDict:
		for tag in mconTagDict[table]["tags"]:
			print(mconTagDict[table]["mcon"],tag["property_name"])
			mutation=Mutation()
			mutation.delete_object_property(mcon_id=mconTagDict[table]["mcon"],property_name=tag["property_name"]).__fields__("success")
			print(client(mutation).delete_object_property.success)

if __name__ == '__main__':
	#-------------------INPUT VARIABLES---------------------
	mcd_id = input("MCD ID: ")
	mcd_token = input("MCD Token: ")
	#-------------------------------------------------------
	tags = getMcons(mcd_id,mcd_token)
	bulkDeleteAllTags(mcd_id,mcd_token,tags)
