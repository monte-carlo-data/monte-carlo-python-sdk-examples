#INSTRUCTIONS:
#1.Create a CSV with 2 columns: [full_table_id, threshold type, sensitivity type,  (must be upper case with the following values: LOW, MEDIUM, HIGH)]
#2. Run this script, providing the mcdId, mcdToken, DWId, and CSV
#Limitation:
#This will make 1 request per table, so 10,000/day request limit via API is still a consideration

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

def bulkSetFreshnessSensitivity(mcdId,mcdToken,csvFileName,mconDict):
	client=Client(session=Session(mcd_id=mcdId,mcd_token=mcdToken))
	imported_sensitivity_counter=0
	with open(csvFileName,"r") as sensitivitiesToImport:
		sensitivities=csv.reader(sensitivitiesToImport,delimiter=",")
		for row in sensitivities:
			if row[0] not in mconDict.keys():
				print("check failed: " +row[0])
				continue
			if mconDict[row[0]]:
				imported_sensitivity_counter+=1
				print("check succeeded " + row[0])
				mutation=Mutation()
				mutation.set_sensitivity(event_type="freshness",mcon=mconDict[row[0]],threshold=dict(level=str(row[1]))).__fields__("success")
				print(row[0],client(mutation).set_sensitivity,row[1])
	print("Successfully imported freshness for " + str(imported_sensitivity_counter) + " tables")

if __name__ == '__main__':
	#-------------------INPUT VARIABLES---------------------
	mcd_id = input("MCD ID: ")
	mcd_token = input("MCD Token: ")
	dw_id = input("DW ID: ")
	csv_file = input("CSV Filename: ")
	#-------------------------------------------------------
	if dw_id and csv_file:
		mcon_dict=getMcons(mcd_id,mcd_token,dw_id)
		bulkSetFreshnessSensitivity(mcd_id,mcd_token,csv_file,mcon_dict)
	if csv_file and not dw_id:
		warehouse_id = getDefaultWarehouse(mcd_id,mcd_token)
		mcon_dict = getMcons(mcd_id,mcd_token,warehouse_id)
		bulkSetFreshnessSensitivity(mcd_id,mcd_token,csv_file,mcon_dict)


import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from monitors import *

# Initialize logger
util_name = __file__.split('/')[-1].split('.')[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))
coloredlogs.install(level='INFO', fmt='%(asctime)s %(levelname)s - %(message)s')


class SetFreshnessSensitivity(Monitors):

	def __init__(self, profile, config_file: str = None, progress: Progress = None):
		"""Creates an instance of SetFreshnessSensitivity.

		Args:
			profile(str): Profile to use stored in montecarlo cli.
			config_file (str): Path to the Configuration File.
			progress(Progress): Progress bar.
		"""

		super().__init__(profile, config_file, progress)
		self.progress_bar = progress

	def validate_input_file(self, directory: str) -> Path:
		"""Ensure contents of inout file satisfy requirements.

		Args:
			directory(str): Project directory.

		Returns:
			Path: Full path to file containing list of tables and freshness configuration.
		"""
