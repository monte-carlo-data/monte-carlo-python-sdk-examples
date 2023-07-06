#Instructions:
#1. Run this script locally, input API Key and Token when Prompted
#2. The script will print out the count of tables from each warehouse connection that are not in a domain (to be muted), in a domain, and in a domain but muted (to be unmuted)
#3. The script will prompt you to confirm the counts of tables to be muted/unmuted per warehouse connection within Monte Carlo (Y/N)
#4. Once you pass a Y response, the muting of those tables will begin

from pycarlo.core import Client, Query, Mutation, Session
import csv
import json
from typing import Optional

def getAllWarehouses(mcdId,mcdToken):
	client=Client(session=Session(mcd_id=mcdId,mcd_token=mcdToken))
	query=Query()
	query.get_user().account.warehouses.__fields__("name","connection_type","uuid")
	warehouses=client(query).get_user.account.warehouses
	warehouse_list=[]
	if len(warehouses) > 0:
		for val in warehouses:
			warehouse_list.append(val.uuid)
	else:
		print("Error: no warehouses connected")
	return warehouse_list

def getAllDomains(mcdId,mcdToken):
	client=Client(session=Session(mcd_id=mcdId,mcd_token=mcdToken))
	query=Query()
	get_all_domains = query.get_all_domains().__fields__("name","uuid","assignments")
	domains=client(query).get_all_domains
	domain_list = []
	for domain in domains:
		domain_list.append(domain["uuid"])
	return domain_list

def get_table_query(dwId,first: Optional[int] = 1000, after: Optional[str] = None) -> Query:
	query = Query()
	get_tables = query.get_tables(first=first, dw_id=dwId, is_deleted=False, **(dict(after=after) if after else {}))
	get_tables.edges.node.__fields__("full_table_id","mcon","is_muted")
	get_tables.page_info.__fields__(end_cursor=True)
	get_tables.page_info.__fields__("has_next_page")
	return query

def get_tables_for_domain_query(domainId,first: Optional[int] = 1000, after: Optional[str] = None) -> Query:
	query = Query()
	get_tables = query.get_tables(first=first, is_deleted=False, domain_id=domainId, **(dict(after=after) if after else {}))
	get_tables.edges.node.__fields__("full_table_id","mcon","is_muted")
	get_tables.edges.node.warehouse.__fields__("uuid")
	get_tables.page_info.__fields__(end_cursor=True)
	get_tables.page_info.__fields__("has_next_page")
	return query

def getMcons(mcdId,mcdToken,warehouses,domains):
	client=Client(session=Session(mcd_id=mcdId,mcd_token=mcdToken))
	table_mcon_dict={}
	domain_mcon_dict={}
	tables_not_in_domain={}
	tables_to_unmute={}
	for warehouse in warehouses:
		print("Warehouse check: " + str(warehouse))
		table_mcon_dict[warehouse] = {}
		domain_mcon_dict[warehouse] = {}
		tables_not_in_domain[warehouse] = {}
		tables_to_unmute[warehouse] = {}
		next_token=None
		while True:
			response = client(get_table_query(dwId=warehouse,after=next_token)).get_tables
			for table in response.edges:
				if table.node.is_muted == False:
					table_mcon_dict[warehouse][table.node.full_table_id] = table.node.mcon
					tables_not_in_domain[warehouse][table.node.full_table_id] = table.node.mcon
			if response.page_info.has_next_page:
				next_token = response.page_info.end_cursor
			else:
				break
	for domain in domains:
		print("Domain check: " + str(domain))
		next_token=None
		while True:
			response = client(get_tables_for_domain_query(domainId=domain,after=next_token)).get_tables
			if len(response.edges) > 100:
				print(domain)
			for table in response.edges:
				warehouse = table.node.warehouse.uuid
				if table.node.is_muted == False:
					domain_mcon_dict[warehouse][table.node.full_table_id] = table.node.mcon
				else:
					#get list of muted tables within Domain to unmute
					tables_to_unmute[warehouse][table.node.full_table_id] = table.node.mcon
			if response.page_info.has_next_page:
				next_token = response.page_info.end_cursor
			else:
				break

	# identify tables not in a domain
	for warehouse in warehouses:
		for table_name in table_mcon_dict[warehouse]:
			if table_name in domain_mcon_dict[warehouse].keys():
				del tables_not_in_domain[warehouse][table_name]
			else:
				continue
	for warehouse in warehouses:
		print("For warehouse: " + str(warehouse))
		print("forMuting: "+str(len(tables_not_in_domain[warehouse])))
		print("inDomain: "+str(len(domain_mcon_dict[warehouse])))
		print("Total: "+str(len(table_mcon_dict[warehouse])))
		print("forUnMuting: "+str(len(tables_to_unmute[warehouse])))
	return [tables_not_in_domain,tables_to_unmute]


def bulkMuteTablesByDomain(mcdId,mcdToken,mconDict):
	tables_not_in_domain = mconDict[0]
	tables_to_unmute = mconDict[1]
	bulkMuteTables(mcdId,mcdToken,tables_not_in_domain,True)
	counter=0
	for warehouse in tables_to_unmute:
		counter += len(tables_to_unmute[warehouse])
	if counter > 0:
		bulkMuteTables(mcdId,mcdToken,tables_to_unmute,False)

def bulkMuteTables(mcdId,mcdToken,mconDict,muteBoolean):
	client=Client(session=Session(mcd_id=mcdId,mcd_token=mcdToken))
	temp_list=[]
	for warehouse in mconDict:
		counter=0
		for item in mconDict[warehouse]:
			temp_payload={}
			temp_payload["mcon"]=mconDict[warehouse][item]
			temp_payload["fullTableId"]=item
			temp_payload["dwId"]=warehouse
			temp_list.append(temp_payload)
			counter+=1
			if len(temp_list) > 9:
				mutation=Mutation()
				mutation.toggle_mute_tables(input=dict(mute=muteBoolean,tables=temp_list)).muted.__fields__("id")
				print(client(mutation).toggle_mute_tables)
				temp_list=[]
			if counter == len(mconDict[warehouse]):
				mutation=Mutation()
				mutation.toggle_mute_tables(input=dict(mute=muteBoolean,tables=temp_list)).muted.__fields__("id")
				print(client(mutation).toggle_mute_tables)
				break
			else:
				continue
		print("Tables muted("+str(muteBoolean)+") for " + str(warehouse) + ": " + str(counter))

if __name__ == '__main__':
	#-------------------INPUT VARIABLES---------------------
	mcd_id = input("MCD ID: ")
	mcd_token = input("MCD Token: ")
	#-------------------------------------------------------
	warehouses = getAllWarehouses(mcd_id,mcd_token)
	domains = getAllDomains(mcd_id,mcd_token)
	mcon_dict = getMcons(mcd_id,mcd_token,warehouses,domains)
	mute = input("Mute? (Y/N): ")
	if mute == "Y":
		bulkMuteTablesByDomain(mcd_id,mcd_token,mcon_dict)
