#Instructions:
#1. Create a CSV with 3 columns in the following order: dataset, tag key, tag value
#	dataset must be lowercase
#2. Run this script, input your API Key ID, Token (generated in Settings -> API within MC UI)
#3. Input the Data Warehouse ID in which the datasets to import tags exist (will check and ignore tables in other warehouses)
	#Note: If you do not know the Data Warehouse ID, you can skip by pressing enter and the script will give you the options to choose from. You'll need to rerun the script after this.
#4. Input the name of the CSV with the tags
#5. This script creates an "import_log.txt" file with some logging details such as datasets that were not found in the dwId or total volume of tags imported
#Note: If you have a list of tags for tables in multiple warehouses, run again for each data warehouse ID 

from pycarlo.core import Client, Query, Mutation, Session
import csv
import json
from typing import Optional
from datetime import datetime

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
        
def get_dataset_query(dwId,first: Optional[int] = 1000, after: Optional[str] = None) -> Query:
    query = Query()
    get_datasets = query.get_datasets(first=first, dw_id=dwId, **(dict(after=after) if after else {}))
    get_datasets.edges.node.__fields__("project","dataset","mcon")
    get_datasets.page_info.__fields__(end_cursor=True)
    get_datasets.page_info.__fields__("has_next_page")
    return query

def getMcons(mcdId,mcdToken,dwId):
    client=Client(session=Session(mcd_id=mcdId,mcd_token=mcdToken))
    dataset_mcon_dict={}
    next_token=None
    while True:
        response = client(get_dataset_query(dwId=dwId,after=next_token)).get_datasets
#         print(response)
        for dataset in response.edges:
            dataset_mcon_dict[dataset.node.dataset.lower()] = dataset.node.mcon
        if response.page_info.has_next_page:
            next_token = response.page_info.end_cursor
        else:
            break
    return dataset_mcon_dict

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
        print("Import time: " + str(datetime.now()), file=open('import_log.txt', 'a'))
        for row in tags:
            print(', '.join(row))
            total_tags += 1
            if row[0].lower() not in mconDict.keys():
                # print a failure message if the dataset in the csv does not exist on the dwId/project:
#                 print("dataset check failed: " + row[0].lower())
                print(("dataset check failed: " + row[0].lower()), file=open('import_log.txt', 'a'))
                continue
            if mconDict[row[0].lower()]:
                # print a success message if the dataset in the csv does not exist on the dwId/project:
#                 print("dataset check succeeded: " + row[0].lower())
                print(("dataset check succeeded: " + row[0].lower()), file=open('import_log.txt', 'a'))
                temp_obj=dict(mconId=mconDict[row[0].lower()],propertyName=row[1],propertyValue=row[2])
                print((temp_obj), file=open('import_log.txt', 'a'))
                print(("\n"), file=open('import_log.txt', 'a'))
                tags_list.append(temp_obj)
                imported_tag_counter += 1
                incremental_tags += 1
                # Uncomment next 2 rows to print the tag counter on each iteration:
#                 print("Tag count: " + str(incremental_tags))
#                 print(("Tag count: " + str(incremental_tags)), file=open('import_log.txt', 'a'))
            if incremental_tags == 99:
                mutation=Mutation()
                client(bulk_tag_query, variables=dict(inputObjectProperties=tags_list))
                print(("100 tags uploaded!" + "\n"), file=open('import_log.txt', 'a'))
                tags_list.clear()
                incremental_tags = 0
        if incremental_tags > 0:
            mutation=Mutation()
            client(bulk_tag_query, variables=dict(inputObjectProperties=tags_list))
            print("Last tag group count: " + str(incremental_tags), file=open('import_log.txt', 'a'))
            print(str(incremental_tags) + " tags uploaded in the last batch!", file=open('import_log.txt', 'a'))
#     print("Successfully Imported " + str(imported_tag_counter) + " Tags")
#     print("Tags list: " + str(tags_list))
    print("END OF EXECUTION: Successfully Imported " + str(imported_tag_counter) + " Tags" + "\n", file=open('import_log.txt', 'a'))
    
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
        bulkImportTagsFromCSV(mcd_id,mcd_token,csv_file,mcon_dict)
