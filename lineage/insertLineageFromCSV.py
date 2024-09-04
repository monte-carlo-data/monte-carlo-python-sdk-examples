#######################################################################################################################################
##
##  I want a CSV with five fields, but only two are required.  Order doesn't matter, case does.
##  All five columns need to be in the CSV
##
##  - source (required)
##    database:schema.object referring to the upstream object 
##
##  - destination (required)
##     database:schema.object referring to the downstream object 
##
##  - source_type
##     Type of object (table,view,external,etc).  If missing, a lookup will be run against the API and the first result used.
##
##  - destination_type
##     Type of object (table,view,external,etc).  If missing, a lookup will be run against the API and the first result used.
##
##  - dwid
##     UUID assigned to the DW by MC.  If not provided, a search will be run and the first result used.  Assumption being you have one DW.
##
#######################################################################################################################################

import argparse
import csv
from pycarlo.core import Client, Query, Mutation

client = Client()
query = Query()

header_list = ("source","destination","source_type","destination_type","dwid")
dw_id = ""
sType = ""
dType = ""


def getDWID():
    get_dwID_query = """
    query getUser {
        getUser {
            account {
                warehouses {
                    uuid
                    connectionType
                }
            }
        }
    }
    """
    response = client(get_dwID_query)
    return response['get_user']['account']['warehouses'][0]['uuid']


def getObjType (objName):
    get_objType = """
        query search {
            search(query: " """ + objName + """ ", limit: 1) {
                results {
                    objectType
                }
            }
        }
        """
    response = client(get_objType)
    return response['search']['results'][0]['objectType']


def insertLineage(fsource,fsType,fdestination,fdType,fdwID):
    insert_lineage_query = """
    mutation{
        createOrUpdateLineageEdge(
        destination: {
            objectId: \"""" + fdestination + """\"
            objectType:  \"""" + fdType + """\" #table,view,external,report, others?
            resourceId: \"3c989167-8e51-4a04-8ac0-2c0b2ed8f0b9\" # warehouseID
        }
        source: {
            objectId: \"""" + fsource + """\"
            objectType:  \"""" + fsType + """\" #table,view,external,report, others?
            resourceId: \"3c989167-8e51-4a04-8ac0-2c0b2ed8f0b9\" # warehouseID
        }
        ){
        edge{
          expireAt
          isCustom
          jobTs
        }
      }
    }
    """
    try:
         response = client(insert_lineage_query)
         return(response)
    except:
         return("failed insert: " + fsource + " -> " + fdestination)
    print(insert_lineage_query)


############################################################################
##                                                                       ##
###########################################################################
parser = argparse.ArgumentParser()
parser.add_argument("-f", "--file", help = "Input file, csv")
parser.add_argument("-dw", "--warehouse", help = "Get the associated DW ID(S)")
args = parser.parse_args()

if args.file:
    with open(args.file, mode='r') as file:
        reader = csv.DictReader(file)
        #################################
        ## validate the column headers ##
        #################################
        if all (key in reader.fieldnames for key in header_list):
            ########################################
            ## Headers are good, build the fields ##
            ########################################            
            for r in reader:
                ######################################################################## 
                ## Want object type to be optional, but also want to see if it's set. ## 
                ## Only hit the API if object type is blank                           ##
                ########################################################################  
                if r['source_type'] == "":
                    sType = getObjType(r['source'])
                else:
                    sType = r['source_type']

                if r['destination_type'] == "":
                    dType = getObjType(r['destination'])
                else:
                    dType = r['destination_type']              
                ########################################################################## 
                ## if dwID is blank, assume that means there's only one so look it up.  ##
                ## but only want to hit the API once, no need to do it over and over.   ##
                ########################################################################## 
                if r['dwid'] == "":
                    if dw_id  == "":
                        dw_id = getDWID()
                    insertLineage(r['source'],sType,r['destination'],dType,dw_id) 
                else:
                    insertLineage(r['source'],sType,r['destination'],dType,r['dwid'])  
        else:
            print("Missing Column (case sensitive, order doesn't matter)")
            print("Expected: ", header_list)
            print("Found: ", reader.fieldnames)        
elif args.warehouse:
    print(getDWID())
else:
    print("Argument required - either '-f <filename>' or '-dw x'")
