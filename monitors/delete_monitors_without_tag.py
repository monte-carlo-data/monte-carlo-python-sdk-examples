#INSTRUCTIONS:
#1. Add your api key and token
#2. Enter the tag key and value for the monitors you'd like to keep
#3. Run the script

from pycarlo.core import Client, Query, Mutation, Session

# Retrieve monitors with designated tag
def get_pref_monitors(client):
    query = Query()
    #Input tags for monitors you'd like to keep
    tag_key = '{Enter Tag Key Here}'
    tag_value = '{Enter Tag Value Here}'
    filter = [{'name': tag_key,'value': tag_value}]
    query.get_monitors(tags=filter).__fields__('uuid','tags','entities')
    search_results = client(query).get_monitors
    monitors_to_keep = []
    # Adding tagged monitors to a list
    for mon in search_results:
        monitors_to_keep.append(mon.uuid)   
    return monitors_to_keep

# Get all custom monitors in MC
def get_all_monitors(client, limit):
    query = Query()
    query.get_monitors(limit=limit).__fields__('uuid', 'monitor_type')
    search_results = client(query).get_monitors
    return search_results

# Delete a single monitor
def delete_monitor(client, uuid, type):
    mutation = Mutation()
    # defining monitor types that can only be deleted with delete_monitor call
    monitor_types = ['STATS', 'FIELD_QUALITY', 'CATEGORIES', 'JSON_SCHEMA']
    error = False
    if type in monitor_types:
        mutation.delete_monitor(monitor_id=uuid)
        response = client(mutation).delete_monitor
        if not response.success:
            error = True
    else:
        mutation.delete_custom_rule(uuid=uuid)
        response = client(mutation).delete_custom_rule
        if not response.uuid:
            error = True
    if error:
        print(f"Deletion not Successful for: {uuid}")
    else:
        print(f"Successfully deleted monitor {uuid}")
    

# Compiles preferred monitors and deletes every other custom monitor
def bulk_delete_monitors(client):
    pref_monitors = get_pref_monitors(client)
    print(f"Found {len(pref_monitors)} tagged monitors: {pref_monitors}")
    all_monitors = get_all_monitors(client,limit=200)
    count = 0
    for mon in all_monitors:
        if mon.uuid not in pref_monitors:
            mon_id = mon.uuid
            print(mon.uuid)
            mon_type = mon.monitor_type
            delete_monitor(client=client, uuid=mon_id, type=mon_type)
            count+=1
    print(f"Deleted {count} monitors")
    

if __name__ == '__main__':
    #-------------------INPUT VARIABLES---------------------
    mcdId = '{Add Key Here}'
    mcdToken = '{Add Token Here}'
    #-------------------------------------------------------
    client= Client(session=Session(mcd_id=mcdId,mcd_token=mcdToken))
    bulk_delete_monitors(client)
    



