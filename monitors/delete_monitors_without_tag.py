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
    count = 0
    # Adding tagged monitors to a list
    for mon in search_results:
        print(mon)
        monitors_to_keep.append(mon.uuid)
        count += 1    
    print(f"Found {count} production monitors")
    return monitors_to_keep

# Get all custom monitors in MC
def get_all_monitors(client, limit):
    query = Query()
    query.get_monitors(limit=limit).__fields__('uuid')
    search_results = client(query).get_monitors
    return search_results

# Delete a single monitor
def delete_monitor(uuid, client):
    mutation = Mutation()
    mutation.delete_custom_rule(uuid=uuid)
    response = client(mutation).delete_custom_rule
    print(f"Successfully deleted monitor {response.uuid}")

# Compiles preferred monitors and deletes every other custom monitor
def bulk_delete_monitors(client):
    pref_monitors = get_pref_monitors(client)
    print(pref_monitors)
    all_monitors = get_all_monitors(client,limit=200)
    count = 0
    for mon in all_monitors:
        if mon.uuid not in prod_monitors:
            #delete_monitor(client, mon.uuid)
            print(f"Deleted monitor {mon.uuid}")
            count+=1
    print(f"Deleted {count} monitors")
    

if __name__ == '__main__':
    #-------------------INPUT VARIABLES---------------------
    mcdId = '{Add Key Here}'
    mcdToken = '{Add Token Here}'
    #-------------------------------------------------------
    client= Client(session=Session(mcd_id=mcdId,mcd_token=mcdToken))
    bulk_delete_monitors(client)


