#INSTRUCTIONS:
#1. Pass your api keys when running script
#2. Replace empty quotes on line 14 with the Audiences associated with the monitors you want deleted
#3. Run the script

from pycarlo.core import Client, Query, Mutation, Session
from typing import Optional

def get_monitors_query(limit: Optional[int] = 1000) -> Query:
	query = Query()
	get_monitors = query.get_monitors(limit=limit,labels=[""])
	get_monitors.__fields__("uuid","monitor_type","resource_id")
	return query

def monitor_aggregator(mcdId,mcdToken):
	client=Client(session=Session(mcd_id=mcdId,mcd_token=mcdToken))
	response = client(get_monitors_query()).get_monitors
	rules = ["VOLUME","CUSTOM_SQL","FRESHNESS"]
	rule_list=[]
	monitor_list=[]
	for monitor in response:
		if monitor["monitor_type"] in rules:
			rule_list.append(monitor["uuid"])
		else:
			monitor_list.append(monitor["uuid"])
	return [rule_list,monitor_list]

def delete_monitor(mcdId,mcdToken,monitor_list):
	client=Client(session=Session(mcd_id=mcdId,mcd_token=mcdToken))
	for monitor_id in monitor_list:
		print(monitor_id)
		mutation=Mutation()
		mutation.delete_monitor(monitor_id=monitor_id).__fields__('success')
		print(client(mutation).delete_monitor)
	
def delete_custom_rules(mcdId,mcdToken,rule_list):
	client=Client(session=Session(mcd_id=mcdId,mcd_token=mcdToken))
	for monitor_id in rule_list:
		print(monitor_id)
		mutation=Mutation()
		mutation.delete_custom_rule(uuid=monitor_id).__fields__('uuid')
		print(client(mutation).delete_custom_rule)


def main(*args, **kwargs):
	# -------------------INPUT VARIABLES---------------------
	mcd_id = input("MCD ID: ")
	mcd_token = input("MCD Token: ")
	# -------------------------------------------------------
	monitor_lists = monitor_aggregator(mcd_id, mcd_token)
	delete_custom_rules(mcd_id, mcd_token, monitor_lists[0])
	delete_monitor(mcd_id, mcd_token, monitor_lists[1])


if __name__ == '__main__':
	main()
