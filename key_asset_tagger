#Change line 11 to filter for however many tables they have

from pycarlo.core import Client, Query, Mutation, Session
import requests
import csv

mcd_profile="mc_prod"
client = Client(session=Session(mcd_profile=mcd_profile))
query1=Query()
query2=Query()
query1.get_tables(first=3000).edges.node.__fields__("mcon","full_table_id")
query2.get_report_url(insight_name="key_assets",report_name="key_assets.csv").__fields__('url')
table_list=client(query1).get_tables.edges
report_url=client(query2).get_report_url.url
r = requests.get(report_url)
key_assets = r.content.decode('utf-8')
reader = csv.reader(key_assets.splitlines(),delimiter=",")
key_asset_list = list(reader)
table_mcon_object={}

for val in table_list:
	table_mcon_object[val.node.full_table_id] = val.node.mcon

count=1
for row in key_asset_list:
	table_id = str(row[1])
	if table_id == "FULL_TABLE_ID":
		continue
	key_asset_score = str(round(float(row[7]),1))
	if table_id in table_mcon_object.keys():
		mcon_id = str(table_mcon_object[table_id])
	else:
		continue

	print(count, mcon_id, key_asset_score)
	mutation=Mutation()
	mutation.create_or_update_object_property(mcon_id=mcon_id,property_name="Key Asset Score",property_value=key_asset_score).object_property.__fields__('id')
	print(client(mutation).create_or_update_object_property.object_property.id)
	count += 1
