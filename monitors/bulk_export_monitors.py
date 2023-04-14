#INSTRUCTIONS:
#NOTE: Only works if Monte Carlo environment has a single resource connected
#1. Pass your api keys when running script, and pass name of .yml file you want to write to
#2. .YML file will be written, which can then be used when syncing Monitors-as-code

from pycarlo.core import Client, Query, Mutation, Session
from typing import Optional
import textwrap

def get_monitors_query(limit: Optional[int] = 1000) -> Query:
	query = Query()
	get_monitors = query.get_monitors(limit=limit)
	get_monitors.__fields__("uuid","monitor_type","resource_id")
	return query

def export_yaml_template(monitorUuids):
	query=Query()
	get_yaml = query.export_monte_carlo_config_templates(monitor_uuids=monitorUuids)
	get_yaml.__fields__("config_template_as_yaml")
	return query

def bulk_export_yaml(mcdId,mcdToken,fileName):
	client=Client(session=Session(mcd_id=mcdId,mcd_token=mcdToken))
	response = client(get_monitors_query()).get_monitors
	monitor_list=[]
	monitor_yaml = {}
	counter=0
	with open(fileName,"w") as yaml_file:
		yaml_file.write("montecarlo:\n")
		for monitor in response:
			counter+=1
			monitor_list.append(monitor.uuid)
			if len(monitor_list) == 20:
				monitor_yaml = client(export_yaml_template(monitor_list)).export_monte_carlo_config_templates
				yaml_file.write(textwrap.indent(monitor_yaml["config_template_as_yaml"],prefix="  "))
				monitor_list=[]
				continue
		monitor_yaml = client(export_yaml_template(monitor_list)).export_monte_carlo_config_templates
		yaml_file.write(textwrap.indent(monitor_yaml["config_template_as_yaml"],prefix="  "))

if __name__ == '__main__':
	#-------------------INPUT VARIABLES---------------------
	mcd_id = input("MCD ID: ")
	mcd_token = input("MCD Token: ")
	filename = input("YAML Filename: ")
	#-------------------------------------------------------
	bulk_export_yaml(mcd_id,mcd_token,filename)
