from pycarlo.core import Client, Query, Mutation, Session
import csv
import datetime

def monitorConverter(mcdProfile,newResourceId,newTimeAxis,newTimeAxisName,newScheduleType,numMonitorsToConvert,parseLogic):
	client = Client(session=Session(mcd_profile=mcdProfile))
	query=Query()
	query.get_all_user_defined_monitors_v2(first=5000,user_defined_monitor_types=["stats"]).edges.node.__fields__('uuid','resource_id','next_execution_time','monitor_time_axis_field_type','monitor_time_axis_field_name','entities')
	count = 1
	old_table_list=[]
	new_table_list=[]
	for val in client(query).get_all_user_defined_monitors_v2.edges:
		x=Query()
		y=Query()
		mutation=Mutation()

		if val.node.resource_id == newResourceId:
			continue

		x.get_monitor(resource_id=val.node.resource_id,uuid=val.node.uuid).__fields__('uuid','type','full_table_id','schedule_config','agg_time_interval','history_days')
		response=client(x).get_monitor

		time_offset = count*3
		first_run_time = datetime.datetime.utcnow() + datetime.timedelta(minutes=time_offset)
		external_table_id = parseLogic
		old_table_list.append(val.node.entities[0])
		new_table_list.append(external_table_id)

		y.get_table(dw_id=newResourceId,full_table_id=external_table_id).__fields__('full_table_id')
		verified_table=client(y).get_table

		mutation.create_or_update_monitor(
			resource_id=newResourceId,
			monitor_type=response.type.lower(),
			time_axis_type=newTimeAxis,
			time_axis_name=newTimeAxisName,
			agg_time_interval=response.agg_time_interval, 
			lookback_days=response.history_days,
			full_table_id=external_table_id,
			schedule_config={
				"schedule_type":newScheduleType,
				"interval_minutes":response.schedule_config.interval_minutes,
				"start_time": first_run_time
				}
			).monitor.__fields__('uuid','monitor_type')
		mutation_response=client(mutation).create_or_update_monitor.monitor.uuid
		print(mutation_response)
		with open("completed_monitors.csv","a") as complete_monitors:
			writer = csv.writer(complete_monitors)
			writer.writerow([response.uuid,mutation_response,val.node.resource_id,val.node.next_execution_time,newTimeAxis,newTimeAxisName,])
			complete_monitors.close()
		print(count)
		print(first_run_time)
		if count == numMonitorsToConvert:
			break
		count += 1

	print(new_table_list)
	print(old_table_list)
	return old_table_list

def monitorDeleter(mcdProfile,listToDelete):
	client = Client(session=Session(mcd_profile=mcdProfile))
	count = 1
	for table_name in listToDelete:
		print(table_name, count)
		query=Query()
		query.get_monitor(monitor_type="stats",full_table_id=table_name).__fields__('uuid')
		value = client(query).get_monitor.uuid
		mutation=Mutation()
		mutation.stop_monitor(monitor_id=value).__fields__('success')
		response=client(mutation).stop_monitor.success
		print(response)
		count += 1

	print("Deletions Complete")

def existingMonitorCSV(mcdProfile, csvName):
	client = Client(session=Session(mcd_profile=mcdProfile))
	query=Query()
	query.get_all_user_defined_monitors_v2(first=5000,user_defined_monitor_types=["stats"]).edges.node.__fields__('uuid','resource_id','next_execution_time','monitor_time_axis_field_type','monitor_time_axis_field_name','entities')
	with open(csvName,'w') as monitor_list:
		writer = csv.writer(monitor_list)
		writer.writerow(['uuid','full_table_id','resource_id','next_execution_time','monitor_time_axis_field_type','monitor_time_axis_field_name'])
		for val in client(query).get_all_user_defined_monitors_v2.edges:
			writer.writerow([val.node.uuid,val.node.entities[0],val.node.resource_id,val.node.next_execution_time,val.node.monitor_time_axis_field_type,val.node.monitor_time_axis_field_name])
		monitor_list.close()

if __name__ == '__main__':
	##################--VARIABLES--########################
	new_resource_id=""
	new_time_axis="custom"
	new_time_axis_name="" #Enter custom SQL Expression
	new_schedule_type="FIXED"
	num_monitors_to_convert=15
	parse_logic="new_project_name:" + val.node.entities[0].split(":")[1] #Current Table Name is val.node.entities[0]
	mcd_profile = "dev_testing"
	#######################################################

	existingMonitorCSV(mcd_profile,"monitor_list_before_deletion.csv")
	old_list= monitorConverter(mcd_profile,new_resource_id,new_time_axis,new_time_axis_name,new_schedule_type,num_monitors_to_convert,parse_logic)
	monitorDeleter(mcd_profile,old_list)
	existingMonitorCSV(mcd_profile,"monitor_list_after_deletion.csv")
