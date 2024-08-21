from pycarlo.core import Client, Mutation, Session, Query
import time

def run_monitors(client, tag_value, tag_key, monitor_group):
	# Get list of tables that are tagged with tag_name
	print("getting list of monitors")
	query = Query()
	complete_breakers = []

	description_prefix = '%s | %s' % (tag_value, monitor_group)
	mcons = []
	monitors_to_run = []
	filter = [{'tag_name': tag_key,'tag_values': [tag_value]}]
	mcons = []
	query.search(query='',tag_filters=filter)
	search_results = client(query).search
	for result in search_results.results:
		print(result)
		mcons.append(result.mcon)
	if mcons:
		print('Getting Monitors')
		for mcon in mcons:
			query = Query()
			query.get_monitors(mcons=[mcon],monitor_types=['CUSTOM_SQL']).__fields__('uuid','description')
			response = client(query).get_monitors
			for monitor in response:
				if monitor.uuid not in monitors_to_run and monitor.description.startswith(description_prefix):
					monitors_to_run.append(monitor.uuid)
	breakers_triggered = []
	for monitor in monitors_to_run:
		print(monitor)
		breakers_triggered.append(trigger_circuit_breaker(client=client, uuid=monitor))
	if breakers_triggered:
		breaker_status = {}
		status_tries = 5
		try_count = 0
		while (len(complete_breakers) < len(breakers_triggered)) and try_count<status_tries:
			try_count = try_count + 1
			print("Getting Status: %s" % try_count)
			time.sleep(15)
			for breaker in breakers_triggered:
				status = resolve_status(client, breaker)
				breaker_status[breaker[0]] = status
			print('Statuses: %s' % breaker_status)
			for breaker in breaker_status:
				if (breaker_status[breaker] == 'PROCESSING_COMPLETE' or breaker_status[breaker] == 'HAS_ERROR') and breaker not in complete_breakers:
					print('Breaker complete: %s' % breaker)
					complete_breakers.append(breaker)
			if try_count == status_tries:
				for breaker in breaker_status:
					if breaker not in complete_breakers:
						print('Breaker complete: %s' % breaker)
						complete_breakers.append(breaker)

		return complete_breakers


def trigger_circuit_breaker(client,uuid):
	print("Starting breaker")
	mutation = Mutation()
	mutation.trigger_circuit_breaker_rule_v2(rule_uuid=uuid).__fields__('job_execution_uuids')
	execution_uuids = client(mutation).trigger_circuit_breaker_rule_v2.job_execution_uuids
	for uuid in execution_uuids:
		print("Found this execution: %s" % uuid)
	return execution_uuids
		
def resolve_status(client, uuids):
	print("Getting status")
	query = Query()
	query.get_circuit_breaker_rule_state_v2(job_execution_uuids=uuids)
	circuit_breaker_states = client(query).get_circuit_breaker_rule_state_v2
	status = circuit_breaker_states[0].status
	print('Status: %s, for: %s' % (status, uuids))
	return status

if __name__ == '__main__':
	# This will run all the SQL monitors associated with the tables that are tagged with the {tag_value} supplied.
	#-------------------INPUT VARIABLES---------------------
	mcdId = '{Insert Key here}'
	mcdToken = '{Insert Token here}'
	tag_key = '{Tag key name here}'
	tag_value = '{Tag value here}'
	# Monitor group is used to match at the monitor description level. The format in the description is: {tag_value} | {monitor_group} 
	monitor_group = '{Monitor group to run}'
	#-------------------------------------------------------
	print("Running monitors associated with tag: %s" % tag_value)
	client=Client(session=Session(mcd_id=mcdId,mcd_token=mcdToken))
	results = run_monitors(client,tag_value=tag_value, tag_key=tag_key, monitor_group=monitor_group)
	print("Final results: %s" % results)
