import configparser
import os
import argparse
from pycarlo.core import Client, Query, Mutation, Session
from typing import Optional

BATCH = 100


def enable_schema_usage(dw_id: str, project: str, dataset: str, rules: list) -> Mutation:
	"""Enable tables under a schema to be monitored.

		Args:
			dw_id(str): Warehouse UUID from MC.
			dataset(str): Target for tables in the project/database.
			project(str): Target for tables in the dataset/schema.
			rules(list(dict)): List of rules for deciding which tables are monitored.

		Returns:
			Mutation: Formed MC Mutation object.

	"""

	not_none_params = {k: v for k, v in locals().items() if v is not None}
	mutation = Mutation()
	update_monitored_table_rule_list = mutation.update_monitored_table_rule_list(**not_none_params)
	update_monitored_table_rule_list.__fields__("id")
	return mutation


def get_table_query(dw_id: str, search: str, first: Optional[int] = BATCH, after: Optional[str] = None) -> Query:
	"""Retrieve table information based on warehouse id and search parameter.

		Args:
			dw_id(str): Warehouse UUID from MC.
			search(str): Database/Schema combination to apply in search filter.
			first(int): Limit of results returned by the response.
			after(str): Cursor value for next batch.

		Returns:
			Query: Formed MC Query object.

	"""

	query = Query()
	# Add . at the end of the schema to search to ensure delimiter is respected
	get_tables = query.get_tables(first=first, dw_id=dw_id, search=f"{search}.", is_deleted=False, is_monitored=True,
								  **(dict(after=after) if after else {}))
	get_tables.edges.node.__fields__("full_table_id", "mcon", "table_type")
	get_tables.page_info.__fields__(end_cursor=True)
	get_tables.page_info.__fields__("has_next_page")
	return query


if __name__ == '__main__':

	# Capture Command Line Arguments
	parser = argparse.ArgumentParser(description='Enable Usage & Monitoring')
	parser.add_argument('--profile', '-p', required=True, default="default",
						help='Specify an MCD profile name. Uses default otherwise')
	parser.add_argument('--warehouse', '-w', required=True,
						help='Warehouse ID')
	parser.add_argument('--input', '-i', required=True,
	                    help='Path to the txt file containing list of full table ids.')
	parser.add_argument('--operation', '-o', choices=['enable', 'disable'], required=False, default='enable',
						help='Enable/Disable tables under usage.')

	args = parser.parse_args()

	# Initialize variables
	profile = args.profile
	warehouse_id = args.warehouse
	input_file = args.input

	if args.operation == 'disable':
		rules = []
		enabled = False
	else:
		enabled = True

	# Read input file and create rules
	mapping = {}
	with open(input_file, 'r') as input_tables:
		for table in input_tables:
			table_filter, table_name = table.split('.')
			content = {'project': table_filter[:table_filter.index(":")],
			           'dataset': table_filter[table_filter.index(":") + 1:],
			           'rules': []}
			if not mapping.get(table_filter):
				mapping[table_filter] = content

			if enabled:
				rule = {
					"isExclude": False,
					"ruleType": "wildcard_pattern",
					"tableRuleAttribute": "table_id",
					"tableRuleText": table_name
				}
				mapping[table_filter]['rules'].append(rule)

	# Read token variables from CLI default's config path ~/.mcd/profiles.ini
	configs = configparser.ConfigParser()
	profile_path = os.path.expanduser("~/.mcd/profiles.ini")
	configs.read(profile_path)
	mcd_id_current = configs[profile]['mcd_id']
	mcd_token_current = configs[profile]['mcd_token']

	client = Client(session=Session(mcd_id=mcd_id_current, mcd_token=mcd_token_current))
	for db_schema in mapping:
		project = mapping[db_schema]['project']
		dataset = mapping[db_schema]['dataset']
		rules = mapping[db_schema]['rules']
		if len(rules) > 100:
			print("Monitor rules allow at most 100 entries. Use a different method to filter out tables i.e. pattern match")
			exit(0)
		print(f"- Step 1: {args.operation.title()} usage for database/schema combination "
		      f"[{project}:{dataset}] and warehouse [{warehouse_id}]...")
		response = client(enable_schema_usage(dw_id=warehouse_id, project=project, dataset=dataset, rules=rules)).update_monitored_table_rule_list
		if isinstance(response, list):
			print(f" [ ✔ success ] monitor rule {args.operation}d\n")
		else:
			print(" [ 𐄂 failure ] an error occurred")
			exit(1)

		if enabled:
			print(f"- Step 2: Retrieving monitored tables matching [{project}:{dataset}] and warehouse [{warehouse_id}]...")
			view_mcons = []
			cursor = None
			while True:
				response = client(get_table_query(dw_id=warehouse_id, search=f"{project}:{dataset}", after=cursor)).get_tables
				for table in response.edges:
					if table.node.table_type in ['VIEW', 'EXTERNAL']:
						view_mcons.append(table.node.mcon)
				if response.page_info.has_next_page:
					next_token = response.page_info.end_cursor
				else:
					break
			print(f" [ ✔ success ] assets identified\n")

			print(f"- Step 3: {args.operation.title()} row count monitoring for views under [{project}:{dataset}] and warehouse [{warehouse_id}]...")
			enable_row_count_query = f"""
				mutation updateToggleSizeCollection($mcon: String!, $enabled: Boolean!) {{
					toggleSizeCollection(
						mcon: $mcon
						enabled: $enabled    
					) {{
						enabled
					}}
				}}
				"""
			for view in view_mcons:
				response = client(enable_row_count_query, variables={"mcon": view, "enabled": enabled})
				if response.toggle_size_collection.enabled:
					print(f" [ ✔ success ] row count {args.operation}d for mcon[{view}]")
				else:
					print(f" [ 𐄂 failure ] unable to apply {args.operation.lower()} action")
					exit(1)
