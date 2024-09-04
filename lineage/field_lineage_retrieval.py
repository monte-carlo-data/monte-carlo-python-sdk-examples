import argparse
import os
from pycarlo.core import Client, Query, Session
from typing import Optional, List
import pandas as pd


def extract_fields(obj, prefix=''):
    """Recursively extracts fields from nested objects and flattens them into a dictionary.

    Args:
        obj: The object to extract fields from. Can be a list, dict, or other type.
        prefix: A string prefix to prepend to the keys in the resulting dictionary.

    Returns:
        A dictionary with flattened keys and their corresponding values.
    """
    data = {}
    if isinstance(obj, list):
        for i, item in enumerate(obj):
            data.update(extract_fields(item, f'{prefix}{i}_'))
    elif isinstance(obj, dict):
        for key, value in obj.items():
            data.update(extract_fields(value, f'{prefix}{key}_'))
    else:
        data[prefix[:-1]] = obj
    return data

def parse_response(response):
    """Parses a response object, converting it to a dictionary if necessary.

    Args:
        response: The response object to parse.

    Returns:
        A dictionary representation of the response.
    """
    if hasattr(response, '__dict__'):
        response = response.__dict__
    if isinstance(response, dict):
        return {k: parse_response(v) for k, v in response.items()}
    elif isinstance(response, list):
        return [parse_response(item) for item in response]
    else:
        return response


def get_table_query(first: Optional[int] = 1000, after: Optional[str] = None) -> Query:
    """Returns a GraphQL query for retrieving table information.

    Args:
        first: The number of records to retrieve in the query.
        after: A cursor for pagination to retrieve the next set of records.

    Returns:
        A GraphQL query object.
    """
    query = Query()
    get_tables = query.get_tables(first=first, is_deleted=False, **(dict(after=after) if after else {}))
    get_tables.edges.node.__fields__("full_table_id", "mcon")
    get_tables.page_info.__fields__(end_cursor=True)
    get_tables.page_info.__fields__("has_next_page")
    return query


def get_table_fields(mcon: Optional[str] = None,
                     first: Optional[int] = 1000) -> Query:
    """Returns a GraphQL query for retrieving fields of a specific table.

    Args:
        mcon: The MCON (Monte Carlo Object Name) of the table.
        first: The number of records to retrieve in the query.

    Returns:
        A GraphQL query object.
    """
    query = Query()
    get_table = query.get_table(mcon=mcon)
    get_table.__fields__("id", "table_id", "full_table_id", "mcon")

    versions = get_table.versions(first=1)
    versions.page_info.__fields__("end_cursor", "has_next_page")
    fields = versions.edges.node.fields(first=first)
    fields.page_info.__fields__("end_cursor", "has_next_page")
    fields.edges.node.__fields__("name", "description")

    return query

def get_column_lineage(mcon: Optional[str] = None, column_name: Optional[str] = None, first: Optional[int] = 1000) -> Query:
    """Returns a GraphQL query for retrieving column lineage information.

    Args:
        mcon: The MCON (MC Object Name) of the table.
        column_name: The name of the column to retrieve lineage for.
        first: The number of records to retrieve in the query.

    Returns:
        A GraphQL query object.
    """
    query = Query()
    get_table = query.get_derived_tables_partial_lineage(mcon=mcon, colum_nname=column_name)
    get_table.destinations.columns.__fields__("columnname", "column_type")

    return query

def get_derived_tables_partial_lineage_string():
    """Returns a GraphQL query string for derived tables partial lineage.
    This query will show the downstream lineage of a column in the BI tool.

    Returns:
        A GraphQL query string.
    """
    query = f"""
    query GetDerivedTablesPartialLineage(
      $column: String,
      $cursor: String,
      $mcon: String,
      $pageSize: Int
    ) {{
      getDerivedTablesPartialLineage(
        column: $column,
        cursor: $cursor,
        mcon: $mcon,
        pageSize: $pageSize
      ) {{
        cursor
        destinations {{
          columns {{
            columnName
            columnType
          }}
          displayName
          mcon
          sourceColumnUsedAsNonSelected
        }}
        isLastPage
        mcon
        sourceColumn
      }}
    }}
    """
    return query


def get_field_names(flat_data):
    """Extracts field names from the flattened data.

    Args:
        flat_data: The flattened data dictionary.

    Returns:
        A list of field names.
    """
    field_names = []

    for k, v in flat_data.items():
        for version in  v['versions']['edges']:
            for each_edge in version['node']['fields']['edges']:
                field_names.append(each_edge['node']['name'])

    return field_names


def get_lineage_data(field_list: List[str], mcon: str, client) -> pd.DataFrame:
    """Retrieves lineage data for a list of fields and returns it as a DataFrame.

    Args:
        field_list: A list of field names.
        mcon: The MCON (MC Object Name) of the table.
        client: The client object to execute the query.

    Returns:
        A DataFrame containing the lineage data.
    """
    results = []

    lineage_query = get_derived_tables_partial_lineage_string()

    total_columns = len(field_list)

    counter = 1
    for column in field_list:
        print(f'Getting data for column {counter}/{total_columns}: {column}')
        response = client(lineage_query, variables={"column": column, "mcon": mcon, "pageSize": 1000}).GetDerivedTablesPartialLineage

        for destination in response['destinations']:
            results.append({
                "table_mcon": mcon,
                "column": column,
                "display_name": response['sourceColumn'],
                "destination_display_name": destination['displayName'],
                "destination_mcon": destination['mcon'],
                "destination_source_column_used_as_non_selected": destination['sourceColumnUsedAsNonSelected']
            })

        counter += 1

    df = pd.DataFrame(results, columns=[
        "table_mcon",
        "column",
        "display_name",
        "destination_display_name",
        "destination_mcon",
        "destination_source_column_used_as_non_selected"
    ])

    return df


def fetch_tables(after=None):
    """Fetches tables using the provided cursor for pagination.

    Args:
        after: A cursor for pagination to retrieve the next set of records.

    Returns:
        The tables retrieved from the query.
    """
    query = get_table_query(after=after)
    client = Client(session=Session(mcd_id, mcd_token))
    tables = client(query).get_tables
    return tables

def table_information():
    """Retrieves information about tables and their MCONs.

    Returns:
        A dictionary mapping full_table_id to MCON.
    """
    results = {}
    next_page_cursor = None
    has_next_page = True

    while has_next_page:
        print(f"Fetching tables with cursor: {next_page_cursor}")
        table_info = fetch_tables(after=next_page_cursor)
        flat_data = extract_fields(table_info)
        edges = flat_data['']['edges']

        for edge in edges:
            node = edge['node']
            results[node['full_table_id']] = node['mcon']

        page_info = table_info['page_info']
        next_page_cursor = page_info['end_cursor']
        has_next_page = page_info['has_next_page']

    return results


def write_dataframe_to_csv(df: pd.DataFrame, file_path: str, delimiter: str = ',', header: bool = True, index: bool = False):
    """Writes a DataFrame to a CSV file.

    Args:
        df: The DataFrame to write.
        file_path: The path to the output CSV file.
        delimiter: The delimiter to use in the CSV file (default is ',').
        header: Whether to write the column names (default is True).
        index: Whether to write row indices (default is False).
    """
    df.to_csv(file_path, sep=delimiter, header=header, index=index)


def get_table_from_mcon(mcon: str) -> str:
    """Transforms an MCON string by replacing ':' and '.' with '__'.

    Args:
        mcon: The MCON string to transform.

    Returns:
        The transformed string.
    """
    parts = mcon.split('++')
    last_part = parts[-1]
    transformed = last_part.replace(':', '__').replace('.', '__')
    return transformed

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run Monte Carlo script with optional parameters.')
    parser.add_argument('--mcd_id', type=str, help='Monte Carlo Key ID')
    parser.add_argument('--mcd_token', type=str, help='Monte Carlo Token')
    parser.add_argument('--csv_file', type=str, default='default.csv', help='CSV file path')
    parser.add_argument('--list_tables', action='store_true', help='List tables if this flag is set')
    parser.add_argument('--table_column_lineage', type=str, help='Enter a mcon and it will write the table column lineage to a csv file')
    parser.add_argument('--find_mcon', type=str, help='Enter a table name like database:schema.table_name to find the mcon')

    args = parser.parse_args()

    mcd_id = args.mcd_id or os.getenv('MONTE_CARLO_KEY_ID')
    mcd_token = args.mcd_token or os.getenv('MONTE_CARLO_TOKEN')
    csv_file = args.csv_file

    if not mcd_id or not mcd_token:
        raise ValueError('Monte Carlo Key ID and Token must be provided either as arguments or environment variables.')

    if args.list_tables:
        tables = table_information()
        for k, v in tables.items():
            print(k, v)

    if args.find_mcon:
        tables = table_information()
        mcon = tables[args.find_mcon]
        print(f'MCON for {args.find_mcon}: {mcon}')

    if args.table_column_lineage:
        mcon = args.table_column_lineage
        table_name = get_table_from_mcon(mcon)
        query = get_table_fields(mcon=mcon)
        client = Client(session=Session(mcd_id, mcd_token))
        table_info = client(query).get_table
        flat_data = extract_fields(table_info)
        field_list = get_field_names(flat_data)
        print(field_list)
        df = get_lineage_data(field_list, mcon, client)
        print(df)

        table_name = get_table_from_mcon(mcon)
        file_name = f'column_lineage_{table_name}.csv'

        write_dataframe_to_csv(df, file_path=file_name)

        # Sample Commands
        # python adhoc/monte_carlo_tableau_lineage.py  --table_column_lineage "MCON++6e3a0f5a-780e-426f-a849-3e09f960aa23++f7f87ea4-7c4d-4d13-8292-a7af55621eca++view++bi_masterdata:masterdata_presentation.vw_mst_arr_waterfall_monthly_net"
        # python adhoc/monte_carlo_tableau_lineage.py  --table_column_lineage "MCON++6e3a0f5a-780e-426f-a849-3e09f960aa23++f7f87ea4-7c4d-4d13-8292-a7af55621eca++view++bi_masterdata:masterdata_presentation.vw_mst_arr_waterfall_monthly_net" --mcd_id "your_monte_carlo_key_id" --mcd_token "your_monte_carlo_token"
        # python adhoc/monte_carlo_tableau_lineage.py  --find_mcon bi_masterdata:masterdata_presentation.vw_mst_arr_waterfall_monthly_net
        # python adhoc/monte_carlo_tableau_lineage.py  --list_tables