import pandas as pd
from typing import List, Dict
from pycarlo.core import Client, Query, Mutation
import time


def get_all_tables(client: Client, batch_size: int = 1000, selected_table_fields: List[str] = None, sleep_in_seconds: float = 0.5):
    """
    Retrieves all tables via getTable API: https://apidocs.getmontecarlo.com/#query-getTable
    Tweak batch_size to increase throughput
    Tweak sleep_in_seconds to stay within API limits
    """
    selected_table_fields = selected_table_fields or [
        'mcon', 'project_name', 'dataset', 'table_id', 'table_type', 'full_table_id'
    ]
    current_cursor = None
    tables = []

    while (True):
        params = {
            'first': batch_size,
            'is_deleted': False
        }
        if current_cursor:
            params['after'] = current_cursor

        query = Query()
        get_tables_query = query.get_tables(**params)
        get_tables_query.page_info()
        get_tables_query.edges.node.__fields__(*selected_table_fields)

        print(get_tables_query)

        response = client(query)
        for table in response.get_tables.edges:
            tables.append({
                field: table.node[field]
                for field in selected_table_fields
            })

        has_next_page = response.get_tables.page_info.has_next_page
        if not has_next_page:
            break
        current_cursor = response.get_tables.page_info.end_cursor

        time.sleep(sleep_in_seconds)

    return tables


def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def get_lineage_graph_for_tables(client: Client, tables: List[Dict], direction: str = 'downstream',
                                 batch_size: int = 20, sleep_in_seconds: float = 0.5):
    """
    Retrieve all lineage edges for tables.
    Tweak batch_size to control throughput
    Tweak sleep_in_seconds to stay within API limits
    """
    if batch_size > 20:
        raise ValueError('batch_size must be between 0 and 20')

    edges = set()
    count = 0
    for chunk in chunker(tables, batch_size):
        query = Query()
        query.get_table_lineage(
            mcons=[table['mcon'] for table in chunk],
            direction=direction
        ).flattened_edges.__fields__('mcon', 'directly_connected_mcons')

        response = client(query)

        if hasattr(response.get_table_lineage, 'flattened_edges'):
            for flattened_edge in response.get_table_lineage.flattened_edges:
                for destination_mcon in flattened_edge.directly_connected_mcons:
                    edges.add((flattened_edge.mcon, destination_mcon))

        count += len(chunk)
        print(f"Fetched lineage for {count} nodes")
        time.sleep(sleep_in_seconds)
    return edges


if __name__ == '__main__':
    client = Client()
    tables = get_all_tables(client)
    print(f"Retrieved {len(tables)} tables")
    edges = get_lineage_graph_for_tables(client, tables)
    print(f"Retrieved {len(edges)} edges")

    tables_df = pd.DataFrame(tables)
    tables_df.to_csv('/tmp/tables.csv')

    edges_df = pd.DataFrame(edges)
    edges_df.to_csv('/tmp/edges.csv')
