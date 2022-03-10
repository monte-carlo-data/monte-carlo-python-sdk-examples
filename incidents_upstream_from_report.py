from email.policy import default
from pycarlo.core import Client, Query, Session
import requests
import csv
import networkx as nx

mcd_profile='dev'
bi_report_id = '123' # ID of object in Looker / Tableau
exclude_incidents_with_status = ['FIXED', 'FALSE_POSITIVE', 'EXPECTED', 'NO_ACTION_NEEDED']
incident_types_to_include = ['CUSTOM_RULE_ANOMALIES','DELETED_TABLES']
incident_sub_types_to_include = ['dimension_anomaly','field_metrics_anomaly','freshness_anomaly','volume_anomaly']

def get_report_quality_status(mcd_profile, bi_report_id, exclude_incidents_with_status, incident_types_to_include, incident_sub_types_to_include):
    # start a Monte Carlo API session
    client = Client(session=Session(mcd_profile=mcd_profile))

    # set column positions in csv 
    row_position = 0
    type_position = 2
    name_position = 3
    dataset_id_position = 5 
    mcon_position = 9

    # get MC lineage directed graph
    get_digraph = Query()
    get_digraph.get_digraph(metadata_version="v2")
    digraph = client(get_digraph).get_digraph

    # get a list of nodes
    download_vertices = requests.get(digraph.vertices)
    decoded_vertices = download_vertices.content.decode('utf-8')
    vertices_csv = csv.reader(decoded_vertices.splitlines(), delimiter=',')
    vertices = list(vertices_csv)

    # get a list of edges in Monte Carlo lineage
    download_edges = requests.get(digraph.edges)
    decoded_edges = download_edges.content.decode('utf-8')

    # create a networkx directed graph
    G = nx.DiGraph()
    G = nx.read_edgelist(decoded_edges.splitlines(), delimiter=',', nodetype=str, create_using=nx.DiGraph)

    for index, sublist in enumerate(vertices):
        if sublist[mcon_position] != 'mcon' and sublist[mcon_position].split('++')[4] == bi_report_id:
            bi_report_id = f'"{index}"'
            break

    # find upstream nodes
    upstream_nodes = [n for n in nx.traversal.bfs_tree(G, bi_report_id, reverse=True) if n != bi_report_id]

    # create a list of tables upstream
    tables_upstream = []
    for upstream_node in upstream_nodes:
        node_id = int(upstream_node.strip('"'))
        if vertices[node_id][type_position] == 'table':
            tables_upstream.append(vertices[node_id][name_position])

    # get incidents
    tables_with_incidents = []
    has_unresolved_incident = False
    get_recent_incidents = Query()
    get_recent_incidents.get_incidents(
        first=100, 
        exclude_feedback=exclude_incidents_with_status, 
        incident_types=incident_types_to_include,
        incident_sub_types=incident_sub_types_to_include
        ).edges.node.events(first=100).edges.node.table.__fields__('full_table_id')
    incidents = client(get_recent_incidents).get_incidents.edges

    for incident in incidents:
        for event_edge in incident.node.events.edges:
            table_with_incident = event_edge.node.table.full_table_id
            if table_with_incident in tables_upstream:
                has_unresolved_incident = True
                tables_with_incidents.append(table_with_incident)
                print(f'Recent unresolved incident upstream on {table_with_incident}.')

    return has_unresolved_incident, tables_with_incidents


has_unresolved_incident, tables_with_incidents = get_report_quality_status(mcd_profile, bi_report_id, exclude_incidents_with_status, incident_types_to_include, incident_sub_types_to_include)
print(has_unresolved_incident)