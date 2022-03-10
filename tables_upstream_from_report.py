
from pycarlo.core import Client, Query, Session
import requests
import csv
import networkx as nx

mcd_profile='dev'
bi_report_id = '123'

# start a Monte Carlo API session
client = Client(session=Session(mcd_profile=mcd_profile))

# set column positions in csv 
row_position = 0
type_position = 2
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

# create a list of table nodes
table_nodes = []
for node in vertices:
    if node[type_position] in ['table']:
        table_nodes.append(node)


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

# find downstream nodes
upstream_nodes = [n for n in nx.traversal.bfs_tree(G, bi_report_id, reverse=True) if n != bi_report_id]

tables_upstream = []
for upstream_node in upstream_nodes:
    node_id = int(upstream_node.strip('"'))
    if vertices[node_id][type_position] == 'table':
        tables_upstream.append(vertices[node_id])

# write affected looker objects to a csv - contains all, including duplicates (should handle this earlier but shrug)
with open('tables_upstream.csv', 'w') as f:
    write = csv.writer(f)
    write.writerow(vertices[0])
    write.writerows(tables_upstream)