
from pycarlo.core import Client, Query, Session
import requests
import csv
import networkx as nx

mcd_profile='dev'
asset_id = 'warehouse:schema.table'

########################################################################
# Fetches all downstream assets from a specified asset.
########################################################################

# start a Monte Carlo API session
client = Client(session=Session(mcd_profile=mcd_profile))

# set column positions in csv 
row_position = 0
type_position = 2
asset_id_position = 3 

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

assets_affected = []

# loop throuh nodes
for find in vertices:

    # find node of interest
    if find[asset_id_position] == asset_id:
        node_id = f'"{find[row_position]}"'
        try:
            # find downstream nodes
            downstream_nodes = [n for n in nx.traversal.bfs_tree(G, node_id) if n != node_id]

            # create list of downstream node metadata
            for downstream_node in downstream_nodes:
                downstream_node_id = int(downstream_node.replace('"', ''))
                assets_affected.append(vertices[downstream_node_id])
        except:
            continue

# write affected objects to a csv
asset_file_name = asset_id.replace(':','.')
with open(f'assets_downstream_from_{asset_file_name}.csv', 'w') as f:
    write = csv.writer(f)
    write.writerow(vertices[0])
    write.writerows(assets_affected)