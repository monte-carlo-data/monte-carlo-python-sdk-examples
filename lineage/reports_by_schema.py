
from pycarlo.core import Client, Query, Session
import requests
import csv
import networkx as nx

mcd_profile='dev'
schema = 'my_dataset'

########################################################################
# Fetches all reports that are downstream of assets in a schema.
########################################################################

# start a Monte Carlo API session
client = Client(session=Session(mcd_profile=mcd_profile))

# set column positions in csv 
row_position = 0
type_position = 2
dataset_id_position = 5 

# get MC lineage directed graph
get_digraph = Query()
get_digraph.get_digraph(metadata_version="v2")
digraph = client(get_digraph).get_digraph

# get a list of nodes
download_vertices = requests.get(digraph.vertices)
decoded_vertices = download_vertices.content.decode('utf-8')
vertices_csv = csv.reader(decoded_vertices.splitlines(), delimiter=',')
vertices = list(vertices_csv)

# create a list of nodes that removes non-looker-nodes
looker_nodes = []
for node in vertices:
    if node[type_position] in ['looker-dashboard', 'looker-explore', 'looker-view', 'looker-look']:
        looker_nodes.append(node)


# get a list of edges in Monte Carlo lineage
download_edges = requests.get(digraph.edges)
decoded_edges = download_edges.content.decode('utf-8')

# create a networkx directed graph
G = nx.DiGraph()
G = nx.read_edgelist(decoded_edges.splitlines(), delimiter=',', nodetype=str, create_using=nx.DiGraph)

looker_dashboards_affected = []

# loop throuh nodes
for node in vertices:
    
    # if node is in the schema we are interested in
    if node[dataset_id_position] == schema:
        node_id = f'"{node[row_position]}"'
        try:
            # find downstream nodes
            downstream_nodes = [n for n in nx.traversal.bfs_tree(G, node_id) if n != node_id]

            # loop through downstream nodes against looker nodes and add to a dependency list if in looker
            for downstream_node in downstream_nodes:
                for looker_node in looker_nodes:
                    if  f'"{looker_node[row_position]}"' == downstream_node:
                        looker_dashboards_affected.append(looker_node)
        except:
            continue

# write affected looker objects to a csv - contains all, including duplicates (should handle this earlier but shrug)
with open('looker_dashboards_affected_dups.csv', 'w') as f:
    write = csv.writer(f)
    write.writerow(vertices[0])
    write.writerows(looker_dashboards_affected)

# remove duplicates
with open('looker_dashboards_affected_dups.csv','r') as in_file, open('looker_dashboards_affected.csv','w') as out_file:
    seen = set()
    for line in in_file:
        if line in seen: continue
        seen.add(line)
        out_file.write(line)
