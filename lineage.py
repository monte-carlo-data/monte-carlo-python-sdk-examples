from pycarlo.core import Client, Mutation, Session

########################################################################
# Class to simplify adding Lineage to Monte Carlo for ML Models
########################################################################

class lineage:
    def __init__(self, customer: None):
        self.client = Client(session=Session(mcd_profile=customer))

    def add_downstream_node(self, node_name, node_id, warehouse_id, tags, source_nodes):
        ''' Arguments:
            * `node_name` (`String`): Object name (table name, report name, etc)
            * `node_id` (`String!`): Object identifier
            * `warehouse_id` (`UUID`): The id of the resource containing the node
        '''
        node_id = self.add_node(node_name, node_id, warehouse_id, tags)
        for source_node in source_nodes:
            self.add_edge(source_node, node_id, warehouse_id)
        return node_id

    def add_node(self, node_name, node_id, resource_id, tags):
        ''' Arguments:
            * `name` (`String`): Object name (table name, report name, etc)
            * `object_id` (`String!`): Object identifier
            * `object_type` (`String!`): Object type
            * `properties` (`[ObjectPropertyInput]`): A list of object properties to be indexed by the search service
            * `resource_id` (`UUID`): The id of the resource containing the node
        '''

        put = Mutation()
        put.create_or_update_lineage_node(
            name=node_name
            ,object_id=node_id
            ,object_type='ML Model'
            ,resource_id=resource_id # ID of Monte Carlo Warehouse to place the node under (Warehouse is a parent object)
            # ,tags=tags
        )
        response = self.client(put)
        return response.create_or_update_lineage_node.node.node_id 
            

    def add_edge(self, source_node, destination_node_id, warehouse_id):
        ''' Arguments:
            * `source` (`NodeInput!`): The destination node
                * object_id
                * object_type
                * tags (optional)
            * `destination` (`NodeInput!`): The destination node
                * object_id
                * object_type
                * tags (optional)
            * `expire_at` (`DateTime`): When the edge will expire
            * `source` (`NodeInput!`): The source node
        '''
        put = Mutation()
        
        source = {
            'object_id': source_node['object_id']
            ,'object_type': source_node['object_type']
            ,'resouce_id': warehouse_id
        },
        destination = {
            'object_id': destination_node_id
            ,'object_type': 'ML_Model'
            ,'resouce_id': warehouse_id
        }
        
        put.create_or_update_lineage_edge(source=source, destination=destination)

        put.create_or_update_lineage_edge(
            source = dict(
                object_id=source_node['object_id']
                ,object_type=source_node['object_type']
                ,resouce_id=warehouse_id
            ),
            destination = dict(
                object_id=destination_node_id
                ,object_type="ML Model"
                ,resouce_id=warehouse_id
            )
        )
        response = self.client(put)
        return response.create_or_update_lineage_edge.edge.edge_id

########################################################################
# Execution example of adding lineage for a new node and a source
#   table to Monte Carlo
########################################################################

# Initialize lineage class in my dev enviornment
l = lineage('dev')

# add details of the new node and edges to upstream nodes
new_node = l.add_downstream_node(
    node_name='My Machine Learning Model' # Name of asset in Monte Carlo
    ,node_id='prod_ml_models.my_machine_learning_model' # ID of asset in Monte Carlo
    ,warehouse_id='6110e6-b92-48f-a71-84b421f32' # ID of Monte Carlo Warehouse to place the node under (Warehouse is a parent object)
    ,source_nodes = [
        dict({
            'object_id': 'prod:ml_data.table_for_ml_model'
            , 'object_type': 'table'
        })
    ]
)

print(f'Created new node: {new_node}!')