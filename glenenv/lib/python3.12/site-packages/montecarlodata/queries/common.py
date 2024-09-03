from collections import namedtuple

# namedtuple defining attributes of a GraphQL Query:
#  query: GraphQL query
#  operation: GraphQL query name, used to extract data from response
GQL = namedtuple("Query", "query operation")
