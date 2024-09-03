from montecarlodata.queries.common import GQL


class Queries:
    create = GQL(
        query="""
        mutation ($description: String!, $scope: IntegrationKeyScope!, $warehouseIds: [UUID]) {
            createIntegrationKey(description: $description, scope: $scope, warehouseIds: $warehouseIds) {
                key {
                    id
                    secret
                }
            }
        }
        """,
        operation="createIntegrationKey",
    )

    delete = GQL(
        query="""
        mutation ($keyId: String!) {
            deleteIntegrationKey(keyId: $keyId) {
                deleted
            }
        }
        """,
        operation="deleteIntegrationKey",
    )

    get_all = GQL(
        query="""
        query ($scope: String, $resourceUuid: UUID) {
            getIntegrationKeys(scope: $scope, resourceUuid: $resourceUuid) {
                id
                description
                scope
                createdTime,
                createdBy {
                    id,
                    firstName,
                    lastName,
                    email
                }
            }
        }
        """,
        operation="getIntegrationKeys",
    )
