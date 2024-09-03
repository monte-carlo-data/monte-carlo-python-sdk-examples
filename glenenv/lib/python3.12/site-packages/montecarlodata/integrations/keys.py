from enum import Enum
from typing import Dict, List, Optional

import click
from tabulate import tabulate

from montecarlodata.common.data import ConnectionType
from montecarlodata.common.user import UserService
from montecarlodata.config import Config
from montecarlodata.errors import complain_and_abort, manage_errors
from montecarlodata.queries.keys import Queries
from montecarlodata.utils import GqlWrapper


class IntegrationKeyScope(Enum):
    Spark = "spark"
    DatabricksMetadata = "databricksmetadata"

    @classmethod
    def values(cls) -> List[str]:
        return list(map(lambda s: s.name, cls))


class IntegrationKeyService:
    _table_headers = ["Id", "Description", "Scope", "Created", "Created By"]

    def __init__(
        self,
        config: Config,
        gql: Optional[GqlWrapper] = None,
        user_service: Optional[UserService] = None,
    ):
        self._gql = gql or GqlWrapper(config)
        self._user_service = user_service or UserService(config=config)

        # used by @manage_errors decorator
        self._abort_on_error = True

    @manage_errors
    def create(
        self,
        description: Optional[str],
        scope: str,
        warehouse_ids: Optional[List[Optional[str]]] = None,
    ):
        response = self._gql.make_request_v2(
            query=Queries.create.query,
            variables=self._resolve_variables(description, scope, warehouse_ids),
            operation=Queries.create.operation,
        )

        click.echo(f"Key id: {response.data.key.id}")  # type: ignore
        click.echo(f"Key secret: {response.data.key.secret}")  # type: ignore

    def _resolve_variables(
        self,
        description: Optional[str],
        scope: str,
        warehouse_ids: Optional[List[Optional[str]]] = None,
    ) -> Dict:
        variables = {"description": description, "scope": scope}

        if scope.lower() == IntegrationKeyScope.Spark.value:
            variables["warehouseIds"] = [self._resolve_lake_warehouse_id()]
        elif warehouse_ids:
            variables["warehouseIds"] = warehouse_ids

        return variables

    def _resolve_lake_warehouse_id(self) -> Optional[str]:
        lake_warehouse_ids = [
            w["uuid"]
            for w in self._user_service.warehouses
            if w["connectionType"] == ConnectionType.DataLake.value
        ]

        num_lakes = len(lake_warehouse_ids)
        if num_lakes == 0:
            complain_and_abort("Unable to resolve data lake connection: no lake connection found.")
        elif num_lakes > 1:
            complain_and_abort(
                "Unable to resolve data lake connection: multiple lake connections found."
            )
        else:
            return lake_warehouse_ids[0]

    @manage_errors
    def delete(self, key_id: str):
        response = self._gql.make_request_v2(
            query=Queries.delete.query,
            variables={"keyId": key_id},
            operation=Queries.delete.operation,
        )

        if response.data.deleted:  # type: ignore
            click.echo("Key has been deleted.")
        else:
            click.echo("Key was not deleted.")

    @manage_errors
    def get_all(
        self,
        scope: Optional[str] = None,
        resource_uuid: Optional[str] = None,
        table_format: str = "fancy_grid",
    ):
        response = self._gql.make_request_v2(
            query=Queries.get_all.query,
            variables={"scope": scope, "resourceUuid": resource_uuid},
            operation=Queries.get_all.operation,
        )

        data = [
            [
                key.id,
                key.description,
                key.scope,
                key.createdTime,
                f"{key.createdBy.firstName} {key.createdBy.lastName}",
            ]
            for key in response.data  # type: ignore
        ]

        table = tabulate(data, headers=self._table_headers, tablefmt=table_format)
        click.echo(table)
