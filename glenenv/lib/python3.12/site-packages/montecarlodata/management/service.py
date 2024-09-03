import csv
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional

import click
from dataclasses_json import LetterCase, dataclass_json
from pycarlo.core import Client, Mutation, Query
from tabulate import tabulate

from montecarlodata.common.common import ConditionalDictionary
from montecarlodata.common.user import UserService
from montecarlodata.config import Config
from montecarlodata.errors import complain_and_abort, manage_errors

DEFAULT_COLLECTION_BLOCK_LIST_PAGE_SIZE = 100


@dataclass_json(letter_case=LetterCase.CAMEL)  # type: ignore
@dataclass
class CollectionBlock:
    resource_id: str
    project: str
    dataset: Optional[str] = None


class ManagementService:
    _LIST_PII_PREFERENCES_HEADERS = ["Status", "Fail Mode"]
    _COLLECTION_BLOCK_LIST_HEADERS = ["Resource", "Project", "Dataset"]
    _mutation_add_to_block_list = """
        mutation addToCollectionBlockList($collectionBlocks: [CollectionBlockInput]!) {
            addToCollectionBlockList(collectionBlocks: $collectionBlocks) {
                success
            }
        }
    """
    _mutation_remove_from_block_list = """
        mutation removeFromCollectionBlockList($collectionBlocks: [CollectionBlockInput]!) {
            removeFromCollectionBlockList(collectionBlocks: $collectionBlocks) {
                success
            }
        }
    """

    def __init__(
        self,
        config: Config,
        mc_client: Client,
        user_service: Optional[UserService] = None,
    ):
        self._abort_on_error = True
        self._mc_client = mc_client
        self._user_service = user_service or UserService(config=config)

    @manage_errors
    def get_pii_preferences(
        self,
        headers: str = "firstrow",
        table_format: str = "fancy_grid",
    ) -> None:
        table = [self._LIST_PII_PREFERENCES_HEADERS]

        query = Query()
        query.get_pii_filtering_preferences()
        preferences = self._mc_client(query).get_pii_filtering_preferences
        table.append(
            [
                "Enabled" if preferences.enabled else "Disabled",
                preferences.fail_mode,  # type: ignore
            ]
        )
        click.echo(tabulate(table, headers=headers, tablefmt=table_format, maxcolwidths=100))

    @manage_errors
    def set_pii_filtering(
        self,
        enabled: Optional[bool] = None,
        fail_mode: Optional[str] = None,
    ) -> None:
        variables = ConditionalDictionary(lambda x: x is not None)
        variables.update({"enabled": enabled, "fail_mode": fail_mode})

        mutation = Mutation()
        mutation.update_pii_filtering_preferences(**variables)
        result = self._mc_client(mutation).update_pii_filtering_preferences
        if result.success:
            click.echo("PII filtering preferences have been updated!")

    @manage_errors
    def get_collection_block_list(
        self,
        resource_name: Optional[str] = None,
        limit: int = DEFAULT_COLLECTION_BLOCK_LIST_PAGE_SIZE,
        headers: str = "firstrow",
        table_format: str = "fancy_grid",
    ) -> None:
        table = [self._COLLECTION_BLOCK_LIST_HEADERS]

        variables = {"first": limit}
        if resource_name:
            resource_id = self._user_service.resource_identifiers.get(resource_name)
            if not resource_id:
                sys.exit(f"No existing resources with name '{resource_name}'.")
            variables["resource_id"] = resource_id

        while True:  # loop through each page of results
            query = Query()
            op = query.get_collection_block_list(**variables)
            op.edges.node.__fields__("resource_id", "project", "dataset")  # type: ignore
            op.page_info()  # type: ignore

            result = self._mc_client(query)
            collection_block_connection = result.get_collection_block_list
            if not collection_block_connection.edges:
                click.echo("No collection blocks found.")
                return
            for edge in collection_block_connection.edges:
                table.append(
                    [
                        self._user_service.resource_identifiers[edge.node.resource_id],  # type: ignore
                        edge.node.project or "",  # type: ignore
                        edge.node.dataset or "",  # type: ignore
                    ]
                )
            if not collection_block_connection.page_info.has_next_page:
                break
            variables["after"] = collection_block_connection.page_info.end_cursor  # type: ignore

        click.echo(tabulate(table, headers=headers, tablefmt=table_format, maxcolwidths=100))

    def update_collection_block_list(
        self,
        adding: bool,
        filename: Optional[str] = None,
        resource_name: Optional[str] = None,
        project: Optional[str] = None,
        dataset: Optional[str] = None,
    ):
        mutation = (
            self._mutation_add_to_block_list if adding else self._mutation_remove_from_block_list
        )

        resource_id = None
        if resource_name:
            resource_id = self._user_service.resource_identifiers.get(resource_name)
            if not resource_id:
                complain_and_abort(f"No existing resources with name '{resource_name}'.")

        collection_blocks_input = (
            self.parse_collection_blocks_file(filename)
            if filename
            else [
                {
                    "resourceId": resource_id,
                    "project": project,
                    "dataset": dataset,
                },
            ]
        )
        operation = self._mc_client(
            mutation, variables={"collectionBlocks": collection_blocks_input}
        )
        result = (
            operation.add_to_collection_block_list  # type: ignore
            if adding
            else operation.remove_from_collection_block_list  # type: ignore
        )
        if result.success:
            click.echo("Collection block list has been updated!")

    def parse_collection_blocks_file(self, filename: str) -> List[Dict]:
        block_list_file = csv.DictReader(open(filename))
        collection_blocks = []
        for row in block_list_file:
            resource_name = row["resource_name"]
            resource_id = self._user_service.resource_identifiers.get(resource_name)
            if not resource_id:
                complain_and_abort(f"No existing resources with name '{resource_name}'.")

            collection_blocks.append(
                CollectionBlock(
                    resource_id=resource_id,  # type: ignore
                    project=row["project"],
                    dataset=row.get("dataset"),
                ).to_dict()  # type: ignore
            )
        return collection_blocks
