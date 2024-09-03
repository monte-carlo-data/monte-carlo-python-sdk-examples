from dataclasses import dataclass
from typing import Dict, List, Optional, Union
from uuid import UUID

from box import Box

from montecarlodata.common.common import boxify
from montecarlodata.config import Config
from montecarlodata.errors import (
    AMBIGUOUS_AGENT_OR_COLLECTOR_MESSAGE,
    complain_and_abort,
)
from montecarlodata.queries.user import GET_USER_QUERY
from montecarlodata.utils import GqlWrapper


@dataclass
class User:
    first_name: Optional[str] = None
    last_name: Optional[str] = None


class UserService:
    def __init__(self, config: Config, request_wrapper: Optional[GqlWrapper] = None):
        self._config = config
        self._request_wrapper = request_wrapper or GqlWrapper(config)

        self._user = self._request_wrapper.make_request(GET_USER_QUERY)  # get user info

    @property
    def user(self) -> User:
        """
        Returns basic user properties
        """
        return User(
            first_name=self._user["getUser"].get("firstName"),  # type: ignore
            last_name=self._user["getUser"].get("lastName"),  # type: ignore
        )

    @property
    def account(self) -> Dict:
        """
        Get account details
        """
        return self._user["getUser"]["account"]  # type: ignore

    @property
    def collectors(self) -> Dict:
        """
        Get collectors in the account
        """
        return self.account.get("dataCollectors", [{}])

    @property
    def agents(self) -> List[Dict]:
        """
        Get agents in the account
        """
        agent_list = []
        for dc in self.collectors:
            for agent in dc.get("agents"):
                agent["dc_id"] = dc.get("uuid", "")
                agent_list.append(agent)
        return agent_list

    @property
    def active_collector(self) -> Dict:
        """
        Get active collector from collectors.

        Errors out on accounts with > 1 collector. Legacy. Do not use. See `get_collector`
        """
        return self.collectors[self._get_active_collector()]

    @property
    def resource_identifiers(self) -> Dict:
        """
        Returns a commutative mapping of resource uuid and name.
        e.g. user_service.resource_identifiers().get("resource_name") == "resource_uuid"
         and user_service.resource_identifiers().get("resource_uuid") == "resource_name"

        Currently only supports warehouse resources.
        """
        resource_identifiers = {}
        for resource in self.warehouses:
            resource_identifiers[resource["uuid"]] = resource["name"]
            resource_identifiers[resource["name"]] = resource["uuid"]
        return resource_identifiers

    @property
    def warehouses(self) -> List[Dict]:
        """
        Get warehouses attached to the account
        """
        return self.account.get("warehouses")  # type: ignore

    @property
    def bi_containers(self) -> List[Dict]:
        """
        Get bi attached to the account
        """
        return self.account.get("bi")  # type: ignore

    @property
    def etl_containers(self) -> List[Dict]:
        """
        Get etl attached to the account
        """
        return self.account.get("etlContainers")  # type: ignore

    @property
    def tableau_accounts(self) -> Dict:
        """
        Get tableau connections attached to the account
        """
        return self.account.get("tableauAccounts")  # type: ignore

    @property
    def active_collection_regions(self) -> List[str]:
        """
        Get a list of active collection regions
        """
        return self.account.get("activeCollectionRegions")  # type: ignore

    @boxify()
    def get_collector(self, dc_id: Optional[str] = None, agent_id: Optional[str] = None) -> Box:
        """
        Get a specific collector

        This is only necessary for client only ops on a specific collector.
        APIs handle this disambiguation already.
        """
        num_of_collectors = len(self.collectors)
        if num_of_collectors == 0:
            complain_and_abort("No collector found.")

        if agent_id:
            agent = self.get_agent(agent_id)
            dc_id = agent["dc_id"]  # type: ignore

        if dc_id:
            for collector in self.collectors:
                if dc_id == collector["uuid"]:
                    return collector
            complain_and_abort(f"Collector with ID '{dc_id}' not found.")
        if num_of_collectors > 1:
            complain_and_abort(AMBIGUOUS_AGENT_OR_COLLECTOR_MESSAGE)
        return self.collectors[0]

    def get_agent(self, agent_id: str) -> Optional[Dict]:
        if len(self.agents) == 0:
            complain_and_abort("No agents found.")

        for agent in self.agents:
            if agent.get("isDeleted") != "true" and agent_id == agent["uuid"]:
                return agent
        complain_and_abort(f"Agent with ID '{agent_id}' not found.")

    def get_collector_agent(self, dc_id: Optional[str] = None) -> Optional[Box]:
        dc = self.get_collector(dc_id)
        for agent in dc.agents:
            if not agent.isDeleted:
                return agent

    def _get_active_collector(self) -> Optional[int]:
        """
        Get active collector - currently only one active collector per account is supported.
        Abort if None are found
        """
        if len(self.collectors) > 1:
            complain_and_abort("This option is only supported in accounts with one collector.")

        for idx, collector in enumerate(self.collectors):
            if collector.get("active"):
                return idx
        complain_and_abort("No active collector found")

    def get_warehouse_for_connection(self, connection_id: Union[str, UUID]) -> Optional[Box]:
        """
        Get warehouse for the given connection id
        """
        for warehouse in self.warehouses:
            for connection in warehouse.get("connections", []):
                if str(connection_id) == connection.get("uuid"):
                    return Box(warehouse)
