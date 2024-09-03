import json
from typing import Dict, List, Optional

from pycarlo.core import Client

from montecarlodata.config import Config
from montecarlodata.integrations.configure.fields import (
    MATCH_AND_CREATE_BI_WH_SRC_GQL_OPERATION,
    MATCH_AND_CREATE_BI_WH_SRC_GQL_RESPONSE_FIELD,
    MATCH_SUCCESSFUL_GQL_RESPONSE_FIELD,
    MATCHING_BI_WH_SRC_GQL_RESPONSE_FIELD,
)
from montecarlodata.queries.bi import MATCH_AND_CREATE_BI_WAREHOUSE_SOURCES
from montecarlodata.utils import GqlWrapper


class BiService:
    def __init__(
        self,
        config: Config,
        mc_client: Client,
        request_wrapper: Optional[GqlWrapper] = None,
    ):
        self._mc_client = mc_client
        self._request_wrapper = request_wrapper or GqlWrapper(config)

    def refresh_bi_to_warehouse_connections(
        self, bi_container_id: str, warehouse_source_details: List[Dict]
    ) -> None:
        if warehouse_source_details:
            warehouse_source_details = [
                self._request_wrapper.convert_snakes_to_camels(wsd)
                for wsd in warehouse_source_details
            ]
            self._mc_client(
                query=MATCH_AND_CREATE_BI_WAREHOUSE_SOURCES,
                operation_name=MATCH_AND_CREATE_BI_WH_SRC_GQL_OPERATION,
                variables={
                    "biContainerId": bi_container_id,
                    "biWarehouseSources": warehouse_source_details,
                },
            )
        else:
            response = self._mc_client(
                query=MATCH_AND_CREATE_BI_WAREHOUSE_SOURCES,
                operation_name=MATCH_AND_CREATE_BI_WH_SRC_GQL_OPERATION,
                variables={
                    "biContainerId": bi_container_id,
                },
            )

            match_success = (
                response.get(MATCH_AND_CREATE_BI_WH_SRC_GQL_RESPONSE_FIELD, {})  # type: ignore
                .get(MATCHING_BI_WH_SRC_GQL_RESPONSE_FIELD, {})
                .get(MATCH_SUCCESSFUL_GQL_RESPONSE_FIELD, False)
            )

            if not match_success:
                print("Matching failed. Please create the BI warehouse sources manually.")
                print(json.dumps(response, indent=2))
                return

        print("Operation completed successfully.")
