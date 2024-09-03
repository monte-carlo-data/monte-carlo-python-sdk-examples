import json
import time
from typing import Dict, Optional

from montecarlodata.iac.schemas import (
    ConfigTemplateDeleteResponse,
    ConfigTemplateUpdateAsyncResponse,
    ConfigTemplateUpdateState,
)
from montecarlodata.queries.iac import (
    CREATE_OR_UPDATE_MONTE_CARLO_CONFIG_TEMPLATE_ASYNC,
    DELETE_MONTE_CARLO_CONFIG_TEMPLATE,
    GET_MONTE_CARLO_CONFIG_TEMPLATE_UPDATE_STATE,
)
from montecarlodata.utils import GqlWrapper


class MonteCarloConfigTemplateClient:
    ASYNC_TIMEOUT_SECONDS = 60 * 15 * 2  # 30m (2x the lambda timeout of 15 minutes)

    def __init__(self, gql_wrapper: GqlWrapper):
        self._gql_wrapper = gql_wrapper

    def apply_config_template(
        self,
        namespace: str,
        config_template_as_dict: Dict,
        resource: Optional[str] = None,
        dry_run: bool = False,
        misconfigured_as_warning: bool = False,
        create_non_ingested_tables: bool = False,
    ) -> Optional[ConfigTemplateUpdateState]:
        response = self.apply_config_template_async(
            namespace=namespace,
            config_template_as_dict=config_template_as_dict,
            resource=resource,
            dry_run=dry_run,
            misconfigured_as_warning=misconfigured_as_warning,
            create_non_ingested_tables=create_non_ingested_tables,
        )

        if response.errors:
            return ConfigTemplateUpdateState(
                state="FAILED",
                resource_modifications=[],
                errors_as_json=response.errors_as_json,
                changes_applied=False,
            )
        update_uuid = response.update_uuid
        start = time.time()
        while time.time() - start < self.ASYNC_TIMEOUT_SECONDS:
            state = self.get_config_template_update_state(update_uuid)  # type: ignore

            if state.state != "PENDING":
                if response.warnings_as_json:
                    response_warnings = json.loads(response.warnings_as_json)
                    state_warnings = {}
                    if state.warnings_as_json:
                        state_warnings = json.loads(state.warnings_as_json)
                    state_warnings.update(response_warnings)
                    state.warnings_as_json = json.dumps(state_warnings)
                return state

            time.sleep(5)

    def apply_config_template_async(
        self,
        namespace: str,
        config_template_as_dict: Dict,
        resource: Optional[str] = None,
        dry_run: bool = False,
        misconfigured_as_warning: bool = False,
        create_non_ingested_tables: bool = False,
    ) -> ConfigTemplateUpdateAsyncResponse:
        response = self._gql_wrapper.make_request_v2(
            query=CREATE_OR_UPDATE_MONTE_CARLO_CONFIG_TEMPLATE_ASYNC,
            operation="createOrUpdateMonteCarloConfigTemplateAsync",
            variables=dict(
                namespace=namespace,
                configTemplateJson=json.dumps(config_template_as_dict),
                dryRun=dry_run,
                misconfiguredAsWarning=misconfigured_as_warning,
                resource=resource,
                createNonIngestedTables=create_non_ingested_tables,
            ),
        )
        return ConfigTemplateUpdateAsyncResponse.from_dict(response.data["response"])  # type: ignore

    def get_config_template_update_state(self, update_uuid: str) -> ConfigTemplateUpdateState:
        response = self._gql_wrapper.make_request_v2(
            query=GET_MONTE_CARLO_CONFIG_TEMPLATE_UPDATE_STATE,
            operation="getMonteCarloConfigTemplateUpdateState",
            variables=dict(updateUuid=update_uuid),
        )

        return ConfigTemplateUpdateState.from_dict(response.data)  # type: ignore

    def delete_config_template(
        self, namespace: str, dry_run: bool = False
    ) -> ConfigTemplateDeleteResponse:
        response = self._gql_wrapper.make_request_v2(
            query=DELETE_MONTE_CARLO_CONFIG_TEMPLATE,
            operation="deleteMonteCarloConfigTemplate",
            variables=dict(
                namespace=namespace,
                dryRun=dry_run,
            ),
        )

        return ConfigTemplateDeleteResponse.from_dict(response.data["response"])  # type: ignore
