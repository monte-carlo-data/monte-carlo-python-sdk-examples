import os

import click
from pycarlo.core import Client, Mutation, Query
from tabulate import tabulate

from montecarlodata.common.data import OnboardingConfiguration
from montecarlodata.config import Config
from montecarlodata.errors import manage_errors
from montecarlodata.integrations.onboarding.base import BaseOnboardingService
from montecarlodata.integrations.onboarding.fields import (
    DATA_LAKE_WAREHOUSE_TYPE,
    DATABRICKS_DELTA_CONNECTION_TYPE,
    EXPECTED_DATABRICKS_GQL_RESPONSE_FIELD,
    EXPECTED_DATABRICKS_SQL_WAREHOUSE_GQL_RESPONSE_FIELD,
)
from montecarlodata.queries.onboarding import (
    TEST_DATABRICKS_CRED_MUTATION,
    DatabricksSqlWarehouseOnboardingQueries,
)

DEFAULT_GATEWAY_URL = os.getenv(
    "MC_INTEGRATION_GATEWAY_URL",
    "https://integrations.getmontecarlo.com/databricks/metadata",
)
# Secret scope and name defined in the data collector:
# https://github.com/monte-carlo-data/data-collector/blob/e5db61eacd10e0388acd9ff8a32fe60644b36797/lambdas/api_lambdas/execute_databricks_actions.py#L61  # noqa
DEFAULT_SECRET_SCOPE = "monte-carlo-collector-gateway-scope"
DEFAULT_SECRET_NAME = "monte-carlo-collector-gateway-secret"


class DatabricksOnboardingService(BaseOnboardingService):
    _DATABRICKS_CONFIG_KEYS = [
        "databricks_workspace_url",
        "databricks_workspace_id",
        "databricks_cluster_id",
        "databricks_token",
    ]

    def __init__(self, config: Config, mc_client: Client, **kwargs):
        super().__init__(config, **kwargs)
        self._mc_client = mc_client

    @manage_errors
    def onboard_databricks_metastore(self, connection_type, **kwargs) -> None:
        """
        Onboard a databricks metastore connection
        """

        validation_query = Query()
        validation_query.validate_connection_type(
            warehouse_type=DATA_LAKE_WAREHOUSE_TYPE, connection_type=connection_type
        )
        self._mc_client(validation_query)

        databricks_config = {key: kwargs[key] for key in self._DATABRICKS_CONFIG_KEYS}

        onboarding_config = OnboardingConfiguration(connection_type=connection_type, **kwargs)
        connection_options = self._build_connection_options(
            **onboarding_config.connection_options,  # type: ignore
        )

        # Create secret
        if not kwargs.get("skip_secret_creation", False):
            mutation = Mutation()
            mutation.create_databricks_secret(
                databricks_config=databricks_config,
                secret_name=kwargs["databricks_secret_key"],
                scope_name=kwargs["databricks_secret_scope"],
                connection_options=connection_options.monolith_connection_payload,
            )
            self._mc_client(mutation)

        # Create notebook
        if not kwargs.get("skip_notebook_creation", False):
            mutation = Mutation()
            mutation.create_databricks_notebook_job(
                databricks_config=databricks_config,
                connection_options=connection_options.monolith_connection_payload,
            )
            resp = self._mc_client(mutation)
            databricks_job_config = resp.create_databricks_notebook_job.databricks
            job_info = dict(
                databricks_job_id=databricks_job_config.workspace_job_id,
                databricks_job_name=databricks_job_config.workspace_job_name,
                databricks_notebook_path=databricks_job_config.workspace_notebook_path,
                databricks_notebook_source=databricks_job_config.notebook_source,
                databricks_notebook_version=databricks_job_config.notebook_version,
            )
        else:
            job_info = dict(
                databricks_job_id=kwargs.pop("databricks_job_id"),
                databricks_job_name=kwargs.pop("databricks_job_name"),
                databricks_notebook_path=kwargs.pop("databricks_notebook_path"),
                databricks_notebook_source=kwargs.pop("databricks_notebook_source"),
            )

        project_info = {}

        if connection_type == DATABRICKS_DELTA_CONNECTION_TYPE:
            project_query = Query()
            project_query.get_projects(
                warehouse_type=DATA_LAKE_WAREHOUSE_TYPE,
            )
            project_result = self._mc_client(project_query)

            project_id = project_result.get_projects.projects[0]
            project_info = {"project_id": project_id}

        job_limits = {
            **dict(
                integration_gateway=dict(
                    gateway_url=DEFAULT_GATEWAY_URL,
                    databricks_secret_key=kwargs["databricks_secret_key"],
                    databricks_secret_scope=kwargs["databricks_secret_scope"],
                )
            ),
            **project_info,
            **job_info,
        }

        # Onboard
        self.onboard(
            validation_query=TEST_DATABRICKS_CRED_MUTATION,
            validation_response=EXPECTED_DATABRICKS_GQL_RESPONSE_FIELD,
            connection_type=connection_type,
            job_limits=job_limits,
            **kwargs,
        )

    @manage_errors
    def update_databricks_notebook(self, **kwargs):
        mutation = Mutation()
        mutation.update_databricks_notebook(**kwargs)
        resp = self._mc_client(mutation)
        click.echo(resp.update_databricks_notebook)

    @manage_errors
    def get_databricks_job_info(self, **kwargs):
        query = Query()
        query.get_databricks_metadata_job_info(**kwargs)
        resp = self._mc_client(query)
        attributes = [
            "workspace_job_id",
            "workspace_job_name",
            "workspace_notebook_path",
            "notebook_source",
            "notebook_version",
        ]
        info_list = [
            {attribute: getattr(info, attribute) for attribute in attributes}
            for info in resp.get_databricks_metadata_job_info
        ]
        click.echo(tabulate(info_list, headers="keys", maxcolwidths=100))

    @manage_errors
    def get_current_databricks_notebook_version(self):
        query = Query()
        query.get_current_databricks_notebook_version()
        resp = self._mc_client(query)
        click.echo(resp.get_current_databricks_notebook_version)

    @manage_errors
    def onboard_databricks_sql_warehouse(self, **kwargs):
        # Onboard
        self.onboard(
            validation_query=DatabricksSqlWarehouseOnboardingQueries.test_credentials.query,
            validation_response=EXPECTED_DATABRICKS_SQL_WAREHOUSE_GQL_RESPONSE_FIELD,
            **kwargs,
        )
