import click
from pycarlo.core import Client

from montecarlodata.collector.validation import CollectorValidationService
from montecarlodata.config import Config
from montecarlodata.errors import manage_errors
from montecarlodata.integrations.onboarding.base import BaseOnboardingService
from montecarlodata.integrations.onboarding.fields import (
    EXPECTED_ADD_CONNECTION_RESPONSE_FIELD,
    PINECONE_CONNECTION_TYPE,
    PINECONE_WAREHOUSE_TYPE,
)
from montecarlodata.queries.onboarding import (
    ADD_CONNECTION_MUTATION,
)


class VectorDbOnboardingService(BaseOnboardingService):
    def __init__(self, config: Config, mc_client: Client, **kwargs):
        super().__init__(config, **kwargs)
        self._validation_service = CollectorValidationService(
            config=config,
            mc_client=mc_client,
            user_service=self._user_service,
            request_wrapper=self._request_wrapper,
        )

    @manage_errors
    def onboard_pinecone(self, **kwargs) -> None:
        credentials_key = self._validation_service.test_new_credentials(
            connection_type=PINECONE_CONNECTION_TYPE,
            **kwargs,
        )

        environment = kwargs.get("environment")
        project_id = kwargs.get("project_id")
        name = kwargs.get("name") or f"{environment}:{project_id}"

        variables = {
            "connection_type": PINECONE_CONNECTION_TYPE,
            "create_warehouse_type": PINECONE_WAREHOUSE_TYPE,
            "dc_id": kwargs.get("dc_id"),
            "key": credentials_key,
            "name": name,
        }

        response = self._request_wrapper.make_request_v2(
            query=ADD_CONNECTION_MUTATION,
            operation=EXPECTED_ADD_CONNECTION_RESPONSE_FIELD,
            variables=variables,
        )

        click.echo(f"Successfully created connection (id={response.data.connection.uuid})")  # type: ignore
