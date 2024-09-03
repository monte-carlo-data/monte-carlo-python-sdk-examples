from montecarlodata.config import Config
from montecarlodata.errors import manage_errors
from montecarlodata.integrations.onboarding.base import BaseOnboardingService
from montecarlodata.integrations.onboarding.fields import (
    EXPECTED_ADD_ETL_CONNECTION_RESPONSE_FIELD,
    EXPECTED_FIVETRAN_RESPONSE_FIELD,
    FIVETRAN_CONNECTION_TYPE,
)
from montecarlodata.queries.onboarding import (
    ADD_ETL_CONNECTION_MUTATION,
    TEST_FIVETRAN_CRED_MUTATION,
)


class FivetranOnboardingService(BaseOnboardingService):
    def __init__(self, config: Config, **kwargs):
        super().__init__(config, **kwargs)

    @manage_errors
    def onboard_fivetran(self, **kwargs) -> None:
        self.onboard(
            validation_query=TEST_FIVETRAN_CRED_MUTATION,
            validation_response=EXPECTED_FIVETRAN_RESPONSE_FIELD,
            connection_query=ADD_ETL_CONNECTION_MUTATION,
            connection_response=EXPECTED_ADD_ETL_CONNECTION_RESPONSE_FIELD,
            connection_type=FIVETRAN_CONNECTION_TYPE,
            **kwargs,
        )
