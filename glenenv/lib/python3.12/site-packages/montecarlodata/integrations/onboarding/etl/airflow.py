from montecarlodata.config import Config
from montecarlodata.errors import manage_errors
from montecarlodata.integrations.onboarding.base import BaseOnboardingService
from montecarlodata.integrations.onboarding.fields import (
    AIRFLOW_CONNECTION_TYPE,
    EXPECTED_ADD_ETL_CONNECTION_RESPONSE_FIELD,
    EXPECTED_TEST_AIRFLOW_RESPONSE_FIELD,
)
from montecarlodata.queries.onboarding import (
    ADD_ETL_CONNECTION_MUTATION,
    TEST_AIRFLOW_CRED_MUTATION,
)


class AirflowOnboardingService(BaseOnboardingService):
    def __init__(self, config: Config, **kwargs):
        super().__init__(config, **kwargs)

    @manage_errors
    def onboard_airflow(self, **kwargs) -> None:
        self.onboard(
            validation_query=TEST_AIRFLOW_CRED_MUTATION,
            validation_response=EXPECTED_TEST_AIRFLOW_RESPONSE_FIELD,
            connection_query=ADD_ETL_CONNECTION_MUTATION,
            connection_response=EXPECTED_ADD_ETL_CONNECTION_RESPONSE_FIELD,
            connection_type=AIRFLOW_CONNECTION_TYPE,
            **kwargs,
        )
