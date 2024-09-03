from montecarlodata.config import Config
from montecarlodata.errors import manage_errors
from montecarlodata.integrations.onboarding.base import BaseOnboardingService
from montecarlodata.integrations.onboarding.fields import (
    ATHENA_CONNECTION_TYPE,
    EXPECTED_ATHENA_GQL_RESPONSE_FIELD,
    EXPECTED_GLUE_GQL_RESPONSE_FIELD,
    GLUE_CONNECTION_TYPE,
)
from montecarlodata.queries.onboarding import (
    TEST_ATHENA_CRED_MUTATION,
    TEST_GLUE_CRED_MUTATION,
)


class GlueAthenaOnboardingService(BaseOnboardingService):
    def __init__(self, config: Config, **kwargs):
        super().__init__(config, **kwargs)

    @manage_errors
    def onboard_glue(self, **kwargs) -> None:
        """
        Onboard a glue connection by validating and adding a connection
        """
        self.onboard(
            validation_query=TEST_GLUE_CRED_MUTATION,
            validation_response=EXPECTED_GLUE_GQL_RESPONSE_FIELD,
            connection_type=GLUE_CONNECTION_TYPE,
            **kwargs,
        )

    @manage_errors
    def onboard_athena(self, **kwargs) -> None:
        """
        Onboard an athena connection by validating and adding a connection
        """
        self.onboard(
            validation_query=TEST_ATHENA_CRED_MUTATION,
            validation_response=EXPECTED_ATHENA_GQL_RESPONSE_FIELD,
            connection_type=ATHENA_CONNECTION_TYPE,
            **kwargs,
        )
