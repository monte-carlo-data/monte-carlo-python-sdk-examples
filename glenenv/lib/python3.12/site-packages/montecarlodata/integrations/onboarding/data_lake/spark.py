from montecarlodata.config import Config
from montecarlodata.errors import manage_errors
from montecarlodata.integrations.onboarding.base import BaseOnboardingService
from montecarlodata.integrations.onboarding.fields import (
    EXPECTED_SPARK_GQL_RESPONSE_FIELD,
    SPARK_CONNECTION_TYPE,
)
from montecarlodata.queries.onboarding import (
    TEST_SPARK_BINARY_MODE_CRED_MUTATION,
    TEST_SPARK_DATABRICKS_CRED_MUTATION,
    TEST_SPARK_HTTP_MODE_CRED_MUTATION,
)

SPARK_BINARY_MODE_CONFIG_TYPE = "binary"
SPARK_HTTP_MODE_CONFIG_TYPE = "http"
SPARK_DATABRICKS_CONFIG_TYPE = "databricks"


class SparkOnboardingService(BaseOnboardingService):
    _MUTATIONS = {
        SPARK_BINARY_MODE_CONFIG_TYPE: TEST_SPARK_BINARY_MODE_CRED_MUTATION,
        SPARK_HTTP_MODE_CONFIG_TYPE: TEST_SPARK_HTTP_MODE_CRED_MUTATION,
        SPARK_DATABRICKS_CONFIG_TYPE: TEST_SPARK_DATABRICKS_CRED_MUTATION,
    }

    def __init__(self, config: Config, **kwargs):
        super().__init__(config, **kwargs)

    @manage_errors
    def onboard_spark(self, config_type: str, **kwargs) -> None:
        """
        Onboard a spark connection by validating and adding a connection
        """
        validation_query = self._MUTATIONS[config_type]
        kwargs["connectionType"] = SPARK_CONNECTION_TYPE
        self.onboard(
            validation_query=validation_query,
            validation_response=EXPECTED_SPARK_GQL_RESPONSE_FIELD,
            connection_type=SPARK_CONNECTION_TYPE,
            **kwargs,
        )
