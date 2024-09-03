from montecarlodata.config import Config
from montecarlodata.errors import manage_errors
from montecarlodata.integrations.onboarding.base import BaseOnboardingService
from montecarlodata.integrations.onboarding.fields import (
    DBT_CLOUD_CONNECTION_TYPE,
    DBT_CLOUD_WEBHOOK_CONNECTION_TYPE,
    EXPECTED_ADD_CONNECTION_RESPONSE_FIELD,
    EXPECTED_DBT_CLOUD_RESPONSE_FIELD,
)
from montecarlodata.queries.onboarding import (
    ADD_CONNECTION_MUTATION,
    TEST_DBT_CLOUD_CRED_MUTATION,
)


class DbtCloudOnboardingService(BaseOnboardingService):
    def __init__(self, config: Config, **kwargs):
        super().__init__(config, **kwargs)

    @manage_errors
    def onboard_dbt_cloud(self, **kwargs) -> None:
        connection_type = DBT_CLOUD_CONNECTION_TYPE

        hmac_secret = kwargs.pop("webhook_hmac_secret", None)
        webhook_id = kwargs.pop("webhook_id", None)
        if hmac_secret is not None:
            connection_type = DBT_CLOUD_WEBHOOK_CONNECTION_TYPE
            kwargs["dbt_cloud_webhook_hmac_secret"] = hmac_secret
        if webhook_id is not None:
            kwargs["dbt_cloud_webhook_id"] = webhook_id

        self.onboard(
            validation_query=TEST_DBT_CLOUD_CRED_MUTATION,
            validation_response=EXPECTED_DBT_CLOUD_RESPONSE_FIELD,
            connection_query=ADD_CONNECTION_MUTATION,
            connection_response=EXPECTED_ADD_CONNECTION_RESPONSE_FIELD,
            connection_type=connection_type,
            **kwargs,
        )
