from montecarlodata.common.common import read_as_base64
from montecarlodata.config import Config
from montecarlodata.errors import manage_errors
from montecarlodata.integrations.onboarding.base import BaseOnboardingService
from montecarlodata.integrations.onboarding.fields import (
    BQ_CONNECTION_TYPE,
    BQ_WAREHOUSE_TYPE,
    EXPECTED_BQ_GQL_RESPONSE_FIELD,
    EXPECTED_GENERIC_DB_GQL_RESPONSE_FIELD,
    EXPECTED_SNOWFLAKE_GQL_RESPONSE_FIELD,
    REDSHIFT_CONNECTION_TYPE,
    REDSHIFT_WAREHOUSE_TYPE,
    SNOWFLAKE_CONNECTION_TYPE,
    SNOWFLAKE_WAREHOUSE_TYPE,
)
from montecarlodata.queries.onboarding import (
    TEST_BQ_CRED_MUTATION,
    TEST_DATABASE_CRED_MUTATION,
    TEST_SNOWFLAKE_CRED_MUTATION,
)


class WarehouseOnboardingService(BaseOnboardingService):
    def __init__(self, config: Config, **kwargs):
        super().__init__(config, **kwargs)

    @manage_errors
    def onboard_redshift(self, **kwargs) -> None:
        """
        Onboard a redshift connection by validating and adding a connection.
        """
        kwargs["connectionType"] = REDSHIFT_CONNECTION_TYPE
        kwargs["warehouseType"] = REDSHIFT_WAREHOUSE_TYPE
        self.onboard(
            validation_query=TEST_DATABASE_CRED_MUTATION,
            validation_response=EXPECTED_GENERIC_DB_GQL_RESPONSE_FIELD,
            connection_type=REDSHIFT_CONNECTION_TYPE,
            **kwargs,
        )

    @manage_errors
    def onboard_snowflake(self, **kwargs) -> None:
        """
        Onboard a snowflake connection by validating and adding a connection.
        """
        kwargs["warehouseType"] = SNOWFLAKE_WAREHOUSE_TYPE
        if kwargs.get("private_key"):
            kwargs["private_key"] = read_as_base64(kwargs.pop("private_key")).decode("utf-8")
        self.onboard(
            validation_query=TEST_SNOWFLAKE_CRED_MUTATION,
            validation_response=EXPECTED_SNOWFLAKE_GQL_RESPONSE_FIELD,
            connection_type=SNOWFLAKE_CONNECTION_TYPE,
            **kwargs,
        )

    @manage_errors
    def onboard_bq(self, **kwargs) -> None:
        """
        Onboard a BigQuery connection by validating and adding a connection.

        Reads and encodes service file as base64.
        """
        kwargs["warehouseType"] = BQ_WAREHOUSE_TYPE
        kwargs["serviceJson"] = read_as_base64(kwargs.pop("ServiceFile")).decode("utf-8")
        self.onboard(
            validation_query=TEST_BQ_CRED_MUTATION,
            validation_response=EXPECTED_BQ_GQL_RESPONSE_FIELD,
            connection_type=BQ_CONNECTION_TYPE,
            **kwargs,
        )
