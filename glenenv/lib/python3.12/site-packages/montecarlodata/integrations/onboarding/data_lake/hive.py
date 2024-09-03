from montecarlodata.config import Config
from montecarlodata.errors import manage_errors
from montecarlodata.integrations.onboarding.base import BaseOnboardingService
from montecarlodata.integrations.onboarding.fields import (
    AWS_RDS_CA_CERT,
    EXPECTED_GENERIC_DB_GQL_RESPONSE_FIELD,
    EXPECTED_HIVE_S3_GQL_RESPONSE_FIELD,
    EXPECTED_HIVE_SQL_GQL_RESPONSE_FIELD,
    HIVE_GET_PARTS_KEY,
    HIVE_MAX_PARTS_DEFAULT_VALUE,
    HIVE_MAX_PARTS_KEY,
    HIVE_MYSQL_CONNECTION_TYPE,
    HIVE_S3_CONNECTION_TYPE,
    HIVE_SQL_CONNECTION_TYPE,
    PRESTO_CATALOG_KEY,
    QL_JOB_TYPE,
)
from montecarlodata.queries.onboarding import (
    TEST_DATABASE_CRED_MUTATION,
    TEST_HIVE_SQL_CRED_MUTATION,
    TEST_S3_CRED_MUTATION,
)


class HiveOnboardingService(BaseOnboardingService):
    def __init__(self, config: Config, **kwargs):
        super().__init__(config, **kwargs)

    @manage_errors
    def onboard_hive_mysql(self, **kwargs) -> None:
        """
        Onboard a hive-mysql connection by validating and adding a connection
        """
        if kwargs["use_ssl"]:
            kwargs["ssl_options"] = {"ca": AWS_RDS_CA_CERT}
            kwargs.pop("use_ssl")

        catalog = kwargs.pop("catalog", None)
        job_limits = {
            HIVE_GET_PARTS_KEY: True,
            HIVE_MAX_PARTS_KEY: HIVE_MAX_PARTS_DEFAULT_VALUE,
        }

        if catalog:
            job_limits[PRESTO_CATALOG_KEY] = catalog

        kwargs["connectionType"] = HIVE_MYSQL_CONNECTION_TYPE
        self.onboard(
            validation_query=TEST_DATABASE_CRED_MUTATION,
            validation_response=EXPECTED_GENERIC_DB_GQL_RESPONSE_FIELD,
            connection_type=HIVE_MYSQL_CONNECTION_TYPE,
            job_limits=job_limits,
            **kwargs,
        )

    @manage_errors
    def onboard_hive_s3(self, **kwargs) -> None:
        """
        Onboard a presto-s3 connection by validating and adding a connection
        """
        kwargs["connectionType"] = HIVE_S3_CONNECTION_TYPE
        self.onboard(
            validation_query=TEST_S3_CRED_MUTATION,
            validation_response=EXPECTED_HIVE_S3_GQL_RESPONSE_FIELD,
            connection_type=HIVE_S3_CONNECTION_TYPE,
            job_types=QL_JOB_TYPE,
            **kwargs,
        )

    @manage_errors
    def onboard_hive_sql(self, **kwargs) -> None:
        """
        Onboard a hive-sql connection by validating and adding a connection
        """
        self.onboard(
            validation_query=TEST_HIVE_SQL_CRED_MUTATION,
            validation_response=EXPECTED_HIVE_SQL_GQL_RESPONSE_FIELD,
            connection_type=HIVE_SQL_CONNECTION_TYPE,
            **kwargs,
        )
