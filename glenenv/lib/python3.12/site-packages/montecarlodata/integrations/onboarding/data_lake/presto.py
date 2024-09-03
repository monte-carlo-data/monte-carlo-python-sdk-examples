from montecarlodata.config import Config
from montecarlodata.errors import manage_errors
from montecarlodata.integrations.onboarding.base import BaseOnboardingService
from montecarlodata.integrations.onboarding.fields import (
    EXPECTED_PRESTO_S3_GQL_RESPONSE_FIELD,
    EXPECTED_PRESTO_SQL_GQL_RESPONSE_FIELD,
    PRESTO_CERT_PREFIX,
    PRESTO_S3_CONNECTION_TYPE,
    PRESTO_SQL_CONNECTION_TYPE,
    QL_JOB_TYPE,
)
from montecarlodata.queries.onboarding import (
    TEST_PRESTO_CRED_MUTATION,
    TEST_S3_CRED_MUTATION,
)


class PrestoOnboardingService(BaseOnboardingService):
    def __init__(self, config: Config, **kwargs):
        super().__init__(config, **kwargs)

    @manage_errors
    def onboard_presto_sql(self, **kwargs) -> None:
        """
        Onboard a presto-sql connection by validating and adding a connection.
        Also, optionally uploads a certificate to the DC bucket.
        """
        self.handle_cert(cert_prefix=PRESTO_CERT_PREFIX, options=kwargs)
        self.onboard(
            validation_query=TEST_PRESTO_CRED_MUTATION,
            validation_response=EXPECTED_PRESTO_SQL_GQL_RESPONSE_FIELD,
            connection_type=PRESTO_SQL_CONNECTION_TYPE,
            **kwargs,
        )

    @manage_errors
    def onboard_presto_s3(self, **kwargs) -> None:
        """
        Onboard a presto-s3 connection by validating and adding a connection
        """
        kwargs["connectionType"] = PRESTO_S3_CONNECTION_TYPE
        self.onboard(
            validation_query=TEST_S3_CRED_MUTATION,
            validation_response=EXPECTED_PRESTO_S3_GQL_RESPONSE_FIELD,
            connection_type=PRESTO_S3_CONNECTION_TYPE,
            job_types=QL_JOB_TYPE,
            **kwargs,
        )
