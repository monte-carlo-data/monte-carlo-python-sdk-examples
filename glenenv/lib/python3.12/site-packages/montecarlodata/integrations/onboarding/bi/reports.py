from montecarlodata.common.common import read_as_base64
from montecarlodata.config import Config
from montecarlodata.errors import manage_errors
from montecarlodata.integrations.onboarding.base import BaseOnboardingService
from montecarlodata.integrations.onboarding.fields import (
    EXPECTED_ADD_BI_RESPONSE_FIELD,
    EXPECTED_LOOKER_GIT_CLONE_RESPONSE_FIELD,
    EXPECTED_LOOKER_METADATA_RESPONSE_FIELD,
    EXPECTED_POWER_BI_RESPONSE_FIELD,
    EXPECTED_TEST_TABLEAU_RESPONSE_FIELD,
    LOOKER_BI_TYPE,
    LOOKER_GIT_CLONE_CONNECTION_TYPE,
    LOOKER_MD_CONNECTION_TYPE,
    POWER_BI_BI_TYPE,
    POWER_BI_CONNECTION_TYPE,
    TABLEAU_BI_TYPE,
    TABLEAU_CONNECTION_TYPE,
)
from montecarlodata.queries.onboarding import (
    ADD_BI_CONNECTION_MUTATION,
    TEST_LOOKER_GIT_CLONE_CRED_MUTATION,
    TEST_LOOKER_METADATA_CRED_MUTATION,
    TEST_POWER_BI_CRED_MUTATION,
    TEST_TABLEAU_CRED_MUTATION,
)


class ReportsOnboardingService(BaseOnboardingService):
    def __init__(self, config: Config, **kwargs):
        super().__init__(config, **kwargs)

    @manage_errors
    def onboard_tableau(self, **kwargs) -> None:
        """
        Onboard a tableau connection
        """
        self.onboard(
            validation_query=TEST_TABLEAU_CRED_MUTATION,
            validation_response=EXPECTED_TEST_TABLEAU_RESPONSE_FIELD,
            connection_query=ADD_BI_CONNECTION_MUTATION,
            connection_response=EXPECTED_ADD_BI_RESPONSE_FIELD,
            connection_type=TABLEAU_CONNECTION_TYPE,
            warehouse_type=TABLEAU_BI_TYPE,
            **kwargs,
        )

    @manage_errors
    def onboard_looker_metadata(self, **kwargs) -> None:
        """
        Onboard a looker metadata connection
        """
        self.onboard(
            validation_query=TEST_LOOKER_METADATA_CRED_MUTATION,
            validation_response=EXPECTED_LOOKER_METADATA_RESPONSE_FIELD,
            connection_query=ADD_BI_CONNECTION_MUTATION,
            connection_response=EXPECTED_ADD_BI_RESPONSE_FIELD,
            connection_type=LOOKER_MD_CONNECTION_TYPE,
            warehouse_type=LOOKER_BI_TYPE,
            **kwargs,
        )

    @manage_errors
    def onboard_looker_git(self, **kwargs) -> None:
        """
        Onboard a looker git ssh connection
        """
        if kwargs.get("ssh_key"):
            kwargs["ssh_key"] = read_as_base64(kwargs.pop("ssh_key")).decode("utf-8")
        self.onboard(
            validation_query=TEST_LOOKER_GIT_CLONE_CRED_MUTATION,
            validation_response=EXPECTED_LOOKER_GIT_CLONE_RESPONSE_FIELD,
            connection_query=ADD_BI_CONNECTION_MUTATION,
            connection_response=EXPECTED_ADD_BI_RESPONSE_FIELD,
            connection_type=LOOKER_GIT_CLONE_CONNECTION_TYPE,
            warehouse_type=LOOKER_BI_TYPE,
            **kwargs,
        )

    @manage_errors
    def onboard_power_bi(self, **kwargs) -> None:
        """
        Onboard a Power BI connection
        """
        self.onboard(
            validation_query=TEST_POWER_BI_CRED_MUTATION,
            validation_response=EXPECTED_POWER_BI_RESPONSE_FIELD,
            connection_query=ADD_BI_CONNECTION_MUTATION,
            connection_response=EXPECTED_ADD_BI_RESPONSE_FIELD,
            connection_type=POWER_BI_CONNECTION_TYPE,
            warehouse_type=POWER_BI_BI_TYPE,
            **kwargs,
        )
