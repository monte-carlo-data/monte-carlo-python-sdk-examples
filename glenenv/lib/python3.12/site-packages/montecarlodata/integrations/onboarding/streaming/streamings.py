import uuid
from copy import deepcopy
from typing import Dict, Optional

import click
from box import Box
from pycarlo.core import Client

from montecarlodata.collector.validation import CollectorValidationService
from montecarlodata.config import Config
from montecarlodata.errors import echo_error, manage_errors
from montecarlodata.integrations.onboarding.base import BaseOnboardingService
from montecarlodata.integrations.onboarding.fields import (
    CONFLUENT_KAFKA_CLUSTER_TYPE,
    CONFLUENT_KAFKA_CONNECT_CLUSTER_TYPE,
    CONFLUENT_KAFKA_CONNECT_CONNECTION_TYPE,
    CONFLUENT_KAFKA_CONNECTION_TYPE,
    EXPECTED_ADD_CONFLUENT_CLUSTER_CONNECTION_RESPONSE_FIELD,
    EXPECTED_ADD_STREAMING_SYSTEM_RESPONSE_FIELD,
    EXPECTED_GET_STREAMING_SYSTEMS_RESPONSE_FIELD,
    MSK_KAFKA_CLUSTER_TYPE,
    MSK_KAFKA_CONNECT_CLUSTER_TYPE,
    MSK_KAFKA_CONNECT_CONNECTION_TYPE,
    MSK_KAFKA_CONNECTION_TYPE,
    SELF_HOSTED_KAFKA_CLUSTER_TYPE,
    SELF_HOSTED_KAFKA_CONNECT_CLUSTER_TYPE,
    SELF_HOSTED_KAFKA_CONNECT_CONNECTION_TYPE,
    SELF_HOSTED_KAFKA_CONNECTION_TYPE,
)
from montecarlodata.queries.onboarding import (
    ADD_STREAMING_CLUSTER_CONNECTION_MUTATION,
    ADD_STREAMING_SYSTEM_MUTATION,
    GET_STREAMING_SYSTEMS_QUERY,
    TEST_CONFLUENT_KAFKA_CONNECT_CRED_MUTATION,
    TEST_CONFLUENT_KAFKA_CRED_MUTATION,
    TEST_MSK_KAFKA_CONNECT_CRED_MUTATION,
    TEST_MSK_KAFKA_CRED_MUTATION,
    TEST_SELF_HOSTED_KAFKA_CONNECT_CRED_MUTATION,
    TEST_SELF_HOSTED_KAFKA_CRED_MUTATION,
)


class StreamingOnboardingService(BaseOnboardingService):
    _CONNECTION_TYPES_TO_CREDS_MUTATIONS_MAPPING = {
        CONFLUENT_KAFKA_CONNECTION_TYPE: TEST_CONFLUENT_KAFKA_CRED_MUTATION,
        CONFLUENT_KAFKA_CONNECT_CONNECTION_TYPE: TEST_CONFLUENT_KAFKA_CONNECT_CRED_MUTATION,
        MSK_KAFKA_CONNECTION_TYPE: TEST_MSK_KAFKA_CRED_MUTATION,
        MSK_KAFKA_CONNECT_CONNECTION_TYPE: TEST_MSK_KAFKA_CONNECT_CRED_MUTATION,
        SELF_HOSTED_KAFKA_CONNECTION_TYPE: TEST_SELF_HOSTED_KAFKA_CRED_MUTATION,
        SELF_HOSTED_KAFKA_CONNECT_CONNECTION_TYPE: TEST_SELF_HOSTED_KAFKA_CONNECT_CRED_MUTATION,
    }

    _CONNECTION_TYPES_TO_CLUSTER_TYPE_MAPPING = {
        CONFLUENT_KAFKA_CONNECTION_TYPE: CONFLUENT_KAFKA_CLUSTER_TYPE,
        CONFLUENT_KAFKA_CONNECT_CONNECTION_TYPE: CONFLUENT_KAFKA_CONNECT_CLUSTER_TYPE,
        MSK_KAFKA_CONNECTION_TYPE: MSK_KAFKA_CLUSTER_TYPE,
        MSK_KAFKA_CONNECT_CONNECTION_TYPE: MSK_KAFKA_CONNECT_CLUSTER_TYPE,
        SELF_HOSTED_KAFKA_CONNECTION_TYPE: SELF_HOSTED_KAFKA_CLUSTER_TYPE,
        SELF_HOSTED_KAFKA_CONNECT_CONNECTION_TYPE: SELF_HOSTED_KAFKA_CONNECT_CLUSTER_TYPE,
    }

    def __init__(self, config: Config, mc_client: Client, **kwargs):
        super().__init__(config, **kwargs)
        self._validation_service = CollectorValidationService(
            config=config,
            mc_client=mc_client,
            user_service=self._user_service,
            request_wrapper=self._request_wrapper,
        )

    @manage_errors
    def create_streaming_system(
        self,
        streaming_system_type: str,
        streaming_system_name: str,
        dc_id: Optional[str] = None,
    ) -> None:
        """
        Creates a streaming system.
        """
        response = self._request_wrapper.make_request_v2(
            query=ADD_STREAMING_SYSTEM_MUTATION,
            operation=EXPECTED_ADD_STREAMING_SYSTEM_RESPONSE_FIELD,
            variables=dict(
                streaming_system_type=streaming_system_type,
                streaming_system_name=streaming_system_name,
                dc_id=dc_id,
            ),
        )
        streaming_system = response.data.streamingSystem  # type: ignore
        click.echo(
            f"Successfully created the {streaming_system.type} streaming system "
            f"{streaming_system.name} with uuid: {streaming_system.uuid}."
        )

    def get_streaming_system(self, streaming_system_uuid: str) -> Optional[Box]:
        """
        Gets a streaming system.
        """
        response = self._request_wrapper.make_request_v2(
            query=GET_STREAMING_SYSTEMS_QUERY,
            operation=EXPECTED_GET_STREAMING_SYSTEMS_RESPONSE_FIELD,
            variables=dict(
                include_clusters=False,
            ),
        )
        for entry in response.data:  # type: ignore
            system = entry.system
            if system.uuid == streaming_system_uuid:
                return system

        echo_error(f"cannot find the stream system with uuid {streaming_system_uuid}")

    @manage_errors
    def test_new_confluent_kafka_credentials(
        self,
        cluster: str,
        api_key: str,
        secret: str,
        url: str,
        dc_id: Optional[uuid.UUID] = None,
    ) -> Optional[str]:
        return self._validation_service.test_new_credentials(
            connection_type=CONFLUENT_KAFKA_CONNECTION_TYPE,
            dc_id=dc_id,
            cluster=cluster,
            api_key=api_key,
            secret=secret,
            url=url,
        )

    @manage_errors
    def test_new_confluent_kafka_connect_credentials(
        self,
        confluent_env: str,
        cluster: str,
        api_key: str,
        secret: str,
        url: Optional[str] = None,
        dc_id: Optional[uuid.UUID] = None,
    ) -> Optional[str]:
        return self._validation_service.test_new_credentials(
            dc_id=dc_id,
            connection_type=CONFLUENT_KAFKA_CONNECT_CONNECTION_TYPE,
            confluent_env=confluent_env,
            cluster=cluster,
            api_key=api_key,
            secret=secret,
            url=url,
        )

    @manage_errors
    def test_new_msk_kafka_credentials(
        self,
        cluster: str,
        url: str,
        auth_type: str,
        auth_token: Optional[str] = None,
        dc_id: Optional[uuid.UUID] = None,
    ) -> Optional[str]:
        return self._validation_service.test_new_credentials(
            dc_id=dc_id,
            connection_type=MSK_KAFKA_CONNECTION_TYPE,
            cluster=cluster,
            auth_type=auth_type,
            auth_token=auth_token,
            url=url,
        )

    @manage_errors
    def test_new_msk_kafka_connect_credentials(
        self,
        cluster_arn: str,
        iam_role_arn: str,
        external_id: Optional[str] = None,
        dc_id: Optional[uuid.UUID] = None,
    ) -> Optional[str]:
        return self._validation_service.test_new_credentials(
            dc_id=dc_id,
            connection_type=MSK_KAFKA_CONNECT_CONNECTION_TYPE,
            cluster_arn=cluster_arn,
            iam_role_arn=iam_role_arn,
            external_id=external_id,
        )

    @manage_errors
    def test_new_self_hosted_kafka_credentials(
        self,
        cluster: str,
        url: str,
        auth_type: str,
        auth_token: Optional[str] = None,
        dc_id: Optional[uuid.UUID] = None,
    ) -> Optional[str]:
        return self._validation_service.test_new_credentials(
            dc_id=dc_id,
            connection_type=SELF_HOSTED_KAFKA_CONNECTION_TYPE,
            cluster=cluster,
            auth_type=auth_type,
            auth_token=auth_token,
            url=url,
        )

    @manage_errors
    def test_new_self_hosted_kafka_connect_credentials(
        self,
        cluster: str,
        url: str,
        auth_type: str,
        auth_token: Optional[str] = None,
        dc_id: Optional[uuid.UUID] = None,
    ) -> Optional[str]:
        return self._validation_service.test_new_credentials(
            dc_id=dc_id,
            connection_type=SELF_HOSTED_KAFKA_CONNECT_CONNECTION_TYPE,
            cluster=cluster,
            auth_type=auth_type,
            auth_token=auth_token,
            url=url,
        )

    @manage_errors
    def onboard_streaming_cluster_connection(self, **kwargs) -> None:
        connection_type = kwargs.get("connection_type")
        if connection_type not in self._CONNECTION_TYPES_TO_CREDS_MUTATIONS_MAPPING.keys():
            echo_error("Not supported streaming connection type")
            click.Abort()

        streaming_system_id = kwargs.get("streaming_system_id")
        streaming_system = None
        if streaming_system_id:
            # if the streaming system exist, then we'll proceed, otherwise, we'd stop.
            # that's because either the streaming system doesn't exist or it doesn't
            # belong to the user's account.
            streaming_system = self.get_streaming_system(streaming_system_uuid=streaming_system_id)

        credential_key = kwargs.get("key")
        if not credential_key:
            # copy params for creating credential key
            params_copy = deepcopy(kwargs)
            if not params_copy.get("dc_id") and params_copy.get("streaming_system_id"):
                params_copy["dc_id"] = streaming_system.dcId  # type: ignore
            credential_key = self._test_and_generate_credential_key(
                connection_type=connection_type,  # type: ignore
                params=params_copy,
            )
            if credential_key:
                kwargs["key"] = credential_key
            else:
                # credentials testing fail or user doesn't want to continue
                click.Abort()

        if not kwargs.get("mc_cluster_id"):
            # for new cluster, we'll add the cluster type here.
            kwargs["new_cluster_type"] = self._CONNECTION_TYPES_TO_CLUSTER_TYPE_MAPPING[
                connection_type
            ]  # type: ignore

        response = self._request_wrapper.make_request_v2(
            query=ADD_STREAMING_CLUSTER_CONNECTION_MUTATION,
            operation=EXPECTED_ADD_CONFLUENT_CLUSTER_CONNECTION_RESPONSE_FIELD,
            variables=kwargs,
        )

        connection = response.data.connection  # type: ignore
        click.echo(
            f"Successfully created the {connection.type} connection {connection.uuid} for cluster "
            f"{connection.streamingCluster.uuid}, in streaming system "
            f"{connection.streamingCluster.streamingSystemUuid}."
        )

    def _test_and_generate_credential_key(
        self,
        connection_type: str,
        params: Dict,
    ) -> Optional[str]:
        parameters: Dict = {
            "cluster": params.get("new_cluster_id"),
        }
        if connection_type == CONFLUENT_KAFKA_CONNECTION_TYPE:
            parameters["url"] = params.get("url")
            parameters["api_key"] = params.get("api_key")
            parameters["secret"] = params.get("secret")
        elif connection_type == CONFLUENT_KAFKA_CONNECT_CONNECTION_TYPE:
            parameters["confluent_env"] = params.get("confluent_env")
            parameters["api_key"] = params.get("api_key")
            parameters["secret"] = params.get("secret")
            if "url" in params:
                parameters["url"] = params.get("url")
        elif connection_type == MSK_KAFKA_CONNECT_CONNECTION_TYPE:
            parameters["cluster_arn"] = params.get("cluster_arn")
            parameters["iam_role_arn"] = params.get("iam_role_arn")
            if "external_id" in params:
                parameters["external_id"] = params.get("external_id")
        elif connection_type in [
            MSK_KAFKA_CONNECTION_TYPE,
            SELF_HOSTED_KAFKA_CONNECTION_TYPE,
            SELF_HOSTED_KAFKA_CONNECT_CONNECTION_TYPE,
        ]:
            parameters["url"] = params.get("url")
            parameters["auth_type"] = params.get("auth_type")
            parameters["auth_token"] = params.get("auth_token")
        else:
            click.echo(f"Not supported connection type {connection_type}")

        return self._validation_service.test_new_credentials(
            connection_type=connection_type,
            dc_id=params.get("dc_id"),
            **parameters,
        )
