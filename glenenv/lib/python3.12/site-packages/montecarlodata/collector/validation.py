import copy
import json
import re
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple, Union

import click
from box import Box
from pycarlo.common.errors import GqlError
from pycarlo.core import Client, Query
from pycarlo.lib.schema import TestCredentialsV2Response

from montecarlodata.common.echo_utils import (
    echo_error_message,
    echo_success_message,
    echo_warning_message,
    styled_error_icon,
    styled_success_icon,
    styled_warning_icon,
)
from montecarlodata.common.user import UserService
from montecarlodata.config import Config
from montecarlodata.errors import complain_and_abort, prompt_connection
from montecarlodata.integrations.onboarding.fields import (
    AZURE_DEDICATED_SQL_POOL_TYPE,
    AZURE_SQL_DATABASE_TYPE,
    CONFLUENT_KAFKA_CONNECT_CONNECTION_TYPE,
    CONFLUENT_KAFKA_CONNECTION_TYPE,
    EXPECTED_TEST_CONFLUENT_KAFKA_CONNECT_CRED_RESPONSE_FIELD,
    EXPECTED_TEST_CONFLUENT_KAFKA_CRED_RESPONSE_FIELD,
    EXPECTED_TEST_MSK_KAFKA_CONNECT_CRED_RESPONSE_FIELD,
    EXPECTED_TEST_MSK_KAFKA_CRED_RESPONSE_FIELD,
    EXPECTED_TEST_PINECONE_CREDENTIALS_RESPONSE_FIELD,
    EXPECTED_TEST_SELF_HOSTED_KAFKA_CONNECT_CRED_RESPONSE_FIELD,
    EXPECTED_TEST_SELF_HOSTED_KAFKA_CRED_RESPONSE_FIELD,
    EXPECTED_TEST_TRANSACTIONAL_DB_CREDENTIALS_RESPONSE_FIELD,
    MARIADB_DB_TYPE,
    MOTHERDUCK_DATABASE_TYPE,
    MSK_KAFKA_CONNECT_CONNECTION_TYPE,
    MSK_KAFKA_CONNECTION_TYPE,
    MYSQL_DB_TYPE,
    ORACLE_DB_TYPE,
    PINECONE_CONNECTION_TYPE,
    POSTGRES_DB_TYPE,
    SAP_HANA_DATABASE_TYPE,
    SELF_HOSTED_KAFKA_CONNECT_CONNECTION_TYPE,
    SELF_HOSTED_KAFKA_CONNECTION_TYPE,
    SQL_SERVER_DB_TYPE,
    TERADATA_DB_TYPE,
    TRANSACTIONAL_CONNECTION_TYPE,
)
from montecarlodata.queries.onboarding import (
    TEST_CONFLUENT_KAFKA_CONNECT_CRED_MUTATION,
    TEST_CONFLUENT_KAFKA_CRED_MUTATION,
    TEST_MSK_KAFKA_CONNECT_CRED_MUTATION,
    TEST_MSK_KAFKA_CRED_MUTATION,
    TEST_PINECONE_CRED_MUTATION,
    TEST_SELF_HOSTED_KAFKA_CONNECT_CRED_MUTATION,
    TEST_SELF_HOSTED_KAFKA_CRED_MUTATION,
    TEST_TRANSACTIONAL_DB_CRED_MUTATION,
)
from montecarlodata.utils import GqlWrapper


class CollectorValidationService:
    _SAVE_CREDENTIALS = "save_credentials"

    _CONNECTION_TYPES_TO_CREDS_MUTATIONS_MAPPING = {
        CONFLUENT_KAFKA_CONNECTION_TYPE: TEST_CONFLUENT_KAFKA_CRED_MUTATION,
        CONFLUENT_KAFKA_CONNECT_CONNECTION_TYPE: TEST_CONFLUENT_KAFKA_CONNECT_CRED_MUTATION,
        MSK_KAFKA_CONNECTION_TYPE: TEST_MSK_KAFKA_CRED_MUTATION,
        MSK_KAFKA_CONNECT_CONNECTION_TYPE: TEST_MSK_KAFKA_CONNECT_CRED_MUTATION,
        SELF_HOSTED_KAFKA_CONNECTION_TYPE: TEST_SELF_HOSTED_KAFKA_CRED_MUTATION,
        SELF_HOSTED_KAFKA_CONNECT_CONNECTION_TYPE: TEST_SELF_HOSTED_KAFKA_CONNECT_CRED_MUTATION,
        PINECONE_CONNECTION_TYPE: TEST_PINECONE_CRED_MUTATION,
        TRANSACTIONAL_CONNECTION_TYPE: TEST_TRANSACTIONAL_DB_CRED_MUTATION,
        TERADATA_DB_TYPE: TEST_TRANSACTIONAL_DB_CRED_MUTATION,
        POSTGRES_DB_TYPE: TEST_TRANSACTIONAL_DB_CRED_MUTATION,
        SQL_SERVER_DB_TYPE: TEST_TRANSACTIONAL_DB_CRED_MUTATION,
        MYSQL_DB_TYPE: TEST_TRANSACTIONAL_DB_CRED_MUTATION,
        ORACLE_DB_TYPE: TEST_TRANSACTIONAL_DB_CRED_MUTATION,
        MARIADB_DB_TYPE: TEST_TRANSACTIONAL_DB_CRED_MUTATION,
        AZURE_DEDICATED_SQL_POOL_TYPE: TEST_TRANSACTIONAL_DB_CRED_MUTATION,
        AZURE_SQL_DATABASE_TYPE: TEST_TRANSACTIONAL_DB_CRED_MUTATION,
        SAP_HANA_DATABASE_TYPE: TEST_TRANSACTIONAL_DB_CRED_MUTATION,
        MOTHERDUCK_DATABASE_TYPE: TEST_TRANSACTIONAL_DB_CRED_MUTATION,
    }

    _CONNECTION_TYPES_TO_OPERATION_TYPE = {
        CONFLUENT_KAFKA_CONNECTION_TYPE: EXPECTED_TEST_CONFLUENT_KAFKA_CRED_RESPONSE_FIELD,
        CONFLUENT_KAFKA_CONNECT_CONNECTION_TYPE: EXPECTED_TEST_CONFLUENT_KAFKA_CONNECT_CRED_RESPONSE_FIELD,  # noqa
        MSK_KAFKA_CONNECTION_TYPE: EXPECTED_TEST_MSK_KAFKA_CRED_RESPONSE_FIELD,
        MSK_KAFKA_CONNECT_CONNECTION_TYPE: EXPECTED_TEST_MSK_KAFKA_CONNECT_CRED_RESPONSE_FIELD,
        SELF_HOSTED_KAFKA_CONNECTION_TYPE: EXPECTED_TEST_SELF_HOSTED_KAFKA_CRED_RESPONSE_FIELD,
        SELF_HOSTED_KAFKA_CONNECT_CONNECTION_TYPE: EXPECTED_TEST_SELF_HOSTED_KAFKA_CONNECT_CRED_RESPONSE_FIELD,  # noqa
        PINECONE_CONNECTION_TYPE: EXPECTED_TEST_PINECONE_CREDENTIALS_RESPONSE_FIELD,
        TRANSACTIONAL_CONNECTION_TYPE: EXPECTED_TEST_TRANSACTIONAL_DB_CREDENTIALS_RESPONSE_FIELD,
        TERADATA_DB_TYPE: EXPECTED_TEST_TRANSACTIONAL_DB_CREDENTIALS_RESPONSE_FIELD,
        POSTGRES_DB_TYPE: EXPECTED_TEST_TRANSACTIONAL_DB_CREDENTIALS_RESPONSE_FIELD,
        SQL_SERVER_DB_TYPE: EXPECTED_TEST_TRANSACTIONAL_DB_CREDENTIALS_RESPONSE_FIELD,
        MYSQL_DB_TYPE: EXPECTED_TEST_TRANSACTIONAL_DB_CREDENTIALS_RESPONSE_FIELD,
        ORACLE_DB_TYPE: EXPECTED_TEST_TRANSACTIONAL_DB_CREDENTIALS_RESPONSE_FIELD,
        MARIADB_DB_TYPE: EXPECTED_TEST_TRANSACTIONAL_DB_CREDENTIALS_RESPONSE_FIELD,
        AZURE_DEDICATED_SQL_POOL_TYPE: EXPECTED_TEST_TRANSACTIONAL_DB_CREDENTIALS_RESPONSE_FIELD,
        AZURE_SQL_DATABASE_TYPE: EXPECTED_TEST_TRANSACTIONAL_DB_CREDENTIALS_RESPONSE_FIELD,
        SAP_HANA_DATABASE_TYPE: EXPECTED_TEST_TRANSACTIONAL_DB_CREDENTIALS_RESPONSE_FIELD,
        MOTHERDUCK_DATABASE_TYPE: EXPECTED_TEST_TRANSACTIONAL_DB_CREDENTIALS_RESPONSE_FIELD,
    }

    def __init__(
        self,
        config: Config,
        mc_client: Client,
        request_wrapper: Optional[GqlWrapper] = None,
        user_service: Optional[UserService] = None,
    ):
        self._mc_client = mc_client
        self._request_wrapper = request_wrapper or GqlWrapper(config)
        self._user_service = user_service or UserService(
            request_wrapper=self._request_wrapper, config=config
        )

    def run_validations(
        self,
        dc_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        only_periodic: bool = False,
    ) -> int:
        """
        Runs all validators for all integrations in the data collector.

        :param dc_id: The optional UUID of the data collector, used to disambiguate.
        :param agent_id: The optional UUID of the agent, used to disambiguate.
        :param only_periodic: Whether only periodic validations must be run or not.
        """
        if agent_id:
            agent = self._user_service.get_agent(agent_id)
            dc_id = agent["dc_id"]  # type: ignore
        dc = self._user_service.get_collector(dc_id)

        # Filter integration list to just integrations for this dc
        all_integrations = [
            integration
            for integration in (
                (self._user_service.warehouses or [])
                + (self._user_service.bi_containers or [])
                + (self._user_service.etl_containers or [])
            )
            if integration.get("dataCollector", {}).get("uuid") == dc.uuid
        ]

        total_failures = 0

        executor = ThreadPoolExecutor(max_workers=4)
        # Start the load operations and mark each future with its URL
        future_to_integration = {
            executor.submit(
                self._run_validations_for_integration, dc, integration, only_periodic
            ): integration
            for integration in all_integrations
        }
        for future in as_completed(future_to_integration):
            integration = future_to_integration[future]
            try:
                integration_failures, integration_messages = future.result()
                total_failures += integration_failures
                for message in integration_messages:
                    click.echo(message)
            except Exception as exc:
                print("%r generated an exception: %s" % (integration, exc))

        # Run storage access validation. If DC has an agent registered, will validate that the
        # agent's storage location is accessible.
        # If DC has no agent registered, will validate that default storage location is accessible.
        click.echo("")
        total_failures += self._run_storage_access_validation(dc_id=dc.uuid)

        click.echo("")
        if total_failures == 0:
            click.echo("All validations passed")
        else:
            click.echo(f"{total_failures} validations failed")
        return total_failures

    def _run_validations_for_integration(
        self, dc: Box, integration: Dict, only_periodic: bool
    ) -> Tuple[int, List[str]]:
        integration_failures = 0
        click.echo(f"Running validations for {integration['name']}")
        messages = []
        connections: List[Dict[str, Any]] = integration.get("connections", [])
        for connection in connections:
            if "type" in connection and "uuid" in connection:
                (
                    connection_failures,
                    connection_messages,
                ) = self._run_connection_validators(
                    dc_uuid=dc.uuid,
                    name=integration.get("name", ""),
                    connection_type=connection["type"].lower(),
                    connection_uuid=connection["uuid"],
                    only_periodic=only_periodic,
                )
                integration_failures += connection_failures
                messages.append("")
                messages.extend(connection_messages)

        return integration_failures, messages

    def _run_connection_validators(
        self,
        dc_uuid: uuid.UUID,
        name: str,
        connection_type: str,
        connection_uuid: uuid.UUID,
        only_periodic: bool,
    ) -> Tuple[int, List[str]]:
        messages = []
        messages.append(f"Validating {connection_type} connection: {name} - {connection_uuid}")
        validations = self._get_supported_validations(
            dc_uuid=dc_uuid,
            connection_type=connection_type,
            only_periodic=only_periodic,
        )
        failures = 0
        for validation in validations:
            ok = False
            warning = False
            try:
                result = self._run_single_validation(
                    connection_uuid=connection_uuid,
                    validation_name=validation,
                )

                assert result is not None
                ok, warning = self._process_validation_result(result)
            except Exception as e:
                echo_error_message(f"\tValidation {validation} failed ({e}).")

            result_icon, failures = self._get_icon(ok, warning, failures)
            messages.append(f"\t{validation}: {result_icon}")

        return failures, messages

    def _get_supported_validations(
        self,
        dc_uuid: Union[uuid.UUID, str],
        connection_type: str,
        only_periodic: bool,
    ) -> List:
        query = Query()
        query.get_supported_validations_v2(dc_id=dc_uuid, connection_type=connection_type)

        try:
            result = self._mc_client(
                query=query,
            ).get_supported_validations_v2

            return [
                validation.name
                for validation in result.supported_validations
                if not only_periodic or validation.periodic_validation
            ]
        except GqlError as e:
            complain_and_abort(f"Unable to get list of supported validators ({e}).")
            return []

    def _run_single_validation(
        self,
        connection_uuid: uuid.UUID,
        validation_name: str,
    ) -> Optional[TestCredentialsV2Response]:
        query = Query()
        query.test_existing_connection_v2(
            connection_id=connection_uuid, validation_name=validation_name
        )

        return self._mc_client(
            query=query,
            idempotent_request_id=str(uuid.uuid4()),
            timeout_in_seconds=40,  # let Monolith timeout first
        ).test_existing_connection_v2  # type: ignore

    def _run_storage_access_validation(
        self,
        dc_id: uuid.UUID,
    ) -> int:
        query = Query()
        query.test_storage_access(dc_id=dc_id)

        click.echo("Validating storage access:")

        failures = 0
        ok = False
        warning = False
        try:
            result = self._mc_client(
                query=query,
                idempotent_request_id=str(uuid.uuid4()),
                timeout_in_seconds=40,  # let Monolith timeout first
            ).test_storage_access

            ok, warning = self._process_validation_result(result)  # type: ignore
        except Exception as e:
            echo_error_message(f"\tValidation validate_storage_access failed ({e}).")

        result_icon, failures = self._get_icon(ok, warning, failures)
        click.echo(f"\tvalidate_storage_access: {result_icon}")

        return failures

    @staticmethod
    def _process_validation_result(result: TestCredentialsV2Response) -> Tuple[bool, bool]:
        ok = False
        warning = False

        # if there are only warnings we log them but consider it a successful validation
        ok = result.success or not bool(result.errors)
        if result.warnings:
            for warning in result.warnings:  # type: ignore
                echo_warning_message(f"\t{warning.cause}")
        if result.errors:
            for error in result.errors:  # type: ignore
                echo_error_message(f"\t{error.cause}")
        elif not result.success:
            warning = True

        return bool(ok), warning

    @staticmethod
    def _get_icon(ok: bool, warning: bool, failures: int) -> Tuple[str, bool]:
        if ok:
            result_icon = styled_warning_icon() if warning else styled_success_icon()
        else:
            result_icon = styled_error_icon()
            failures += 1
        return result_icon, bool(failures)

    def test_new_credentials(
        self,
        connection_type: str,
        dc_id: Optional[uuid.UUID] = None,
        generate_key: bool = True,
        skip_validation: bool = False,
        validate_only: bool = False,
        **kwargs: Any,
    ) -> Optional[str]:
        if not dc_id:
            dc_id = self._user_service.get_collector().uuid
        assert dc_id is not None

        kwargs["dc_id"] = str(dc_id)

        if not skip_validation:
            supported_validation_tests = self._get_supported_validations(
                dc_uuid=str(dc_id),
                connection_type=connection_type,
                only_periodic=False,
            )
            for validation_name in supported_validation_tests:
                variables = copy.deepcopy(kwargs)
                variables["validation_name"] = validation_name

                operation_name = self._CONNECTION_TYPES_TO_OPERATION_TYPE[connection_type]
                operation_name_snake = self._camel_to_snake(operation_name)
                validation_result = (
                    self._mc_client(
                        query=self._CONNECTION_TYPES_TO_CREDS_MUTATIONS_MAPPING[connection_type],
                        operation_name=operation_name,
                        variables=GqlWrapper.convert_snakes_to_camels(variables),
                        idempotent_request_id=str(uuid.uuid4()),
                        timeout_in_seconds=40,  # let Monolith timeout first
                    )
                    .__getattr__(operation_name_snake)  # type: ignore
                    .validation_result
                )
                ok, warning = self._process_validation_result(
                    result=validation_result,  # type: ignore
                )

                result_icon, _ = self._get_icon(ok, warning, 0)
                click.echo(f"\t{validation_name}: {result_icon}")

                if not ok:
                    # validation failed
                    raise click.Abort()

        if not validate_only:
            prompt_connection(
                message="Validations passed! Would you like to continue?",
                skip_prompt=not generate_key,
            )
            variables = copy.deepcopy(kwargs)
            variables["validation_name"] = self._SAVE_CREDENTIALS
            new_key_response = self._request_wrapper.make_request_v2(
                query=self._CONNECTION_TYPES_TO_CREDS_MUTATIONS_MAPPING[connection_type],
                operation=self._CONNECTION_TYPES_TO_OPERATION_TYPE[connection_type],
                variables=variables,
            )

            validation_result = new_key_response.data.validationResult  # type: ignore
            if not validation_result.success:
                click.echo("creating credential key failed.")
                click.echo(json.dumps(validation_result))
                click.Abort()

            return new_key_response.data.key  # type: ignore
        else:
            echo_success_message(message="Validations passed!")

    @staticmethod
    def _camel_to_snake(text: str) -> str:
        return re.sub("([A-Z]+)", r"_\1", text).lower()
