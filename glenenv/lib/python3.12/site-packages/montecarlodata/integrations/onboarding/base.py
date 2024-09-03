import json
import os
from typing import Dict, Optional

import click
from tabulate import tabulate

from montecarlodata.common.data import (
    ConnectionOptions,
    OnboardingConfiguration,
    ValidationResult,
)
from montecarlodata.common.user import UserService
from montecarlodata.config import Config
from montecarlodata.errors import complain_and_abort, prompt_connection
from montecarlodata.integrations.onboarding.fields import (
    ADD_CONNECTION_FAILED_VERBIAGE,
    ADD_CONNECTION_SUCCESS_VERBIAGE,
    CONFIRM_CONNECTION_VERBIAGE,
    CONNECTION_TEST_FAILED_VERBIAGE,
    CONNECTION_TEST_SUCCESS_VERBIAGE,
    DATA_LAKE_WAREHOUSE_TYPE,
    EXPECTED_ADD_CONNECTION_RESPONSE_FIELD,
    GQL_TO_FRIENDLY_CONNECTION_MAP,
    MAIN_CONNECTION_TYPES,
    S3_CERT_MECHANISM,
    SKIP_ADD_CONNECTION_VERBIAGE,
    VALIDATIONS_FAILED_VERBIAGE,
)
from montecarlodata.queries.onboarding import ADD_CONNECTION_MUTATION
from montecarlodata.utils import AwsClientWrapper, GqlWrapper


class BaseOnboardingService:
    _VALIDATION_STATUS_HEADERS = ["Validation", "Result", "Details"]
    _VALIDATION_PASSED = "Passed"
    _VALIDATION_FAILED = "Failed"
    _ETL_CONNECTION_TYPES = ["fivetran", "airflow"]

    def __init__(
        self,
        config: Config,
        user_service: Optional[UserService] = None,
        request_wrapper: Optional[GqlWrapper] = None,
        aws_wrapper: Optional[AwsClientWrapper] = None,
    ):
        self._abort_on_error = True  # Aborts methods with deco on unhandled error
        self._dc_outputs = None

        self._user_service = user_service or UserService(config=config)
        self._request_wrapper = request_wrapper or GqlWrapper(config)
        self._aws_wrapper = aws_wrapper

    def onboard(self, **kwargs):
        """
        Convenience wrapper to validate and add a connection.
        """
        onboarding_config = self._get_onboard_config(**kwargs)

        result = self._validate_connection(
            query=onboarding_config.validation_query,  # type: ignore
            response_field=onboarding_config.validation_response,  # type: ignore
            **onboarding_config.connection_options.monolith_base_payload,  # type: ignore
        )

        if not onboarding_config.connection_options.validate_only:  # type: ignore
            prompt = (
                VALIDATIONS_FAILED_VERBIAGE
                if result.has_warnings  # type: ignore
                else CONFIRM_CONNECTION_VERBIAGE
            )
            prompt_connection(
                message=prompt,
                skip_prompt=onboarding_config.connection_options.auto_yes,  # type: ignore
            )
            self._add_connection(
                temp_path=result.credentials_key,  # type: ignore
                onboarding_config=onboarding_config,
            )
        else:
            click.echo(SKIP_ADD_CONNECTION_VERBIAGE)

    def add_connection(self, key: str, **kwargs):
        onboarding_config = self._get_onboard_config(**kwargs)
        self._add_connection(temp_path=key, onboarding_config=onboarding_config)

    def handle_cert(self, cert_prefix: str, options: Dict) -> None:
        """
        Handles cert payload from either an s3 path or file. Uploading the latter
        Options is updated if successful.
        """
        active_agent = self._user_service.get_collector_agent(options.get("dc_id"))
        if options.get("cert_file") is not None:
            if active_agent:
                complain_and_abort("cert_file option is not supported for agents.")

            self._aws_wrapper = self._aws_wrapper or AwsClientWrapper(
                profile_name=options.get("aws_profile"),
                region_name=options.get("aws_region"),
            )
            # get bucket name from arn
            bucket_name = self._get_dc_property(prop="PrivateS3BucketArn").split(":")[5]  # type: ignore
            object_name = os.path.join(cert_prefix, os.path.basename(options["cert_file"]))
            self._aws_wrapper.upload_file(
                bucket_name=bucket_name,
                object_name=object_name,
                file_path=options["cert_file"],
            )

            click.echo(f"Uploaded '{options['cert_file']}' to s3://{bucket_name}/{object_name}")
            options["cert_s3"] = object_name

        ssl_options = {}
        if options.get("cert_s3") is not None:
            # reformat to generic options and specify a mechanism
            ssl_options = {
                "mechanism": S3_CERT_MECHANISM,
                "cert": options.pop("cert_s3"),
            }
        options.pop("cert_file", None)

        if options.get("skip_cert_verification") is not None:
            ssl_options["skip_verification"] = options.pop("skip_cert_verification")

        if ssl_options:
            options["ssl_options"] = ssl_options

    def _validate_connection(
        self, query: str, response_field: str, **kwargs
    ) -> Optional[ValidationResult]:
        """
        Validate connection before adding using the expected gql response_field
        (e.g. testPrestoCredentials)
        """
        response = self._request_wrapper.make_request_v2(
            query=query, operation=response_field, variables=kwargs
        )

        summary = self._build_validation_summary(response.data)  # type: ignore
        temp_path = response.data.get("key")  # type: ignore
        warnings = response.data.get("warnings")  # type: ignore

        if temp_path is not None:
            click.echo(f"{CONNECTION_TEST_SUCCESS_VERBIAGE}{summary}")
            return ValidationResult(
                has_warnings=warnings is not None and len(warnings) > 0,
                credentials_key=temp_path,
            )

        complain_and_abort(f"{CONNECTION_TEST_FAILED_VERBIAGE}{summary}")

    def _build_validation_summary(self, response: Dict, table_format: str = "fancy_grid") -> str:
        data = []
        if response.get("validations"):
            for passed in response.get("validations"):  # type: ignore
                data.append([passed.get("message"), self._VALIDATION_PASSED, None])

        if response.get("warnings"):
            for failed in response.get("warnings", []):
                data.append(
                    [
                        failed.get("message"),
                        self._VALIDATION_FAILED,
                        failed.get("data", {}).get("error"),
                    ]
                )

        if data:
            return "\n" + tabulate(
                data, headers=self._VALIDATION_STATUS_HEADERS, tablefmt=table_format
            )

        # do not provide a summary if no detailed response is available
        return ""

    def _add_connection(self, temp_path: str, onboarding_config: OnboardingConfiguration) -> bool:
        """
        Add connection and setup any associated jobs
        """
        connection_request = {
            "key": temp_path,
            "connectionType": onboarding_config.connection_type,
        }

        connection_request.update(self._disambiguate_warehouses(onboarding_config))

        # Set optional properties
        if onboarding_config.warehouse_name:
            connection_request["name"] = onboarding_config.warehouse_name
        if onboarding_config.job_types:
            connection_request["jobTypes"] = onboarding_config.job_types  # type: ignore
        if onboarding_config.job_limits:
            connection_request["jobLimits"] = json.dumps(onboarding_config.job_limits)
        if onboarding_config.connection_options and onboarding_config.connection_options.dc_id:
            connection_request["dcId"] = onboarding_config.connection_options.dc_id

        response = self._request_wrapper.make_request_v2(
            query=onboarding_config.connection_query or ADD_CONNECTION_MUTATION,
            operation=onboarding_config.connection_response
            or EXPECTED_ADD_CONNECTION_RESPONSE_FIELD,
            variables=connection_request,
        )

        connection_id = response.data.get("connection", {}).get("uuid")  # type: ignore
        if connection_id is not None:
            friendly_connection_str = GQL_TO_FRIENDLY_CONNECTION_MAP.get(
                onboarding_config.connection_type, onboarding_config.connection_type.capitalize()
            )
            click.echo(f"{ADD_CONNECTION_SUCCESS_VERBIAGE}" f"{friendly_connection_str}.")
            return True
        complain_and_abort(ADD_CONNECTION_FAILED_VERBIAGE)
        return False

    def _disambiguate_warehouses(self, onboarding_config: OnboardingConfiguration) -> Dict:
        """
        Determine type of connection request to build using the following criteria -
            1) If it is an ETL connection there should not be any warehouse set so we can skip this
               step
            2) If it is a main connection type or a traditional warehouse (i.e. not a lake), create
               a new warehouse.
            3) If a name has been passed, create the connection in that warehouse.
            4) If no name has been passed but there is only one warehouse, use it.
            5) Otherwise fail.
        """
        is_etl_connection = onboarding_config.connection_type in self._ETL_CONNECTION_TYPES

        if is_etl_connection:
            return {}

        warehouse_type = (
            onboarding_config.warehouse_type or DATA_LAKE_WAREHOUSE_TYPE
        )  # default to data-lake
        connection_type = onboarding_config.connection_type
        create_warehouse = onboarding_config.create_warehouse

        # Main connections require the creation of a new warehouse.
        if (
            connection_type in MAIN_CONNECTION_TYPES or warehouse_type != DATA_LAKE_WAREHOUSE_TYPE
        ) and create_warehouse:
            return {"createWarehouseType": warehouse_type}

        name = onboarding_config.warehouse_name

        dw_id = self._get_warehouse_id_from_name(name)
        return {"dwId": dw_id}

    def _get_warehouse_id_from_name(self, name: Optional[str]) -> Optional[str]:
        warehouses = self._user_service.warehouses

        # If it is not a new warehouse, find the warehouse with the given name to create the new
        # connection belonging to it. If the name is not found, it is an error.
        if name:
            warehouses = [warehouse for warehouse in warehouses if warehouse.get("name") == name]
            if warehouses:
                return warehouses[0]["uuid"]
            complain_and_abort("Could not find warehouse with given name")
        # Name is optional if there is only one warehouse.
        elif len(warehouses) == 1:
            return warehouses[0]["uuid"]
        # If more than one warehouse exists, name is required.
        else:
            complain_and_abort("Name is required to disambiguate lake warehouses")

    def _get_bi_container_id_from_connection_id(
        self, bi_connection_id: Optional[str]
    ) -> Optional[str]:
        """
        Exchange a Connection UUID for the parent BI Container's UUID
        """
        bi_containers = self._user_service.bi_containers
        for bi_container in bi_containers:
            connections = bi_container.get("connections")
            for connection in connections:  # type: ignore
                if connection["uuid"] == bi_connection_id:
                    return bi_container["uuid"]
        complain_and_abort("Could not find BI container with given connection ID")

    def _get_dc_property(self, prop: str) -> Optional[str]:
        """
        Retrieve property from DC stack outputs
        """
        self._dc_outputs = self._dc_outputs or self._aws_wrapper.get_stack_outputs(  # type: ignore
            self._user_service.active_collector["stackArn"]
        )  # cache lookup
        for output in self._dc_outputs:
            if output["OutputKey"] == prop:
                return output["OutputValue"]

    @staticmethod
    def _build_connection_options(**kwargs) -> ConnectionOptions:
        """
        Create connection options from arguments
        """
        connection_options = ConnectionOptions(**kwargs)

        if connection_options.monolith_connection_payload:
            connection_options.monolith_base_payload["connectionOptions"] = (  # type: ignore
                connection_options.monolith_connection_payload
            )
        return connection_options

    def _get_onboard_config(self, **kwargs) -> OnboardingConfiguration:
        agent_id = kwargs.get("agent_id")
        if agent_id:
            agent = self._user_service.get_agent(agent_id)
            kwargs["dc_id"] = agent["dc_id"]  # type: ignore
        onboarding_config = OnboardingConfiguration(
            **kwargs
        )  # not passed as arg to avoid having to change all clients

        is_etl_connection = onboarding_config.connection_type in self._ETL_CONNECTION_TYPES

        onboarding_config.connection_options = self._build_connection_options(
            **onboarding_config.connection_options  # type: ignore
        )

        if is_etl_connection:
            onboarding_config.warehouse_name = (
                onboarding_config.connection_options.monolith_base_payload.pop(  # type: ignore
                    "etl_name", onboarding_config.warehouse_name
                )
            )
        else:
            onboarding_config.warehouse_name = (
                onboarding_config.connection_options.monolith_base_payload.pop(  # type: ignore
                    "warehouseName", onboarding_config.connection_type
                )
            )
        # Prefer explicitly set types.
        if not onboarding_config.warehouse_type and not is_etl_connection:
            onboarding_config.warehouse_type = (
                onboarding_config.connection_options.monolith_base_payload.pop(  # type: ignore
                    "warehouseType", DATA_LAKE_WAREHOUSE_TYPE
                )
            )

        return onboarding_config
