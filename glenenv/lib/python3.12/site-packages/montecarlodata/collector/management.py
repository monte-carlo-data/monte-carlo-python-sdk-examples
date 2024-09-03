import copy
import uuid
from datetime import datetime
from typing import Dict, List, Optional, cast
from urllib import parse

import click
from pycarlo.common.errors import GqlError
from pycarlo.common.retries import Backoff, ExponentialBackoffJitter
from pycarlo.core import Client, Query
from tabulate import tabulate

from montecarlodata.collector.fields import (
    ADD_DC_PROMPT_VERBIAGE,
    ADD_DC_REGION_PROMPT_VERBIAGE,
    DEFAULT_COLLECTION_REGION,
    EXPECTED_ADD_DC_RESPONSE_FIELD,
    EXPECTED_GENERATE_TEMPLATE_GQL_RESPONSE_FIELD,
)
from montecarlodata.common.user import UserService
from montecarlodata.config import Config
from montecarlodata.errors import complain_and_abort, manage_errors, prompt_connection
from montecarlodata.queries.collector import ADD_COLLECTOR_RECORD, GENERATE_COLLECTOR_TEMPLATE
from montecarlodata.utils import AwsClientWrapper, GqlWrapper


class CollectorManagementService:
    _DEFAULT_COLLECTOR_VAL = "-"
    _LIST_COLLECTOR_HEADERS = [
        "AWS Stack ARN",
        "ID",
        "Version",
        "Template",
        "Last updated",
        "Active",
    ]
    _LEGACY_TEMPLATE = "cloudformation:July-2019"
    _JANUS_TEMPLATE_VARIANT = "janus"
    _JANUS_UNSUPPORTED_PARAMETERS = [
        "CreateCrossAccountRole",
        "EnableGatewayAccessLogging",
    ]
    _JANUS_TITLE_CASE_PARAMETERS = [
        "subnetCIDRs",
        "vpcCIDR",
        "workerLambdaConcurrentExecutions",
    ]
    _PING_RETRY_INITIAL_WAIT_SECONDS = 1
    _PING_RETRY_MAX_ATTEMPTS = 4

    def __init__(
        self,
        config: Config,
        mc_client: Client,
        retry_policy: Optional[Backoff] = None,
        request_wrapper: Optional[GqlWrapper] = None,
        aws_wrapper: Optional[AwsClientWrapper] = None,
        aws_profile_override: Optional[str] = None,
        aws_region_override: Optional[str] = None,
        user_service: Optional[UserService] = None,
    ):
        self._abort_on_error = True
        self._mc_client = mc_client
        self._ping_retry_policy = retry_policy or ExponentialBackoffJitter(
            self._PING_RETRY_INITIAL_WAIT_SECONDS,
            pow(2, self._PING_RETRY_MAX_ATTEMPTS),
        )
        self._collector_region = aws_region_override or DEFAULT_COLLECTION_REGION

        self._request_wrapper = request_wrapper or GqlWrapper(config)
        self._aws_wrapper = aws_wrapper or AwsClientWrapper(
            profile_name=aws_profile_override,
            region_name=self._collector_region,
        )
        self._user_service = user_service or UserService(
            request_wrapper=self._request_wrapper, config=config
        )

    @manage_errors
    def echo_template(self, **kwargs) -> None:
        """
        Echos the most recent template for this account
        """
        click.echo(self._get_template_url_from_launch(self._get_template_launch_url(**kwargs)))

    @manage_errors
    def echo_collectors(
        self,
        headers: str = "firstrow",
        table_format: str = "fancy_grid",
        active_only: bool = False,
    ) -> None:
        """
        Echo collectors in a fancy grid.
        """
        table = [self._LIST_COLLECTOR_HEADERS]
        for collector in self._user_service.collectors or []:
            if active_only and not collector.get("active"):
                continue

            template_provider = collector.get("templateProvider")
            template_variant = collector.get("templateVariant")
            template_version = collector.get("templateVersion")

            if template_provider:
                template = f"{template_provider}:{template_variant}:{template_version}"
            else:
                template = self._LEGACY_TEMPLATE
            last_updated = (
                datetime.fromisoformat(collector.get("lastUpdated")).strftime("%Y-%m-%d %H:%M:%S")
                if collector.get("lastUpdated")
                else self._DEFAULT_COLLECTOR_VAL
            )

            table.append(
                [
                    (
                        collector.get("stackArn")
                        if collector.get("stackArn")
                        else self._DEFAULT_COLLECTOR_VAL
                    ),
                    collector["uuid"],
                    collector.get("codeVersion") or template_version or self._DEFAULT_COLLECTOR_VAL,
                    template,
                    last_updated,
                    collector.get("active"),
                ]
            )
        click.echo(tabulate(table, headers=headers, tablefmt=table_format, maxcolwidths=100))

    @manage_errors
    def add_collector(self, no_prompt: bool = False) -> None:
        """
        Add a collector to the account. Prompts to launch browser.
        """
        response = self._request_wrapper.make_request_v2(
            query=ADD_COLLECTOR_RECORD,
            operation=EXPECTED_ADD_DC_RESPONSE_FIELD,
        )
        dc_id = response.data.dc.uuid  # type: ignore
        click.echo(f"Created collector record with ID '{dc_id}'")
        if no_prompt:
            return

        prompt_connection(message=ADD_DC_PROMPT_VERBIAGE)  # exits on No
        collection_region = click.prompt(
            ADD_DC_REGION_PROMPT_VERBIAGE,
            show_choices=True,
            default=DEFAULT_COLLECTION_REGION,
            show_default=True,
            type=click.Choice(self._user_service.active_collection_regions),
        )
        self.launch_quick_create_link(dry=False, dc_id=dc_id, collection_region=collection_region)

    @manage_errors
    def launch_quick_create_link(self, dry: bool = True, **kwargs) -> None:
        """
        Open browser with a quick create link for deploying a data collector

        https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/cfn-console-create-stacks-quick-create-links.html
        """
        launch_url = self._get_template_launch_url(**kwargs)
        if dry:
            click.echo(launch_url)
            return
        click.launch(launch_url)

    @manage_errors
    def upgrade_template(
        self, update_infra: bool = False, new_params: Optional[Dict] = None, **kwargs
    ) -> None:
        """
        Upgrades the DC attached to this account
        """
        new_params = new_params or {}
        collector_details = self._generate_collector_template(update_infra=update_infra, **kwargs)
        try:
            # If collector_details is None, then indexing into it, below, will throw an exception.
            # This is fine as the operation will simply fail and the user will be informed. So
            # let's just cast() the type to avoid typing errors about this on every line.
            collector_details = cast(Dict, collector_details)

            dc_id = collector_details["uuid"]
            stack_arn = collector_details["stackArn"]
            is_active = collector_details["active"]
            gateway_id = collector_details["apiGatewayId"]
            template_variant = collector_details["templateVariant"]
            template_url = self._get_template_url_from_launch(
                collector_details["templateLaunchUrl"]
            )

            if not is_active:
                complain_and_abort(
                    "Cannot upgrade an inactive collector. Please contact Monte Carlo"
                )
        except KeyError as err:
            complain_and_abort(
                f"Missing expected property ({err}). "
                "The collector may not have been deployed before"
            )
        else:
            click.echo(f"Updating '{stack_arn}'")
            update_variant = update_infra and template_variant != self._JANUS_TEMPLATE_VARIANT
            self._upgrade(
                stack_arn=stack_arn,
                template_url=template_url,
                gateway_id=gateway_id,
                new_params=new_params,
                update_variant=update_variant,
            )
            click.echo("Update completed")

            self.ping_dc(dc_id=dc_id)
            click.echo("Upgrade completed successfully! Have a nice day!")

    @manage_errors
    def deploy_template(
        self,
        stack_name: str,
        termination_protection: bool,
        new_params: Optional[Dict] = None,
        **kwargs,
    ) -> None:
        """
        Deploys a template for this account
        """
        click.echo(f"Deploying '{stack_name}' in '{self._collector_region}'")
        collector_details = self._generate_collector_template(**kwargs)
        is_active = collector_details["active"]  # type: ignore
        if is_active:
            complain_and_abort("Cannot deploy with an already active collector.")

        template_url = self._get_template_url_from_launch(
            collector_details["templateLaunchUrl"],  # type: ignore
        )
        self._deploy(
            stack_name=stack_name,
            template_url=template_url,
            termination_protection=termination_protection,
            new_params=new_params,
        )

    def _upgrade(
        self,
        stack_arn: str,
        template_url: str,
        gateway_id: str,
        new_params: Dict,
        update_variant: bool,
    ) -> None:
        """
        Upgrade the collector stack and deploy the gateway
        """
        parameters = self._update_parameters(
            stack_arn=stack_arn,
            param_overrides=new_params,
            update_variant=update_variant,
        )
        if self._aws_wrapper.upgrade_stack(
            stack_id=stack_arn, template_link=template_url, parameters=parameters
        ):
            self._aws_wrapper.deploy_gateway(gateway_id=gateway_id)
        else:
            complain_and_abort("Failed to upgrade. Please review CF events for details")

    def ping_dc(self, dc_id: Optional[str] = None, trace_id: Optional[str] = None):
        """
        Pings the data collector.

        :param dc_id: The UUID of the data collector.
        :param trace_id: A custom UUID value used to correlate the ping response.
        """
        dc = self._user_service.get_collector(dc_id)
        trace_id = trace_id or str(uuid.uuid4())

        click.echo(f"Pinging data collector to verify it is operational (trace ID: {trace_id})")

        query = Query()
        query.ping_data_collector(dc_id=dc.uuid, trace_id=trace_id)

        try:
            result = self._mc_client(
                query=query, retry_backoff=self._ping_retry_policy
            ).ping_data_collector
            if result.trace_id != trace_id:
                complain_and_abort(
                    f"Ping response contains a mismatched trace ID ({result.trace_id})."
                )
        except GqlError as e:
            complain_and_abort(
                "Data collector could not be reached after "
                f"{self._PING_RETRY_MAX_ATTEMPTS} ping attempts ({e})."
            )
        else:
            click.echo(f"Data collector responded to ping (trace ID: {result.trace_id})")

    def _deploy(
        self,
        stack_name: str,
        template_url: str,
        termination_protection: bool,
        new_params: Optional[Dict] = None,
    ) -> None:
        """
        Deploy (create) a collector stack for the first time
        """
        parameters = self._build_param_list(existing_params=[], new_params=new_params)
        if not self._aws_wrapper.create_stack(
            stack_name=stack_name,
            template_link=template_url,
            termination_protection=termination_protection,
            parameters=parameters,
        ):
            complain_and_abort("Failed to deploy. Please review CF events for details")

    def _generate_collector_template(self, update_infra: bool = False, **kwargs) -> Optional[Dict]:
        """
        Generate the latest template and returns any associated dc properties
        (i.e. from the DataCollectorModel)
        """
        options = {"region": self._collector_region, "update_infra": update_infra}
        if kwargs.get("collection_region"):
            options["region"] = kwargs["collection_region"]  # overwrite client

        if kwargs.get("dc_id"):
            options["dc_id"] = kwargs["dc_id"]

        response = self._request_wrapper.make_request_v2(
            query=GENERATE_COLLECTOR_TEMPLATE,
            operation=EXPECTED_GENERATE_TEMPLATE_GQL_RESPONSE_FIELD,
            variables=options,
        )
        return response.data.get("dc")  # type: ignore

    def _get_template_launch_url(self, **kwargs) -> str:
        """
        Get the template launch url
        """
        return self._generate_collector_template(**kwargs).get("templateLaunchUrl")  # type: ignore

    @staticmethod
    def _get_template_url_from_launch(launch_url: str) -> str:
        """
        Extract the template url from expected launch url structure
        """
        return parse.parse_qs(parse.urlparse(launch_url).fragment).get("templateURL", [])[0]

    def _update_parameters(
        self, stack_arn: str, param_overrides: Optional[Dict], update_variant: bool
    ):
        existing_params = self._aws_wrapper.get_stack_parameters(stack_id=stack_arn)
        # we only allow updates to janus template variant
        if update_variant:
            converted_parameters, removed_parameters = self._adapt_parameters_for_janus(
                existing_params
            )

            combined_parameters = (
                self._build_param_list(
                    existing_params=existing_params,
                    # add the converted parameters, and consider overrides
                    new_params={**converted_parameters, **(param_overrides or {})},
                )
                or []
            )

            # remove any parameters that are no longer supported by janus template variant
            new_parameters = [
                param
                for param in combined_parameters
                if param["ParameterKey"] not in removed_parameters
            ]
            return new_parameters

        return self._build_param_list(existing_params, param_overrides)

    @staticmethod
    def _adapt_parameters_for_janus(parameters: List[Dict]):
        converted_parameters = {}
        removed_parameters = []
        for parameter in parameters or []:
            key = parameter["ParameterKey"]
            if key in CollectorManagementService._JANUS_TITLE_CASE_PARAMETERS:
                title_case_key = key[0].upper() + key[1:]
                converted_parameters[title_case_key] = parameter["ParameterValue"]
                removed_parameters.append(key)
            elif key in CollectorManagementService._JANUS_UNSUPPORTED_PARAMETERS:
                removed_parameters.append(key)

        return converted_parameters, removed_parameters

    @staticmethod
    def _build_param_list(
        existing_params: List[Dict], new_params: Optional[Dict] = None
    ) -> List[Dict]:
        """
        Get a list of parameters by replacing (or extending) any new params into the existing
        stack params
        """
        existing_params = copy.deepcopy(existing_params)
        new_params = copy.deepcopy(new_params)

        for param in existing_params or []:
            if new_params and new_params.get(param["ParameterKey"]) is not None:
                param["ParameterValue"] = new_params[param["ParameterKey"]]
                param["UsePreviousValue"] = False
                del new_params[param["ParameterKey"]]
            else:
                del param["ParameterValue"]
                param["UsePreviousValue"] = True
        if new_params:
            # Handle any completely new params (i.e. those that were not in the existing struct)
            # Can be found for params added between versions
            existing_params.extend(
                [{"ParameterKey": k, "ParameterValue": v} for k, v in new_params.items()]
            )

        return existing_params
