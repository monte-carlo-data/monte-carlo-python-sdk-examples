import json
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import (
    Dict,
    List,
    Optional,
)
from urllib.parse import urljoin

import click
import questionary
import requests
from dataclasses_json import (
    DataClassJsonMixin,
    Undefined,
    dataclass_json,
)
from pycarlo.core import (
    Client,
    Mutation,
    Query,
)
from pycarlo.lib.schema import GenericScalar
from sgqlc.types import Variable
from tabulate import tabulate

from montecarlodata import settings
from montecarlodata.agents.fields import (
    AWS,
    AWS_ASSUMABLE_ROLE,
    AZURE_FUNCTION_APP_KEY,
    AZURE_STORAGE_ACCOUNT_KEYS,
    GCP_JSON_SERVICE_ACCOUNT_KEY,
    REMOTE_AGENT,
)
from montecarlodata.common.common import ConditionalDictionary, read_as_json_string
from montecarlodata.common.user import UserService
from montecarlodata.config import Config
from montecarlodata.errors import complain_and_abort, manage_errors

DOCKER_TAGS_REQUEST_PAGE_SIZE = 50
DEFAULT_TAGS_PAGE_SIZE = 10
DOCKER_HUB_TAGS_BASE_URL = "https://hub.docker.com/v2/namespaces/"

# Adapted from https://docs.aws.amazon.com/lambda/latest/dg/API_Invoke.html#API_Invoke_RequestSyntax
LAMBDA_ARN_REGEX = re.compile(
    r"^arn:(aws[a-zA-Z-]*)?:lambda:([a-z]{2}(-gov)?-[a-z]+-\d{1}):(\d{12}):function:"
    r"([a-zA-Z0-9-_\.]+)(:(\$LATEST|[a-zA-Z0-9-_]+))?$"
)
LAMBDA_ARN_REGEX_GROUP_REGION = 2
LAMBDA_ARN_REGEX_GROUP_ACCOUNT_ID = 4


@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass()
class DockerTag(DataClassJsonMixin):
    name: str
    last_updated: str


@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass()
class DockerTagsResult(DataClassJsonMixin):
    results: List[DockerTag]


class AgentService:
    _AGENT_FRIENDLY_HEADERS = [
        "Agent ID",
        "Agent Type / Platform",
        "DC ID",
        "Endpoint",
        "Version",
        "Last updated (UTC)",
        "Active",
    ]

    def __init__(
        self,
        config: Config,
        mc_client: Client,
        user_service: Optional[UserService] = None,
    ):
        self._abort_on_error = True
        self._mc_client = mc_client
        self._user_service = user_service or UserService(config=config)
        self._image_org = config.mcd_agent_image_org
        self._image_repo = config.mcd_agent_image_repo
        self._image_name = f"{config.mcd_agent_image_host}/{self._image_org}/{self._image_repo}"

    @property
    def _ecr_image_name(self) -> str:
        if self._image_name.endswith("/pre-release-agent"):
            return "404798114945.dkr.ecr.*.amazonaws.com/mcd-pre-release-agent"
        return "752656882040.dkr.ecr.*.amazonaws.com/mcd-agent"

    @manage_errors
    def create_agent(self, agent_type, platform, storage, auth_type, endpoint, **kwargs) -> None:
        """
        Register an agent by validating connection and creating an AgentModel in the monolith.
        """

        dry_run = kwargs.get("dry_run", False)
        agent_request = {
            "agent_type": agent_type,
            "endpoint": endpoint,
            "storage_type": storage,
            "platform": platform,
            "auth_type": auth_type,
            "dry_run": dry_run,
        }

        if kwargs.get("dc_id"):
            agent_request["data_collector_id"] = kwargs["dc_id"]

        aws_region: Optional[str] = None
        if platform == AWS and agent_type == REMOTE_AGENT:
            matches = LAMBDA_ARN_REGEX.match(endpoint)
            if matches:
                aws_region = matches.group(LAMBDA_ARN_REGEX_GROUP_REGION)
        if auth_type == GCP_JSON_SERVICE_ACCOUNT_KEY:
            agent_request["credentials"] = read_as_json_string(kwargs["key_file"])
        elif auth_type == AWS_ASSUMABLE_ROLE:
            creds = {"aws_assumable_role": kwargs["assumable_role"]}
            if kwargs["external_id"]:
                creds["external_id"] = kwargs["external_id"]
            if aws_region:
                creds["aws_region"] = aws_region
            agent_request["credentials"] = json.dumps(creds)
        elif auth_type == AZURE_STORAGE_ACCOUNT_KEYS:
            creds = {"azure_connection_string": kwargs["connection_string"]}
            agent_request["credentials"] = json.dumps(creds)
        elif auth_type == AZURE_FUNCTION_APP_KEY:
            creds = {"app_key": kwargs["app_key"]}
            agent_request["credentials"] = json.dumps(creds)

        mutation = Mutation()
        # the trailing call to __fields__ is needed to force selection of all possible fields
        mutation.create_or_update_agent(**agent_request).__fields__()
        result = self._mc_client(mutation).create_or_update_agent

        self._validate_response(result.validation_result)

        if result.agent_id is not None:
            click.echo("Agent successfully registered!\n" f"AgentId: {result.agent_id}")
        elif dry_run:
            if result.validation_result.success:
                click.echo("Dry run completed successfully!")
            else:
                complain_and_abort("Dry run failed.")
        else:
            complain_and_abort("Failed to register agent.")

    @manage_errors
    def delete_agent(self, agent_id) -> None:
        """
        Deregister an Agent (deletes AgentModel from monolith)
        """
        variables = dict(agent_id=agent_id)

        mutation = Mutation()
        mutation.delete_agent(**variables)
        result = self._mc_client(mutation).delete_agent

        if result.success:
            click.echo(f"Agent {agent_id} deregistered.")
        else:
            complain_and_abort("Failed to deregister agent.")

    @manage_errors
    def echo_agents(
        self,
        show_inactive: bool = False,
        headers: str = "firstrow",
        table_format: str = "fancy_grid",
    ):
        """
        Display agents in an easy-to-read table.
        """

        table = [self._AGENT_FRIENDLY_HEADERS]
        for agent in self._user_service.agents:
            is_active = not agent.get("isDeleted")
            if not show_inactive and not is_active:
                continue
            full_type = f"{agent.get('agentType', '')} / {agent.get('platform', '')}"
            last_updated = (
                datetime.fromisoformat(
                    agent.get(
                        "lastUpdatedTime",
                    )  # type: ignore
                ).strftime("%Y-%m-%d %H:%M:%S")
                if agent.get("lastUpdatedTime")
                else "-"
            )

            table += [
                [
                    agent.get("uuid") or "",
                    full_type,
                    agent.get("dc_id") or "",
                    agent.get("endpoint") or "",
                    agent.get("imageVersion") or "-",
                    last_updated,
                    is_active,
                ]
            ]

        # If the account has no agents, add 1 line of empty values so tabulate() creates a pretty
        # empty table
        if len(table) == 1:
            table += ["" for _ in self._AGENT_FRIENDLY_HEADERS]

        click.echo(tabulate(table, headers=headers, tablefmt=table_format, maxcolwidths=100))

    @staticmethod
    def _validate_response(validation_result):
        output = {}

        if validation_result.errors:
            errors = []
            for error in validation_result.errors:
                error_output = dict(
                    message=error.friendly_message or "",
                    cause=error.cause or "",
                    resolution=error.resolution or "",
                )
                if settings.MCD_VERBOSE_ERRORS and error.stack_trace:
                    stack_trace_filename = f"mcd_error_trace_{uuid.uuid4()}.txt"
                    with open(stack_trace_filename, "w") as stack_trace_file:
                        stack_trace_file.write(f"-----------\n{error.stack_trace}\n")
                    error_output["stack_trace"] = f"Stack trace written to {stack_trace_filename}"

                errors.append(error_output)
            output["errors"] = errors
        if validation_result.warnings:
            output["warnings"] = [
                dict(
                    message=warning.friendly_message or "",
                    cause=warning.cause or "",
                    resolution=warning.resolution or "",
                )
                for warning in validation_result.warnings
            ]

        # If there are any errors or warnings returned, display them
        if "errors" in output or "warnings" in output:
            click.echo(json.dumps(output, indent=4))

    @manage_errors
    def check_agent_health(self, agent_id):
        variables = dict(agent_id=agent_id)
        agent = self._user_service.get_agent(agent_id)
        if agent.get("agentType") != "REMOTE_AGENT":  # type: ignore
            storage_query = Query()
            storage_query.test_data_store_reachability(**variables)
            storage_result = self._mc_client(
                storage_query,
                idempotent_request_id=str(uuid.uuid4()),
                timeout_in_seconds=40,  # let Monolith timeout first
            ).test_data_store_reachability

            if not storage_result.success:
                return self._validate_response(storage_result)
            return click.echo("Agent health check succeeded!")
        else:
            variables["validation_name"] = "validate_agent_reachability"
            agent_reachability_query = Query()
            agent_reachability_query.test_agent_reachability(**variables)
            agent_reachability_result = self._mc_client(
                agent_reachability_query,
                idempotent_request_id=str(uuid.uuid4()),
                timeout_in_seconds=40,  # let Monolith timeout first
            ).test_agent_reachability

            if not agent_reachability_result.success:
                return self._validate_response(agent_reachability_result)

            variables["validation_name"] = "validate_storage_access"
            agent_storage_query = Query()
            agent_storage_query.test_agent_reachability(**variables)
            agent_storage_result = self._mc_client(
                agent_storage_query,
                idempotent_request_id=str(uuid.uuid4()),
                timeout_in_seconds=40,  # let Monolith timeout first
            ).test_agent_reachability

            if not agent_storage_result.success:
                return self._validate_response(agent_storage_result)

            if agent_reachability_result.additional_data.returned_data:
                click.echo(
                    json.dumps(
                        agent_reachability_result.additional_data.returned_data,
                        indent=4,
                    )
                )
            return click.echo("Agent health check succeeded!")

    @manage_errors
    def upgrade_agent(self, parameters: Optional[Dict] = None, **kwargs):
        image = self._choose_image(**kwargs)
        click.echo(f"Upgrading agent with image '{image}'")
        variables = {
            "agent_id": kwargs["agent_id"],
            "image": image,
            "parameters": Variable("parameters"),
        }

        mutation = Mutation(parameters=GenericScalar)
        mutation.upgrade_agent(**variables)
        result = self._mc_client(
            mutation,
            variables={
                "parameters": parameters,
            },
            idempotent_request_id=str(uuid.uuid4()),
            timeout_in_seconds=40,  # let Monolith timeout first
        ).upgrade_agent

        click.echo("Upgrade succeeded!")
        if result.upgrade_result:
            click.echo(json.dumps(result.upgrade_result, indent=4))

    @manage_errors
    def echo_operation_logs(self, **kwargs):
        parameters = ConditionalDictionary(lambda x: x is not None)
        parameters.update(kwargs)  # don't send null values
        logs_query = Query()
        logs_query.get_agent_operation_logs(**parameters)
        result = self._mc_client(
            logs_query,
            idempotent_request_id=str(uuid.uuid4()),
            timeout_in_seconds=40,  # let Monolith timeout first
        )
        logs_results = result.get_agent_operation_logs

        events = [
            {
                "timestamp": event.timestamp,
                "commands": event.payload.get("commands") if event.payload else {},  # type: ignore
                "operation_name": (event.payload.get("operation_name") if event.payload else None),  # type: ignore
                "trace_id": event.payload.get("trace_id") if event.payload else None,  # type: ignore
            }
            for event in logs_results
        ]
        click.echo(json.dumps(events, indent=4))

    @manage_errors
    def echo_aws_upgrade_logs(self, start_time: Optional[str], **kwargs):
        parameters = ConditionalDictionary(lambda x: x is not None)
        parameters["start_time"] = (
            start_time
            or (datetime.now() - timedelta(hours=12)).astimezone(timezone.utc).isoformat()
        )
        parameters.update(kwargs)  # don't send null values
        query = Query()
        query.get_aws_agent_upgrade_logs(**parameters)
        result = self._mc_client(
            query,
            idempotent_request_id=str(uuid.uuid4()),
            timeout_in_seconds=40,  # let Monolith timeout first
        )
        results = result.get_aws_agent_upgrade_logs

        events = [
            {
                "timestamp": event.timestamp,
                "logical_resource_id": event.logical_resource_id,
                "resource_status": event.resource_status,
                "resource_status_reason": event.resource_status_reason,
                "resource_type": event.resource_type,
            }
            for event in results
        ]
        click.echo(json.dumps(events, indent=4))

    @manage_errors
    def echo_aws_template(self, **kwargs):
        infra_details = self._get_aws_infra_details(**kwargs)
        click.echo(infra_details.get("template"))

    @manage_errors
    def echo_aws_template_parameters(self, **kwargs):
        infra_details = self._get_aws_infra_details(**kwargs)
        parameters = infra_details.get("parameters")
        click.echo(json.dumps(parameters, indent=4))

    def _get_aws_infra_details(self, **kwargs) -> Dict:
        parameters = ConditionalDictionary(lambda x: x is not None)
        parameters.update(kwargs)  # don't send null values
        query = Query()
        query.get_aws_agent_infra_details(**parameters)
        result = self._mc_client(
            query,
            idempotent_request_id=str(uuid.uuid4()),
            timeout_in_seconds=40,  # let Monolith timeout first
        )
        result = result.get_aws_agent_infra_details
        return {
            "template": result.template,
            "parameters": result.parameters,
        }

    def _choose_image(self, agent_id: str, image_tag: Optional[str] = None) -> Optional[str]:
        platform = self._user_service.get_agent(agent_id).get("platform")  # type: ignore

        if not image_tag or not self._validate_tag(image_tag):
            versions = self._get_recent_versions(platform)  # type: ignore
            image_tag = (
                versions[0]
                if not image_tag
                else questionary.select("Please choose a valid image tag", choices=versions).ask()
            )

        if not image_tag:
            # user canceled prompt without choosing a response
            raise click.Abort()

        if platform == "AWS":
            if image_tag.endswith("-lambda"):
                image_tag = image_tag[: -len("-lambda")]
            return f"{self._ecr_image_name}:{image_tag}"
        return f"{self._image_name}:{image_tag}"

    def _validate_tag(self, image_tag: str) -> bool:
        image_path = f"{self._image_org}/repositories/{self._image_repo}"
        url: str = urljoin(DOCKER_HUB_TAGS_BASE_URL, f"{image_path}/tags/{image_tag}")
        response = requests.head(url=url)
        return response.status_code == 200

    def _get_recent_versions(self, platform: str) -> List[str]:
        image_path = f"{self._image_org}/repositories/{self._image_repo}"
        url = urljoin(
            DOCKER_HUB_TAGS_BASE_URL,
            f"{image_path}/tags?page_size={DOCKER_TAGS_REQUEST_PAGE_SIZE}&page=1",
        )
        response = requests.get(url=url)
        response.raise_for_status()
        payload: DockerTagsResult = DockerTagsResult.from_json(response.text)
        image_variant = (
            "cloudrun"
            if platform == "GCP"
            else ("azure" if platform == "AZURE" else "lambda" if platform == "AWS" else "generic")
        )
        click.echo(f"Defaulting to {image_variant} image variants because platform is {platform}")
        payload.results = list(
            filter(
                lambda t: image_variant in t.name and "latest" not in t.name,
                payload.results,
            )
        )[:DEFAULT_TAGS_PAGE_SIZE]
        payload.results.sort(key=lambda t: t.last_updated, reverse=True)
        return [tag.name for tag in payload.results]
