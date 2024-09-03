import copy
import csv
import dataclasses
import json
import secrets
import time
from collections import OrderedDict
from typing import Dict, Iterable, List, Optional, Union, cast

import click
from box import Box, BoxList
from tabulate import tabulate

from montecarlodata.agents.agent import (
    LAMBDA_ARN_REGEX,
    LAMBDA_ARN_REGEX_GROUP_ACCOUNT_ID,
)
from montecarlodata.agents.fields import AWS
from montecarlodata.common.common import struct_match
from montecarlodata.common.data import (
    AWSArn,
    BucketRow,
    DcResourceProperties,
    EventProperties,
    ResourceProperties,
)
from montecarlodata.common.user import UserService
from montecarlodata.config import Config
from montecarlodata.errors import complain_and_abort, manage_errors, prompt_connection
from montecarlodata.queries.onboarding import EventOnboardingDataQueries
from montecarlodata.utils import AwsClientWrapper, GqlWrapper


class CloudResourceService:
    MCD_EVENT_TYPE_MD = "metadata"
    MCD_EVENT_TYPE_QL = "query-logs"
    MCD_EVENT_TYPE_FRIENDLY_TO_COLLECTOR_OUTPUT_MAP = {
        MCD_EVENT_TYPE_MD: "MetadataEventQueue",
        MCD_EVENT_TYPE_QL: "QueryLogEventQueue",
    }
    MCD_EVENT_Q_SENDER_ID = "__sender"

    _MCD_TOPIC_NAME_PATTERN = "monte-carlo-data-{}-{}-events-topic"
    _MCD_TOPIC_POLICY_SID_PATTERN = "monte-carlo-data-{}-{}-ID"
    _MCD_EVENT_ID_PATTERN = "{}{}{}-notification"

    _MCD_ROLE_PREFIX = "monte-carlo-integration-role"
    _MCD_POLICY_PREFIX = "monte-carlo-integration-cli-policy"
    _MCD_TAGS = [{"Key": "MonteCarloData", "Value": ""}]
    _LIST_EMR_FRIENDLY_HEADERS = ["Name", "Id", "State", "LogUri"]

    def __init__(
        self,
        config: Config,
        user_service: Optional[UserService] = None,
        aws_wrapper: Optional[AwsClientWrapper] = None,
        aws_profile_override: Optional[str] = None,
        aws_region_override: Optional[str] = None,
        request_wrapper: Optional[GqlWrapper] = None,
    ):
        self._abort_on_error = True
        self._profile = aws_profile_override
        self._region = aws_region_override
        self._request_wrapper = request_wrapper or GqlWrapper(config)

        self._user_service = user_service or UserService(
            config=config, request_wrapper=self._request_wrapper
        )
        self._aws_wrapper = aws_wrapper or AwsClientWrapper(
            profile_name=self._profile, region_name=self._region
        )

    @manage_errors
    def create_role(
        self,
        path_to_policy_doc: str,
        dc_id: Optional[str] = None,
        agent_id: Optional[str] = None,
    ) -> None:
        """
        Creates a DC compatible role from the provided policy doc
        """
        current_time = str(time.time())
        external_id = self._generate_random_token()
        role_name = f"{self._MCD_ROLE_PREFIX}-{current_time}"
        policy_name = f"{self._MCD_POLICY_PREFIX}-{current_time}"

        try:
            policy = json.dumps(self._read_json(path_to_policy_doc))
        except json.decoder.JSONDecodeError as err:
            complain_and_abort(f"The provided policy is not valid JSON - {err}")
        else:
            account_id = self._get_aws_account_id(dc_id=dc_id, agent_id=agent_id)
            trust_policy = self._generate_trust_policy(
                account_id=account_id, external_id=external_id
            )
            role_arn = self._aws_wrapper.create_role(
                role_name=role_name, trust_policy=trust_policy, tags=self._MCD_TAGS
            )
            click.echo(f"Created role with ARN - '{role_arn}' and external id - '{external_id}'.")

            self._aws_wrapper.attach_inline_policy(
                role_name=role_name, policy_name=policy_name, policy_doc=policy
            )
            click.echo("Success! Attached provided policy.")

    def _get_aws_account_id(
        self, dc_id: Optional[str] = None, agent_id: Optional[str] = None
    ) -> str:
        """
        Returns the AWS account id of the collector's agent if collector has a remote AWS agent.
        Otherwise, returns the AWS account id of the collector itself.
        """
        if agent_id and not dc_id:
            agent_dict = self._user_service.get_agent(agent_id=agent_id)
            dc_id = agent_dict["dc_id"]  # type: ignore

        collector = self._user_service.get_collector(dc_id=dc_id)
        if not collector.active:
            complain_and_abort("The collector is not active")
        agent = self._user_service.get_collector_agent(dc_id=dc_id)

        if agent:
            matches = LAMBDA_ARN_REGEX.match(agent.endpoint)
            if agent.platform != AWS:
                complain_and_abort("The collector is configured with a non-AWS agent")
            elif matches:
                try:
                    return matches.group(LAMBDA_ARN_REGEX_GROUP_ACCOUNT_ID)
                except IndexError:
                    pass

        if not collector.customerAwsAccountId:
            complain_and_abort(
                "Missing expected property (customerAwsAccountId). "
                "The collector may not have been deployed before"
            )
        return collector.customerAwsAccountId

    @staticmethod
    def _generate_trust_policy(account_id: str, external_id: str) -> str:
        """
        Generates a DC compatible trust policy
        """
        return json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": f"arn:aws:iam::{account_id}:root"},
                        "Action": "sts:AssumeRole",
                        "Condition": {"StringEquals": {"sts:ExternalId": external_id}},
                    }
                ],
            }
        )

    @staticmethod
    def _generate_random_token(length: Optional[int] = 16) -> str:
        """
        Generates a random token (e.g. for the external id)
        """
        return secrets.token_urlsafe(length)

    @staticmethod
    def _read_json(path: str) -> Dict:
        """
        Reads a JSON file from the path.
        """
        with open(path) as file:
            return json.load(file)  # loads for the purpose of validating

    @manage_errors
    def list_emr_clusters(
        self,
        only_log_locations: bool = False,
        created_after: Optional[str] = None,
        states: Optional[List] = None,
        no_grid: bool = False,
        headers: str = "firstrow",
        table_format: str = "fancy_grid",
    ):
        """
        Displays information about EMR cluster (name, id, state and log location).
        If only_log_locations is True,
        displays a deduplicated list of EMR log locations.
        """
        clusters = self._aws_wrapper.get_emr_cluster_details(
            created_after=created_after, states=states
        )
        if only_log_locations:
            self._list_emr_clusters_only_log_locations(clusters)
        elif no_grid:
            self._list_emr_clusters_details_no_wait(clusters)
        else:
            self._list_emr_cluster_details(clusters, headers=headers, table_format=table_format)

    def _list_emr_cluster_details(
        self,
        clusters: Iterable[Dict],
        headers: str = "firstrow",
        table_format: str = "fancy_grid",
    ):
        rows = [self._emr_details_row(cluster) for cluster in clusters if cluster]
        click.echo(
            tabulate(
                [self._LIST_EMR_FRIENDLY_HEADERS] + rows,
                headers=headers,
                tablefmt=table_format,
            )
        )

    def _list_emr_clusters_details_no_wait(self, clusters: Iterable[Dict]):
        """
        Display information row by row, used when it can take long to wait for all data
        to format the table.
        """
        found = False
        for cluster in clusters:
            if not cluster:  # Should never happen.
                continue
            found = True
            # Prints line by line to show progress as the list may be long.
            click.echo(
                tabulate([self._emr_details_row(cluster)], tablefmt="plain", maxcolwidths=100)
            )
        if not found:
            click.echo("No clusters found")

    @staticmethod
    def _list_emr_clusters_only_log_locations(clusters: Iterable[Dict]):
        log_locations = set()
        for cluster in clusters:
            if not cluster:  # Should never happen.
                continue
            log_locations.add(cluster.get("LogUri"))
        if not log_locations:
            click.echo("No clusters found")
        else:
            for location in log_locations:
                click.echo(location)

    @staticmethod
    def _emr_details_row(cluster: Dict) -> List[Optional[str]]:
        return [
            cluster["Name"],
            cluster["Id"],
            cluster["Status"]["State"],
            cluster.get("LogUri"),
        ]

    @manage_errors
    def add_events(self, dc_id: Optional[str] = None, auto_yes: bool = False, **kwargs) -> None:
        """
        Adds events for bucket in a repeatable manner (e.g. can be re-run on error in
        between steps) -
            1. Derives collection and resource related properties
            2. Creates topic or updates topic policy if necessary
            3. Updates (or sets) SQS policy with topic if not already set
            4. Subscribes queue to topic if not already done
            5. Creates notification if not already set

        All these steps are idempotent, so if if a permission or resource already exists it will not
        be duplicated.

        This is done via SDK rather than CloudFormation (CF) as CF does not support updating
        notification settings, SQS policies or topic policies for existing resources without using a
        custom resources (i.e. lambda functions), which is a less flexible variant of the actions
        performed here as it is bound by CF inputs and any steps and would still use an AWS SDK,
        without providing any of the common benefits of IaC. Additionally, cross-region and
        cross-account operations are non-trivial in CF, especially in a generic way without prior
        knowledge of the environment.

        Note - the only newly created resource (i.e. SNS topic) can be created externally and
        passed as an option.
        """
        click.echo("[Gathering collector and resource details]")
        collector = self.get_and_validate_active_collector(dc_id=dc_id)
        event_details = self._initialize_events(collector_props=collector, **kwargs)
        self._create_or_update_event_topic(event_details=event_details, skip_prompts=auto_yes)
        self._update_event_q(event_details=event_details, skip_prompts=auto_yes)
        self._create_topic_subscription(event_details=event_details, skip_prompts=auto_yes)
        self._create_event_notification(event_details=event_details, skip_prompts=auto_yes)
        click.echo("\nProcess complete. Have a nice day!")

    def _initialize_events(
        self,
        event_type: str,
        collector_props: Box,
        collector_aws_profile: Optional[str] = None,
        bucket_aws_profile: Optional[str] = None,
        buckets_filename: Optional[str] = None,
        bucket_name: Optional[str] = None,
        prefix: Optional[str] = None,
        suffix: Optional[str] = None,
        **kwargs,
    ) -> EventProperties:
        """
        Collect (or lookup) any relevant event related properties and initialize any clients.

        Note - modifies collector_props by adding `outputs`.
        """
        collector_arn = AWSArn(id=collector_props.stackArn)
        collector_profile = collector_aws_profile or self._profile
        cust_resources_profile = bucket_aws_profile or self._profile

        # Create a client for collector resources and any customer resources.
        # This is necessary as the collector resources might be in a different
        # account/region than customer resources.
        collection_client = AwsClientWrapper(
            profile_name=collector_profile, region_name=collector_arn.region
        )

        collector_props.outputs = collection_client.get_stack_outputs(
            stack_id=collector_props.stackArn
        )
        event_queue_arn = self._get_relevant_q(event_type, collector_props.outputs)

        resource_properties = self._get_resource_properties(
            cust_resources_profile=cust_resources_profile,
            buckets_filename=buckets_filename,
            bucket_name=bucket_name,
            prefix=prefix,
            suffix=suffix,
            **kwargs,
        )

        return EventProperties(
            event_type=event_type,
            event_queue_arn=event_queue_arn,  # type: ignore (assume this won't be None here)
            collection_region=collector_arn.region,
            collection_account_id=collector_arn.account,
            collection_client=collection_client,
            resource_properties=resource_properties,
        )

    def create_event_topic(self, dc_id: Optional[str] = None, auto_yes: bool = False, **kwargs):
        collector = self.get_and_validate_active_collector(dc_id=dc_id)
        event_details = self._populate_event_details_from_saved_config(collector, **kwargs)
        self._create_or_update_event_topic(event_details=event_details, skip_prompts=auto_yes)

    def create_bucket_side_event_infrastructure(
        self, dc_id: Optional[str] = None, auto_yes: bool = False, **kwargs
    ):
        collector = self.get_and_validate_active_collector(dc_id=dc_id)
        event_details = self._populate_event_details_from_saved_config(collector, **kwargs)
        self._create_topic_subscription(event_details=event_details, skip_prompts=auto_yes)
        self._create_event_notification(event_details=event_details, skip_prompts=auto_yes)

    def update_event_queue(self, dc_id: Optional[str] = None, auto_yes: bool = False, **kwargs):
        collector = self.get_and_validate_active_collector(dc_id=dc_id)
        event_details = self._populate_event_details_from_saved_config(
            collector, collection_client=True, **kwargs
        )
        self._update_event_q(event_details=event_details, skip_prompts=auto_yes)

    def save_initial_event_properties(self, dc_id: Optional[str] = None, **kwargs):
        collector = self.get_and_validate_active_collector(dc_id=dc_id)
        event_details = self._populate_event_details_from_saved_config(
            collector, collection_client=True, **kwargs
        )
        self._save_event_properties(event_details)

    def delete_event_properties(self):
        self._delete_event_properties()

    def _get_clients_for_buckets(self, buckets, cust_resources_profile):
        region_to_client = {}
        bucket_to_client = {}
        temp_client = AwsClientWrapper(profile_name=cust_resources_profile)
        for bucket in buckets:
            bucket_name = bucket["bucket_name"]
            region = temp_client.get_bucket_location(bucket_name=bucket_name)
            if region not in region_to_client:
                region_to_client[region] = AwsClientWrapper(
                    profile_name=cust_resources_profile, region_name=region
                )

            bucket_to_client[bucket_name] = region_to_client[region]

        return bucket_to_client

    def parse_buckets_file(self, buckets_filename: str) -> List[BucketRow]:
        buckets_file = csv.DictReader(open(buckets_filename))
        return [
            BucketRow(
                bucket_name=row["bucket_name"],
                prefix=row.get("prefix"),
                suffix=row.get("suffix"),
            )
            for row in buckets_file
        ]

    def _get_resource_properties(
        self,
        cust_resources_profile: Optional[str] = None,
        buckets_filename: Optional[str] = None,
        bucket_name: Optional[str] = None,
        prefix: Optional[str] = None,
        suffix: Optional[str] = None,
        **kwargs,
    ) -> List[ResourceProperties]:
        if not buckets_filename and not bucket_name:
            return []

        if bucket_name:
            args = {}
            if prefix:
                args["prefix"] = prefix
            if suffix:
                args["suffix"] = suffix
            buckets = [{"bucket_name": bucket_name, **args}]

        if buckets_filename:
            bucket_rows = self.parse_buckets_file(buckets_filename)
            buckets = [dataclasses.asdict(bucket_row) for bucket_row in bucket_rows]

        bucket_clients = self._get_clients_for_buckets(buckets, cust_resources_profile)

        return [
            ResourceProperties(
                bucket_name=bucket["bucket_name"],
                client=bucket_clients[bucket["bucket_name"]],
                region=bucket_clients[bucket["bucket_name"]].region,
                account_id=bucket_clients[bucket["bucket_name"]].get_caller_identity(),
                prefix=bucket.get("prefix"),
                suffix=bucket.get("suffix"),
                **kwargs,
            )
            for bucket in buckets
        ]

    def _populate_event_details_from_saved_config(
        self,
        collector_props: Box,
        event_type: Optional[str] = None,
        collector_aws_profile: Optional[str] = None,
        bucket_aws_profile: Optional[str] = None,
        collection_client: bool = False,
        buckets_filename: Optional[str] = None,
        bucket_name: Optional[str] = None,
        prefix: Optional[str] = None,
        suffix: Optional[str] = None,
        **kwargs,
    ):
        collector_arn = AWSArn(id=collector_props.stackArn)
        collector_profile = collector_aws_profile or self._profile
        cust_resources_profile = bucket_aws_profile or self._profile

        response = self._request_wrapper.make_request_v2(
            query=EventOnboardingDataQueries.get.query,
            operation=EventOnboardingDataQueries.get.operation,
        )

        config = {"resource_properties": []}
        if response.data:
            config = json.loads(response.data.config)  # type: ignore
            if not collection_client:
                bucket_clients = self._get_clients_for_buckets(
                    config["resource_properties"], cust_resources_profile
                )
            config["resource_properties"] = [
                ResourceProperties(
                    bucket_name=bucket["bucket_name"],
                    client=(
                        bucket_clients[bucket["bucket_name"]] if not collection_client else None
                    ),
                    region=bucket.get("region"),
                    account_id=bucket.get("account_id"),
                    prefix=bucket.get("prefix"),
                    suffix=bucket.get("suffix"),
                    topic_arn=bucket.get("topic_arn"),
                    topic_name=bucket.get("topic_name"),
                )
                for bucket in config["resource_properties"]
            ]

        if not event_type and (config_event_type := config.get("event_type")):
            event_type = cast(str, config_event_type)

        resource_properties = self._get_resource_properties(
            cust_resources_profile=cust_resources_profile,
            buckets_filename=buckets_filename,
            bucket_name=bucket_name,
            prefix=prefix,
            suffix=suffix,
            **kwargs,
        )

        if collection_client:
            aws_client = AwsClientWrapper(
                profile_name=collector_profile, region_name=collector_arn.region
            )
            collector_props.outputs = aws_client.get_stack_outputs(
                stack_id=collector_props.stackArn
            )
            aws_options = dict(
                collection_account_id=aws_client.get_caller_identity(),
                collection_client=aws_client,
                event_queue_arn=config.get("event_queue_arn")
                or self._get_relevant_q(
                    event_type,  # type: ignore
                    collector_props.outputs,
                ),
            )

        else:
            aws_options = dict(
                collection_client=None,
                collection_account_id=config.get("collection_account_id"),
            )

        overrides = {
            "event_type": event_type,
            "resource_properties": resource_properties,
            "collection_region": collector_arn.region,
        }

        event_args = {
            **config,
            **aws_options,
            **{key: value for key, value in overrides.items() if value},
        }

        return EventProperties(
            **event_args,
        )

    def _save_event_properties(self, event_properties: EventProperties):
        config = event_properties.serializable()
        self._request_wrapper.make_request_v2(
            query=EventOnboardingDataQueries.save.query,
            operation=EventOnboardingDataQueries.save.operation,
            variables={"config": json.dumps(config)},
        )

    def _delete_event_properties(self):
        self._request_wrapper.make_request_v2(
            query=EventOnboardingDataQueries.delete.query,
            operation=EventOnboardingDataQueries.delete.operation,
        )

    def _create_or_update_event_topic(
        self, event_details: EventProperties, skip_prompts: bool = False
    ) -> None:
        """
        Creates topic (if necessary) and/or updates policy to allow access from bucket
        (put) & event queue (subscribe)
        """
        click.echo("[Creating or updating topic properties]")
        q_topic_policy_id = self._MCD_TOPIC_POLICY_SID_PATTERN.format(
            event_details.event_type, event_details.event_queue_name
        )

        region_to_resources = {}
        for resource in event_details.resource_properties:
            region = resource.region
            if region not in region_to_resources:
                region_to_resources[region] = []

            region_to_resources[region].append(resource)

        for region, resources in region_to_resources.items():
            new_statements = []
            update_statements = {}
            region_bucket_arns = [resource.bucket_arn for resource in resources]

            # Create an event topic if an MC one does not already exist in the region
            # for the queue type
            topic_arn = resources[0].topic_arn
            if topic_arn is None:
                name = self._MCD_TOPIC_NAME_PATTERN.format(region, event_details.event_type)
                prompt = (
                    f"Create topic '{name}' in '{region}' "
                    f"({resources[0].account_id})? This is an idempotent operation."
                )
                prompt_connection(message=prompt, skip_prompt=skip_prompts)
                topic_arn = resources[0].client.create_sns_topic(name=name)

            for resource in resources:
                resource.topic_arn = topic_arn
                resource.topic_name = self._validate_topic_arn(resource_details=resource).resource

            topic_attributes = resources[0].client.get_topic_attributes(arn=resource.topic_arn)

            region_bucket_topic_policy_id = self._MCD_TOPIC_POLICY_SID_PATTERN.format(
                event_details.event_type, resources[0].topic_name
            )

            existing_bucket_policy = self._get_preexisting_topic_policy(
                topic_policy_id=region_bucket_topic_policy_id,
                topic_attributes=topic_attributes,
            )

            if not existing_bucket_policy:
                new_statements.append(
                    self._generate_sns_bucket_policy(
                        sid=region_bucket_topic_policy_id,
                        topic_arn=topic_arn,
                        bucket_arns=region_bucket_arns,
                    )
                )
            else:
                update_statements[region_bucket_topic_policy_id] = self._extend_sns_bucket_policy(
                    existing_policy=existing_bucket_policy,
                    bucket_arns=region_bucket_arns,
                )

            if not self._is_preexisting_topic_policy(
                topic_policy_id=q_topic_policy_id, topic_attributes=topic_attributes
            ):
                new_statements.append(
                    self._generate_sns_q_policy(
                        sid=q_topic_policy_id,
                        event_details=event_details,
                        topic_arn=topic_arn,
                    )
                )

            # Update policy if necessary

            new_statement_count = len(new_statements) + len(update_statements)
            if new_statement_count > 0:
                click.echo(
                    f"Adding statements (x{new_statement_count}) to '{resources[0].topic_name}' -"
                )
                click.echo(json.dumps([*new_statements, *update_statements.values()], indent=4))
                prompt_connection(message="Please confirm", skip_prompt=skip_prompts)
                resources[0].client.set_topic_attributes(
                    arn=topic_arn,
                    name="Policy",
                    value=self._extend_topic_policy(
                        topic_attributes=topic_attributes,
                        new_statements=new_statements,
                        update_statements=update_statements,
                    ),
                )
            else:
                click.echo(f"No policy changes required for '{resources[0].topic_name}'")

        self._save_event_properties(event_details)

    def _update_event_q(
        self,
        event_details: EventProperties,
        skip_prompts: bool = False,
    ) -> None:
        """
        Create or update queue policy with topic
        """
        click.echo("[Updating event queue properties]")
        attributes = (
            event_details.collection_client.get_q_attributes(
                name=event_details.event_queue_name, attributes=["Policy"]
            )
            or {}
        )

        topic_arns = list(
            OrderedDict.fromkeys(
                [resource.topic_arn for resource in event_details.resource_properties]
            ).keys()
        )

        if not attributes.get("Attributes", {}).get("Policy"):
            # No policy set (e.g. is a new q). Create
            new_policy = self._generate_q_policy(
                event_details=event_details,
                topic_arns=topic_arns,  # type: ignore (assume all topic arns are valid)
            )
            click.echo(f"No existing policy found. Adding -\n{json.dumps(new_policy, indent=4)}")
        else:
            # Update existing if necessary
            existing_policy = Box(json.loads(attributes.Attributes.Policy))  # type: ignore
            new_policy = self._extend_q_policy(
                existing_policy=existing_policy,
                event_details=event_details,
                topic_arns=topic_arns,  # type: ignore (assume all topic arns are valid)
            )
            if struct_match(
                existing_policy,
                new_policy,  # type: ignore (assume this won't be None here)
            ):
                click.echo(f"Policy for '{event_details.event_queue_name}' requires no changes")
                return
            click.echo(f"Updating existing policy into -\n{json.dumps(new_policy, indent=4)}")

        prompt_connection(message="Please confirm", skip_prompt=skip_prompts)
        event_details.collection_client.set_q_attributes(
            name=event_details.event_queue_name,
            attributes=dict(Policy=json.dumps(new_policy)),
        )

        self._save_event_properties(event_details)

    def _create_event_notification(
        self, event_details: EventProperties, skip_prompts: bool = False
    ) -> None:
        """
        Creates event notification if one does not already exist for the topic
        """
        click.echo("[Creating event notification]")
        for resource in event_details.resource_properties:
            setting_id = self._MCD_EVENT_ID_PATTERN.format(
                AWSArn(id=resource.topic_arn).resource.lower(),  # type: ignore
                f"-{resource.prefix}" if resource.prefix else "",
                f"-{resource.suffix}" if resource.suffix else "",
            )

            # Get existing notification settings for bucket as AWS does not support inserting
            # (only a put operation)
            existing_config = resource.client.get_bucket_event_config(name=resource.bucket_name)
            existing_config.pop("ResponseMetadata", None)
            message = (
                "Updating existing notifications into -"
                if existing_config
                else "Creating notification -"
            )

            new_config = self._merge_event_notifications(
                setting_id=setting_id,
                existing_config=existing_config,
                resource_details=resource,
            )

            if struct_match(existing_config, new_config):
                click.echo(f"Notifications for '{resource.bucket_name}' require no changes")
                continue

            click.echo(f"{message}\n{json.dumps(new_config, indent=4)}")
            prompt_connection(message="Please confirm", skip_prompt=skip_prompts)
            resource.client.set_bucket_event_config(
                name=resource.bucket_name, notification_config=new_config
            )

        self._save_event_properties(event_details)

    def _create_topic_subscription(
        self, event_details: EventProperties, skip_prompts: bool = False
    ) -> None:
        """
        Create topic subscription for endpoint if one for the endpoint already does not exist
        """
        click.echo("[Creating topic subscription]")
        for resource in event_details.resource_properties:
            should_continue = False
            subscriptions = list(
                resource.client.list_topic_subscriptions(arn=resource.topic_arn) or []
            )
            if subscriptions:
                for subscription in [sub for sublist in subscriptions for sub in sublist]:
                    if subscription.get("Endpoint") == event_details.event_queue_arn:
                        click.echo(
                            f"Subscription for '{event_details.event_queue_name}' already exists"
                        )
                        should_continue = True
            if should_continue:
                continue
            click.echo(
                f"Creating subscription for '{event_details.event_queue_name}'"
                f" on '{resource.topic_name}'"
            )
            prompt_connection(message="Please confirm", skip_prompt=skip_prompts)
            resource.client.subscribe_to_topic(
                arn=resource.topic_arn,
                endpoint=event_details.event_queue_arn,
                attributes=dict(RawMessageDelivery="true"),
            )

        self._save_event_properties(event_details)

    def _get_relevant_q(self, event_type: str, outputs: Union[Box, BoxList]) -> None:
        """
        Get queue arn from stack outputs based on the friendly key.

        Raise error if not found (e.g. events are not enabled)
        """
        if (
            not event_type
            or event_type not in self.MCD_EVENT_TYPE_FRIENDLY_TO_COLLECTOR_OUTPUT_MAP.keys()
        ):
            complain_and_abort(f"Event type was not defined or is invalid '{event_type}'.")

        for output in outputs:
            if output.OutputKey == self.MCD_EVENT_TYPE_FRIENDLY_TO_COLLECTOR_OUTPUT_MAP[event_type]:
                return output.OutputValue
        complain_and_abort(
            "Failed to find the relevant event queue. Are events enabled for this collector?"
        )

    @staticmethod
    def _validate_topic_arn(resource_details: ResourceProperties) -> AWSArn:
        """
        Sanity check ARN (e.g. if provided) is valid (format + region)
        """
        try:
            arn = AWSArn(
                id=resource_details.topic_arn,  # type: ignore
            )
        except IndexError:
            complain_and_abort(f"SNS topic ARN '{resource_details.topic_arn}' is not a valid.")
        else:
            if resource_details.region != arn.region:
                complain_and_abort(
                    f"Topic region ({arn.region}) does not match bucket "
                    f"region ({resource_details.region})"
                )
        return arn

    @staticmethod
    def _get_preexisting_topic_policy(topic_policy_id: str, topic_attributes: Box) -> Optional[Box]:
        for statement in Box(json.loads(topic_attributes.Attributes.Policy)).Statement:
            if statement.get("Sid") and statement.Sid == topic_policy_id:
                return statement
        return None

    def _is_preexisting_topic_policy(self, topic_policy_id: str, topic_attributes: Box) -> bool:
        """
        Check if a policy for this bucket already exists. Returns true if it does.
        """
        return bool(self._get_preexisting_topic_policy(topic_policy_id, topic_attributes))

    @staticmethod
    def _extend_topic_policy(
        topic_attributes: Box, new_statements: List[Dict], update_statements: Dict
    ) -> str:
        """
        Merge new statements with existing
        """
        policy = Box(json.loads(topic_attributes.Attributes.Policy))
        new_policy_statement = []
        for statement in policy.Statement:
            if statement.get("Sid") and statement.Sid in update_statements:
                new_policy_statement.append(update_statements[statement.Sid])
            else:
                new_policy_statement.append(statement)
        new_policy_statement.extend(new_statements)
        policy.Statement = new_policy_statement
        return json.dumps(policy)

    @staticmethod
    def _extend_sns_bucket_policy(existing_policy: Box, bucket_arns: List[str]) -> Optional[Dict]:
        """
        Merge new statements with existing
        """
        new_policy = Box(copy.deepcopy(existing_policy))
        source_arn = (
            new_policy.get("Condition", {}).get("StringEquals", {}).get("aws:SourceArn", [])
        )
        if source_arn:
            # Source ARN can be a list or string :/
            if not isinstance(source_arn, List):
                source_arn = [source_arn]

            source_arn = list(OrderedDict.fromkeys([*source_arn, *bucket_arns]).keys())

            new_policy.Condition.StringEquals["aws:SourceArn"] = source_arn
            if len(source_arn) == 1:
                new_policy.Condition.StringEquals["aws:SourceArn"] = source_arn[0]
            return new_policy

        # Existing policy was create outside of tooling / different from steps outlined
        # in documentation
        complain_and_abort("Queue policy exists, but not in a supported format")
        return None

    @staticmethod
    def _extend_q_policy(
        existing_policy: Box,
        event_details: EventProperties,
        topic_arns: Optional[Iterable[str]],
    ) -> Optional[Dict]:
        """
        Merge new statements with existing
        """
        new_policy = Box(copy.deepcopy(existing_policy))
        for statement in new_policy.Statement:
            if statement.get("Sid") and statement.Sid == CloudResourceService.MCD_EVENT_Q_SENDER_ID:
                source_arn = statement.get("Condition", {}).get("ArnLike", {}).get("aws:SourceArn")
                if source_arn:
                    # Source ARN can be a list or string :/
                    list_topic_arns = list(topic_arns)  # type: ignore
                    if isinstance(source_arn, List):
                        source_arn.extend(list_topic_arns)
                    else:
                        source_arn = [source_arn, *list_topic_arns]
                    # Remove duplicates, while still retaining order/type for computing diff.
                    statement.Condition.ArnLike["aws:SourceArn"] = list(
                        OrderedDict.fromkeys(source_arn).keys()
                    )
                    if len(statement.Condition.ArnLike["aws:SourceArn"]) == 1:
                        statement.Condition.ArnLike["aws:SourceArn"] = statement.Condition.ArnLike[
                            "aws:SourceArn"
                        ][0]
                    return new_policy
        # Existing policy was create outside of tooling / different from steps
        # outlined in documentation
        complain_and_abort("Queue policy exists, but not in a supported format")
        return None

    def _merge_event_notifications(
        self,
        setting_id: str,
        existing_config: Dict,
        resource_details: ResourceProperties,
    ) -> Dict:
        """
        Merge new config with existing based on if and how topic config was set
        """
        config = copy.deepcopy(existing_config)
        if config.get("TopicConfigurations"):
            # existing topic notification configurations
            for topic in config["TopicConfigurations"]:
                if topic.get("Id") == setting_id:
                    return config  # No changes required; can short circuit
            config["TopicConfigurations"].append(
                self._generate_bucket_to_topic_config(
                    setting_id=setting_id, resource_details=resource_details
                )
            )
        else:
            # existing queue or lambda notification configurations
            config["TopicConfigurations"] = [
                self._generate_bucket_to_topic_config(
                    setting_id=setting_id, resource_details=resource_details
                )
            ]
        return config

    @staticmethod
    def _generate_bucket_to_topic_config(
        setting_id: str, resource_details: ResourceProperties
    ) -> Dict:
        return dict(
            Id=setting_id,
            TopicArn=resource_details.topic_arn,
            Events=["s3:ObjectCreated:*", "s3:ObjectRemoved:*"],
            Filter=dict(
                Key=dict(
                    FilterRules=[
                        dict(Name="Prefix", Value=resource_details.prefix or ""),
                        dict(Name="Suffix", Value=resource_details.suffix or ""),
                    ]
                )
            ),
        )

    @staticmethod
    def _generate_sns_bucket_policy(sid: str, topic_arn: str, bucket_arns: List[str]) -> Dict:
        return {
            "Sid": sid,
            "Effect": "Allow",
            "Principal": {"AWS": "*"},
            "Action": "SNS:Publish",
            "Resource": topic_arn,
            "Condition": {"StringEquals": {"aws:SourceArn": bucket_arns}},
        }

    @staticmethod
    def _generate_sns_q_policy(sid: str, event_details: EventProperties, topic_arn: str) -> Dict:
        return {
            "Sid": sid,
            "Effect": "Allow",
            "Principal": {"AWS": event_details.collection_account_id},
            "Action": "sns:Subscribe",
            "Resource": topic_arn,
        }

    @staticmethod
    def _generate_q_policy(event_details: EventProperties, topic_arns: Optional[Iterable[str]]):
        return {
            "Version": "2008-10-17",
            "Statement": list(
                filter(
                    None,
                    [
                        {
                            "Sid": "__owner",
                            "Effect": "Allow",
                            "Principal": {
                                "AWS": f"arn:aws:iam::{event_details.collection_account_id}:root"
                            },
                            "Action": "SQS:*",
                            "Resource": event_details.event_queue_arn,
                        },
                        (
                            {
                                "Sid": CloudResourceService.MCD_EVENT_Q_SENDER_ID,
                                "Effect": "Allow",
                                "Principal": {"AWS": "*"},
                                "Action": "SQS:SendMessage",
                                "Resource": event_details.event_queue_arn,
                                "Condition": {"ArnLike": {"aws:SourceArn": list(topic_arns)}},
                            }
                            if topic_arns
                            else None
                        ),
                    ],
                )
            ),
        }

    def get_and_validate_active_collector(
        self, dc_id: Optional[str] = None, agent_id: Optional[str] = None
    ) -> Box:
        """
        Get a specific collector and validate that it is active.
        """
        collector = self._user_service.get_collector(dc_id=dc_id, agent_id=agent_id)
        if not collector.stackArn or not collector.active:
            complain_and_abort("Cannot setup events for an inactive collector.")
        return collector

    def get_dc_resource_props(
        self,
        *,
        collector_props: Box,
        collector_aws_profile: Optional[str] = None,
        resource_aws_profile: Optional[str] = None,
        resource_aws_region: Optional[str] = None,
        get_stack_outputs: bool = True,
        get_stack_params: bool = True,
        **_,
    ) -> DcResourceProperties:
        """
        Collect (or lookup) DC and resource properties. Note - modifies collector_props by
        adding `outputs` and `parameters`.

        Multiple clients are created as collector resources might be in a different
        account/region than customer resources.
        """
        collector_arn = AWSArn(id=collector_props.stackArn)
        collector_profile = collector_aws_profile or self._profile
        cust_resources_profile = resource_aws_profile or self._profile

        collection_region = collector_arn.region
        resources_region = resource_aws_region or collection_region

        collection_client = AwsClientWrapper(
            profile_name=collector_profile, region_name=collection_region
        )
        resources_client = AwsClientWrapper(
            profile_name=cust_resources_profile, region_name=resources_region
        )

        if get_stack_outputs:
            collector_props.outputs = self._parse_stack_prop_list(
                key="OutputKey",
                val="OutputValue",
                struct=collection_client.get_stack_outputs(stack_id=collector_props.stackArn),
            )

        if get_stack_params:
            collector_props.parameters = self._parse_stack_prop_list(
                key="ParameterKey",
                val="ParameterValue",
                struct=collection_client.get_stack_parameters(stack_id=collector_props.stackArn),
            )

        return DcResourceProperties(
            collector_arn=collector_arn,
            collector_props=collector_props,
            collection_region=collection_region,
            resources_region=resources_region,
            collection_client=collection_client,
            resources_client=resources_client,
        )

    @staticmethod
    def _parse_stack_prop_list(key: str, val: str, struct: Union[List[Dict], Box]) -> Box:
        """
        Parse stack props into an easier to use collapsed format
        """
        return Box({row[key]: row[val] for row in struct}, camel_killer_box=True)
