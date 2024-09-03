import json
import re
from typing import Any, Dict, Generator, Iterable, List, Optional

import requests
from boto3 import Session
from box import Box, BoxList
from retry import retry

import montecarlodata.settings as settings
from montecarlodata.common.common import boxify
from montecarlodata.common.data import MonolithResponse
from montecarlodata.config import Config
from montecarlodata.errors import abort_on_gql_errors, complain_and_abort, manage_errors


class Wrapper:
    def __init__(
        self,
        abort_on_error: bool = True,
        disable_handle_errors: bool = False,
    ):
        self._abort_on_error = abort_on_error
        self._disable_handle_errors = disable_handle_errors


class GqlWrapper(Wrapper):
    def __init__(self, config: Config, **kwargs):
        self._mcd_id = config.mcd_id
        self._mcd_token = config.mcd_token
        self._endpoint = config.mcd_api_endpoint

        super().__init__(**kwargs)

    def make_request(self, query: str, variables: Optional[Dict] = None) -> Optional[Dict]:
        """
        Make a GraphQl request to the MCD API. Aborts on error if set.
        """
        response = self._make_request(query=query, variables=variables)
        if response:
            if self._abort_on_error and response.get("errors"):
                complain_and_abort(response["errors"])
            return response["data"]

    def make_request_v2(
        self, query: str, operation: str, variables: Optional[Dict] = None
    ) -> MonolithResponse:
        """
        Make a GraphQl request to the MCD API and parse out the results into data and errors.
        Aborts on error if set.
        """
        response = self._make_request(
            query=query,
            variables=(self.convert_snakes_to_camels(variables) if variables else variables),
        )
        assert response, "No response from server"
        data = response.get("data", {}).get(operation)
        if data:
            try:
                data = Box(data)
            except ValueError:
                data = BoxList(data)
        parsed_response = MonolithResponse(data=data, errors=response.get("errors"))

        if self._abort_on_error:
            abort_on_gql_errors(parsed_response)
        return parsed_response

    @classmethod
    def convert_snakes_to_camels(cls, dict_: Dict) -> Dict:
        """
        Converts dictionary keys from snake_case to camelCase as Gql is very opinionated
        """
        converted = {}
        for k, v in dict_.items():
            k = re.sub(r"_([a-z])", lambda x: x.group(1).upper(), k)
            if isinstance(v, Dict):
                v = cls.convert_snakes_to_camels(v)
            converted[k] = v
        return converted

    @manage_errors
    def _make_request(self, query: str, variables: Optional[Dict] = None) -> Optional[Dict]:
        headers = {
            "x-mcd-id": self._mcd_id,
            "x-mcd-token": self._mcd_token,
            "Content-Type": "application/json",
        }
        payload = {"query": query, "variables": variables or {}}

        if settings.MCD_USER_ID_HEADER:
            headers = {"user-id": settings.MCD_USER_ID_HEADER}  # override for local development

        response = self._post(headers=headers, payload=payload)
        response.raise_for_status()

        return json.loads(response.text)

    @retry(tries=3, delay=0.2, backoff=2, max_delay=1)
    def _post(self, headers: Dict, payload: Dict) -> requests.Response:
        return requests.post(self._endpoint, json=payload, headers=headers)


class AwsClientWrapper(Wrapper):
    _CF_CREATE_WAITER = "stack_create_complete"
    _CF_UPDATE_WAITER = "stack_update_complete"
    _DEFAULT_WAITER_CONFIG = {"Delay": 5, "MaxAttempts": 720}
    _DEFAULT_CAPABILITIES = "CAPABILITY_IAM"
    _STACK_CREATE_STATE = "CREATE_COMPLETE"
    _STACK_UPDATE_STATE = "UPDATE_COMPLETE"
    # Map from the shorthand state form available in the AWS CLI to the actual states.
    _EMR_STATES = {
        "active": ["STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING", "TERMINATING"],
        "terminated": ["TERMINATED"],
        "failed": ["TERMINATED_WITH_ERRORS"],
    }

    def __init__(
        self,
        profile_name: Optional[str] = None,
        region_name: Optional[str] = None,
        session: Optional[Session] = None,
    ):
        super().__init__()

        self._session = session or self._get_session(
            profile_name=profile_name,
            region_name=region_name,
        )
        self._test_if_region_is_set()

    @property
    def region(self) -> str:
        return self._session.region_name  # type: ignore

    @manage_errors
    def get_stack_details(self, stack_id: str) -> Dict:
        """
        Retrieve stack details (description) from DC
        """
        return self._session.client("cloudformation").describe_stacks(StackName=stack_id)

    @manage_errors
    @boxify()
    def get_stack_outputs(self, stack_id: str) -> Box:
        """
        Convenience utility to retrieve stack outputs from DC
        """
        return self.get_stack_details(stack_id)["Stacks"][0]["Outputs"]

    @manage_errors
    def get_stack_parameters(self, stack_id: str) -> List[Dict]:
        """
        Convenience utility to retrieve stack parameters from the DC
        """
        return self.get_stack_details(stack_id)["Stacks"][0].get("Parameters")

    @manage_errors
    def create_stack(
        self,
        stack_name: str,
        template_link: str,
        termination_protection: bool,
        parameters: Optional[List[Dict]] = None,
    ) -> bool:
        """
        Attempts to deploy the DC stack - returning True on success
        """
        deployment_options: Dict = dict(
            StackName=stack_name,
            TemplateURL=template_link,
            Capabilities=[self._DEFAULT_CAPABILITIES],
            EnableTerminationProtection=termination_protection,
        )
        if parameters:
            deployment_options["Parameters"] = parameters

        response = self._session.client("cloudformation").create_stack(**deployment_options)
        stack_id = response["StackId"]
        return self._wait_on_stacks(
            stack_id=stack_id,
            waiter=self._CF_CREATE_WAITER,
            success_state=self._STACK_CREATE_STATE,
        )

    @manage_errors
    def upgrade_stack(self, stack_id: str, template_link: str, parameters: List[Dict]) -> bool:
        """
        Attempts to upgrade the DC stack - returning True on success
        """
        self._session.client("cloudformation").update_stack(
            StackName=stack_id,
            TemplateURL=template_link,
            Capabilities=[self._DEFAULT_CAPABILITIES],
            Parameters=parameters,
        )
        return self._wait_on_stacks(
            stack_id=stack_id,
            waiter=self._CF_UPDATE_WAITER,
            success_state=self._STACK_UPDATE_STATE,
        )

    @manage_errors
    def deploy_gateway(self, gateway_id: str, stage: str = "prod") -> None:
        """
        Attempts to deploy the DC gateway
        """
        self._session.client("apigateway").create_deployment(restApiId=gateway_id, stageName=stage)

    @manage_errors
    def upload_file(self, bucket_name: str, object_name: str, file_path: str) -> None:
        """
        Upload a file to s3://bucket/object from file path
        """
        self._session.client("s3").upload_file(file_path, bucket_name, object_name)

    @manage_errors
    def create_role(self, role_name: str, trust_policy: str, tags: Optional[List] = None) -> str:
        """
        Creates an IAM role and returns the ARN on success
        """
        return self._session.client("iam").create_role(
            RoleName=role_name, AssumeRolePolicyDocument=trust_policy, Tags=tags
        )["Role"]["Arn"]

    @manage_errors
    def attach_inline_policy(self, role_name: str, policy_name: str, policy_doc: str) -> str:
        """
        Attaches an inline policy to an existing role
        """
        return self._session.client("iam").put_role_policy(
            RoleName=role_name, PolicyName=policy_name, PolicyDocument=policy_doc
        )

    @manage_errors
    def get_emr_cluster_details(
        self, created_after: Optional[str] = None, states: Optional[List] = None
    ) -> Iterable[Dict]:
        """
        Retrieves details of all EMR clusters
        """
        client = self._session.client("emr")
        params = {}
        if created_after:
            params["CreatedAfter"] = created_after
        if states:
            params["ClusterStates"] = [
                mapped for state in states for mapped in self._EMR_STATES[state]
            ]
        pages = client.get_paginator("list_clusters").paginate(**params)
        for page in pages:
            for cluster in page["Clusters"]:
                yield client.describe_cluster(ClusterId=cluster["Id"]).get("Cluster")

    def get_bucket_location(self, bucket_name: str) -> str:
        """
        Get the location of a bucket
        """
        region = self._session.client("s3").get_bucket_location(Bucket=bucket_name)[
            "LocationConstraint"
        ]
        if region is None:
            return (
                settings.DEFAULT_AWS_REGION
            )  # Buckets in us-east-1 have a LocationConstraint of null
        return region

    def get_caller_identity(self) -> str:
        """
        Get the Account ID of the caller
        """
        return self._session.client("sts").get_caller_identity()["Account"]

    def create_sns_topic(self, name: str) -> str:
        """
        Create an SNS topic in the account
        """
        return self._session.client("sns").create_topic(Name=name)[
            "TopicArn"
        ]  # This action is idempotent

    @boxify()
    def get_topic_attributes(self, arn: str) -> Box:
        """
        Get attributes for a topic
        """
        return self._session.client("sns").get_topic_attributes(TopicArn=arn)

    def set_topic_attributes(self, arn: str, name: str, value: str) -> None:
        """
        Set attributes for a topic
        """
        return self._session.client("sns").set_topic_attributes(
            TopicArn=arn, AttributeName=name, AttributeValue=value
        )

    @boxify()
    def get_q_attributes(self, name: str, attributes: List[str]) -> Box:
        """
        Get attributes for a sqs
        """
        return self._session.client("sqs").get_queue_attributes(
            QueueUrl=self.get_queue_url(name), AttributeNames=attributes
        )

    def set_q_attributes(self, name: str, attributes: List[str]) -> Box:
        """
        Set attributes for a sqs
        """
        return self._session.client("sqs").set_queue_attributes(
            QueueUrl=self.get_queue_url(name), Attributes=attributes
        )

    def get_queue_url(self, name: str) -> str:
        """
        Get queue url from name
        """
        return self._session.client("sqs").get_queue_url(QueueName=name)["QueueUrl"]

    @boxify()
    def get_bucket_event_config(
        self,
        name: str,
    ) -> Box:
        """
        Get existing bucket config
        """
        return self._session.client("s3").get_bucket_notification_configuration(Bucket=name)

    def set_bucket_event_config(self, name: str, notification_config: Dict) -> None:
        """
        Update bucket config
        """
        return self._session.client("s3").put_bucket_notification_configuration(
            Bucket=name, NotificationConfiguration=notification_config
        )

    def list_topic_subscriptions(self, arn: str) -> Generator[List, None, None]:
        """
        List subscriptions for a topic
        """
        for page in (
            self._session.client("sns")
            .get_paginator("list_subscriptions_by_topic")
            .paginate(TopicArn=arn)
        ):
            if page and page.get("Subscriptions"):
                yield page["Subscriptions"]

    def subscribe_to_topic(
        self,
        arn: str,
        endpoint: str,
        protocol: str = "sqs",
        attributes: Optional[Dict] = None,
    ) -> None:
        """
        Subscribe to a topic
        """
        self._session.client("sns").subscribe(
            TopicArn=arn,
            Endpoint=endpoint,
            Protocol=protocol,
            Attributes=attributes,
        )

    def upload_stream_to_s3(self, bucket: str, key: str, data: Any):
        """
        Uploads a file-like object to S3
        """
        self._session.client("s3").upload_fileobj(data, bucket, key)

    @manage_errors
    def _test_if_region_is_set(self) -> None:
        """
        Test if region is set by creating a client. Abort on failure
        """
        self._session.client("cloudformation")  # throws NoRegion if not set

    @manage_errors
    def _get_session(self, profile_name: Optional[str], region_name: Optional[str]) -> Session:
        """
        Create a session using named profile/region if set. Uses AWS defaults if not set.
        """
        session_args = (("profile_name", profile_name), ("region_name", region_name))
        # throws ProfileNotFound if invalid:
        return Session(**{k: v for k, v in session_args if v})

    def _wait_on_stacks(self, stack_id: str, waiter: str, success_state: str) -> bool:
        """
        Wait for stack to finish operation
        """
        self._session.client("cloudformation").get_waiter(waiter).wait(
            StackName=stack_id, WaiterConfig=self._DEFAULT_WAITER_CONFIG
        )
        return self.get_stack_details(stack_id)["Stacks"][0]["StackStatus"] == success_state

    @boxify(use_snakes=True, default_box_attr=None, default_box=True)
    def describe_redshift_cluster(self, cluster_id: str) -> Box:
        return self._session.client("redshift").describe_clusters(ClusterIdentifier=cluster_id)[
            "Clusters"
        ][0]

    @boxify(use_snakes=True)
    def describe_cluster_subnet_groups(self, subnet_group: str) -> Box:
        return self._session.client("redshift").describe_cluster_subnet_groups(
            ClusterSubnetGroupName=subnet_group
        )["ClusterSubnetGroups"][0]

    @boxify(use_snakes=True)
    def describe_vpc(self, vpc_id: str) -> Box:
        return self._session.client("ec2").describe_vpcs(VpcIds=[vpc_id])["Vpcs"][0]

    @boxify(use_snakes=True)
    def describe_routes(
        self,
        filter_vals: List[str],
        filter_key: str = "association.subnet-id",
    ) -> Box:
        return self._session.client("ec2").describe_route_tables(
            Filters=[{"Name": filter_key, "Values": filter_vals}]
        )

    @boxify(use_snakes=True, default_box_attr=None, default_box=True)
    def get_glue_tables(self, database_name: str, page_token: Optional[str] = None):
        return self._session.client("glue").get_tables(
            DatabaseName=database_name,
            **(dict(NextToken=page_token) if page_token else {}),
        )

    @boxify(use_snakes=True, default_box_attr=None, default_box=True)
    def get_athena_workgroup(self, workgroup: str):
        return self._session.client("athena").get_work_group(WorkGroup=workgroup)["WorkGroup"]
