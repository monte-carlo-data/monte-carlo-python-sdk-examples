from typing import Iterator, List, Optional, Tuple

import click
from tabulate import tabulate

from montecarlodata.common.common import is_overlap
from montecarlodata.common.data import AWSArn, DcResourceProperties
from montecarlodata.common.resources import CloudResourceService
from montecarlodata.errors import complain_and_abort, manage_errors
from montecarlodata.integrations.onboarding.fields import REDSHIFT_CONNECTION_TYPE


class NetworkDiscoveryService:
    MCD_NETWORK_REC_RESOURCE_TYPE_MAP = {REDSHIFT_CONNECTION_TYPE: "rs_network_recommender"}

    def __init__(
        self,
        cloud_resource_service: Optional[CloudResourceService] = None,
        *args,
        **kwargs,
    ):
        self._abort_on_error = True  # Used in decorator
        self._cloud_resource_service = cloud_resource_service or CloudResourceService(
            *args, **kwargs
        )

    @manage_errors
    def recommend_network_dispatcher(
        self, *, resource_type: str, dc_id: Optional[str] = None, **kwargs
    ) -> None:
        """
        Make a network connection recommendation by resource to help simplify steps
        required to onboard.
        """
        try:
            dispatch = self.MCD_NETWORK_REC_RESOURCE_TYPE_MAP[resource_type]
        except KeyError:
            complain_and_abort(f"Unsupported resource type - '{resource_type}'.")
        else:
            collector = self._cloud_resource_service.get_and_validate_active_collector(dc_id=dc_id)
            dc_resource_props = self._cloud_resource_service.get_dc_resource_props(
                collector_props=collector, **kwargs
            )

            getattr(self, dispatch)(dc_resource_props=dc_resource_props, **kwargs)

    def rs_network_recommender(
        self, *, dc_resource_props: DcResourceProperties, resource_identifier: str, **_
    ) -> None:
        """
        Alpha recommender for how to connect the DC with RS.

        Connections can be made with either IP filtering or peering. Peering has the following
        scenarios -
            1. Deployed without a VPC
            2. Overlapping CIDR ranges
            3. Same account peering
            4. Multi account peering

        Multi-region is supported. Transit gateways are supported, but not considered by the
        recommender as MC currently pushes peering.

        https://docs.getmontecarlo.com/docs/aws-networking-troubleshoot-guide#after-data-collector-deployment
        """
        self._check_collector_validity(dc_resource_props)

        # Retrieve properties from the redshift cluster
        dc_resource_props.resource_props = (
            dc_resource_props.resources_client.describe_redshift_cluster(
                cluster_id=resource_identifier
            )
        )
        dc_resource_props.resource_arn = AWSArn(
            dc_resource_props.resource_props.cluster_namespace_arn  # type: ignore
        )

        does_redshift_dc_share_an_account = (
            dc_resource_props.resource_arn.account == dc_resource_props.collector_arn.account
        )
        redshift_security_groups, redshift_in_vpc = self._get_rs_sg(dc_resource_props)

        redshift_vpc_range = dc_resource_props.resources_client.describe_vpc(
            dc_resource_props.resource_props.vpc_id  # type: ignore
        ).cidr_block
        collector_vpc_range = dc_resource_props.collector_props.parameters.vpc_cidr

        base_header = "The redshift cluster '{}' is{}publicly accessible."

        # Build recommendations based on the cluster's availability (public or not).
        if dc_resource_props.resource_props.publicly_accessible:  # type: ignore
            header = base_header.format(resource_identifier, " ")
            recommendations = self._get_ip_filtering_verbiage(
                dc_resource_props, redshift_security_groups
            )
        else:
            if not redshift_in_vpc:
                # Clusters deployed outside of a VPC are essentially a legacy feature and not
                # supported.
                complain_and_abort(
                    "Recommender does not support non-public clusters deployed outside a VPC."
                )

            header = base_header.format(resource_identifier, " not ")
            recommendation_intro = self._get_peering_intro_verbiage(
                does_redshift_dc_share_an_account
            )

            if is_overlap(redshift_vpc_range, collector_vpc_range):
                # This case is considered a recommendation and not an error as is a
                # "supportable path".
                recommendations = [
                    *recommendation_intro,
                    self._get_overlap_verbiage(collector_vpc_range, redshift_vpc_range),
                ]
            else:
                redshift_routes = self._get_rs_routes(dc_resource_props)

                recommendations = [
                    *recommendation_intro,
                    *self._get_rs_cross_account_verbiage(
                        dc_resource_props, does_redshift_dc_share_an_account
                    ),
                    *self._get_rs_requester_verbiage(
                        dc_resource_props,
                        does_redshift_dc_share_an_account,
                        redshift_vpc_range,
                    ),
                    *self._get_rs_accepter_verbiage(
                        dc_resource_props, redshift_security_groups, redshift_routes
                    ),
                    *self._get_extra_verbiage(),
                ]

        self._echo_recommendation([*recommendations], header_col=header)

    @staticmethod
    def _check_collector_validity(
        dc_resource_props: DcResourceProperties,
        default_unset_val: Optional[str] = "N/A",
    ) -> None:
        """
        Error on unsupported collectors (e.g. using an existing VPC).
        """
        if (
            "existing_vpc_id" in dc_resource_props.collector_props.parameters
            and dc_resource_props.collector_props.parameters.existing_vpc_id != default_unset_val
        ):
            complain_and_abort(
                "Recommender does not support collectors deployed in customer managed VPCs."
            )

    @staticmethod
    def _get_rs_sg(
        dc_resource_props: DcResourceProperties,
        expected_status: Optional[str] = "active",
    ) -> Tuple[List[str], bool]:
        """
        Parse the security groups associated with the RS cluster.

        Returns a list of SGs and a flag to indicate the cluster is deployed in a VPC (yes=True).
        """
        if all(
            prop in dc_resource_props.resource_props  # type: ignore
            for prop in ["vpc_security_groups", "cluster_security_groups"]
        ):
            if (
                len(dc_resource_props.resource_props.vpc_security_groups) > 0  # type: ignore
                and len(dc_resource_props.resource_props.cluster_security_groups) > 0  # type: ignore
            ):
                # This should not be possible as AWS does not support this case.
                complain_and_abort(
                    "Recommender does not support clusters with ambiguous hosting. "
                    "Cluster cannot have security groups both inside and outside a VPC."
                )
            elif len(dc_resource_props.resource_props.vpc_security_groups) > 0:  # type: ignore
                return [
                    sg.vpc_security_group_id
                    for sg in dc_resource_props.resource_props.vpc_security_groups  # type: ignore
                    if sg.status.lower() == expected_status
                ], True
            elif len(dc_resource_props.resource_props.cluster_security_groups) > 0:  # type: ignore
                return [
                    sg.cluster_security_group_name
                    for sg in dc_resource_props.resource_props.cluster_security_groups  # type: ignore
                    if sg.status.lower() == expected_status
                ], False
        return [], False

    @staticmethod
    def _get_rs_routes(dc_resource_props: DcResourceProperties) -> Iterator[str]:
        """
        Get routes for the cluster.
        """
        if "cluster_subnet_group_name" not in dc_resource_props.resource_props:  # type: ignore
            complain_and_abort("Recommender does not support clusters deployed outside a VPC.")

        rs_subnets = [
            subnet.subnet_identifier
            for subnet in dc_resource_props.resources_client.describe_cluster_subnet_groups(
                subnet_group=dc_resource_props.resource_props.cluster_subnet_group_name,  # type: ignore
            ).subnets
        ]

        for subnet in rs_subnets or []:
            route = dc_resource_props.resources_client.describe_routes(
                filter_vals=[subnet]
            ).route_tables
            if len(route) == 0:
                route = dc_resource_props.resources_client.describe_routes(
                    filter_vals=["true"], filter_key="association.main"
                ).route_tables
            yield route[0].route_table_id

    @staticmethod
    def _get_ip_filtering_verbiage(
        dc_resource_props: DcResourceProperties, redshift_security_groups: List[str]
    ) -> List[str]:
        """
        Generate verbiage for IP filtering.
        """
        return [
            "IP filtering is recommended. See steps below.",
            "",
            f"Whitelist port '{dc_resource_props.resource_props.endpoint.port}' for "  # type: ignore
            f"'{dc_resource_props.collector_props.outputs.public_ip}/32' in any of the following "
            f"redshift security groups - {redshift_security_groups}.",
            "https://docs.getmontecarlo.com/docs/network-connectivity#ip-filtering",
        ]

    @staticmethod
    def _get_rs_cross_account_verbiage(
        dc_resource_props: DcResourceProperties, does_redshift_dc_share_an_account: bool
    ) -> List[str]:
        """
        Generate verbiage for cross account stack stack. This is an optional step based on
        the accounts used.
        """
        if does_redshift_dc_share_an_account:
            return []
        return [
            "- Deploy the 'Create Peering Cross Account assumable role CloudFormation stack' "
            "with the following values -",
            f"1. Data Collector AWS Account ID: {dc_resource_props.collector_arn.account}",
            "https://docs.getmontecarlo.com/docs/peering-templates#create-peering-cross-account-assumable-role-cloudformation-stack",
            "",
        ]

    @staticmethod
    def _get_rs_requester_verbiage(
        dc_resource_props: DcResourceProperties,
        does_redshift_dc_share_an_account: bool,
        redshift_vpc_range: str,
    ) -> List[str]:
        """
        Generate verbiage for requester stack.
        """
        cross_role_verbiage = []
        if not does_redshift_dc_share_an_account:
            # Only visible if the previous stack was necessary
            cross_role_verbiage = [
                "8. VPC peer role for cross AWS account connections: "
                "<PeeringRole from the output of the previous stack>"
            ]

        return [
            "- Deploy the  'Create Requester CloudFormation stack' with the following values -",
            f"1. Monte Carlo Data Collector VPC ID: '{dc_resource_props.collector_props.outputs.vpc_id}'",  # noqa: E501
            f"2. Monte Carlo Data Collector Security Group ID: '{dc_resource_props.collector_props.outputs.security_group}'",  # noqa: E501
            f"3. Monte Carlo Data Collector Route Table ID: '{dc_resource_props.collector_props.outputs.private_route_table}'",  # noqa: E501
            f"4. Warehouse/resource VPC ID: '{dc_resource_props.resource_props.vpc_id}'",  # type: ignore
            f"5. Warehouse/resource AWS Account ID: '{dc_resource_props.resource_arn.account}'",  # type: ignore
            f"6. Warehouse/resource AWS Region: '{dc_resource_props.resource_arn.region}'",  # type: ignore
            f"7. Warehouse/resource CIDR Block: '{redshift_vpc_range}'",
            *cross_role_verbiage,
            "https://docs.getmontecarlo.com/docs/peering-templates#create-requester-cloudformation-stack",
            "",
        ]

    @staticmethod
    def _get_rs_accepter_verbiage(
        dc_resource_props: DcResourceProperties,
        redshift_security_groups: List[str],
        redshift_routes: Iterator[str],
    ) -> List[str]:
        """
        Generate verbiage for accepter stack.
        """
        return [
            "- Deploy the  'Create Accepter CloudFormation stack' with the following values -",
            f"1. Monte Carlo Data Collector CIDR Block: '{dc_resource_props.collector_props.parameters.vpc_cidr}'",  # noqa: E501
            "2. Monte Carlo Data Collector Peering Connection: <PeeringConnection from the output of the previous stack>",  # noqa: E501
            f"3. Resource / Warehouse Security Group: {NetworkDiscoveryService._get_friendly_sg_verbiage(redshift_security_groups)}",  # noqa: E501
            *[
                f"{count + 3}. Resource / Warehouse Route Table #{count}: '{route}'"
                for count, route in enumerate(set(redshift_routes), start=1)
            ],
            "https://docs.getmontecarlo.com/docs/peering-templates#create-accepter-cloudformation-stack",
            "",
        ]

    @staticmethod
    def _get_overlap_verbiage(collector_vpc_range: str, redshift_vpc_range: str) -> str:
        """
        Generate verbiage for overlaps.
        """
        return (
            f"Collector range ({collector_vpc_range}) overlaps with "
            f"resource ({redshift_vpc_range}). "
            f"Peering is not possible when peered VPCs use overlapping CIDR blocks. "
            f"Please redeploy the collector with a custom CIDR block then rerun recommender."
        )

    @staticmethod
    def _get_peering_intro_verbiage(
        does_redshift_dc_share_an_account: bool,
    ) -> List[str]:
        """
        Generate verbiage for peering header / intro.
        """
        section_count = 2 if does_redshift_dc_share_an_account else 3

        return [
            f"VPC Peering is recommended. See the outlined steps for each "
            f"of the {section_count} sections below.",
            "",
            "The CloudFormation template can be found by following the link at the end of "
            "each section.",
            f"Please complete all {section_count} sections, and for any additional help, "
            "reach out to your Monte Carlo representative.",
            "",
        ]

    @staticmethod
    def _get_extra_verbiage() -> List[str]:
        """
        Generate any extra verbiage (i.e. not path specific).
        """
        return [
            "If the cluster's subnets use a non-default ACL the collector CIDR "
            "Block also likely need to be whitelisted."
        ]

    @staticmethod
    def _get_friendly_sg_verbiage(redshift_security_groups: List[str]) -> str:
        """
        Get Human readable SG groups
        """
        return (
            f'Any of the following - {",".join(redshift_security_groups)}'
            if len(redshift_security_groups) > 1
            else f"'{redshift_security_groups[0]}'"
        )

    @staticmethod
    def _echo_recommendation(
        recs: List[str],
        header_col: str = "Recommendations",
        table_format: str = "pipe",
    ) -> None:
        """
        Display recommendations in a fancy table.
        """
        click.echo(
            tabulate(
                [[header_col]] + [[rec] for rec in recs],
                headers="firstrow",
                tablefmt=table_format,
            )
        )
