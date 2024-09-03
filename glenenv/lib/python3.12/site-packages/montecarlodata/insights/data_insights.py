import os
import urllib.request
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import click
import requests
from tabulate import tabulate

from montecarlodata.config import Config
from montecarlodata.errors import complain_and_abort, manage_errors
from montecarlodata.fs_utils import mkdirs
from montecarlodata.insights.fields import (
    EXPECTED_GET_INSIGHTS_FIELD,
    EXPECTED_GET_REPORT_FIELD,
    FILE_SCHEME,
    INSIGHTS_DEFAULT_EXTENSION,
    LIST_INSIGHTS_HEADERS,
    S3_SCHEME,
    SCHEME_DELIM,
)
from montecarlodata.queries.insights import GET_INSIGHT_REPORT, GET_INSIGHTS
from montecarlodata.utils import AwsClientWrapper, GqlWrapper


class InsightsService:
    def __init__(self, config: Config, request_wrapper: Optional[GqlWrapper] = None):
        self._abort_on_error = True

        self._request_wrapper = request_wrapper or GqlWrapper(config)

        self.scheme_handler = {
            FILE_SCHEME: self._save_insight_to_disk,
            S3_SCHEME: self._save_insight_to_s3,
        }

    @manage_errors
    def echo_insights(
        self,
        headers: str = "firstrow",
        table_format: str = "fancy_grid",
    ) -> None:
        """
        Echo insights as a pretty table
        """
        table = [LIST_INSIGHTS_HEADERS]
        for insight in self._request_wrapper.make_request_v2(
            query=GET_INSIGHTS,
            operation=EXPECTED_GET_INSIGHTS_FIELD,
        ).data:  # type: ignore
            table.append(
                [
                    f"{insight.title} ({insight.name})",
                    (
                        insight.description
                        if insight.description.endswith(".")
                        else f"{insight.description}."
                    ),
                    insight.available,
                ]
            )
        click.echo(tabulate(table, headers=headers, tablefmt=table_format, maxcolwidths=100))

    @manage_errors
    def get_insight(
        self,
        insight: str,
        destination: str,
        aws_profile: Optional[str] = None,
        dry: bool = False,
    ) -> None:
        """
        Get insight, if available, and either persist to S3 or local file system
        """
        parsed_destination = urlparse(destination)
        scheme = parsed_destination.scheme
        if scheme == S3_SCHEME and not aws_profile:
            raise ValueError("Cannot use an s3 destination without specifying an AWS profile.")

        netloc_with_path = parsed_destination.geturl().replace(f"{scheme}{SCHEME_DELIM}", "", 1)

        try:
            handler = self.scheme_handler[scheme]
        except KeyError:
            complain_and_abort("Scheme either missing or not supported.")
        else:
            insight_url = self._get_insight_url(insight=insight)
            if dry:
                click.echo(insight_url)
                return
            click.echo(f"Saving insight to '{destination}'.")
            handler(
                insight_url=insight_url,
                destination=netloc_with_path,
                aws_profile=aws_profile,
            )
            click.echo("Complete. Have a nice day!")

    def _get_insight_url(self, insight: str) -> str:
        """
        Get insight URL from the monolith
        """
        url = self._request_wrapper.make_request_v2(
            query=GET_INSIGHT_REPORT,
            operation=EXPECTED_GET_REPORT_FIELD,
            variables=dict(
                insight_name=insight,
                report_name=str(Path(insight).with_suffix(INSIGHTS_DEFAULT_EXTENSION)),
            ),
        ).data.url  # type: ignore
        if not url:
            complain_and_abort(
                "Insight not found. This insight might not be available for your account."
            )
        return url

    def _save_insight_to_disk(self, insight_url: str, destination: str, **kwargs) -> None:
        """
        Save insight to the local filesystem
        """
        if not destination:
            complain_and_abort(
                f"Invalid path. Expected format: '{FILE_SCHEME}{SCHEME_DELIM}folder/file.csv'"
            )
        mkdirs(os.path.dirname(destination))
        urllib.request.urlretrieve(insight_url, destination)

    def _save_insight_to_s3(
        self, insight_url: str, destination: str, aws_profile: Optional[str] = None
    ) -> None:
        """
        Save insight to S3
        """
        try:
            bucket, key = destination.split("/", maxsplit=1)
            if not key:
                raise ValueError
        except (ValueError, AttributeError):
            complain_and_abort(
                f"Invalid path. Expected format: '{S3_SCHEME}{SCHEME_DELIM}bucket/key.csv'"
            )
        else:
            AwsClientWrapper(profile_name=aws_profile).upload_stream_to_s3(
                data=requests.get(insight_url, stream=True).raw, bucket=bucket, key=key
            )
