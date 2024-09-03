from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Optional, Union, cast
from uuid import UUID

from dataclasses_json import LetterCase, dataclass_json

from pycarlo.common import get_logger, http
from pycarlo.common.files import BytesFileReader, JsonFileReader, to_path
from pycarlo.core import Client, Query
from pycarlo.features.dbt.queries import (
    GET_DBT_UPLOAD_URL,
    SEND_DBT_ARTIFACTS_EVENT,
)
from pycarlo.features.pii import PiiFilterer, PiiService
from pycarlo.features.user import UserService

logger = get_logger(__name__)


class InvalidArtifactsException(Exception):
    pass


class InvalidFileFormatException(Exception):
    pass


@dataclass_json(letter_case=LetterCase.CAMEL)  # type: ignore
@dataclass
class DbtArtifacts:
    manifest: str
    run_results: str
    logs: Optional[str]


class DbtImporter:
    """
    Import dbt run artifacts to Monte Carlo
    """

    DEFAULT_PROJECT_NAME = "default-project"
    DEFAULT_JOB_NAME = "default-job"

    def __init__(
        self,
        mc_client: Optional[Client] = None,
        user_service: Optional[UserService] = None,
        pii_service: Optional[PiiService] = None,
        print_func: Callable = logger.info,
    ):
        self._mc_client = mc_client or Client()
        self._user_service = user_service or UserService(mc_client=self._mc_client)
        self._pii_service = pii_service or PiiService(mc_client=self._mc_client)
        self._print_func = print_func
        self._pii_filterer = self._init_pii_filterer()

    def import_run(
        self,
        manifest_path: Union[Path, str],
        run_results_path: Union[Path, str],
        logs_path: Optional[Union[Path, str]] = None,
        project_name: str = DEFAULT_PROJECT_NAME,
        job_name: str = DEFAULT_JOB_NAME,
        resource_id: Optional[Union[str, UUID]] = None,
    ):
        """
        Import artifacts from a single dbt command execution.

        :param manifest_path: local path to the dbt manifest file (manifest.json)
        :param run_results_path: local path to the dbt run results file (run_results.json)
        :param logs_path: local path to a file containing dbt run logs
        :param project_name: Project name (perhaps a logical group of dbt models, analogous to a
                             project in dbt Cloud)
        :param job_name: Job name (perhaps a logical sequence of dbt commands, analogous to a
                         job in dbt Cloud)
        :param resource_id: identifier of a Monte Carlo resource (warehouse or lake) to use to
                            resolve dbt models to tables, this will be required if you have more
                            than one
        """
        # get resource
        resource = self._user_service.get_resource(resource_id)

        # read local artifacts
        manifest = JsonFileReader(manifest_path).read()
        run_results = JsonFileReader(run_results_path).read()
        logs = BytesFileReader(logs_path).read() if logs_path else None

        # extract dbt invocation id (and verify it is the same for each artifact)
        invocation_id = self._get_invocation_id(
            manifest_path=manifest_path,
            manifest=manifest,
            run_results_path=run_results_path,
            run_results=run_results,
        )

        # upload artifacts to S3 (using pre-signed URLs)
        artifacts = DbtArtifacts(
            manifest=self._upload_artifact(
                project_name=project_name,
                invocation_id=invocation_id,
                file_path=to_path(manifest_path),
                content=manifest,
            ),
            run_results=self._upload_artifact(
                project_name=project_name,
                invocation_id=invocation_id,
                file_path=to_path(run_results_path),
                content=run_results,
            ),
            logs=self._upload_artifact(
                project_name=project_name,
                invocation_id=invocation_id,
                file_path=to_path(logs_path),  # type: ignore
                content=logs,
            )
            if logs
            else None,
        )

        # publish event indicating run artifacts are ready for processing
        self._mc_client(
            query=SEND_DBT_ARTIFACTS_EVENT,
            variables=dict(
                projectName=project_name,
                jobName=job_name,
                invocationId=invocation_id,
                artifacts=artifacts.to_dict(),  # type: ignore
                resourceId=str(resource.id),
            ),
        )

        self._print_func("Finished sending run artifacts to Monte Carlo")

    def _get_invocation_id(
        self,
        manifest_path: Union[Path, str],
        manifest: Dict,
        run_results_path: Union[Path, str],
        run_results: Dict,
    ) -> str:
        manifest_invocation_id = self._extract_invocation_id(path=manifest_path, data=manifest)
        run_results_invocation_id = self._extract_invocation_id(
            path=run_results_path, data=run_results
        )

        if manifest_invocation_id != run_results_invocation_id:
            raise InvalidArtifactsException(
                "dbt invocation ids do not match between manifest and run results files"
            )

        return manifest_invocation_id

    @staticmethod
    def _extract_invocation_id(path: Union[Path, str], data: Dict) -> str:
        try:
            return data["metadata"]["invocation_id"]
        except KeyError:
            raise InvalidArtifactsException(
                f"Unable to get dbt invocation id from '{path}'. Unexpected file format"
            )

    def _upload_artifact(
        self,
        project_name: str,
        invocation_id: str,
        file_path: Path,
        content: Union[bytes, str, Dict],
    ) -> str:
        self._print_func(f"Uploading {file_path.name}...")
        http.upload(
            method="put",
            url=self._get_presigned_url(
                project_name=project_name, invocation_id=invocation_id, file_name=file_path.name
            ),
            content=self._pii_filterer.filter_content(content),
        )
        return file_path.name

    def _get_presigned_url(self, project_name: str, invocation_id: str, file_name: str) -> str:
        response = cast(
            Query,
            self._mc_client(
                query=GET_DBT_UPLOAD_URL,
                variables=dict(
                    projectName=project_name, invocationId=invocation_id, fileName=file_name
                ),
            ),
        )

        return cast(str, response.get_dbt_upload_url)

    def _init_pii_filterer(self):
        pii_filters = self._pii_service.get_pii_filters_config()
        return PiiFilterer(filters_config=pii_filters)
