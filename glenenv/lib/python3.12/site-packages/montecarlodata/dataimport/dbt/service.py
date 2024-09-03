from typing import Optional
from uuid import UUID

import click
from pycarlo.core.client import Client
from pycarlo.features.dbt import DbtImporter
from pycarlo.features.exceptions import MultipleResourcesFoundException
from pycarlo.features.pii import PiiService

from montecarlodata.common.user import UserService
from montecarlodata.config import Config
from montecarlodata.errors import complain_and_abort, manage_errors


class DbtImportService:
    def __init__(
        self,
        config: Config,
        mc_client: Client,
        user_service: Optional[UserService] = None,
        pii_service: Optional[PiiService] = None,
    ):
        self._mc_client = mc_client
        self._user_service = user_service or UserService(config)
        self._pii_service = pii_service or PiiService(self._mc_client)

    @manage_errors
    def import_run(
        self,
        project_name: str,
        job_name: str,
        manifest_path: str,
        run_results_path: str,
        logs_path: Optional[str],
        connection_id: Optional[UUID],
    ):
        # find resource id associated with given connection id
        resource_id = self._find_resource_id(connection_id) if connection_id else None

        try:
            # execute import
            DbtImporter(
                mc_client=self._mc_client,
                print_func=click.echo,
                pii_service=self._pii_service,
            ).import_run(
                project_name=project_name,
                job_name=job_name,
                manifest_path=manifest_path,
                run_results_path=run_results_path,
                logs_path=logs_path,
                resource_id=resource_id,
            )
        except MultipleResourcesFoundException:
            # if multiple resources exist, ask user to specify a connection id
            complain_and_abort("Multiple resources found, please specify a connection id.")

    def _find_resource_id(self, connection_id: UUID) -> Optional[str]:
        warehouse = self._user_service.get_warehouse_for_connection(connection_id)
        if warehouse is None:
            complain_and_abort(f"Could not find a connection with id: {connection_id}")
        return warehouse.uuid  # type: ignore
