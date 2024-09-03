from typing import Dict, Optional
from uuid import UUID

import click

from montecarlodata.common.common import create_mc_client
from montecarlodata.dataimport.dbt import DbtImportService


@click.group(help="Import data.", name="import")
def import_subcommand():
    """
    Group for any import related subcommands
    """
    pass


@import_subcommand.command()
@click.option(
    "--project-name",
    type=click.STRING,
    default="default-project",
    show_default=True,
    help="Project name (perhaps a logical group of dbt models,"
    " analogous to a project in dbt Cloud)",
)
@click.option(
    "--job-name",
    type=click.STRING,
    default="default-job",
    show_default=True,
    help="Job name (perhaps a logical sequence of dbt executions, analogous to a job in dbt Cloud)",
)
@click.option(
    "--manifest",
    type=click.Path(exists=True),
    required=True,
    help="Path to the dbt manifest file (manifest.json)",
)
@click.option(
    "--run-results",
    type=click.Path(exists=True),
    required=True,
    help="Path to the dbt run results file (run_results.json)",
)
@click.option(
    "--logs",
    type=click.Path(exists=True),
    required=False,
    help="Path to a file containing dbt run logs",
)
@click.option(
    "--connection-id",
    type=click.UUID,
    required=False,
    help="Identifier of warehouse or lake connection to use to resolve dbt models to tables. "
    "Required if you have more than one warehouse or lake connection.",
)
@click.pass_obj
def dbt_run(
    ctx: Dict,
    project_name: str,
    job_name: str,
    manifest: str,
    run_results: str,
    logs: Optional[str],
    connection_id: Optional[UUID],
):
    """
    Import dbt run artifacts.
    """
    DbtImportService(config=ctx["config"], mc_client=create_mc_client(ctx)).import_run(
        project_name=project_name,
        job_name=job_name,
        manifest_path=manifest,
        run_results_path=run_results,
        logs_path=logs,
        connection_id=connection_id,
    )
