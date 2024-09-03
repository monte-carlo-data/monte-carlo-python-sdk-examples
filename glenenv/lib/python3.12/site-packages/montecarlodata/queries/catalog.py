IMPORT_DBT_MANIFEST = """
mutation importDbtManifest($dbtSchemaVersion: String!, $manifestNodesJson: String!, $projectName: String) {
  importDbtManifest(
    dbtSchemaVersion: $dbtSchemaVersion,
    manifestNodesJson: $manifestNodesJson,
    projectName: $projectName
  ) {
    response {
      nodeIdsImported
    }
  }
}
"""

IMPORT_DBT_RUN_RESULTS = """
mutation importDbtRunResults($dbtSchemaVersion: String!, $runResultsJson: String!, $projectName: String, $runId: String, $runLogs: String) {
  importDbtRunResults(
    dbtSchemaVersion: $dbtSchemaVersion,
    runResultsJson: $runResultsJson,
    projectName: $projectName,
    runId: $runId,
    runLogs: $runLogs
  ) {
    response {
      numResultsImported
    }
  }
}
"""

CREATE_PROJECT = """
mutation createDbtProject($projectName: String!, $source: DbtProjectSource!) {
  createDbtProject(
    projectName: $projectName,
    source: $source
  ) {
    dbtProject {
      projectName
      source
    }
  }
}
"""
