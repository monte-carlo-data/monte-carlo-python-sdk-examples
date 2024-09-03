CREATE_OR_UPDATE_MONTE_CARLO_CONFIG_TEMPLATE_ASYNC = """
mutation createOrUpdateMonteCarloConfigTemplateAsync(
    $namespace: String!,
    $configTemplateJson: String!,
    $dryRun: Boolean,
    $misconfiguredAsWarning: Boolean,
    $resource: String,
    $createNonIngestedTables: Boolean
) {
    createOrUpdateMonteCarloConfigTemplateAsync(
        configTemplateJson: $configTemplateJson,
        namespace: $namespace,
        dryRun: $dryRun,
        misconfiguredAsWarning: $misconfiguredAsWarning,
        resource: $resource,
        createNonIngestedTables: $createNonIngestedTables
    ) {
        response {
            updateUuid
            errorsAsJson
            warningsAsJson
        }
    }
}
"""

GET_MONTE_CARLO_CONFIG_TEMPLATE_UPDATE_STATE = """
query getMonteCarloConfigTemplateUpdateState($updateUuid: UUID!) {
    getMonteCarloConfigTemplateUpdateState(updateUuid: $updateUuid) {
        state
        resourceModifications {
            type
            description
            isSignificantChange
            diffString
            resourceType
            resourceIndex
        }
        changesApplied
        errorsAsJson
        warningsAsJson
    }
}
"""

DELETE_MONTE_CARLO_CONFIG_TEMPLATE = """
mutation deleteMonteCarloConfigTemplate($namespace: String!, $dryRun: Boolean) {
  deleteMonteCarloConfigTemplate(
    namespace: $namespace,
    dryRun: $dryRun
  ) {
    response {
      changesApplied
      numDeleted
    }
  }
}
"""
