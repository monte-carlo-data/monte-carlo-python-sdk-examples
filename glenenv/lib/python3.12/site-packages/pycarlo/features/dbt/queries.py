GET_DBT_UPLOAD_URL = """
query getDbtUploadUrl(
  $projectName: String!,
  $invocationId: String!,
  $fileName: String!) {
  getDbtUploadUrl(
    projectName: $projectName,
    invocationId: $invocationId,
    fileName: $fileName
  )
}
"""

SEND_DBT_ARTIFACTS_EVENT = """
mutation sendDbtArtifactsEvent(
  $projectName: String!,
  $jobName: String!,
  $invocationId: UUID!,
  $artifacts: DbtArtifactsInput!,
  $resourceId: UUID) {
  sendDbtArtifactsEvent(
    projectName: $projectName,
    jobName: $jobName,
    invocationId: $invocationId,
    artifacts: $artifacts,
    resourceId: $resourceId
  ) {
    ok
  }
}
"""
