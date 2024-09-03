MUTATION_TRIGGER_CAAS_MIGRATION_TEST = """
    mutation triggerPlatformMigrationTest($dcUuid: UUID) {
        triggerPlatformMigrationTest(
            dcUuid: $dcUuid
        ) {
            migrationUuid
        }
    }
"""

QUERY_CAAS_MIGRATION_TEST_STATUS = """
    query getPlatformMigrationStatus($dcUuid: UUID, $migrationUuid: UUID!) {
      getPlatformMigrationStatus(
        dcUuid: $dcUuid
        migrationUuid: $migrationUuid
      ) {
        output
      }
    }
"""

QUERY_GET_SERVICES = """
query getUser {
  getUser {
    account {
      dataCollectors {
        uuid
        deploymentType
        active
        stackArn
        agents {
          isDeleted
          endpoint
        }
      }
    }
  }
}
"""
