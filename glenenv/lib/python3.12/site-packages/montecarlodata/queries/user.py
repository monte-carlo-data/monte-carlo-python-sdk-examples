# Queries related to users

GET_USER_QUERY = """
query getUser {
  getUser {
    firstName
    lastName
    account {
      uuid
      activeCollectionRegions
      warehouses {
        uuid
        name
        connectionType
        connections {
          uuid
          type
          createdOn
          jobTypes
          connectionIdentifiers {
            key
            value
          }
        }
        dataCollector {
          uuid
          customerAwsRegion
        }
      }
      bi {
        uuid
        name
        connections {
          uuid
          type
          createdOn
          jobTypes
          connectionIdentifiers {
            key
            value
          }
        }
        dataCollector {
          uuid,
          customerAwsRegion
        }
      }
      etlContainers {
          uuid
          name
          connections {
          uuid
          type
          createdOn
          jobTypes
          connectionIdentifiers {
            key
            value
          }
        }
        dataCollector {
          uuid,
          customerAwsRegion
        }
      }
      tableauAccounts {
        uuid
        dataCollector {
          uuid,
          customerAwsRegion
        }
      }
      dataCollectors {
        uuid
        stackArn
        active
        customerAwsAccountId
        templateProvider
        templateVariant
        templateVersion
        codeVersion
        lastUpdated
        agents {
          uuid
          agentType
          platform
          storageType
          endpoint
          createdTime
          isDeleted
          imageVersion
          lastUpdatedTime
        }
      }
    }
  }
}
"""
