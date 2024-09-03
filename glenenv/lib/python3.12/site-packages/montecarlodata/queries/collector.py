# Queries related to managing the collector

GENERATE_COLLECTOR_TEMPLATE = """
mutation generateCollectorTemplate($region:String, $dcId:UUID, $updateInfra:Boolean) {
  generateCollectorTemplate(region:$region, dcId:$dcId, updateInfra: $updateInfra) {
    dc {
      uuid
      templateLaunchUrl
      stackArn
      customerAwsAccountId
      active
      apiGatewayId
      templateVariant
    }
  }
}
"""

ADD_COLLECTOR_RECORD = """
mutation createCollectorRecord {
  createCollectorRecord {
    dc {
      uuid
    }
  }
}
"""

TEST_TELNET_CONNECTION = """
query testTelnetConnection($host:String, $port:Int, $timeout:Int, $dcId:UUID) {
  testTelnetConnection(host:$host, port:$port, timeout:$timeout, dcId:$dcId) {
    success
    validations {
      type
      message
    }
  }
}
"""

TEST_TCP_OPEN_CONNECTION = """
query testTcpOpenConnection($host:String, $port:Int, $timeout:Int, $dcId:UUID) {
  testTcpOpenConnection(host:$host, port:$port, timeout:$timeout, dcId:$dcId) {
    success
    validations {
      type
      message
    }
  }
}
"""
