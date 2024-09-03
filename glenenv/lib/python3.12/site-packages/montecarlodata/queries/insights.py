# Queries related to insights

GET_INSIGHTS = """
query getInsights {
  getInsights {
    name
    description
    title
    usage
    available
    reports {
      name
      description
    }
  }
}
"""

GET_INSIGHT_REPORT = """
query getReportUrl($insightName: String!, $reportName: String!) {
  getReportUrl(insightName: $insightName, reportName: $reportName) {
    url
  }
}
"""
