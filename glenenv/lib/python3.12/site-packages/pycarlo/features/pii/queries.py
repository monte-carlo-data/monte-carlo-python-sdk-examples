# Queries related to PII Filtering

GET_PII_PREFERENCES = """
query getPiiFilteringPreferences {
  getPiiFilteringPreferences {
    enabled,
    failMode
  }
}
"""

GET_PII_FILTERS = """
query getPiiFilters {
  getPiiFilters {
    name,
    pattern,
    enabled
  }
}
"""
