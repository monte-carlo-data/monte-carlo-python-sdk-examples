GET_USER_WAREHOUSES = """
query getUserWarehouses {
  getUser {
    account {
      warehouses {
        uuid
        name
        connectionType
      }
    }
  }
}
"""
