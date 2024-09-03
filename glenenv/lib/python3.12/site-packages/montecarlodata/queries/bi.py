MATCH_AND_CREATE_BI_WAREHOUSE_SOURCES = """
mutation matchAndCreateBiWarehouseSources($biContainerId: UUID!, $biWarehouseSources: [BiWarehouseSourcesInput]) {
 matchAndCreateBiWarehouseSources(biContainerId: $biContainerId, biWarehouseSources: $biWarehouseSources) {
        matchingBiWarehouseSources{
    matchSuccessful
    biWarehouseSources {
      warehouseResourceId
      warehouseResourceType
      biWarehouseId
    }
    rawBiWarehouseConnections
    rawWarehouseConnections
  }
 }
}"""
