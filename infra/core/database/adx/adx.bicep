param name string
param location string = resourceGroup().location
param tags object = {}
param databaseName string

resource adxCluster 'Microsoft.Kusto/clusters@2023-08-15' = {
  name: name
  location: location
  tags: tags
  sku: {
    name: 'Dev(No SLA)_Standard_E2a_v4'
    tier: 'Basic'
  }
  properties: {
    enableAutoStop: true
    engineType: 'V3'   
  }
}

resource adxDatabase 'Microsoft.Kusto/clusters/databases@2023-08-15' = {
  parent: adxCluster
  name: databaseName
  kind: 'ReadWrite'
}

output adxClusterName string = adxCluster.name
output adxDatabaseName string = adxDatabase.name
