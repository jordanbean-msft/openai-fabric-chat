param name string
param location string = resourceGroup().location
param tags object = {}

param databaseName string = ''

// Because databaseName is optional in main.bicep, we make sure the database name is set here.
var defaultDatabaseName = 'retail'
var actualDatabaseName = !empty(databaseName) ? databaseName : defaultDatabaseName

module adx '../core/database/adx/adx.bicep' = {
  name: '${name}-adx'
  params: {
    databaseName: actualDatabaseName
    name: name
    location: location
    tags: tags
  }
}

output adxClusterName string = adx.outputs.adxClusterName
output adxDatabaseName string = adx.outputs.adxDatabaseName
