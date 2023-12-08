param location string = 'eastus'
param dataLakeName string = 'adlsg2tst001'
param sqlServerName string = 'az-eastus-sql-server-tst-001'
param sqlAdminLoginUserName string = 'sqladmin'
param azADLoginUserName string = 'pranov.cs.dev@gmail.com'
param sqlDatabaseName string = 'az-eastus-sql-db-control-tst-001'
param databricksWorkspaceName string = 'az-eastus-adb-tst-001'
param keyVaultName string = 'az-eastus-kv-tst-001'
param dataFactoryName string = 'adfv2-eastus-tst-001'
@secure()
param sqlAdminLoginPassword string 
var managedResourceGroupName = 'databricks-rg-${databricksWorkspaceName}-${uniqueString(databricksWorkspaceName, resourceGroup().id)}'



resource dataLakeStore 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: dataLakeName
  location: location
  kind: 'StorageV2'
  sku: {
    name: 'Standard_LRS'
  }
  properties: {
    allowBlobPublicAccess: true
    isHnsEnabled: true
    supportsHttpsTrafficOnly: true
  }
}


resource sqlServer 'Microsoft.Sql/servers@2023-05-01-preview' = {
  name: sqlServerName
  location: location
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    administratorLogin: sqlAdminLoginUserName
    administratorLoginPassword: sqlAdminLoginPassword
    administrators: {
      administratorType: 'ActiveDirectory'
      azureADOnlyAuthentication: false
      login: azADLoginUserName
      principalType: 'User'
    }
    minimalTlsVersion: '1.2'
    publicNetworkAccess: 'Enabled'
  }
}

resource sqlDatabase 'Microsoft.Sql/servers/databases@2023-05-01-preview' = {
  parent: sqlServer
  name: sqlDatabaseName
  location: location
  sku : {
    name: 'Basic'
    size: 'Basic'
    tier: 'Basic'
  }
  properties: {
    autoPauseDelay: 120
    zoneRedundant: false
  }
}

resource databricksWorkspace 'Microsoft.Databricks/workspaces@2023-02-01' = {
  name : databricksWorkspaceName
  location: location
  sku: {
    name: 'standard'
    tier: 'standard'
  }
  properties: {
    managedResourceGroupId: subscriptionResourceId('Microsoft.Resources/resourceGroups', managedResourceGroupName)
    parameters: {
      enableNoPublicIp: {
        value : false
      }
    }
  }
}

resource dataFactory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: dataFactoryName
  location: location
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    publicNetworkAccess: 'Enabled'
    
  }
}



resource keyvault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: keyVaultName
  location: location
  properties: {
    sku: {
      family: 'A'
      name: 'standard'
    }
    enableRbacAuthorization: true
    enableSoftDelete: true
    softDeleteRetentionInDays: 60
    tenantId: subscription().tenantId
    networkAcls: {
      defaultAction: 'Allow'
      bypass: 'AzureServices'
    }
  }
}







