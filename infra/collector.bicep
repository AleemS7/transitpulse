// Azure resources for the feed collector: Function App (Flex Consumption), its storage, Key Vault,
// Application Insights and a monthly budget. Deploy into an existing resource group.

@description('Environment code used in resource names.')
param env string = 'pr'

param location string = resourceGroup().location

@description('Databricks workspace URL, e.g. https://dbc-xxxx.cloud.databricks.com')
param databricksHost string

@description('Address that receives budget alerts.')
param alertEmail string

@description('Object ID of the person deploying; gets rights to set Key Vault secrets.')
param deployerPrincipalId string

@description('Two-digit suffix; bump it if a globally unique name is taken.')
param suffix string = '01'

param monthlyBudget int = 5
param budgetStartDate string = utcNow('yyyy-MM-01')

var prefix = 'tp-atl-${env}'
var tags = {
  app: 'transitpulse'
  env: env
  owner: 'transitpulse'
  costCenter: 'portfolio'
}

var roles = {
  storageBlobDataOwner: 'b7e6dc6d-f1e8-4753-8033-0f276bb0955b'
  keyVaultSecretsUser: '4633458b-17de-408a-b874-0445c86b69e6'
  keyVaultSecretsOfficer: 'b86a8fe4-44ce-4948-aee5-eccb2c155cd7'
}

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: 'tpatl${env}stcollect${suffix}'
  location: location
  tags: tags
  sku: { name: 'Standard_LRS' }
  kind: 'StorageV2'
  properties: {
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    allowSharedKeyAccess: false
    supportsHttpsTrafficOnly: true
  }
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: storage
  name: 'default'
}

resource deploymentContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: 'deployments'
}

resource deadletterContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: 'deadletter'
}

resource logs 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: '${prefix}-log-${suffix}'
  location: location
  tags: tags
  properties: {
    sku: { name: 'PerGB2018' }
    retentionInDays: 30
    workspaceCapping: { dailyQuotaGb: json('0.1') }
  }
}

resource appInsights 'Microsoft.Insights/components@2020-02-02' = {
  name: '${prefix}-appi-collector-${suffix}'
  location: location
  tags: tags
  kind: 'web'
  properties: {
    Application_Type: 'web'
    WorkspaceResourceId: logs.id
  }
}

resource vault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: '${prefix}-kv-${suffix}'
  location: location
  tags: tags
  properties: {
    tenantId: subscription().tenantId
    sku: { family: 'A', name: 'standard' }
    enableRbacAuthorization: true
  }
}

resource plan 'Microsoft.Web/serverfarms@2024-04-01' = {
  name: '${prefix}-asp-collector-${suffix}'
  location: location
  tags: tags
  kind: 'functionapp'
  sku: { name: 'FC1', tier: 'FlexConsumption' }
  properties: { reserved: true }
}

resource functionApp 'Microsoft.Web/sites@2024-04-01' = {
  name: '${prefix}-func-collector-${suffix}'
  location: location
  tags: tags
  kind: 'functionapp,linux'
  identity: { type: 'SystemAssigned' }
  properties: {
    serverFarmId: plan.id
    httpsOnly: true
    siteConfig: {
      minTlsVersion: '1.2'
      appSettings: [
        { name: 'AzureWebJobsStorage__accountName', value: storage.name }
        { name: 'APPLICATIONINSIGHTS_CONNECTION_STRING', value: appInsights.properties.ConnectionString }
        { name: 'TP_DATABRICKS_HOST', value: databricksHost }
        { name: 'TP_DATABRICKS_TOKEN', value: '@Microsoft.KeyVault(VaultName=${vault.name};SecretName=databricks-token)' }
        { name: 'TP_RAIL_API_KEY', value: '@Microsoft.KeyVault(VaultName=${vault.name};SecretName=marta-rail-api-key)' }
        { name: 'TP_LANDING_ROOT', value: '/Volumes/workspace/bronze/landing' }
        { name: 'TP_DEADLETTER_ACCOUNT_URL', value: storage.properties.primaryEndpoints.blob }
        { name: 'TP_DEADLETTER_CONTAINER', value: deadletterContainer.name }
      ]
    }
    functionAppConfig: {
      deployment: {
        storage: {
          type: 'blobContainer'
          value: '${storage.properties.primaryEndpoints.blob}${deploymentContainer.name}'
          authentication: { type: 'SystemAssignedIdentity' }
        }
      }
      scaleAndConcurrency: {
        maximumInstanceCount: 40
        instanceMemoryMB: 512
      }
      runtime: { name: 'python', version: '3.12' }
    }
  }
}

resource functionStorageRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storage.id, functionApp.id, roles.storageBlobDataOwner)
  scope: storage
  properties: {
    principalId: functionApp.identity.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', roles.storageBlobDataOwner)
  }
}

resource functionVaultRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(vault.id, functionApp.id, roles.keyVaultSecretsUser)
  scope: vault
  properties: {
    principalId: functionApp.identity.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', roles.keyVaultSecretsUser)
  }
}

resource deployerVaultRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(vault.id, deployerPrincipalId, roles.keyVaultSecretsOfficer)
  scope: vault
  properties: {
    principalId: deployerPrincipalId
    principalType: 'User'
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', roles.keyVaultSecretsOfficer)
  }
}

resource budget 'Microsoft.Consumption/budgets@2023-11-01' = {
  name: '${prefix}-budget'
  properties: {
    category: 'Cost'
    amount: monthlyBudget
    timeGrain: 'Monthly'
    timePeriod: { startDate: budgetStartDate }
    notifications: {
      actualOver20Percent: {
        enabled: true
        operator: 'GreaterThan'
        threshold: 20
        thresholdType: 'Actual'
        contactEmails: [ alertEmail ]
      }
      forecastOver100Percent: {
        enabled: true
        operator: 'GreaterThan'
        threshold: 100
        thresholdType: 'Forecasted'
        contactEmails: [ alertEmail ]
      }
    }
  }
}

output functionAppName string = functionApp.name
output keyVaultName string = vault.name
output storageAccountName string = storage.name
