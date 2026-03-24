// ---------------------------------------------------------------------------
// Open Mirroring Debezium Pipeline — Infrastructure as Code
// Deploys: Container App Environment, Container App (with KEDA EventHub
// scaling), Storage Account + checkpoint blob container.
// ---------------------------------------------------------------------------

// ── Parameters ──────────────────────────────────────────────────────────────

@description('Azure region for all resources.')
param location string = resourceGroup().location

@description('Environment name used as a prefix for all resource names.')
param environmentName string

@description('Docker image URI (e.g. myacr.azurecr.io/open-mirroring-debezium:latest).')
param containerImage string

@secure()
@description('EventHub connection string (sends/listens).')
param eventhubConnectionString string

@description('EventHub name (topic).')
param eventhubName string

@description('EventHub consumer group.')
param eventhubConsumerGroup string = '$Default'

@description('Fabric workspace ID for OneLake mirroring.')
param fabricWorkspaceId string

@description('Fabric mirrored database ID for OneLake mirroring.')
param fabricMirroredDbId string

// ── Naming ──────────────────────────────────────────────────────────────────

var resourceToken = toLower(uniqueString(resourceGroup().id, environmentName))
var storageName = 'st${replace(resourceToken, '-', '')}'
var envName = '${environmentName}-env'
var appName = '${environmentName}-app'
var checkpointContainerName = 'checkpoints'

// ── Storage Account — EventHub checkpoint store ─────────────────────────────

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageName
  location: location
  kind: 'StorageV2'
  sku: {
    name: 'Standard_LRS'
  }
  properties: {
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
    allowBlobPublicAccess: false
  }
}

resource blobServices 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: storageAccount
  name: 'default'
}

resource checkpointContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobServices
  name: checkpointContainerName
  properties: {
    publicAccess: 'None'
  }
}

// Construct the blob connection string from account keys.
var storageConnectionString = 'DefaultEndpointsProtocol=https;AccountName=${storageAccount.name};AccountKey=${storageAccount.listKeys().keys[0].value};EndpointSuffix=${environment().suffixes.storage}'

// ── Container App Environment ───────────────────────────────────────────────

resource containerAppEnv 'Microsoft.App/managedEnvironments@2024-03-01' = {
  name: envName
  location: location
  properties: {}
}

// ── Container App ───────────────────────────────────────────────────────────

resource containerApp 'Microsoft.App/containerApps@2024-03-01' = {
  name: appName
  location: location
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    managedEnvironmentId: containerAppEnv.id
    configuration: {
      // Secrets referenced by env vars and KEDA scale rule.
      secrets: [
        {
          name: 'eventhub-connection-string'
          value: eventhubConnectionString
        }
        {
          name: 'checkpoint-blob-connection-string'
          value: storageConnectionString
        }
      ]
    }
    template: {
      containers: [
        {
          name: 'consumer'
          image: containerImage
          resources: {
            cpu: json('0.5')
            memory: '1Gi'
          }
          env: [
            {
              name: 'EVENTHUB_CONNECTION_STRING'
              secretRef: 'eventhub-connection-string'
            }
            {
              name: 'EVENTHUB_NAME'
              value: eventhubName
            }
            {
              name: 'EVENTHUB_CONSUMER_GROUP'
              value: eventhubConsumerGroup
            }
            {
              name: 'CHECKPOINT_BLOB_CONNECTION_STRING'
              secretRef: 'checkpoint-blob-connection-string'
            }
            {
              name: 'CHECKPOINT_BLOB_CONTAINER'
              value: checkpointContainerName
            }
            {
              name: 'ONELAKE_WORKSPACE_ID'
              value: fabricWorkspaceId
            }
            {
              name: 'ONELAKE_MIRRORED_DB_ID'
              value: fabricMirroredDbId
            }
          ]
        }
      ]
      // KEDA-based autoscaling on EventHub consumer group lag.
      scale: {
        minReplicas: 0
        maxReplicas: 5
        rules: [
          {
            name: 'eventhub-lag'
            custom: {
              type: 'azure-eventhub'
              metadata: {
                consumerGroup: eventhubConsumerGroup
                unprocessedEventThreshold: '64'
                blobContainer: checkpointContainerName
              }
              auth: [
                {
                  secretRef: 'eventhub-connection-string'
                  triggerParameter: 'connection'
                }
                {
                  secretRef: 'checkpoint-blob-connection-string'
                  triggerParameter: 'storageConnection'
                }
              ]
            }
          }
        ]
      }
    }
  }
}

// ── Outputs ─────────────────────────────────────────────────────────────────

output containerAppFqdn string = containerApp.properties.configuration.ingress.?fqdn ?? ''
output containerAppName string = containerApp.name
output storageAccountName string = storageAccount.name
output managedIdentityPrincipalId string = containerApp.identity.principalId
