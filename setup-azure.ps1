# Azure infrastructure setup for RTI Demo
# Run these commands in Azure CLI (az login first)
#
# TWO INGESTION MODES:
#
#   Mode A (custom_endpoint) — Eventstream has a built-in Event Hub endpoint
#     - No Azure Event Hub needed for PHONE telemetry
#     - Azure Diagnostic Settings still need a real Event Hub (can't target Eventstream directly)
#     - Simpler: fewer Azure resources for phone data
#     - config.json: "ingestion_mode": "custom_endpoint"
#
#   Mode B (azure_event_hub) — Azure Event Hub as intermediary (DEFAULT)
#     - Azure Diagnostic Settings → Azure Event Hub → Eventstream → Eventhouse
#     - Phone simulator → Azure Event Hub → Eventstream → Eventhouse
#     - More control, works for both data streams uniformly
#     - config.json: "ingestion_mode": "azure_event_hub"
#
# If using Mode A for phone data only, you can skip creating the phone-telemetry hub.
# Azure Activity Log always needs an Azure Event Hub (Mode B) because Azure
# Diagnostic Settings only support Azure Event Hub as a target.

# ============================================================
# 1. Resource Group
# ============================================================
az group create --name rg-rti-demo --location westeurope

# ============================================================
# 2. Event Hub Namespace + Event Hubs
# ============================================================
az eventhubs namespace create `
  --name eh-rti-demo `
  --resource-group rg-rti-demo `
  --location westeurope `
  --sku Standard

az eventhubs eventhub create `
  --name azure-activity `
  --namespace-name eh-rti-demo `
  --resource-group rg-rti-demo `
  --partition-count 2 `
  --message-retention 1

az eventhubs eventhub create `
  --name phone-telemetry `
  --namespace-name eh-rti-demo `
  --resource-group rg-rti-demo `
  --partition-count 2 `
  --message-retention 1

# ============================================================
# 3. Get Event Hub Connection String
# ============================================================
az eventhubs namespace authorization-rule keys list `
  --resource-group rg-rti-demo `
  --namespace-name eh-rti-demo `
  --name RootManageSharedAccessKey `
  --query primaryConnectionString `
  --output tsv

# ============================================================
# 4. Configure Azure Activity Log → Event Hub
# ============================================================
# Get subscription ID
$SUB_ID = az account show --query id --output tsv

# Create diagnostic setting to stream Activity Log
az monitor diagnostic-settings subscription create `
  --name rti-demo-to-eventhub `
  --location westeurope `
  --event-hub-name azure-activity `
  --event-hub-auth-rule "/subscriptions/$SUB_ID/resourceGroups/rg-rti-demo/providers/Microsoft.EventHub/namespaces/eh-rti-demo/authorizationRules/RootManageSharedAccessKey" `
  --logs '[{\"category\":\"Administrative\",\"enabled\":true},{\"category\":\"Security\",\"enabled\":true},{\"category\":\"Alert\",\"enabled\":true},{\"category\":\"Policy\",\"enabled\":true}]'

# ============================================================
# 5. Generate sample Azure Activity (for demo data)
# ============================================================
# These operations create Activity Log entries
az storage account create --name rtidemostore01 --resource-group rg-rti-demo --location westeurope --sku Standard_LRS
az storage account delete --name rtidemostore01 --resource-group rg-rti-demo --yes

# ============================================================
# CLEANUP (run after demo)
# ============================================================
# az monitor diagnostic-settings subscription delete --name rti-demo-to-eventhub
# az group delete --name rg-rti-demo --yes --no-wait
