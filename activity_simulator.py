# activity_simulator.py
# Simulates Azure Activity Log events and sends to Fabric Eventstream Custom Endpoint
# Usage: pip install azure-eventhub && python activity_simulator.py

import json
import random
import time
import datetime
from azure.eventhub import EventHubProducerClient, EventData

# ============================================================
# Eventstream Custom Endpoint (Event Hub-compatible)
# ============================================================
CONN_STR = ""  # Set your Eventstream Custom Endpoint connection string here
INTERVAL_SEC = 5

OPERATIONS = [
    # Compute
    ("Microsoft.Compute/virtualMachines/start/action", "Administrative"),
    ("Microsoft.Compute/virtualMachines/deallocate/action", "Administrative"),
    ("Microsoft.Compute/virtualMachines/restart/action", "Administrative"),
    ("Microsoft.Compute/virtualMachines/write", "Administrative"),
    ("Microsoft.Compute/virtualMachines/delete", "Administrative"),
    ("Microsoft.Compute/virtualMachineScaleSets/write", "Administrative"),
    ("Microsoft.Compute/disks/write", "Administrative"),
    # Storage
    ("Microsoft.Storage/storageAccounts/write", "Administrative"),
    ("Microsoft.Storage/storageAccounts/listkeys/action", "Security"),
    ("Microsoft.Storage/storageAccounts/delete", "Administrative"),
    ("Microsoft.Storage/storageAccounts/blobServices/containers/write", "Administrative"),
    # Networking
    ("Microsoft.Network/networkSecurityGroups/write", "Security"),
    ("Microsoft.Network/networkSecurityGroups/securityRules/write", "Security"),
    ("Microsoft.Network/virtualNetworks/write", "Administrative"),
    ("Microsoft.Network/loadBalancers/write", "Administrative"),
    ("Microsoft.Network/publicIPAddresses/write", "Administrative"),
    ("Microsoft.Network/applicationGateways/write", "Administrative"),
    ("Microsoft.Network/privateDnsZones/write", "Administrative"),
    # Identity & security
    ("Microsoft.Authorization/roleAssignments/write", "Administrative"),
    ("Microsoft.Authorization/roleAssignments/delete", "Administrative"),
    ("Microsoft.Authorization/policyAssignments/write", "Policy"),
    ("Microsoft.Authorization/policyAssignments/delete", "Policy"),
    ("Microsoft.Security/alerts/dismiss/action", "Alert"),
    ("Microsoft.Security/securityContacts/write", "Security"),
    ("Microsoft.ManagedIdentity/userAssignedIdentities/write", "Administrative"),
    # Databases
    ("Microsoft.Sql/servers/firewallRules/write", "Security"),
    ("Microsoft.Sql/servers/databases/write", "Administrative"),
    ("Microsoft.Sql/servers/databases/delete", "Administrative"),
    ("Microsoft.DocumentDB/databaseAccounts/write", "Administrative"),
    ("Microsoft.DBforPostgreSQL/flexibleServers/write", "Administrative"),
    ("Microsoft.Cache/redis/write", "Administrative"),
    # App services
    ("Microsoft.Web/sites/restart/action", "Administrative"),
    ("Microsoft.Web/sites/write", "Administrative"),
    ("Microsoft.Web/sites/slots/write", "Administrative"),
    ("Microsoft.Web/serverfarms/write", "Administrative"),
    # Containers
    ("Microsoft.ContainerService/managedClusters/write", "Administrative"),
    ("Microsoft.ContainerService/managedClusters/delete", "Administrative"),
    ("Microsoft.ContainerRegistry/registries/write", "Administrative"),
    ("Microsoft.ContainerRegistry/registries/push/action", "Administrative"),
    # Key Vault
    ("Microsoft.KeyVault/vaults/read", "Administrative"),
    ("Microsoft.KeyVault/vaults/write", "Administrative"),
    ("Microsoft.KeyVault/vaults/secrets/write", "Security"),
    ("Microsoft.KeyVault/vaults/keys/create/action", "Security"),
    # Monitoring & management
    ("Microsoft.Insights/diagnosticSettings/write", "Administrative"),
    ("Microsoft.Insights/actionGroups/write", "Administrative"),
    ("Microsoft.Insights/metricAlerts/write", "Administrative"),
    ("Microsoft.OperationalInsights/workspaces/write", "Administrative"),
    # Deployments & IaC
    ("Microsoft.Resources/deployments/write", "Administrative"),
    ("Microsoft.Resources/deployments/validate/action", "Administrative"),
    ("Microsoft.Resources/subscriptions/resourceGroups/write", "Administrative"),
    # Messaging & events
    ("Microsoft.EventHub/namespaces/write", "Administrative"),
    ("Microsoft.ServiceBus/namespaces/write", "Administrative"),
    ("Microsoft.EventGrid/topics/write", "Administrative"),
    # AI & ML
    ("Microsoft.CognitiveServices/accounts/write", "Administrative"),
    ("Microsoft.MachineLearningServices/workspaces/write", "Administrative"),
    # Fabric / Power BI
    ("Microsoft.Fabric/capacities/read", "Administrative"),
    ("Microsoft.Fabric/capacities/write", "Administrative"),
    ("Microsoft.PowerBI/workspaces/write", "Administrative"),
]

RESOURCE_GROUPS = [
    "rg-rti-demo", "rg-prod-01", "rg-dev-02", "rg-staging",
    "rg-data-platform", "rg-ml-workspace", "rg-aks-cluster",
    "rg-networking-hub", "rg-security", "rg-monitoring",
    "rg-web-frontend", "rg-api-backend", "rg-shared-services",
    "rg-disaster-recovery", "rg-sandbox-01",
]

CALLERS = [
    "pidoudet@microsoft.com", "admin@contoso.com",
    "svc-deploy@contoso.com", "alice@contoso.com",
    "bob@contoso.com", "charlie@contoso.com",
    "svc-cicd@contoso.com", "svc-monitoring@contoso.com",
    "diana@contoso.com", "eve@contoso.com",
    "frank@contoso.com", "svc-terraform@contoso.com",
    "grace@contoso.com", "svc-aks@contoso.com",
    "hank@contoso.com", "svc-backup@contoso.com",
]

REGIONS = [
    "westeurope", "eastus", "eastus2", "westus2", "northeurope",
    "southeastasia", "japaneast", "australiaeast", "uksouth",
    "centralus", "canadacentral", "brazilsouth", "koreacentral",
    "francecentral", "germanywestcentral",
]
LEVELS = ["Information", "Warning", "Error"]
RESULTS = ["Success", "Success", "Success", "Success", "Failure"]
SUB_ID = "e80f8e45-cde4-42ed-b913-695b2938f23b"


def generate_event():
    op, cat = random.choice(OPERATIONS)
    rg = random.choice(RESOURCE_GROUPS)
    region = random.choice(REGIONS)
    res_type = op.rsplit("/", 1)[0]
    result = random.choice(RESULTS)
    level = "Error" if result == "Failure" else random.choice(["Information", "Warning"])

    return {
        "Timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "OperationName": op,
        "OperationId": f"op-{random.randint(10000,99999)}",
        "Status": "Succeeded" if result == "Success" else "Failed",
        "Level": level,
        "ResourceGroup": rg,
        "ResourceType": res_type,
        "ResourceId": f"/subscriptions/{SUB_ID}/resourceGroups/{rg}/providers/{res_type}/res-{random.randint(1,500)}",
        "Caller": random.choice(CALLERS),
        "CallerIpAddress": f"{random.randint(10,192)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
        "Category": cat,
        "ResultType": result,
        "ResultSignature": "OK" if result == "Success" else "Forbidden",
        "CorrelationId": f"corr-{random.randint(10000,99999)}",
        "Region": region,
        "Properties": {"statusCode": "200" if result == "Success" else "403"},
    }


def main():
    producer = EventHubProducerClient.from_connection_string(CONN_STR)
    print("Connected to Eventstream Custom Endpoint (Activity-Stream)")
    print(f"Generating 5-20 events every {INTERVAL_SEC}s")
    print(f"  {len(OPERATIONS)} operation types, {len(RESOURCE_GROUPS)} resource groups,")
    print(f"  {len(CALLERS)} callers, {len(REGIONS)} regions")
    print("Press Ctrl+C to stop.\n")

    total = 0
    errors = 0
    try:
        while True:
            n = random.randint(5, 20)
            events = [generate_event() for _ in range(n)]

            try:
                batch = producer.create_batch()
                for e in events:
                    batch.add(EventData(json.dumps(e)))
                producer.send_batch(batch)
                total += n

                ts = datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S")
                ops = [e["OperationName"].split("/")[-1] for e in events]
                levels = [e["Level"][0] for e in events]  # I/W/E
                print(f"[{ts}] ✓{n} (total: {total}) | {', '.join(ops)} [{'/'.join(levels)}]")
            except Exception as ex:
                errors += n
                print(f"  Send error: {ex}")

            time.sleep(INTERVAL_SEC)
    except KeyboardInterrupt:
        print(f"\nStopped. Total: {total}, errors: {errors}")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
