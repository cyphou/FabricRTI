# Real-Time Intelligence (RTI) Demo Plan
## Azure Subscription + Phone Telemetry

---

## 1. Demo Objective & Story

**Narrative**: A company monitors its Azure infrastructure health and employee mobile device fleet in real-time using Fabric RTI. Operations teams get instant visibility into Azure subscription activity (deployments, failures, policy changes) and phone telemetry (location, battery, app crashes) — all in one unified platform with AI-assisted investigation and automated alerting.

**Key RTI capabilities showcased**:
- Real-Time Hub (centralized catalog of streaming data)
- Eventstreams (ingestion + transformation, no-code)
- Eventhouse / KQL Database (high-perf analytics store)
- KQL Queryset + Copilot (natural language → KQL)
- Real-Time Dashboard (live visualizations)
- Activator (automated alerts & actions)
- Copilot / Operational Agent (AI-powered autonomous investigation)

---

## 2. Architecture Overview

```
┌──────────────────────────────────┐    ┌──────────────────────────────────┐
│   Azure Subscription Telemetry   │    │       Phone Telemetry            │
│                                  │    │                                  │
│  Azure Activity Log              │    │  Simulated phone sensors         │
│  → Diagnostic Settings           │    │  (Python script / Power Automate)│
│  → Event Hub                     │    │  → Event Hub / Custom Endpoint   │
│                                  │    │                                  │
└──────────┬───────────────────────┘    └──────────┬───────────────────────┘
           │                                       │
           ▼                                       ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                    Fabric Real-Time Hub                                  │
│                (Centralized streaming catalog)                           │
└──────────────────────────┬───────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                        Eventstreams                                      │
│                                                                          │
│  ┌─────────────────┐  ┌──────────────┐  ┌────────────────────────────┐  │
│  │ Azure Sub Stream │  │ Phone Stream │  │ Transformations:           │  │
│  │ (Event Hub src)  │  │ (Custom EP)  │  │ - Filter / Manage Fields   │  │
│  │                  │  │              │  │ - Aggregate (5s windows)   │  │
│  └──────┬───────────┘  └──────┬───────┘  │ - Derived streams          │  │
│         │                     │          └────────────────────────────┘  │
└─────────┼─────────────────────┼──────────────────────────────────────────┘
          │                     │
          ▼                     ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                        Eventhouse                                        │
│                                                                          │
│  KQL Database: "RTI-Demo-DB"                                             │
│  ┌────────────────────────┐  ┌────────────────────────────┐              │
│  │ Table: AzureActivity   │  │ Table: PhoneTelemetry      │              │
│  │ - OperationName        │  │ - DeviceId                 │              │
│  │ - ResourceGroup        │  │ - BatteryLevel             │              │
│  │ - Status               │  │ - Latitude / Longitude     │              │
│  │ - Caller               │  │ - AppName                  │              │
│  │ - Level (Error/Info)   │  │ - CrashCount               │              │
│  │ - Timestamp            │  │ - SignalStrength            │              │
│  └────────────────────────┘  │ - Timestamp                │              │
│                              └────────────────────────────┘              │
└──────────────────┬───────────────────────────────────────────────────────┘
                   │
        ┌──────────┼──────────────────┐
        ▼          ▼                  ▼
┌───────────┐ ┌──────────┐  ┌─────────────────┐
│ KQL       │ │ Real-Time│  │   Activator     │
│ Queryset  │ │Dashboard │  │   (Alerts)      │
│ + Copilot │ │          │  │                 │
└───────────┘ └──────────┘  └─────────────────┘
```

---

## 3. Prerequisites & Setup

### Azure Resources Needed
| Resource | Purpose | Estimated Setup |
|----------|---------|-----------------|
| Azure Event Hub Namespace | Receive Activity Log + phone events | 5 min |
| Event Hub: `azure-activity` | Azure subscription activity stream | 2 min |
| Event Hub: `phone-telemetry` | Phone sensor data stream | 2 min |
| Azure Subscription Diagnostic Settings | Route Activity Log → Event Hub | 3 min |
| Fabric Workspace (F4+ capacity) | Host all RTI items | 2 min |

### Fabric Items to Create
| Item | Name | Purpose |
|------|------|---------|
| Eventhouse | `RTI-Demo-Eventhouse` | Analytics engine + storage |
| KQL Database | `RTI-Demo-DB` | Tables for both data streams |
| Eventstream #1 | `AzureActivity-Stream` | Ingest Azure Activity Log |
| Eventstream #2 | `PhoneTelemetry-Stream` | Ingest phone sensor data |
| KQL Queryset | `RTI-Demo-Queries` | Ad-hoc analysis + Copilot |
| Real-Time Dashboard | `Operations-Dashboard` | Live monitoring views |
| Activator | `RTI-Demo-Alerts` | Automated alert rules |

---

## 4. Step-by-Step Demo Build

### Phase 1: Azure Infrastructure Setup (15 min)

#### 1.1 Create Event Hub Namespace + Event Hubs
```bash
# Create Resource Group
az group create --name rg-rti-demo --location westeurope

# Create Event Hub Namespace
az eventhubs namespace create \
  --name eh-rti-demo \
  --resource-group rg-rti-demo \
  --location westeurope \
  --sku Standard

# Create Event Hubs
az eventhubs eventhub create \
  --name azure-activity \
  --namespace-name eh-rti-demo \
  --resource-group rg-rti-demo \
  --partition-count 2 \
  --message-retention 1

az eventhubs eventhub create \
  --name phone-telemetry \
  --namespace-name eh-rti-demo \
  --resource-group rg-rti-demo \
  --partition-count 2 \
  --message-retention 1
```

#### 1.2 Route Azure Activity Log to Event Hub
1. Azure Portal → **Monitor** → **Activity Log** → **Export Activity Logs**
2. Add Diagnostic Setting:
   - Name: `rti-demo-to-eventhub`
   - Categories: **Administrative**, **Security**, **Alert**, **Policy**
   - Destination: **Stream to an Event Hub** → select `eh-rti-demo` / `azure-activity`
3. Save → Activity log events now flow to Event Hub

#### 1.3 Generate Azure Activity (for demo data)
Run a few operations to generate events:
```bash
# Create/delete resources to generate activity
az storage account create --name rtidemostore01 --resource-group rg-rti-demo --location westeurope --sku Standard_LRS
az storage account delete --name rtidemostore01 --resource-group rg-rti-demo --yes
az vm list --resource-group rg-rti-demo    # read operation
```

---

### Phase 2: Phone Telemetry Simulator (10 min)

#### 2.1 Python Simulator Script
Create a script that sends simulated phone telemetry to Event Hub:

```python
# phone_simulator.py
import json
import random
import time
import datetime
from azure.eventhub import EventHubProducerClient, EventData

CONNECTION_STR = "<YOUR_EVENT_HUB_CONNECTION_STRING>"
EVENTHUB_NAME = "phone-telemetry"

DEVICES = [
    {"id": "iPhone-001", "user": "Alice", "os": "iOS 18"},
    {"id": "Galaxy-002", "user": "Bob", "os": "Android 15"},
    {"id": "Pixel-003", "user": "Charlie", "os": "Android 15"},
    {"id": "iPhone-004", "user": "Diana", "os": "iOS 18"},
    {"id": "Galaxy-005", "user": "Eve", "os": "Android 14"},
]

APPS = ["Teams", "Outlook", "Edge", "OneDrive", "Authenticator", "Power BI"]

# Base coordinates (Paris area)
BASE_LAT, BASE_LON = 48.8566, 2.3522

def generate_event(device):
    return {
        "DeviceId": device["id"],
        "User": device["user"],
        "OS": device["os"],
        "BatteryLevel": max(5, random.gauss(65, 20)),
        "SignalStrength": random.randint(-110, -50),  # dBm
        "Latitude": BASE_LAT + random.uniform(-0.05, 0.05),
        "Longitude": BASE_LON + random.uniform(-0.05, 0.05),
        "AppName": random.choice(APPS),
        "CpuUsage": round(random.uniform(5, 95), 1),
        "MemoryUsageMB": random.randint(200, 4000),
        "CrashCount": 1 if random.random() < 0.05 else 0,  # 5% crash rate
        "NetworkType": random.choice(["5G", "4G", "WiFi"]),
        "Timestamp": datetime.datetime.utcnow().isoformat() + "Z"
    }

def main():
    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        eventhub_name=EVENTHUB_NAME
    )
    print("Sending phone telemetry events... (Ctrl+C to stop)")
    try:
        while True:
            batch = producer.create_batch()
            for device in DEVICES:
                event = generate_event(device)
                batch.add(EventData(json.dumps(event)))
            producer.send_batch(batch)
            print(f"[{datetime.datetime.utcnow().strftime('%H:%M:%S')}] Sent {len(DEVICES)} events")
            time.sleep(3)  # Send every 3 seconds
    except KeyboardInterrupt:
        print("Stopped.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
```

#### 2.2 Install & Run
```bash
pip install azure-eventhub
python phone_simulator.py
```

> **Alternative (no code)**: Use the Eventstream **Sample Data** source (Bicycles or Stock Market) to demonstrate without Azure resources. Good for dry-run rehearsal.

---

### Phase 3: Fabric RTI Setup (20 min)

#### 3.1 Create Eventhouse + KQL Database
1. Fabric Portal → Workspace → **+ New item** → **Eventhouse**
2. Name: `RTI-Demo-Eventhouse`
3. A KQL Database `RTI-Demo-Eventhouse` is auto-created (rename to `RTI-Demo-DB`)

#### 3.2 Create Eventstream #1 — Azure Activity Log
1. **+ New item** → **Eventstream** → Name: `AzureActivity-Stream`
2. Enable **Enhanced capabilities**
3. **Add source** → **Azure Event Hubs**
   - Namespace: `eh-rti-demo`
   - Event Hub: `azure-activity`
   - Consumer group: `$Default`
   - Data format: JSON
4. **Add destination** → **Eventhouse**
   - Database: `RTI-Demo-DB`
   - Table: `AzureActivity` (create new)
   - Ingestion mode: **Direct ingestion**
5. **Publish** the eventstream

#### 3.3 Create Eventstream #2 — Phone Telemetry
1. **+ New item** → **Eventstream** → Name: `PhoneTelemetry-Stream`
2. Enable **Enhanced capabilities**
3. **Add source** → **Azure Event Hubs**
   - Namespace: `eh-rti-demo`
   - Event Hub: `phone-telemetry`
   - Data format: JSON
4. *(Optional)* Add transformation:
   - **Manage Fields**: rename columns, cast types
   - **Filter**: exclude events where `BatteryLevel > 100`
5. **Add destination** → **Eventhouse**
   - Database: `RTI-Demo-DB`
   - Table: `PhoneTelemetry` (create new)
6. **Publish** the eventstream

#### 3.4 Verify Data Ingestion
In the KQL Database, run quick checks:
```kql
AzureActivity
| take 10

PhoneTelemetry
| take 10

PhoneTelemetry
| summarize count() by bin(Timestamp, 1m)
| render timechart
```

---

### Phase 4: KQL Queryset + Copilot (10 min)

#### 4.1 Create KQL Queryset
1. **+ New item** → **KQL Queryset** → Name: `RTI-Demo-Queries`
2. Connect to `RTI-Demo-DB`

#### 4.2 Demo Queries — Azure Activity

```kql
// Failed operations in the last hour
AzureActivity
| where Level == "Error"
| summarize FailureCount = count() by OperationName, ResourceGroup
| order by FailureCount desc

// Activity timeline by operation type
AzureActivity
| summarize EventCount = count() by bin(Timestamp, 5m), Level
| render timechart

// Who is making changes?
AzureActivity
| where Level != "Informational"
| summarize Actions = count() by Caller
| top 10 by Actions
| render piechart
```

#### 4.3 Demo Queries — Phone Telemetry

```kql
// Low battery devices
PhoneTelemetry
| where BatteryLevel < 20
| summarize LastSeen = max(Timestamp), AvgBattery = avg(BatteryLevel) by DeviceId, User
| order by AvgBattery asc

// App crash report
PhoneTelemetry
| where CrashCount > 0
| summarize TotalCrashes = sum(CrashCount) by AppName, OS
| order by TotalCrashes desc
| render columnchart

// Signal strength heatmap by device
PhoneTelemetry
| summarize AvgSignal = avg(SignalStrength) by DeviceId, bin(Timestamp, 5m)
| render timechart

// Device locations (for map visualization)
PhoneTelemetry
| summarize arg_max(Timestamp, *) by DeviceId
| project DeviceId, User, City, Latitude, Longitude, BatteryLevel, NetworkType
```

#### 4.4 Copilot Demo (Natural Language → KQL)
Use Copilot in the KQL Queryset to demonstrate AI-assisted investigation:
- *"Show me all failed Azure operations grouped by resource group in the last 2 hours"*
- *"Which phones have battery below 20% right now?"*
- *"Show crash trends by app name over the last hour"*
- *"Find devices with poor signal strength below -90 dBm"*

---

### Phase 5: Real-Time Dashboard (15 min)

#### 5.1 Create Dashboard
1. **+ New item** → **Real-Time Dashboard** → Name: `Operations-Dashboard`
2. **Add data source** → KQL Database: `RTI-Demo-DB`

#### 5.2 Dashboard Pages & Tiles

**Page 1: Azure Infrastructure Overview**
| Tile | Visual Type | KQL Query |
|------|-------------|-----------|
| Activity Timeline | Time chart | `AzureActivity \| summarize count() by bin(Timestamp, 5m), Level \| render timechart` |
| Failed Operations | Table | `AzureActivity \| where Level == "Error" \| project Timestamp, OperationName, ResourceGroup, Caller` |
| Operations by Caller | Pie chart | `AzureActivity \| summarize count() by Caller \| render piechart` |
| Error Rate KPI | Stat (single value) | `AzureActivity \| summarize ErrorRate = round(100.0 * countif(Level == "Error") / count(), 1)` |

**Page 2: Phone Fleet Health**
| Tile | Visual Type | KQL Query |
|------|-------------|-----------|
| Battery Levels | Multi-line chart | `PhoneTelemetry \| summarize avg(BatteryLevel) by DeviceId, bin(Timestamp, 1m) \| render timechart` |
| Device Locations | Map | `PhoneTelemetry \| summarize arg_max(Timestamp, *) by DeviceId \| project DeviceId, Latitude, Longitude` |
| App Crashes | Column chart | `PhoneTelemetry \| where CrashCount > 0 \| summarize sum(CrashCount) by AppName \| render columnchart` |
| Signal Strength | Heatmap | `PhoneTelemetry \| summarize avg(SignalStrength) by DeviceId, bin(Timestamp, 5m)` |
| Fleet Status | Table | `PhoneTelemetry \| summarize arg_max(Timestamp, *) by DeviceId \| project DeviceId, User, BatteryLevel, NetworkType, CpuUsage` |

**Page 3: Combined Operations View**
| Tile | Visual Type | KQL Query |
|------|-------------|-----------|
| Event Ingestion Rate | Time chart | Both tables union, count by 1-min bins |
| Active Alerts | Stat | Count of open alert conditions |

#### 5.3 Dashboard Features to Demo
- **Copilot in Dashboard**: Add a tile using natural language (e.g., *"Show top 5 devices by CPU usage as a bar chart"*)
- **Auto-refresh**: Enable 30-second auto-refresh
- **Parameters**: Add a `TimeRange` parameter (last 15min / 1h / 6h / 24h)
- **Cross-filtering**: Click on a device in the map → filters other tiles
- **Drillthrough**: Click a failed operation → see details

---

### Phase 6: Activator — Automated Alerts (10 min)

#### 6.1 Alert Rules to Create

| Rule Name | Source | Condition | Action |
|-----------|--------|-----------|--------|
| Azure Error Spike | KQL: `AzureActivity` | Error count > 5 in 10-min window | Teams message to Ops channel |
| Low Battery Alert | KQL: `PhoneTelemetry` | `BatteryLevel < 15` for any device | Email to device owner |
| App Crash Burst | KQL: `PhoneTelemetry` | `CrashCount > 3` per app in 5 min | Trigger Pipeline (incident creation) |
| Device Offline | KQL: `PhoneTelemetry` | No events from device for 5 min (heartbeat) | Teams notification |

#### 6.2 Setup Steps
1. From the Real-Time Dashboard → tile options → **Create Activator alert**
2. Or: **+ New item** → **Activator** → Name: `RTI-Demo-Alerts`
3. Connect to Eventstream or KQL query as event source
4. Define object: `DeviceId` (phone) or `ResourceGroup` (Azure)
5. Add rule → set condition → configure action (email/Teams/Pipeline)
6. **Test rule**: Use the "Preview" to see how often it would fire on historical data
7. **Start** the Activator

---

### Phase 7: Copilot / Operational Agent (10 min)

#### 7.1 What to Showcase
The **Operational Agent** (Copilot for RTI) enables autonomous, AI-driven investigation of real-time data:

- **Natural language querying**: Ask questions in plain English, get KQL + results
- **Proactive insights**: Copilot surfaces anomalies and patterns automatically
- **Dashboard tile creation**: Build visuals from natural language descriptions
- **Investigation workflow**: Start from an alert → ask Copilot to investigate root cause

#### 7.2 Demo Script for Copilot / Operational Agent

1. **Open KQL Queryset** → Click Copilot
2. Ask: *"What are the most common error operations in Azure in the last hour?"*
3. Copilot generates KQL → show the results
4. Follow up: *"Which user triggered the most failures?"*
5. Follow up: *"Are there any phones with consistently declining battery?"*
6. **Switch to Dashboard** → Edit mode → Add tile with Copilot
7. Ask: *"Create a map showing all device locations colored by battery level"*
8. Copilot generates KQL + selects map visual → Apply

#### 7.3 Operational Agent Scenario
Present the **agentic** capability:
- *"Monitor my Azure subscription for any security-related events and alert me if there's a privilege escalation or role assignment change"*
- The agent can autonomously:
  - Write the detection KQL query
  - Create an Activator rule
  - Set up the notification workflow
- This demonstrates the shift from **reactive monitoring** to **proactive AI-driven operations**

---

## 5. Demo Flow / Talk Track (45-60 min)

| Time | Section | Key Message | Action |
|------|---------|-------------|--------|
| 0-3 min | Intro | "RTI = end-to-end streaming analytics in Fabric" | Show architecture slide |
| 3-8 min | Real-Time Hub | "One place to discover all streaming data" | Browse Real-Time Hub, show Azure + phone streams |
| 8-15 min | Eventstreams | "No-code ingestion + transformation" | Open both eventstreams, show data flowing, show transformations |
| 15-20 min | Eventhouse | "High-perf analytics on streaming data" | Show tables, row counts growing in real-time |
| 20-30 min | KQL + Copilot | "From question to answer in seconds" | Run queries, then switch to Copilot natural language |
| 30-40 min | Dashboard | "Live operational views for everyone" | Walk through all 3 pages, demo auto-refresh, Copilot tile creation |
| 40-48 min | Activator | "Automated response — no human in the loop" | Show rules, trigger a simulated alert, show Teams notification |
| 48-55 min | Operational Agent | "AI-driven autonomous monitoring" | Copilot investigation flow, proactive anomaly detection |
| 55-60 min | Wrap-up | "All in Fabric, unified, governed, scalable" | Q&A |

---

## 6. Tips for a Great Demo

### Before the Demo
- [ ] Run phone simulator for 30+ min to have meaningful data
- [ ] Trigger several Azure operations (create/delete resources) for Activity Log data
- [ ] Pre-build the dashboard and verify all tiles render
- [ ] Test Activator rules fire correctly
- [ ] Have a backup: use **Sample Data** source if Event Hub connectivity fails
- [ ] Set dashboard auto-refresh to 30s

### During the Demo
- [ ] Start with the "big picture" (Real-Time Hub) before drilling into details
- [ ] Show data flowing live — this is the "wow factor"
- [ ] Use Copilot to show accessibility for non-KQL users
- [ ] Deliberately trigger an alert scenario (e.g., drop battery in simulator → watch alert fire)
- [ ] Let audience suggest a question for Copilot (interactive moment)

### Fallback Plans
| Risk | Mitigation |
|------|------------|
| Event Hub connectivity issues | Use Eventstream Sample Data (Bicycles) |
| No Azure Activity events | Pre-generate by running CLI commands before demo |
| Copilot unavailable | Have pre-written KQL queries ready |
| Activator slow to fire | Show the "Preview" mode with historical data |

---

## 7. Cleanup After Demo

```bash
# Remove Azure resources
az group delete --name rg-rti-demo --yes --no-wait

# In Fabric: delete workspace or individual items
```

---

## 8. Reference Links

- [RTI Overview](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/overview)
- [RTI End-to-End Tutorial](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/tutorial-introduction)
- [Eventstreams Overview](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/event-streams/overview)
- [Create Real-Time Dashboard](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/dashboard-real-time-create)
- [Fabric Activator](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/data-activator/activator-introduction)
- [Event Hub Source for Eventstream](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/event-streams/add-source-azure-event-hubs)
