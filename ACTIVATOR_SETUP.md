# Data Activator (Reflex) — Setup Guide

## Status

| Component | Status | ID |
|-----------|--------|----|
| Reflex Container | ✅ Created | `b52f624e-d172-427c-9c0b-fb545f73fe9f` |
| Definition Upload | ❌ Blocked | Fabric REST API `updateDefinition` requires internal `importArtifactRequest` field |
| Manual Configuration | 📋 Instructions below | 4 alert rules to configure via UI |

> **API Limitation**: The Fabric REST API for Reflex items does not support programmatic rule
> configuration via `updateDefinition`. The endpoint returns `"The importArtifactRequest field
> is required"` — an internal Activator field not exposed in the public API. This is a known
> limitation. Rules must be configured via the Fabric UI.

## Open the Activator

1. Go to: https://app.powerbi.com/groups/92200814-2aa1-4f40-8073-f28b0ef79a0d/reflexes/b52f624e-d172-427c-9c0b-fb545f73fe9f
2. Or: Fabric Portal → RTI-Demo workspace → **Demo-Alerts**

## Option A: Stream-Based Alerts (Recommended)

Route events directly from the Eventstream to the Activator for **true real-time alerting** (sub-second latency, no polling).

### Setup Steps

1. Open **Phone-Stream** in Fabric → Edit mode
2. From the default stream node, click **+ Add destination** → **Activator** (Reflex)
3. Select workspace **RTI-Demo** → item **Demo-Alerts**
4. This creates an `eventstreamSource` inside the Activator
5. Repeat for **Activity-Stream**

### Alert 1: Low Battery Alert 🔋 (from Phone-Stream)

| Setting | Value |
|---------|-------|
| Source | Phone-Stream (Eventstream) |
| Object | Device (keyed by `DeviceId`) |
| Attribute | `BatteryLevel` (Number) |
| Trigger | Becomes less than **15** |
| Aggregation | Latest |
| Window | 5 minutes |
| Action | Teams Message |

### Alert 2: Weak Signal Alert 📶 (from Phone-Stream)

| Setting | Value |
|---------|-------|
| Source | Phone-Stream (Eventstream) |
| Object | Device (keyed by `DeviceId`) |
| Attribute | `SignalStrength` (Number) |
| Trigger | Becomes less than **-90** |
| Aggregation | Latest |
| Window | 5 minutes |
| Action | Teams Message |

### Alert 3: App Crash Burst 💥 (from Phone-Stream)

| Setting | Value |
|---------|-------|
| Source | Phone-Stream (Eventstream) |
| Object | App (keyed by `AppName`) |
| Attribute | `CrashCount` (Number) |
| Trigger | Becomes greater than **3** |
| Aggregation | Sum |
| Window | 5 minutes |
| Action | Teams Message |

### Alert 4: Azure Error Spike ⚠️ (from Activity-Stream)

| Setting | Value |
|---------|-------|
| Source | Activity-Stream (Eventstream) |
| Filter | `Level == "Error"` (apply in Activator event view or Eventstream operator) |
| Object | Operation (keyed by `OperationName`) |
| Attribute | Event count |
| Trigger | Becomes greater than **5** |
| Aggregation | Count |
| Window | 10 minutes |
| Action | Teams Message |

### Activator Configuration After Stream Connection

Once the Eventstreams are connected as sources in the Activator:

1. Open **Demo-Alerts** → you'll see the Eventstream sources listed
2. Click on a source → **Assign your data**
3. Choose the Object name (e.g., "Device") and identity column (e.g., `DeviceId`)
4. Select the measured attribute (e.g., `BatteryLevel`)
5. Click **New Rule** on the attribute
6. Configure the trigger condition (e.g., "Becomes less than 15")
7. Set the time window (e.g., "Latest over 5 minutes")
8. Add action → **Teams Message** → enter recipient
9. Keep rules **stopped** initially; toggle on when ready to activate

## Option B: KQL Query-Based Alerts (Alternative)

Uses KQL queries polling the Eventhouse every 60 seconds. Simpler setup but higher latency.

### Alert 1: Low Battery Alert 🔋

| Setting | Value |
|---------|-------|
| Source Type | KQL Query |
| Eventhouse | RTI-Demo-Eventhouse |
| Query | `PhoneTelemetry \| where Timestamp > ago(5m) \| summarize arg_max(Timestamp, *) by DeviceId \| project Timestamp, DeviceId, BatteryLevel, BatteryCharging, User` |
| Interval | 60 seconds |
| Object | Device (keyed by `DeviceId`) |
| Attribute | `BatteryLevel` — Becomes less than **15** — Latest — 5 min |

### Alert 2: Azure Error Spike ⚠️

| Setting | Value |
|---------|-------|
| Source Type | KQL Query |
| Eventhouse | RTI-Demo-Eventhouse |
| Query | `AzureActivity \| where Timestamp > ago(10m) \| where Level == 'Error' \| summarize ErrorCount = count() by OperationName` |
| Interval | 60 seconds |
| Object | Operation (keyed by `OperationName`) |
| Attribute | `ErrorCount` — Becomes greater than **5** — Sum — 10 min |

### Alert 3: App Crash Burst 💥

| Setting | Value |
|---------|-------|
| Source Type | KQL Query |
| Eventhouse | RTI-Demo-Eventhouse |
| Query | `PhoneTelemetry \| where Timestamp > ago(5m) \| where CrashCount > 0 \| summarize TotalCrashes = sum(CrashCount) by AppName` |
| Interval | 60 seconds |
| Object | App (keyed by `AppName`) |
| Attribute | `TotalCrashes` — Becomes greater than **3** — Sum — 5 min |

### Alert 4: Weak Signal Alert 📶

| Setting | Value |
|---------|-------|
| Source Type | KQL Query |
| Eventhouse | RTI-Demo-Eventhouse |
| Query | `PhoneTelemetry \| where Timestamp > ago(5m) \| summarize arg_max(Timestamp, *) by DeviceId \| project Timestamp, DeviceId, SignalStrength, NetworkType, User` |
| Interval | 60 seconds |
| Object | Device (keyed by `DeviceId`) |
| Attribute | `SignalStrength` — Becomes less than **-90** — Latest — 5 min |

### KQL Setup Steps

1. Open **Demo-Alerts** → click **Get data** → **KQL Database**
2. Select **RTI-Demo-Eventhouse** → paste query → set interval 60s
3. **Assign your data** → set Object + identity column
4. Select attribute → **New Rule** → configure trigger + action

## Reference Files

- `ReflexEntities.json` — Complete 24-entity definition (for Git sync or future API support)
