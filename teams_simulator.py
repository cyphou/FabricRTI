# teams_simulator.py
# Simulates correlated Teams telemetry: Call Quality + Network Probes + Device Health + M365 Health
# ============================================================
# DESIGN PRINCIPLE: Anomalies are SUBTLE and require CROSS-LAYER CORRELATION to detect.
# No single metric crosses a simple threshold. The value of RTI + Data Agent is that
# it can join signals across Device/Network/Server layers and detect patterns that
# threshold-based monitoring completely misses.
# ============================================================
# Anomaly scenarios (hard to detect):
#   1. THERMAL THROTTLE — Long meeting → gradual CPU rise → GPU clock drops → video degrades
#      Looks like: normal CPU (70-85%), but frame rate drops slowly. No single metric is red.
#   2. WIFI CHANNEL CONTENTION — Microwave in kitchen on F2 at lunch → intermittent RSSI dips
#      Looks like: random quality dips on WiFi users on one floor, not all. Ethernet users fine.
#   3. ISP PEERING DEGRADATION — One ISP's route to Teams relays degrades, not all traffic
#      Looks like: only some buildings affected, only for Teams traffic, web browsing is fine.
#   4. VPN SPLIT-TUNNEL MISCONFIGURATION — Teams media routed through VPN tunnel
#      Looks like: remote users have higher RTT only during calls, ping to relay is fine.
#   5. CASCADING DNS — DNS resolver intermittent → Teams auth delay → call joins fail
#      Looks like: call setup failures, but no network outage. Server looks healthy.
#   6. AUDIO DRIVER REGRESSION — Windows Update pushed new driver, specific headset model
#      Looks like: echo/quality issues on specific peripheral model + OS combo only.
#   7. MEETING STORM — Too many concurrent calls on one subnet exhaust QoS bandwidth
#      Looks like: quality degrades for everyone on a subnet when >N concurrent calls.
#   8. ASYMMETRIC DEGRADATION — Upload fine, download degrades (or vice versa)
#      Looks like: screen sharing works but receiving video doesn't, or audio is one-way.
# ============================================================
# Usage: pip install azure-eventhub && python teams_simulator.py

import json
import random
import time
import datetime
import uuid
import math
from azure.eventhub import EventHubProducerClient, EventData

# ============================================================
# Eventstream Custom Endpoint (Event Hub-compatible)
# ============================================================
CONN_STR = ""  # Set your Eventstream Custom Endpoint connection string here
INTERVAL_SEC = 3  # seconds between batches

# ============================================================
# Reference data — buildings, subnets, users, devices
# ============================================================

BUILDINGS = [
    {"name": "HQ-Paris",      "city": "Paris",         "floors": ["F1","F2","F3","F4","F5"],
     "subnets": ["10.1.1.0/24","10.1.2.0/24","10.1.3.0/24"], "isp": "Orange"},
    {"name": "Office-London",  "city": "London",        "floors": ["F1","F2","F3"],
     "subnets": ["10.2.1.0/24","10.2.2.0/24"], "isp": "BT"},
    {"name": "Office-NYC",     "city": "New York",      "floors": ["F1","F2","F3","F4"],
     "subnets": ["10.3.1.0/24","10.3.2.0/24","10.3.3.0/24"], "isp": "Verizon"},
    {"name": "Office-Tokyo",   "city": "Tokyo",         "floors": ["F1","F2"],
     "subnets": ["10.4.1.0/24","10.4.2.0/24"], "isp": "NTT"},
    {"name": "Office-Sydney",  "city": "Sydney",        "floors": ["F1","F2"],
     "subnets": ["10.5.1.0/24","10.5.2.0/24"], "isp": "Telstra"},
    {"name": "Office-Berlin",  "city": "Berlin",        "floors": ["F1","F2","F3"],
     "subnets": ["10.6.1.0/24","10.6.2.0/24"], "isp": "T-Mobile"},
    {"name": "Office-Mumbai",  "city": "Mumbai",        "floors": ["F1","F2"],
     "subnets": ["10.7.1.0/24","10.7.2.0/24"], "isp": "Jio"},
    {"name": "Remote-Home",    "city": "Various",       "floors": ["Home"],
     "subnets": ["192.168.1.0/24","192.168.0.0/24"], "isp": "Various"},
]

USERS = [
    # name, building_idx, floor, device_type, vpn
    ("alice@contoso.com",   0, "F2", "Laptop",  False),
    ("bob@contoso.com",     0, "F3", "Laptop",  False),
    ("charlie@contoso.com", 1, "F1", "Laptop",  False),
    ("diana@contoso.com",   1, "F2", "Desktop", False),
    ("eve@contoso.com",     2, "F1", "Laptop",  False),
    ("frank@contoso.com",   2, "F3", "Laptop",  False),
    ("grace@contoso.com",   3, "F1", "Laptop",  False),
    ("hank@contoso.com",    4, "F1", "Desktop", False),
    ("iris@contoso.com",    5, "F2", "Laptop",  False),
    ("jack@contoso.com",    6, "F1", "Laptop",  False),
    ("karen@contoso.com",   7, "Home", "Laptop", True),  # Remote + VPN
    ("leo@contoso.com",     7, "Home", "Laptop", True),
    ("mona@contoso.com",    7, "Home", "Laptop", True),
    ("nate@contoso.com",    0, "F4", "MeetingRoom", False),
    ("olivia@contoso.com",  2, "F2", "Phone",   False),
    ("paul@contoso.com",    0, "F1", "Laptop",  False),
    ("quinn@contoso.com",   1, "F3", "Laptop",  False),
    ("rosa@contoso.com",    3, "F2", "Desktop", False),
    ("sam@contoso.com",     5, "F1", "Laptop",  False),
    ("tina@contoso.com",    4, "F2", "Laptop",  False),
    ("uma@contoso.com",     6, "F2", "Laptop",  True),   # Office but VPN
    ("victor@contoso.com",  0, "F5", "Laptop",  False),
    ("wendy@contoso.com",   2, "F4", "Laptop",  False),
    ("xander@contoso.com",  7, "Home", "Desktop", True),
    ("yara@contoso.com",    0, "F3", "Laptop",  False),
]

PERIPHERALS = [
    ("Jabra Evolve2 85",    "Headset"),
    ("Poly Voyager Focus",  "Headset"),
    ("Surface Headphones 2","Headset"),
    ("Jabra Speak 750",     "Speaker"),
    ("Poly Sync 60",        "Speaker"),
    ("AirPods Pro",         "Bluetooth"),
    ("Built-in Speaker",    "Builtin"),
    ("Logitech Zone Vibe",  "Headset"),
]

OS_VERSIONS = ["Windows 11 23H2", "Windows 11 24H2", "macOS 15.2", "macOS 14.5", "iOS 18.1", "Android 15"]
TEAMS_VERSIONS = ["24.25.1.0", "24.24.3.0", "24.23.0.0", "24.20.1.0"]  # last one is outdated
AUDIO_DRIVERS = [("10.0.22621.4", "OK"), ("10.0.22000.1", "Outdated"), ("6.3.9600.0", "Outdated"), ("10.0.26100.1", "OK")]
CALL_TYPES = ["P2P", "GroupCall", "Meeting", "PSTN"]
MEDIA_TYPES = ["Audio", "Video", "ScreenShare"]
AUDIO_CODECS = ["OPUS", "SILK", "SATIN"]
VIDEO_RESOLUTIONS = ["1080p", "720p", "480p", "360p"]
MEDIA_PATHS = ["Direct", "Direct", "Direct", "Relay", "TURN"]

TEAMS_RELAY_ENDPOINTS = [
    "13.107.64.0:3478",    # Teams Transport Relay
    "52.112.0.0:3478",     # Teams Media Relay
    "52.120.0.0:3478",     # Teams TURN
    "teams.microsoft.com",
]

M365_SERVICES = ["MicrosoftTeams", "Exchange", "SharePoint", "OneDrive", "AzureAD"]

# ============================================================
# State management — per-user and per-building
# ============================================================

# Per-user device state
device_state = {}
for i, (upn, bldg_idx, floor, dev_type, vpn) in enumerate(USERS):
    bldg = BUILDINGS[bldg_idx]
    periph = random.choice(PERIPHERALS)
    driver = random.choice(AUDIO_DRIVERS)
    device_state[upn] = {
        "device_id": f"{dev_type[:3].upper()}-{i+1:03d}",
        "device_name": f"{upn.split('@')[0]}-{dev_type.lower()}",
        "device_type": dev_type,
        "building": bldg["name"],
        "city": bldg["city"],
        "floor": floor,
        "subnet": random.choice(bldg["subnets"]),
        "isp": bldg["isp"],
        "vpn": vpn,
        "os": random.choice(OS_VERSIONS),
        "teams_version": random.choice(TEAMS_VERSIONS),
        "peripheral": periph[1],
        "peripheral_model": periph[0],
        "audio_driver": driver[0],
        "audio_driver_status": driver[1],
        "cpu_base": random.uniform(15, 40),
        "ram_base": random.uniform(40, 65),
        "in_call": False,
        "call_start": None,
        "call_id": None,
        "call_duration_ticks": 0,    # for thermal throttle scenario
        "thermal_state": 0.0,        # 0=cold, accumulates during long calls
    }

# Per-building network state (baseline)
network_state = {}
for bldg in BUILDINGS:
    network_state[bldg["name"]] = {
        "rtt_base": random.uniform(8, 25) if bldg["name"] != "Remote-Home" else random.uniform(15, 45),
        "jitter_base": random.uniform(2, 8),
        "loss_base": random.uniform(0, 0.3),
        "rssi_base": random.randint(-55, -40) if bldg["name"] != "Remote-Home" else random.randint(-70, -55),
        "bandwidth_base": random.uniform(80, 200) if bldg["name"] != "Remote-Home" else random.uniform(20, 60),
    }

# Per-subnet concurrent call tracking (for meeting storm)
subnet_call_count = {}

# Active M365 incident (None = healthy)
active_incident = None
incident_counter = 0

# ============================================================
# SUBTLE ANOMALY SCENARIOS — Each one is hard to detect alone
# Only cross-layer correlation reveals the root cause.
# ============================================================

# Scenario state — multiple can be active simultaneously
scenarios = {
    # Scenario 1: THERMAL THROTTLE
    # After 5+ min in a call, laptop CPU rises gradually → GPU throttles → video degrades
    # Symptom: VideoFrameRate drops slowly from 28→18→12 fps. CPU never exceeds 85%.
    # Why it's hard: CPU is "normal" (70-85%), no network issue. Only correlating
    # call duration + GPU + frame rate reveals the pattern.
    "thermal_throttle": {
        "active": False, "targets": set(), "start": None, "duration": 0,
    },

    # Scenario 2: ISP PEERING — One ISP's route to specific Teams relay degrades
    # Symptom: RTT to ONE relay endpoint rises, others are fine. Only buildings
    # with that ISP are affected. Non-Teams traffic is fine (can't see that).
    # Why it's hard: Average RTT per building barely moves (3 relays, only 1 bad).
    # Need to look at per-endpoint RTT to see the pattern.
    "isp_peering": {
        "active": False, "isp": None, "relay": None,
        "start": None, "duration": 0,
        "rtt_add": 0, "loss_add": 0,
    },

    # Scenario 3: WIFI CHANNEL CONTENTION — Lunchtime microwave on one floor
    # Symptom: Short RSSI dips (not sustained) on 2.4GHz channels (1,6,11) only.
    # 5GHz users on same floor are fine. Ethernet users fine.
    # Why it's hard: RSSI averages look OK. Only burst pattern + channel + floor
    # correlation reveals it. Call quality dips are intermittent.
    "wifi_contention": {
        "active": False, "building": None, "floor": None,
        "start": None, "duration": 0, "channel_24ghz": True,
    },

    # Scenario 4: VPN MEDIA BYPASS FAILURE — Split tunnel configured but
    # Teams media traffic routes through tunnel anyway (mis-detected endpoint)
    # Symptom: VPN users have normal ping but call RTT is 40ms higher.
    # Network probes show VPN latency is fine. Only call-time RTT is elevated.
    # Why it's hard: Network looks healthy. Device looks healthy. Only comparing
    # probe RTT vs call RTT for VPN users reveals the discrepancy.
    "vpn_media_bypass": {
        "active": False, "start": None, "duration": 0,
        "extra_rtt": 0,
    },

    # Scenario 5: DNS RESOLVER INTERMITTENT — Corporate DNS returns slowly 10% of time
    # Symptom: Random call setup failures (~8% rate). No network outage.
    # Server health is "operational". DNS probe times occasionally spike but
    # average DNS is only slightly elevated (15ms→22ms).
    # Why it's hard: Call failures look random. Network probes mostly healthy.
    # Only correlating DNS spikes with call setup failure timestamps reveals it.
    "dns_intermittent": {
        "active": False, "building": None,
        "start": None, "duration": 0,
        "spike_pct": 0,  # percentage of DNS queries that are slow
    },

    # Scenario 6: AUDIO DRIVER REGRESSION — Windows Update pushed driver 10.0.26100.1
    # that has echo cancellation bugs with Jabra Evolve2 85 headsets
    # Symptom: Users with that exact combo report "echo" (we model as jitter spikes).
    # Quality score drops only for that peripheral+driver combo, everyone else is fine.
    # Why it's hard: Looks like random quality issues on a few users. Only grouping by
    # AudioDriverVersion + PeripheralModel reveals the pattern.
    "driver_regression": {
        "active": False, "start": None, "duration": 0,
        "target_driver": "10.0.26100.1",
        "target_peripheral": "Jabra Evolve2 85",
    },

    # Scenario 7: MEETING STORM — All-hands meeting causes >12 concurrent calls on
    # one subnet, exhausting QoS bandwidth reservation
    # Symptom: Quality degrades for all users on that subnet proportional to concurrency.
    # Network probes show normal bandwidth (probes don't consume QoS quota).
    # Why it's hard: Network looks fine. Devices look fine. Only counting concurrent
    # calls per subnet and correlating with quality drop reveals it.
    "meeting_storm": {
        "active": False, "subnet": None, "building": None,
        "start": None, "duration": 0,
        "forced_calls": [],  # users forced into calls
    },

    # Scenario 8: ASYMMETRIC DEGRADATION — Download path degrades but upload is fine
    # Symptom: Screen sharing outbound works, but receiving video freezes.
    # Audio (symmetric, low bandwidth) is fine. Video receive frame rate drops.
    # Why it's hard: Audio quality score is OK. Upload metrics are OK.
    # Only looking at Video + direction "Inbound" reveals the issue.
    "asymmetric": {
        "active": False, "building": None,
        "start": None, "duration": 0,
    },
}

# ============================================================
# Scenario lifecycle — inject, evolve, expire
# ============================================================

def evolve_scenarios(now, tick):
    """Manage scenario lifecycle. Each has independent probability and duration."""
    global active_incident, incident_counter

    # Expire finished scenarios
    for key, sc in scenarios.items():
        if sc["active"] and sc["start"] and (now - sc["start"]).total_seconds() > sc["duration"]:
            sc["active"] = False
            if key == "meeting_storm":
                # Release forced callers
                for upn in sc.get("forced_calls", []):
                    if upn in device_state:
                        device_state[upn]["in_call"] = False
                sc["forced_calls"] = []

    # --- Scenario 1: Thermal throttle (always passively active for long calls) ---
    # No injection needed — handled per-user in generate functions
    scenarios["thermal_throttle"]["active"] = True  # always on

    # --- Scenario 2: ISP Peering (~1.5% per tick, 3-8 min) ---
    if random.random() < 0.015 and not scenarios["isp_peering"]["active"]:
        isp = random.choice(["Orange", "BT", "Verizon", "NTT", "Telstra"])
        relay = random.choice(TEAMS_RELAY_ENDPOINTS)
        scenarios["isp_peering"].update({
            "active": True, "isp": isp, "relay": relay,
            "start": now, "duration": random.randint(180, 480),
            "rtt_add": random.uniform(25, 60),  # subtle: +25-60ms to ONE relay
            "loss_add": random.uniform(0.5, 2.0),  # subtle: +0.5-2% loss
        })
        print(f"  🔍 Scenario 2: ISP peering — {isp} → {relay} (+{scenarios['isp_peering']['rtt_add']:.0f}ms RTT)")

    # --- Scenario 3: WiFi contention (~2% per tick, 2-6 min) ---
    if random.random() < 0.02 and not scenarios["wifi_contention"]["active"]:
        bldg = random.choice(BUILDINGS[:7])
        floor = random.choice(bldg["floors"])
        scenarios["wifi_contention"].update({
            "active": True, "building": bldg["name"], "floor": floor,
            "start": now, "duration": random.randint(120, 360),
            "channel_24ghz": True,
        })
        print(f"  🔍 Scenario 3: WiFi contention — {bldg['name']}/{floor} (2.4GHz)")

    # --- Scenario 4: VPN media bypass failure (~1% per tick, 5-10 min) ---
    if random.random() < 0.01 and not scenarios["vpn_media_bypass"]["active"]:
        scenarios["vpn_media_bypass"].update({
            "active": True, "start": now,
            "duration": random.randint(300, 600),
            "extra_rtt": random.uniform(30, 55),  # subtle: 30-55ms extra on calls only
        })
        print(f"  🔍 Scenario 4: VPN media bypass failure (+{scenarios['vpn_media_bypass']['extra_rtt']:.0f}ms call RTT)")

    # --- Scenario 5: DNS intermittent (~1.2% per tick, 3-7 min) ---
    if random.random() < 0.012 and not scenarios["dns_intermittent"]["active"]:
        bldg = random.choice(BUILDINGS[:7])
        scenarios["dns_intermittent"].update({
            "active": True, "building": bldg["name"],
            "start": now, "duration": random.randint(180, 420),
            "spike_pct": random.uniform(8, 18),  # subtle: 8-18% of DNS queries slow
        })
        print(f"  🔍 Scenario 5: DNS intermittent — {bldg['name']} ({scenarios['dns_intermittent']['spike_pct']:.0f}% slow)")

    # --- Scenario 6: Driver regression (persistent, ~0.5% to start, long duration) ---
    if random.random() < 0.005 and not scenarios["driver_regression"]["active"]:
        scenarios["driver_regression"].update({
            "active": True, "start": now,
            "duration": random.randint(600, 1200),  # 10-20 min (simulates until rollback)
        })
        print(f"  🔍 Scenario 6: Driver regression — {scenarios['driver_regression']['target_driver']} + {scenarios['driver_regression']['target_peripheral']}")

    # --- Scenario 7: Meeting storm (~0.8% per tick, 2-5 min) ---
    if random.random() < 0.008 and not scenarios["meeting_storm"]["active"]:
        # Pick a subnet with multiple users
        subnet_users = {}
        for upn, bldg_idx, floor, dt, vpn in USERS:
            sn = device_state[upn]["subnet"]
            subnet_users.setdefault(sn, []).append(upn)
        busy_subnets = [(sn, users) for sn, users in subnet_users.items() if len(users) >= 3]
        if busy_subnets:
            sn, users = random.choice(busy_subnets)
            bldg_name = device_state[users[0]]["building"]
            # Force most users on this subnet into calls
            forced = users[:min(len(users), random.randint(4, 8))]
            for upn in forced:
                device_state[upn]["in_call"] = True
                device_state[upn]["call_start"] = now
                device_state[upn]["call_id"] = f"allhands-{uuid.uuid4().hex[:8]}"
            scenarios["meeting_storm"].update({
                "active": True, "subnet": sn, "building": bldg_name,
                "start": now, "duration": random.randint(120, 300),
                "forced_calls": forced,
            })
            print(f"  🔍 Scenario 7: Meeting storm — {sn} ({len(forced)} concurrent calls)")

    # --- Scenario 8: Asymmetric degradation (~1% per tick, 3-6 min) ---
    if random.random() < 0.01 and not scenarios["asymmetric"]["active"]:
        bldg = random.choice(BUILDINGS[:7])
        scenarios["asymmetric"].update({
            "active": True, "building": bldg["name"],
            "start": now, "duration": random.randint(180, 360),
        })
        print(f"  🔍 Scenario 8: Asymmetric degradation — {bldg['name']} (download path)")


# ============================================================
# Event generators — subtle, correlated, cross-layer
# ============================================================

def generate_network_probe(building, now):
    """One probe per subnet. Anomalies are subtle — averages look OK."""
    events = []
    for subnet in building["subnets"]:
        ns = network_state[building["name"]]

        # Baseline with natural noise
        rtt = ns["rtt_base"] + random.gauss(0, 2)
        jitter = ns["jitter_base"] + random.gauss(0, 1)
        loss = ns["loss_base"] + abs(random.gauss(0, 0.1))
        bw = ns["bandwidth_base"] + random.gauss(0, 5)
        rssi = ns["rssi_base"] + random.randint(-3, 3)
        dns = random.uniform(3, 12)
        proxy_lat = random.uniform(1, 4) if building["name"] != "Remote-Home" else random.uniform(5, 20)
        vpn_lat = random.uniform(8, 20) if building["name"] == "Remote-Home" else 0.0
        wifi_ch = random.choice([1, 6, 11, 36, 40, 44])
        wifi_interf = random.uniform(0, 8)

        # Pick a target endpoint for this probe
        target = random.choice(TEAMS_RELAY_ENDPOINTS)

        # --- Scenario 2: ISP peering — only affects ONE relay + matching ISP ---
        sc2 = scenarios["isp_peering"]
        if sc2["active"] and building["isp"] == sc2["isp"] and target == sc2["relay"]:
            rtt += sc2["rtt_add"]
            loss += sc2["loss_add"]
            # Other relays are fine — this is the subtle part

        # --- Scenario 3: WiFi contention — intermittent RSSI dips on 2.4GHz ---
        sc3 = scenarios["wifi_contention"]
        probe_floor = random.choice(building["floors"])
        if (sc3["active"] and sc3["building"] == building["name"]
                and sc3["floor"] == probe_floor and wifi_ch in [1, 6, 11]):
            # Intermittent: 30% of probes see a dip, not all
            if random.random() < 0.30:
                rssi -= random.randint(8, 18)  # subtle dip, not catastrophic
                wifi_interf += random.uniform(15, 35)
                jitter += random.uniform(3, 8)

        # --- Scenario 5: DNS intermittent — most queries fine, some spike ---
        sc5 = scenarios["dns_intermittent"]
        if sc5["active"] and sc5["building"] == building["name"]:
            if random.random() < sc5["spike_pct"] / 100.0:
                dns = random.uniform(80, 250)  # spike, but only N% of the time

        # --- Scenario 7: Meeting storm — probes DON'T show degradation ---
        # (probes use ICMP/UDP, not QoS-tagged media traffic → looks fine)

        # --- Scenario 8: Asymmetric — probes are symmetric, can't detect ---

        probe_result = "Success"
        if dns > 100:
            probe_result = "DNSFail"
        elif rtt > 150:
            probe_result = "HighLatency"

        events.append({
            "Timestamp": now.isoformat(),
            "ProbeId": f"probe-{uuid.uuid4().hex[:8]}",
            "SubnetId": subnet,
            "Building": building["name"],
            "Floor": probe_floor,
            "City": building["city"],
            "TargetEndpoint": target,
            "RTT": round(max(1, rtt), 1),
            "PacketLoss": round(max(0, min(loss, 100)), 2),
            "Jitter": round(max(0, jitter), 1),
            "Bandwidth": round(max(1, bw), 1),
            "DNSResolutionMs": round(max(1, dns), 1),
            "SSID": f"{building['name']}-WiFi" if building["name"] != "Remote-Home" else "HomeNetwork",
            "RSSI": max(-95, rssi),
            "WiFiChannel": wifi_ch,
            "WiFiInterference": round(max(0, wifi_interf), 1),
            "ProxyLatency": round(max(0, proxy_lat), 1),
            "VPNLatency": round(max(0, vpn_lat), 1),
            "ISP": building["isp"],
            "ProbeResult": probe_result,
        })
    return events


def generate_device_health(upn, now):
    """Device health. Thermal throttle is the subtle one — CPU is 'normal' 70-85%."""
    ds = device_state[upn]

    cpu = ds["cpu_base"] + random.gauss(0, 3)
    ram = ds["ram_base"] + random.gauss(0, 3)
    gpu = random.uniform(5, 20)
    disk_free = random.uniform(20, 200)
    teams_process = "Running"
    periph_connected = True
    camera = True
    driver_status = ds["audio_driver_status"]

    # In-call bump (normal)
    if ds["in_call"]:
        cpu += random.uniform(10, 25)
        ram += random.uniform(5, 12)
        gpu += random.uniform(15, 35)
        ds["call_duration_ticks"] += 1

        # --- Scenario 1: Thermal throttle — GRADUAL, not sudden ---
        # After ~100 ticks in call (~5 min), thermal builds up slowly
        ticks_in_call = ds["call_duration_ticks"]
        if ticks_in_call > 100:  # ~5 min at 3s interval
            # Thermal increases slowly: 0→1.0 over next 200 ticks
            thermal_progress = min(1.0, (ticks_in_call - 100) / 200.0)
            ds["thermal_state"] = thermal_progress
            # CPU rises slightly (stays in 70-85% range — looks normal!)
            cpu += thermal_progress * random.uniform(8, 15)
            # GPU shows thermal throttle — this is the key signal
            gpu = max(gpu, 40 + thermal_progress * random.uniform(20, 40))
            # But CPU and GPU individually don't cross alarm thresholds
    else:
        ds["call_duration_ticks"] = 0
        ds["thermal_state"] = max(0, ds["thermal_state"] - 0.05)  # cool down

    # --- Scenario 5: DNS intermittent — Teams process might restart ---
    sc5 = scenarios["dns_intermittent"]
    if sc5["active"] and sc5["building"] == ds["building"]:
        # Very rarely, Teams process restarts due to auth timeout
        if random.random() < 0.005:
            teams_process = "Crashed"

    # --- Scenario 6: Driver regression — driver status stays "OK" ---
    # The driver IS installed, it's just buggy. Status looks fine.

    return {
        "Timestamp": now.isoformat(),
        "DeviceId": ds["device_id"],
        "UserUPN": upn,
        "DeviceName": ds["device_name"],
        "DeviceType": ds["device_type"],
        "CPU": round(min(100, max(0, cpu)), 1),
        "RAM": round(min(100, max(0, ram)), 1),
        "DiskFree": round(disk_free, 1),
        "GPUUsage": round(min(100, max(0, gpu)), 1),
        "OSVersion": ds["os"],
        "TeamsVersion": ds["teams_version"],
        "TeamsProcess": teams_process,
        "AudioDriverVersion": ds["audio_driver"],
        "AudioDriverStatus": driver_status,
        "PeripheralConnected": periph_connected,
        "PeripheralType": ds["peripheral"] if periph_connected else "None",
        "PeripheralModel": ds["peripheral_model"] if periph_connected else "",
        "CameraConnected": camera,
        "NetworkAdapter": "Ethernet" if ds["device_type"] == "Desktop" else "WiFi",
        "AdapterSpeed": 1000 if ds["device_type"] == "Desktop" else random.choice([300, 600, 867]),
        "City": ds["city"],
        "Building": ds["building"],
        "Floor": ds["floor"],
        "SubnetId": ds["subnet"],
    }


def generate_call_quality(upn, now):
    """Call quality. Multiple subtle degradations stack — no single one is alarming."""
    ds = device_state[upn]

    if not ds["in_call"]:
        if random.random() < 0.12:
            ds["in_call"] = True
            ds["call_start"] = now
            ds["call_id"] = f"call-{uuid.uuid4().hex[:12]}"
            ds["call_duration_ticks"] = 0
        else:
            return None

    # End call with some probability (longer calls less likely to end)
    call_minutes = ds["call_duration_ticks"] * INTERVAL_SEC / 60.0
    end_prob = 0.08 if call_minutes < 5 else 0.04 if call_minutes < 15 else 0.02
    if random.random() < end_prob and not scenarios["meeting_storm"]["active"]:
        ds["in_call"] = False
        return None

    ns = network_state[ds["building"]]

    # Baseline quality with natural variation
    jitter = ns["jitter_base"] + random.gauss(0, 1.5)
    loss = ns["loss_base"] + abs(random.gauss(0, 0.15))
    rtt = ns["rtt_base"] + random.gauss(0, 3)
    fps = 28 + random.gauss(0, 1.5)
    resolution = "1080p"
    video_loss = loss * random.uniform(0.8, 1.2)
    direction = random.choice(["Inbound", "Outbound"])
    media_type = random.choice(MEDIA_TYPES)
    failure_reason = ""

    # Track per-subnet concurrency
    sn = ds["subnet"]
    subnet_call_count[sn] = subnet_call_count.get(sn, 0) + 1

    # =====================================================
    # Apply subtle scenario effects — each one is small!
    # =====================================================

    # --- Scenario 1: Thermal throttle — gradual FPS drop ---
    if ds["thermal_state"] > 0.2 and media_type in ("Video", "ScreenShare"):
        # FPS degrades proportionally to thermal state
        fps -= ds["thermal_state"] * random.uniform(8, 15)
        # Resolution downscales gradually
        if ds["thermal_state"] > 0.5:
            resolution = "720p"
        if ds["thermal_state"] > 0.8:
            resolution = "480p"
        # Subtle jitter increase from thermal
        jitter += ds["thermal_state"] * random.uniform(2, 5)

    # --- Scenario 2: ISP peering — slight RTT increase for some calls ---
    sc2 = scenarios["isp_peering"]
    if sc2["active"] and ds["isp"] == sc2["isp"]:
        # Only ~40% of calls route through the degraded relay
        if random.random() < 0.40:
            rtt += sc2["rtt_add"] * random.uniform(0.7, 1.0)
            loss += sc2["loss_add"] * random.uniform(0.5, 1.0)

    # --- Scenario 3: WiFi contention — intermittent for WiFi users on that floor ---
    sc3 = scenarios["wifi_contention"]
    if (sc3["active"] and sc3["building"] == ds["building"]
            and sc3["floor"] == ds["floor"] and ds["device_type"] != "Desktop"):
        # Only 25% of call segments affected (intermittent bursts)
        if random.random() < 0.25:
            jitter += random.uniform(5, 12)
            loss += random.uniform(0.5, 2.0)
            fps -= random.uniform(3, 8)

    # --- Scenario 4: VPN media bypass — extra RTT only during calls ---
    sc4 = scenarios["vpn_media_bypass"]
    if sc4["active"] and ds["vpn"]:
        rtt += sc4["extra_rtt"]
        jitter += random.uniform(3, 7)

    # --- Scenario 5: DNS intermittent — call setup failures ---
    sc5 = scenarios["dns_intermittent"]
    if sc5["active"] and ds["building"] == sc5["building"]:
        # New calls have a chance of failing to set up
        if ds["call_duration_ticks"] < 2 and random.random() < sc5["spike_pct"] / 100.0:
            failure_reason = "CallSetupTimeout"  # generic — doesn't say "DNS"
            return {
                "Timestamp": now.isoformat(), "CallId": ds["call_id"],
                "UserUPN": upn, "DeviceId": ds["device_id"],
                "CallType": random.choice(CALL_TYPES), "MediaType": "Audio",
                "Direction": direction, "Duration": 0,
                "AudioJitter": 0, "AudioPacketLoss": 0, "AudioRTT": 0,
                "AudioCodec": "", "VideoFrameRate": 0, "VideoResolution": "",
                "VideoPacketLoss": 0,
                "NetworkType": "WiFi" if ds["device_type"] != "Desktop" else "Ethernet",
                "SubnetId": sn, "LocalIP": "", "ReflexiveIP": "",
                "MediaPath": "", "VPNActive": ds["vpn"],
                "DeviceCPU": round(ds["cpu_base"] + random.uniform(5, 15), 1),
                "DeviceRAM": round(ds["ram_base"] + random.uniform(3, 10), 1),
                "Peripheral": ds["peripheral"], "PeripheralModel": ds["peripheral_model"],
                "QualityScore": 1.0, "QualityLabel": "Poor",
                "FailureReason": failure_reason,
                "City": ds["city"], "Building": ds["building"], "Floor": ds["floor"],
            }

    # --- Scenario 6: Driver regression — jitter spikes for specific combo ---
    sc6 = scenarios["driver_regression"]
    if (sc6["active"] and ds["audio_driver"] == sc6["target_driver"]
            and ds["peripheral_model"] == sc6["target_peripheral"]):
        # Echo cancellation failure → manifests as periodic jitter bursts
        if random.random() < 0.40:
            jitter += random.uniform(8, 20)
            # Quality degrades but not catastrophically
            loss += random.uniform(0.3, 1.0)

    # --- Scenario 7: Meeting storm — QoS bandwidth exhaustion ---
    sc7 = scenarios["meeting_storm"]
    if sc7["active"] and sn == sc7["subnet"]:
        concurrent = len(sc7["forced_calls"])
        if concurrent > 3:
            # Each additional call above 3 degrades everyone's quality
            overload = (concurrent - 3) / 5.0  # 0→1 scale
            jitter += overload * random.uniform(5, 15)
            loss += overload * random.uniform(0.5, 3.0)
            fps -= overload * random.uniform(5, 12)
            if overload > 0.5:
                resolution = "720p"

    # --- Scenario 8: Asymmetric — only affects inbound video ---
    sc8 = scenarios["asymmetric"]
    if sc8["active"] and sc8["building"] == ds["building"]:
        if direction == "Inbound" and media_type == "Video":
            fps -= random.uniform(10, 20)
            video_loss += random.uniform(1, 4)
            resolution = random.choice(["480p", "360p"])
        # Outbound and Audio are completely fine

    # --- Natural VPN penalty (always, but small) ---
    if ds["vpn"]:
        rtt += random.uniform(5, 15)
        jitter += random.uniform(1, 3)

    # Calculate quality score (MOS-like, 1-5)
    score = 4.5  # baseline is "good", not "perfect"
    score += random.gauss(0, 0.1)
    if jitter > 15: score -= min(1.0, (jitter - 15) / 25)
    if loss > 1: score -= min(1.0, (loss - 1) / 4)
    if rtt > 60: score -= min(0.8, (rtt - 60) / 120)
    if fps < 20 and media_type in ("Video", "ScreenShare"):
        score -= min(0.8, (20 - fps) / 15)
    score = max(1.0, min(5.0, score))

    label = "Good" if score >= 3.5 else ("Acceptable" if score >= 2.5 else "Poor")

    return {
        "Timestamp": now.isoformat(),
        "CallId": ds["call_id"],
        "UserUPN": upn,
        "DeviceId": ds["device_id"],
        "CallType": random.choice(CALL_TYPES),
        "MediaType": media_type,
        "Direction": direction,
        "Duration": int((now - ds["call_start"]).total_seconds()) if ds["call_start"] else 0,
        "AudioJitter": round(max(0, jitter), 1),
        "AudioPacketLoss": round(max(0, min(loss, 100)), 2),
        "AudioRTT": round(max(1, rtt), 1),
        "AudioCodec": random.choice(AUDIO_CODECS),
        "VideoFrameRate": round(max(0, fps), 1) if media_type != "Audio" else 0,
        "VideoResolution": resolution if media_type != "Audio" else "",
        "VideoPacketLoss": round(max(0, min(video_loss, 100)), 2) if media_type != "Audio" else 0,
        "NetworkType": "Ethernet" if ds["device_type"] == "Desktop" else (
            "Cellular" if ds["device_type"] == "Phone" else "WiFi"),
        "SubnetId": sn,
        "LocalIP": f"{sn.split('/')[0][:-1]}{random.randint(10, 250)}",
        "ReflexiveIP": f"{random.randint(40, 220)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}",
        "MediaPath": random.choice(MEDIA_PATHS),
        "VPNActive": ds["vpn"],
        "DeviceCPU": round(min(100, ds["cpu_base"] + random.uniform(10, 30)), 1),
        "DeviceRAM": round(min(100, ds["ram_base"] + random.uniform(5, 18)), 1),
        "Peripheral": ds["peripheral"],
        "PeripheralModel": ds["peripheral_model"],
        "QualityScore": round(score, 1),
        "QualityLabel": label,
        "FailureReason": failure_reason,
        "City": ds["city"],
        "Building": ds["building"],
        "Floor": ds["floor"],
    }


def generate_service_health(now):
    """M365 service health. Incidents are rare and subtle (Sev2, degradation, not outage)."""
    global active_incident, incident_counter

    if active_incident is None:
        # Healthy heartbeat (~20% of ticks)
        if random.random() < 0.2:
            return {
                "Timestamp": now.isoformat(),
                "IncidentId": "",
                "ServiceName": "MicrosoftTeams",
                "Status": "ServiceOperational",
                "Severity": "",
                "Title": "All services operating normally",
                "ImpactDescription": "",
                "Classification": "Advisory",
                "StartTime": "",
                "EndTime": "",
                "AffectedRegions": "",
                "AffectedUsers": 0,
                "IsResolved": True,
            }
        return None

    inc = active_incident
    return {
        "Timestamp": now.isoformat(),
        "IncidentId": inc["id"],
        "ServiceName": inc["service"],
        "Status": random.choice(["ServiceDegradation", "InvestigatingIssue"]),
        "Severity": inc["severity"],
        "Title": inc["title"],
        "ImpactDescription": f"Impact to {inc['service']} users in {', '.join(inc['regions'])}",
        "Classification": "Incident",
        "StartTime": inc["start"].isoformat(),
        "EndTime": "",
        "AffectedRegions": ",".join(inc["regions"]),
        "AffectedUsers": random.randint(100, 5000),
        "IsResolved": False,
    }


# ============================================================
# Helper: Create EventData with application property for routing
# ============================================================

def make_event(payload, table_name):
    """Create an EventData with _table both in payload and as an application property.
    The application property is used by Eventstream's 'Dynamic schema via headers'
    to route events to separate Eventhouse tables from a single stream."""
    payload["_table"] = table_name
    ed = EventData(json.dumps(payload))
    ed.properties = {"_table": table_name}
    return ed


# ============================================================
# Main loop
# ============================================================

def main():
    producer = EventHubProducerClient.from_connection_string(CONN_STR)
    print("=" * 65)
    print("  Teams Issue Detection Simulator — Subtle Anomaly Edition")
    print("=" * 65)
    print(f"  Users:       {len(USERS)}")
    print(f"  Buildings:   {len(BUILDINGS)}")
    print(f"  Scenarios:   8 (thermal, ISP peering, WiFi contention, VPN bypass,")
    print(f"               DNS intermittent, driver regression, meeting storm, asymmetric)")
    print(f"  Tables:      TeamsCallQuality, NetworkProbe, DeviceHealth, M365ServiceHealth")
    print(f"  Interval:    {INTERVAL_SEC}s")
    print()
    print("  ⚠  Anomalies are SUBTLE — no single metric crosses a threshold.")
    print("  ⚠  Cross-layer correlation is required to detect root cause.")
    print()
    print("Press Ctrl+C to stop.\n")

    total_events = {"call": 0, "network": 0, "device": 0, "service": 0}
    tick = 0

    try:
        while True:
            now = datetime.datetime.now(datetime.timezone.utc)
            tick += 1

            # Reset per-tick subnet counters
            subnet_call_count.clear()

            # Evolve scenarios
            evolve_scenarios(now, tick)

            batch = producer.create_batch()
            batch_count = 0

            # --- Network probes (every tick) ---
            for bldg in BUILDINGS:
                for event in generate_network_probe(bldg, now):
                    batch.add(make_event(event, "NetworkProbe"))
                    batch_count += 1
                    total_events["network"] += 1

            # --- Device health (every 3rd tick ~9s) ---
            if tick % 3 == 0:
                for upn, *_ in USERS:
                    event = generate_device_health(upn, now)
                    batch.add(make_event(event, "DeviceHealth"))
                    batch_count += 1
                    total_events["device"] += 1

            # --- Call quality (every tick for active callers) ---
            for upn, *_ in USERS:
                event = generate_call_quality(upn, now)
                if event:
                    batch.add(make_event(event, "TeamsCallQuality"))
                    batch_count += 1
                    total_events["call"] += 1

            # --- Service health (every tick) ---
            sh_event = generate_service_health(now)
            if sh_event:
                batch.add(make_event(sh_event, "M365ServiceHealth"))
                batch_count += 1
                total_events["service"] += 1

            producer.send_batch(batch)

            ts = now.strftime("%H:%M:%S")
            # Active scenarios
            active = [k for k, v in scenarios.items() if v["active"] and k != "thermal_throttle"]
            thermals = sum(1 for u in USERS if device_state[u[0]]["thermal_state"] > 0.3)
            calls_active = sum(1 for u in USERS if device_state[u[0]]["in_call"])

            sc_str = f" | 🔍 {', '.join(active)}" if active else ""
            th_str = f" | 🌡️{thermals} thermal" if thermals > 0 else ""
            print(f"[{ts}] ✓{batch_count} (call:{total_events['call']} net:{total_events['network']} "
                  f"dev:{total_events['device']} svc:{total_events['service']}) | "
                  f"Calls:{calls_active}{th_str}{sc_str}")

            time.sleep(INTERVAL_SEC)

    except KeyboardInterrupt:
        print(f"\nStopped. Total events: {sum(total_events.values())}")
        for k, v in total_events.items():
            print(f"  {k}: {v}")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
