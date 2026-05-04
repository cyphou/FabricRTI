# phone_simulator.py
# Simulates phone telemetry and sends to Fabric Eventstream Custom Endpoint
# Usage: pip install azure-eventhub && python phone_simulator.py

import json
import random
import time
import datetime
from azure.eventhub import EventHubProducerClient, EventData

# ============================================================
# Eventstream Custom Endpoint (Event Hub-compatible)
# ============================================================
CONN_STR = ""  # Set your Eventstream Custom Endpoint connection string here
INTERVAL_SEC = 3  # seconds between batches

# 25 cities across every continent with real coordinates
CITIES = [
    # Europe
    ("Paris", 48.8566, 2.3522), ("London", 51.5074, -0.1278), ("Berlin", 52.5200, 13.4050),
    ("Madrid", 40.4168, -3.7038), ("Rome", 41.9028, 12.4964), ("Amsterdam", 52.3676, 4.9041),
    ("Stockholm", 59.3293, 18.0686), ("Warsaw", 52.2297, 21.0122),
    # Americas
    ("New York", 40.7128, -74.0060), ("San Francisco", 37.7749, -122.4194),
    ("Chicago", 41.8781, -87.6298), ("Toronto", 43.6532, -79.3832),
    ("Mexico City", 19.4326, -99.1332), ("São Paulo", -23.5505, -46.6333),
    ("Buenos Aires", -34.6037, -58.3816), ("Bogotá", 4.7110, -74.0721),
    # Asia
    ("Tokyo", 35.6762, 139.6503), ("Singapore", 1.3521, 103.8198),
    ("Dubai", 25.2048, 55.2708), ("Mumbai", 19.0760, 72.8777),
    ("Seoul", 37.5665, 126.9780), ("Bangkok", 13.7563, 100.5018),
    # Oceania & Africa
    ("Sydney", -33.8688, 151.2093), ("Cape Town", -33.9249, 18.4241),
    ("Nairobi", -1.2921, 36.8219),
]

USERS = [
    "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hank",
    "Iris", "Jack", "Karen", "Leo", "Mona", "Nate", "Olivia", "Paul",
    "Quinn", "Rosa", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xander",
    "Yara", "Zane", "Aisha", "Ben", "Cleo", "Dmitri", "Elena", "Felix",
    "Gia", "Hugo", "Ines", "Jules", "Kai", "Luna", "Marco", "Nina",
    "Oscar", "Priya", "Ravi", "Suki", "Tomas", "Ursula", "Vlad", "Wren",
    "Xiomara", "Yusuf",
]

PHONE_MODELS = [
    ("iPhone", "iOS 18"), ("Galaxy", "Android 15"), ("Pixel", "Android 15"),
    ("OnePlus", "Android 14"), ("Xiaomi", "Android 14"),
]

# Generate 100 devices spread across cities
DEVICES = []
for i in range(100):
    city_name, lat, lon = CITIES[i % len(CITIES)]
    model_name, os_name = PHONE_MODELS[i % len(PHONE_MODELS)]
    DEVICES.append({
        "id": f"{model_name}-{i+1:03d}",
        "user": USERS[i % len(USERS)],
        "os": os_name,
        "city": city_name,
        "lat": lat + random.uniform(-0.02, 0.02),  # slight offset per device
        "lon": lon + random.uniform(-0.02, 0.02),
    })

APPS = ["Teams", "Outlook", "Edge", "OneDrive", "Authenticator", "Power BI"]

battery_state = {d["id"]: random.uniform(40, 100) for d in DEVICES}
position_state = {d["id"]: {"lat": d["lat"], "lon": d["lon"], "heading": random.uniform(0, 360)} for d in DEVICES}
import math


def generate_event(device):
    device_id = device["id"]
    battery_state[device_id] = max(5, battery_state[device_id] - random.uniform(0, 0.5))
    if random.random() < 0.02:
        battery_state[device_id] = random.uniform(80, 100)

    # Realistic movement: ~30-60 km/h in a city, heading drifts
    pos = position_state[device_id]
    pos["heading"] += random.uniform(-30, 30)  # turn up to 30° per tick
    speed_km_per_tick = random.uniform(0.01, 0.05)  # ~12-60 km/h at 3s interval
    delta_lat = speed_km_per_tick / 111.0 * math.cos(math.radians(pos["heading"]))
    delta_lon = speed_km_per_tick / (111.0 * math.cos(math.radians(pos["lat"]))) * math.sin(math.radians(pos["heading"]))
    pos["lat"] += delta_lat
    pos["lon"] += delta_lon
    # Keep within ~20km of city center, bounce back if too far
    dist_lat = pos["lat"] - device["lat"]
    dist_lon = pos["lon"] - device["lon"]
    if abs(dist_lat) > 0.18 or abs(dist_lon) > 0.18:  # ~20km
        pos["heading"] = math.degrees(math.atan2(-(dist_lon), -(dist_lat))) + random.uniform(-20, 20)

    return {
        "Timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "DeviceId": device_id,
        "User": device["user"],
        "OS": device["os"],
        "City": device["city"],
        "BatteryLevel": round(battery_state[device_id], 1),
        "BatteryCharging": random.random() < 0.1,
        "SignalStrength": random.randint(-110, -50),
        "Latitude": round(pos["lat"], 6),
        "Longitude": round(pos["lon"], 6),
        "AppName": random.choice(APPS),
        "CpuUsage": round(random.uniform(5, 95), 1),
        "MemoryUsageMB": random.randint(200, 4000),
        "CrashCount": 1 if random.random() < 0.05 else 0,
        "NetworkType": random.choice(["5G", "4G", "WiFi"]),
        "ScreenWidth": 1080,
        "ScreenHeight": 2340,
        "UserAgent": f"RTIDemo/1.0 ({device['os']})",
    }


def main():
    producer = EventHubProducerClient.from_connection_string(CONN_STR)
    print("Connected to Eventstream Custom Endpoint (Phone-Stream)")
    print(f"Simulating {len(DEVICES)} devices, interval={INTERVAL_SEC}s")
    print("Press Ctrl+C to stop.\n")

    events_sent = 0
    errors = 0
    try:
        while True:
            events = [generate_event(d) for d in DEVICES]

            try:
                batch = producer.create_batch()
                for e in events:
                    batch.add(EventData(json.dumps(e)))
                producer.send_batch(batch)
                events_sent += len(events)

                ts = datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S")
                batteries = ", ".join(
                    f"{d['id']}={battery_state[d['id']]:.0f}%"
                    for d in DEVICES
                )
                print(f"[{ts}] ✓{len(events)} (total: {events_sent}) | {batteries}")
            except Exception as ex:
                errors += len(events)
                print(f"  Send error: {ex}")

            time.sleep(INTERVAL_SEC)
    except KeyboardInterrupt:
        print(f"\nStopped. Total events: {events_sent}, errors: {errors}")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
