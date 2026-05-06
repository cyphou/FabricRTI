# aws_simulator.py
# Simulates AWS telemetry in CSV format across 3 tables:
#   AWSCloudTrail, AWSVPCFlowLogs, AWSCloudWatchMetrics
# ============================================================
# FORMAT: CSV (no header row — columns match KQL table order)
# ROUTING: Event Hub application property _table for Eventstream
#          "Dynamic schema via headers" → "Separate tables for each schema"
# ============================================================
# Anomaly scenarios (subtle, cross-table correlation needed):
#   1. CREDENTIAL STUFFING — Many AssumeRole failures from diverse IPs,
#      same user-agent. No single IP exceeds threshold.
#   2. DATA EXFILTRATION — Slow S3 GetObject increase + unusual egress
#      in VPC flow logs. Each metric alone looks normal.
#   3. CRYPTO MINING — EC2 CPU creeps up + GPU spikes + network out
#      increases. Not a sudden jump — gradual over 15 minutes.
#   4. LATERAL MOVEMENT — After one successful login from unusual IP,
#      API calls fan out across regions. No single region is alarming.
# ============================================================
# Usage:
#   Direct to Kusto (default):  python aws_simulator.py
#   Via Eventstream:            python aws_simulator.py --eventhub
#   Dry run:                    python aws_simulator.py --dry-run

import csv
import io
import json
import os
import random
import sys
import time
import datetime
import uuid

# ============================================================
# Mode: "direct" (Kusto streaming) or "eventhub" (Eventstream)
# ============================================================
MODE = "eventhub" if "--eventhub" in sys.argv else ("dry" if "--dry-run" in sys.argv else "direct")
INTERVAL_SEC = 4  # seconds between batches

# Eventstream Custom Endpoint (Event Hub-compatible) — set via env var
CONN_STR = os.environ.get("AWS_EVENTHUB_CONN_STR", "")

# Kusto direct ingestion
KUSTO_URI = "https://trd-19dhtm16qjthdvwa1z.z3.kusto.fabric.microsoft.com"
KUSTO_DB = "RTI-Demo-Eventhouse"

# ============================================================
# Reference data
# ============================================================

ACCOUNTS = [
    {"id": "111122223333", "name": "prod-main"},
    {"id": "444455556666", "name": "prod-data"},
    {"id": "777788889999", "name": "dev-sandbox"},
]

REGIONS = [
    "us-east-1", "us-west-2", "eu-west-1", "eu-central-1",
    "ap-southeast-1", "ap-northeast-1",
]

AZS = {
    "us-east-1": ["us-east-1a", "us-east-1b", "us-east-1c"],
    "us-west-2": ["us-west-2a", "us-west-2b"],
    "eu-west-1": ["eu-west-1a", "eu-west-1b", "eu-west-1c"],
    "eu-central-1": ["eu-central-1a", "eu-central-1b"],
    "ap-southeast-1": ["ap-southeast-1a", "ap-southeast-1b"],
    "ap-northeast-1": ["ap-northeast-1a", "ap-northeast-1c"],
}

VPCS = {
    "us-east-1": ["vpc-prod-east", "vpc-data-east"],
    "us-west-2": ["vpc-prod-west"],
    "eu-west-1": ["vpc-prod-eu", "vpc-data-eu"],
    "eu-central-1": ["vpc-dev-eu"],
    "ap-southeast-1": ["vpc-prod-apac"],
    "ap-northeast-1": ["vpc-prod-jp"],
}

INSTANCE_TYPES = [
    "t3.medium", "t3.large", "m5.xlarge", "m5.2xlarge",
    "c5.xlarge", "c5.4xlarge", "r5.xlarge", "p3.2xlarge",
]

# CloudTrail events
CT_EVENTS = [
    # (EventName, EventSource, ResourceType, ReadOnly)
    ("DescribeInstances", "ec2.amazonaws.com", "AWS::EC2::Instance", True),
    ("RunInstances", "ec2.amazonaws.com", "AWS::EC2::Instance", False),
    ("TerminateInstances", "ec2.amazonaws.com", "AWS::EC2::Instance", False),
    ("StartInstances", "ec2.amazonaws.com", "AWS::EC2::Instance", False),
    ("StopInstances", "ec2.amazonaws.com", "AWS::EC2::Instance", False),
    ("CreateSecurityGroup", "ec2.amazonaws.com", "AWS::EC2::SecurityGroup", False),
    ("AuthorizeSecurityGroupIngress", "ec2.amazonaws.com", "AWS::EC2::SecurityGroup", False),
    ("GetObject", "s3.amazonaws.com", "AWS::S3::Object", True),
    ("PutObject", "s3.amazonaws.com", "AWS::S3::Object", False),
    ("ListBuckets", "s3.amazonaws.com", "AWS::S3::Bucket", True),
    ("DeleteBucket", "s3.amazonaws.com", "AWS::S3::Bucket", False),
    ("CreateBucket", "s3.amazonaws.com", "AWS::S3::Bucket", False),
    ("AssumeRole", "sts.amazonaws.com", "AWS::IAM::Role", False),
    ("GetCallerIdentity", "sts.amazonaws.com", "AWS::IAM::User", True),
    ("ConsoleLogin", "signin.amazonaws.com", "AWS::IAM::User", False),
    ("CreateUser", "iam.amazonaws.com", "AWS::IAM::User", False),
    ("AttachRolePolicy", "iam.amazonaws.com", "AWS::IAM::Policy", False),
    ("PutRolePolicy", "iam.amazonaws.com", "AWS::IAM::Policy", False),
    ("CreateFunction20150331", "lambda.amazonaws.com", "AWS::Lambda::Function", False),
    ("InvokeFunction", "lambda.amazonaws.com", "AWS::Lambda::Function", False),
    ("DescribeDBInstances", "rds.amazonaws.com", "AWS::RDS::DBInstance", True),
    ("CreateDBSnapshot", "rds.amazonaws.com", "AWS::RDS::DBSnapshot", False),
    ("GetSecretValue", "secretsmanager.amazonaws.com", "AWS::SecretsManager::Secret", True),
]

IAM_USERS = [
    "admin@company.com", "devops-bot", "ci-pipeline",
    "alice@company.com", "bob@company.com", "carol@company.com",
    "deploy-role", "monitoring-role", "backup-role",
    "data-engineer@company.com", "sre-oncall@company.com",
]

USER_AGENTS = [
    "aws-cli/2.15.0 Python/3.11.7",
    "Boto3/1.34.0 Python/3.12.0",
    "console.amazonaws.com",
    "terraform/1.7.0",
    "aws-sdk-java/2.20.0",
    "CloudFormation",
]

NORMAL_IPS = [
    "10.0.1.50", "10.0.2.100", "10.0.3.200", "172.16.0.10",
    "203.0.113.10", "203.0.113.20", "198.51.100.5",
]

S3_BUCKETS = [
    "arn:aws:s3:::prod-data-lake", "arn:aws:s3:::prod-logs",
    "arn:aws:s3:::prod-backups", "arn:aws:s3:::dev-artifacts",
]

# ============================================================
# EC2 fleet
# ============================================================
ec2_fleet = []
for i in range(20):
    acct = random.choice(ACCOUNTS)
    region = random.choice(REGIONS)
    ec2_fleet.append({
        "instance_id": f"i-{uuid.uuid4().hex[:12]}",
        "instance_type": random.choice(INSTANCE_TYPES),
        "account_id": acct["id"],
        "region": region,
        "az": random.choice(AZS[region]),
        "vpc": random.choice(VPCS[region]),
        "subnet": f"subnet-{uuid.uuid4().hex[:8]}",
        "eni": f"eni-{uuid.uuid4().hex[:8]}",
        "private_ip": f"10.{random.randint(0,255)}.{random.randint(1,254)}.{random.randint(1,254)}",
        "asg": random.choice(["web-asg", "api-asg", "worker-asg", "batch-asg", ""]),
        "tags": random.choice(["env=prod", "env=dev", "env=staging", "team=data", "team=platform"]),
        # baseline metrics
        "cpu_base": random.uniform(10, 45),
        "mem_base": random.uniform(30, 60),
        "net_in_base": random.randint(50000, 500000),
        "net_out_base": random.randint(20000, 200000),
        "gpu_base": random.uniform(0, 15) if "p3" in random.choice(INSTANCE_TYPES) else 0.0,
    })

# ============================================================
# Scenario state
# ============================================================
scenarios = {
    "credential_stuffing": {
        "active": False, "tick_start": 0, "duration": 40,
        "target_user": "admin@company.com",
        "bot_ua": "python-requests/2.31.0",
    },
    "data_exfiltration": {
        "active": False, "tick_start": 0, "duration": 50,
        "target_instance_idx": 0,  # index into ec2_fleet
    },
    "crypto_mining": {
        "active": False, "tick_start": 0, "duration": 60,
        "target_instances": [2, 5],  # indices into ec2_fleet
    },
    "lateral_movement": {
        "active": False, "tick_start": 0, "duration": 35,
        "attacker_ip": "45.33.32.156",
        "compromised_user": "ci-pipeline",
    },
}


def evolve_scenarios(tick):
    """Randomly activate one scenario at a time, with gaps."""
    active_count = sum(1 for s in scenarios.values() if s["active"])
    if active_count == 0 and tick > 10 and random.random() < 0.04:
        name = random.choice(list(scenarios.keys()))
        scenarios[name]["active"] = True
        scenarios[name]["tick_start"] = tick
    # Deactivate expired
    for name, s in scenarios.items():
        if s["active"] and (tick - s["tick_start"]) > s["duration"]:
            s["active"] = False


# ============================================================
# CSV helper
# ============================================================

def to_csv_row(values):
    """Convert a list of values to a CSV row string."""
    buf = io.StringIO()
    writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
    writer.writerow(values)
    return buf.getvalue().strip()


def make_csv_event(values, table_name):
    """Create EventData with CSV payload and _table routing property."""
    csv_row = to_csv_row(values)
    ed = EventData(csv_row.encode("utf-8"))
    ed.properties = {"_table": table_name}
    ed.content_type = "text/csv"
    return ed


# ============================================================
# Generators
# ============================================================

def generate_cloudtrail(now, tick):
    """Generate 3-8 CloudTrail events per tick."""
    events = []
    n = random.randint(3, 8)

    # Credential stuffing: many AssumeRole failures from rotating IPs
    sc = scenarios["credential_stuffing"]
    if sc["active"]:
        progress = (tick - sc["tick_start"]) / sc["duration"]
        # Not all at once — ramp up gradually
        extra = int(2 + progress * 4)
        for _ in range(extra):
            ip = f"{random.randint(1,223)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"
            row = [
                now.isoformat(),
                str(uuid.uuid4()),
                "AssumeRole",
                "sts.amazonaws.com",
                random.choice(REGIONS),
                ACCOUNTS[0]["id"],
                sc["target_user"],
                sc["bot_ua"],  # same user-agent is the tell
                ip,
                '{"RoleArn":"arn:aws:iam::111122223333:role/AdminRole"}',
                "Failure",
                "AccessDenied",
                "User is not authorized",
                "AWS::IAM::Role",
                "arn:aws:iam::111122223333:role/AdminRole",
                False,  # MFA
                False,  # ReadOnly
            ]
            events.append(row)

    # Lateral movement: fan out API calls across regions
    sc_lat = scenarios["lateral_movement"]
    if sc_lat["active"]:
        progress = (tick - sc_lat["tick_start"]) / sc_lat["duration"]
        # Start with recon (Describe*), then escalate
        if progress < 0.3:
            api_pool = [e for e in CT_EVENTS if e[3]]  # ReadOnly
        elif progress < 0.6:
            api_pool = [e for e in CT_EVENTS if "Secret" in e[2] or "Policy" in e[2]]
        else:
            api_pool = [e for e in CT_EVENTS if not e[3]]  # Write
        for _ in range(random.randint(1, 3)):
            ev = random.choice(api_pool)
            row = [
                now.isoformat(),
                str(uuid.uuid4()),
                ev[0], ev[1],
                random.choice(REGIONS),  # different regions each time
                random.choice(ACCOUNTS)["id"],
                sc_lat["compromised_user"],
                "aws-cli/2.15.0 Python/3.11.7",
                sc_lat["attacker_ip"],
                "{}",
                "Success",
                "", "",
                ev[2],
                f"arn:aws:{ev[1].split('.')[0]}::recon-target",
                False, ev[3],
            ]
            events.append(row)

    # Normal traffic
    for _ in range(n):
        ev = random.choice(CT_EVENTS)
        acct = random.choice(ACCOUNTS)
        user = random.choice(IAM_USERS)

        # Data exfiltration: more S3 GetObject than usual (subtle increase)
        sc_exfil = scenarios["data_exfiltration"]
        if sc_exfil["active"] and ev[0] == "GetObject" and random.random() < 0.4:
            n += 1  # one extra GetObject — barely visible in aggregates

        status = "Success" if random.random() < 0.95 else "Failure"
        err_code = "" if status == "Success" else random.choice(["AccessDenied", "ValidationException", "ThrottlingException"])
        err_msg = "" if status == "Success" else "Request failed"

        row = [
            now.isoformat(),
            str(uuid.uuid4()),
            ev[0], ev[1],
            random.choice(REGIONS),
            acct["id"],
            user,
            random.choice(USER_AGENTS),
            random.choice(NORMAL_IPS),
            "{}",
            status,
            err_code, err_msg,
            ev[2],
            f"arn:aws:{ev[1].split('.')[0]}::{acct['id']}:resource/{uuid.uuid4().hex[:8]}",
            random.random() < 0.7,  # MFA
            ev[3],  # ReadOnly
        ]
        events.append(row)

    return events


def generate_vpc_flow_logs(now, tick):
    """Generate VPC flow log entries per instance."""
    events = []
    for inst in ec2_fleet:
        if random.random() < 0.4:
            continue  # not every instance every tick

        n_flows = random.randint(1, 3)
        for _ in range(n_flows):
            dst_ip = f"{random.randint(1,223)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"
            dst_port = random.choice([443, 80, 8443, 5432, 3306, 6379, 22, 8080])
            proto = 6  # TCP
            direction = random.choice(["ingress", "egress"])
            action = "ACCEPT" if random.random() < 0.92 else "REJECT"
            packets = random.randint(5, 500)
            nbytes = packets * random.randint(100, 1500)

            # Data exfiltration: more egress bytes from target instance
            sc_exfil = scenarios["data_exfiltration"]
            if sc_exfil["active"]:
                target = ec2_fleet[sc_exfil["target_instance_idx"]]
                if inst["instance_id"] == target["instance_id"] and direction == "egress":
                    progress = (tick - sc_exfil["tick_start"]) / sc_exfil["duration"]
                    # Gradual increase — not a spike
                    nbytes = int(nbytes * (1 + progress * 3))
                    packets = int(packets * (1 + progress * 2))
                    dst_port = 443  # looks like normal HTTPS

            # Crypto mining: unusual outbound to non-standard ports
            sc_crypto = scenarios["crypto_mining"]
            if sc_crypto["active"]:
                idx_list = sc_crypto["target_instances"]
                for idx in idx_list:
                    if inst["instance_id"] == ec2_fleet[idx]["instance_id"] and direction == "egress":
                        if random.random() < 0.15:
                            dst_port = random.choice([3333, 4444, 8333, 14444])  # mining pools
                            packets = random.randint(50, 200)
                            nbytes = packets * random.randint(200, 800)

            row = [
                now.isoformat(),
                str(uuid.uuid4()),
                inst["account_id"],
                inst["vpc"],
                inst["subnet"],
                inst["eni"],
                inst["private_ip"],
                dst_ip,
                random.randint(32768, 65535) if direction == "egress" else dst_port,
                dst_port if direction == "egress" else random.randint(32768, 65535),
                proto,
                packets,
                nbytes,
                action,
                "OK",
                inst["region"],
                direction,
                random.choice(["IPv4", "IPv4", "IPv4", "IPv6"]),
            ]
            events.append(row)

    return events


def generate_cloudwatch_metrics(now, tick):
    """Generate CloudWatch metrics per instance."""
    events = []
    for inst in ec2_fleet:
        cpu = inst["cpu_base"] + random.gauss(0, 5)
        mem = inst["mem_base"] + random.gauss(0, 3)
        net_in = int(inst["net_in_base"] * random.uniform(0.7, 1.3))
        net_out = int(inst["net_out_base"] * random.uniform(0.7, 1.3))
        gpu = inst["gpu_base"] + random.gauss(0, 2)
        disk_r = random.randint(10, 500)
        disk_w = random.randint(10, 300)
        status_check = 0

        # Crypto mining: gradual CPU + GPU + network increase
        sc_crypto = scenarios["crypto_mining"]
        if sc_crypto["active"]:
            for idx in sc_crypto["target_instances"]:
                if inst["instance_id"] == ec2_fleet[idx]["instance_id"]:
                    progress = (tick - sc_crypto["tick_start"]) / sc_crypto["duration"]
                    # Gradual — starts at normal, creeps up
                    cpu += progress * 35  # peaks at +35% above base
                    gpu += progress * 60  # GPU is the real tell but only if you look
                    net_out = int(net_out * (1 + progress * 4))

        cpu = max(0, min(100, cpu))
        mem = max(0, min(100, mem))
        gpu = max(0, min(100, gpu))

        row = [
            now.isoformat(),
            str(uuid.uuid4()),
            inst["account_id"],
            inst["instance_id"],
            inst["instance_type"],
            inst["region"],
            inst["az"],
            round(cpu, 1),
            net_in,
            net_out,
            disk_r,
            disk_w,
            round(mem, 1),
            round(gpu, 1),
            status_check,
            inst["asg"],
            inst["tags"],
        ]
        events.append(row)

    return events


# ============================================================
# Main loop
# ============================================================

def main():
    if MODE == "dry":
        dry_run(ticks=5)
        return

    print("=" * 65)
    print("  AWS Multi-Cloud Simulator — CSV Format + Schema Routing")
    print("=" * 65)
    print(f"  Mode:        {MODE}")
    print(f"  Accounts:    {len(ACCOUNTS)}")
    print(f"  Regions:     {len(REGIONS)}")
    print(f"  EC2 Fleet:   {len(ec2_fleet)}")
    print(f"  Tables:      AWSCloudTrail, AWSVPCFlowLogs, AWSCloudWatchMetrics")
    print(f"  Format:      CSV")
    print(f"  Scenarios:   4 (credential stuffing, data exfiltration, crypto mining, lateral movement)")
    print(f"  Interval:    {INTERVAL_SEC}s")
    print()
    print("Press Ctrl+C to stop.\n")

    # Set up the ingestion backend
    if MODE == "eventhub":
        from azure.eventhub import EventHubProducerClient, EventData as _ED
        producer = EventHubProducerClient.from_connection_string(CONN_STR)
        sender = _eventhub_sender(producer, _ED)
    else:
        import requests as _req
        import urllib3 as _u3
        _u3.disable_warnings()
        from azure.identity import AzureCliCredential
        cred = AzureCliCredential(process_timeout=30)
        token = cred.get_token(KUSTO_URI + "/.default").token
        sender = _kusto_sender(_req, token)

    total = {"trail": 0, "flow": 0, "cw": 0}
    tick = 0

    try:
        while True:
            now = datetime.datetime.now(datetime.timezone.utc)
            tick += 1
            evolve_scenarios(tick)

            trails = generate_cloudtrail(now, tick)
            flows = generate_vpc_flow_logs(now, tick)
            cw = generate_cloudwatch_metrics(now, tick) if tick % 2 == 0 else []

            sender(trails, "AWSCloudTrail")
            sender(flows, "AWSVPCFlowLogs")
            if cw:
                sender(cw, "AWSCloudWatchMetrics")

            total["trail"] += len(trails)
            total["flow"] += len(flows)
            total["cw"] += len(cw)
            count = len(trails) + len(flows) + len(cw)

            ts = now.strftime("%H:%M:%S")
            active = [k for k, v in scenarios.items() if v["active"]]
            sc_str = f" | 🔍 {', '.join(active)}" if active else ""
            print(f"[{ts}] ✓{count} (trail:{total['trail']} flow:{total['flow']} "
                  f"cw:{total['cw']}){sc_str}")

            time.sleep(INTERVAL_SEC)

    except KeyboardInterrupt:
        print(f"\nStopped. Total: {sum(total.values())}")
        for k, v in total.items():
            print(f"  {k}: {v}")
    finally:
        if MODE == "eventhub":
            producer.close()


CSV_HEADERS = {
    "AWSCloudTrail": "Timestamp,EventId,EventName,EventSource,AWSRegion,AccountId,UserIdentity,UserAgent,SourceIPAddress,RequestParameters,ResponseStatus,ErrorCode,ErrorMessage,ResourceType,ResourceARN,MFAUsed,ReadOnly",
    "AWSVPCFlowLogs": "Timestamp,FlowId,AccountId,VpcId,SubnetId,InterfaceId,SrcAddr,DstAddr,SrcPort,DstPort,Protocol,Packets,Bytes,Action,LogStatus,AWSRegion,Direction,TrafficType",
    "AWSCloudWatchMetrics": "Timestamp,MetricId,AccountId,InstanceId,InstanceType,AWSRegion,AvailabilityZone,CPUUtilization,NetworkIn,NetworkOut,DiskReadOps,DiskWriteOps,MemoryUsed,GPUUtilization,StatusCheckFailed,AutoScalingGroup,Tags",
}


def _eventhub_sender(producer, EventData):
    """Return a sender function that batches CSV events to Event Hub."""
    def send(rows, table_name):
        if not rows:
            return
        header = CSV_HEADERS[table_name]
        batch = producer.create_batch()
        for row in rows:
            csv_row = to_csv_row(row)
            body = f"{header}\n{csv_row}"
            ed = EventData(body.encode("utf-8"))
            ed.properties = {"_table": table_name}
            ed.content_type = "text/csv"
            batch.add(ed)
        producer.send_batch(batch)
    return send


def _kusto_sender(requests_mod, token):
    """Return a sender function that uses Kusto streaming ingestion."""
    h = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}

    def send(rows, table_name, retries=5):
        if not rows:
            return
        # Build CSV block for .ingest inline
        csv_lines = "\n".join(to_csv_row(r) for r in rows)
        csl = f".ingest inline into table {table_name} with (format='csv') <|\n{csv_lines}"
        payload = {"db": KUSTO_DB, "csl": csl}
        for attempt in range(retries):
            try:
                r = requests_mod.post(
                    KUSTO_URI + "/v1/rest/mgmt", json=payload,
                    headers=h, verify=False, timeout=30,
                )
                if r.status_code == 200:
                    return
                # Token expired — refresh
                if r.status_code == 401 and attempt < retries - 1:
                    from azure.identity import AzureCliCredential
                    new_token = AzureCliCredential(process_timeout=30).get_token(KUSTO_URI + "/.default").token
                    h["Authorization"] = "Bearer " + new_token
                    continue
            except Exception:
                pass
            time.sleep(1 * (attempt + 1))
    return send


def dry_run(ticks=5):
    """Print sample CSV rows without sending to Event Hub."""
    print("--- DRY RUN: Sample CSV output ---\n")
    for tick in range(1, ticks + 1):
        now = datetime.datetime.now(datetime.timezone.utc)
        evolve_scenarios(tick)

        trails = generate_cloudtrail(now, tick)
        flows = generate_vpc_flow_logs(now, tick)
        cw = generate_cloudwatch_metrics(now, tick) if tick % 2 == 0 else []

        print(f"[Tick {tick}] CloudTrail: {len(trails)}, FlowLogs: {len(flows)}, CloudWatch: {len(cw)}")

        if trails:
            sample = to_csv_row(trails[0])
            print(f"  CT sample: {sample[:120]}...")
        if flows:
            sample = to_csv_row(flows[0])
            print(f"  FL sample: {sample[:120]}...")
        if cw:
            sample = to_csv_row(cw[0])
            print(f"  CW sample: {sample[:120]}...")
        print()

    # Show routing info
    print("--- Routing properties per table ---")
    print("  AWSCloudTrail:        ed.properties = {'_table': 'AWSCloudTrail'}")
    print("  AWSVPCFlowLogs:       ed.properties = {'_table': 'AWSVPCFlowLogs'}")
    print("  AWSCloudWatchMetrics: ed.properties = {'_table': 'AWSCloudWatchMetrics'}")
    print()
    print("--- Eventstream config ---")
    print("  Source: Event Hub / Custom Endpoint, Extended Features")
    print("  Schema handling: Dynamic schema via headers")
    print("    Header: _table")
    print("    Value 'AWSCloudTrail'        → CloudTrailSchema")
    print("    Value 'AWSVPCFlowLogs'       → VPCFlowLogsSchema")
    print("    Value 'AWSCloudWatchMetrics' → CloudWatchSchema")
    print("  Destination: Eventhouse, Separate tables for each schema, Payload only")


if __name__ == "__main__":
    main()
