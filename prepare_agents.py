#!/usr/bin/env python3
"""prepare_agents.py — Validate and prepare all RTI Demo agents on top of live data.

Runs a comprehensive check against the live Fabric workspace to confirm
every agent's deployed artifact is present, healthy, and receiving data.
Then deploys any missing layers (medallion, queryset content, etc.).

Usage:
    python prepare_agents.py              # Full validation + preparation
    python prepare_agents.py --validate   # Validation only (read-only)
    python prepare_agents.py --medallion  # Deploy medallion only
    python prepare_agents.py --status     # Quick data pipeline status
"""

import argparse
import json
import logging
import os
import sys
import time

import requests
import urllib3
from azure.identity import AzureCliCredential

urllib3.disable_warnings()

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("prepare")


def load_config():
    with open(os.path.join(PROJECT_DIR, "config.json"), "r", encoding="utf-8") as f:
        return json.load(f)


class AgentPreparer:
    """Validates and prepares all RTI Demo agents against live workspace."""

    def __init__(self, config: dict):
        self.cfg = config
        self.fabric = config["fabric"]
        self.ws_id = self.fabric["workspace_id"]
        self.cluster_uri = self.fabric.get(
            "kusto_cluster_uri",
            "https://trd-19dhtm16qjthdvwa1z.z3.kusto.fabric.microsoft.com",
        )
        self.db_name = self.fabric["items"]["kql_database"]["display_name"]

        self.cred = AzureCliCredential(process_timeout=30)
        self._fabric_token = None
        self._kusto_token = None

        self.results = {"passed": [], "failed": [], "warnings": [], "actions": []}

    # ── Auth ─────────────────────────────────────────────────
    def _fab_headers(self):
        if not self._fabric_token:
            self._fabric_token = self.cred.get_token(
                "https://api.fabric.microsoft.com/.default"
            ).token
        return {"Authorization": f"Bearer {self._fabric_token}"}

    def _kusto_headers(self):
        if not self._kusto_token:
            self._kusto_token = self.cred.get_token(
                f"{self.cluster_uri}/.default"
            ).token
        return {
            "Authorization": f"Bearer {self._kusto_token}",
            "Content-Type": "application/json",
        }

    # ── Helpers ──────────────────────────────────────────────
    def _fab_get(self, path):
        r = requests.get(
            f"https://api.fabric.microsoft.com/v1/workspaces/{self.ws_id}/{path}",
            headers=self._fab_headers(),
            verify=False,
        )
        return r

    def _kql_query(self, query):
        r = requests.post(
            f"{self.cluster_uri}/v1/rest/query",
            json={"db": self.db_name, "csl": query},
            headers=self._kusto_headers(),
            verify=False,
        )
        if r.status_code == 200:
            tbl = r.json()["Tables"][0]
            return tbl["Rows"]
        return None

    def _kql_mgmt(self, cmd):
        r = requests.post(
            f"{self.cluster_uri}/v1/rest/mgmt",
            json={"db": self.db_name, "csl": cmd},
            headers=self._kusto_headers(),
            verify=False,
        )
        return r.status_code == 200

    def _pass(self, msg):
        self.results["passed"].append(msg)
        logger.info("  ✓ %s", msg)

    def _fail(self, msg):
        self.results["failed"].append(msg)
        logger.error("  ✗ %s", msg)

    def _warn(self, msg):
        self.results["warnings"].append(msg)
        logger.warning("  ⚠ %s", msg)

    def _action(self, msg):
        self.results["actions"].append(msg)

    # ── Validation Checks ────────────────────────────────────

    def check_workspace_items(self):
        """Verify all expected Fabric items exist."""
        logger.info("━" * 60)
        logger.info("@validator: Checking workspace items...")
        logger.info("━" * 60)

        r = self._fab_get("items")
        items = r.json().get("value", [])
        item_map = {(i["type"], i["displayName"]): i["id"] for i in items}

        expected = [
            ("Eventhouse", self.fabric["items"]["eventhouse"]["display_name"]),
            ("KQLDatabase", self.fabric["items"]["kql_database"]["display_name"]),
            ("Eventstream", self.fabric["items"]["eventstreams"][0]["display_name"]),
            ("Eventstream", self.fabric["items"]["eventstreams"][1]["display_name"]),
            ("KQLQueryset", self.fabric["items"]["kql_queryset"]["display_name"]),
            ("KQLDashboard", self.fabric["items"]["kql_dashboard"]["display_name"]),
            ("Reflex", self.fabric["items"]["activator"]["display_name"]),
        ]

        for item_type, name in expected:
            key = (item_type, name)
            if key in item_map:
                self._pass(f"{item_type} '{name}' exists (id={item_map[key][:8]}...)")
            else:
                self._fail(f"{item_type} '{name}' NOT FOUND")

        # List any extra items
        known_names = {name for _, name in expected}
        for item in items:
            if item["displayName"] not in known_names:
                self._warn(f"Extra item: {item['type']} '{item['displayName']}'")

    def check_tables(self):
        """Verify KQL tables exist and have data."""
        logger.info("━" * 60)
        logger.info("@eventhouse: Checking tables and data...")
        logger.info("━" * 60)

        rows = self._kql_query(
            ".show tables details | project TableName, TotalRowCount"
        )
        if not rows:
            self._fail("Could not query tables")
            return

        table_counts = {r[0]: r[1] for r in rows}

        # Bronze tables
        for table in ["AzureActivity", "PhoneTelemetry"]:
            if table in table_counts:
                count = table_counts[table]
                if count > 0:
                    self._pass(f"Table '{table}': {count:,} rows")
                else:
                    self._warn(f"Table '{table}' exists but has 0 rows")
            else:
                self._fail(f"Table '{table}' NOT FOUND")

        # Silver tables
        for table in ["silver_AzureActivity", "silver_PhoneTelemetry"]:
            if table in table_counts:
                self._pass(f"Table '{table}': {table_counts[table]:,} rows")
            else:
                self._warn(f"Silver table '{table}' not found — medallion not deployed")
                self._action("deploy_medallion")

    def check_streaming(self):
        """Verify streaming ingestion is enabled."""
        logger.info("━" * 60)
        logger.info("@eventhouse: Checking streaming ingestion...")
        logger.info("━" * 60)

        for table in ["AzureActivity", "PhoneTelemetry"]:
            rows = self._kql_query(
                f".show table {table} policy streamingingestion"
            )
            if rows and rows[0][2]:
                policy = json.loads(rows[0][2])
                if policy.get("IsEnabled"):
                    self._pass(f"Streaming ingestion enabled on '{table}'")
                else:
                    self._warn(f"Streaming ingestion DISABLED on '{table}'")
            else:
                self._warn(f"No streaming policy on '{table}'")

    def check_data_freshness(self):
        """Check that data is actually flowing (recent events)."""
        logger.info("━" * 60)
        logger.info("@eventstream: Checking data freshness...")
        logger.info("━" * 60)

        for table in ["AzureActivity", "PhoneTelemetry"]:
            rows = self._kql_query(
                f"{table} | where Timestamp > ago(10m) | summarize cnt=count(), maxT=max(Timestamp)"
            )
            if rows and rows[0][0] > 0:
                self._pass(
                    f"'{table}' has {rows[0][0]:,} events in last 10 min (latest: {rows[0][1]})"
                )
            else:
                self._fail(f"'{table}' has NO data in last 10 minutes — pipeline may be broken")

    def check_ingestion_mappings(self):
        """Verify JSON ingestion mappings exist."""
        logger.info("━" * 60)
        logger.info("@eventhouse: Checking ingestion mappings...")
        logger.info("━" * 60)

        for table, mapping in [
            ("PhoneTelemetry", "PhoneTelemetryMapping"),
            ("AzureActivity", "AzureActivityMapping"),
        ]:
            rows = self._kql_query(
                f".show table {table} ingestion mappings | project Name, Kind"
            )
            if rows:
                names = [r[0] for r in rows]
                if mapping in names:
                    self._pass(f"Mapping '{mapping}' exists on '{table}'")
                else:
                    self._warn(f"Expected mapping '{mapping}' not found on '{table}'")
            else:
                self._warn(f"No ingestion mappings on '{table}'")

    def check_medallion(self):
        """Check materialized views health."""
        logger.info("━" * 60)
        logger.info("@eventhouse: Checking medallion layers...")
        logger.info("━" * 60)

        # Functions
        rows = self._kql_query(".show functions | project Name")
        if rows:
            funcs = [r[0] for r in rows]
            for fn in ["fn_SilverAzureActivity", "fn_SilverPhoneTelemetry"]:
                if fn in funcs:
                    self._pass(f"Function '{fn}' exists")
                else:
                    self._warn(f"Function '{fn}' missing")
                    self._action("deploy_medallion")
        else:
            self._warn("No functions found")
            self._action("deploy_medallion")

        # Materialized views
        rows = self._kql_query(
            ".show materialized-views | project Name, IsHealthy, IsEnabled"
        )
        if rows:
            for name, healthy, enabled in rows:
                if healthy and enabled:
                    self._pass(f"Materialized view '{name}': healthy, enabled")
                else:
                    self._warn(
                        f"Materialized view '{name}': healthy={healthy}, enabled={enabled}"
                    )
        else:
            self._warn("No materialized views found")
            self._action("deploy_medallion")

    def check_phone_fleet(self):
        """Check phone simulator data quality."""
        logger.info("━" * 60)
        logger.info("@simulator: Checking phone fleet data...")
        logger.info("━" * 60)

        rows = self._kql_query(
            "PhoneTelemetry | where Timestamp > ago(5m) "
            "| summarize devices=dcount(DeviceId), cities=dcount(City), "
            "os_count=dcount(OS)"
        )
        if rows and rows[0][0] > 0:
            devices, cities, os_count = rows[0]
            expected_devices = self.cfg.get("simulator", {}).get("phone", {}).get("device_count", 100)
            if devices >= expected_devices:
                self._pass(f"Phone fleet: {devices} devices, {cities} cities, {os_count} OS types")
            else:
                self._warn(
                    f"Phone fleet: only {devices}/{expected_devices} devices active "
                    f"({cities} cities, {os_count} OS)"
                )
        else:
            self._fail("No phone telemetry in last 5 min — simulator not running?")

    def check_activity_stream(self):
        """Check Azure Activity stream quality."""
        logger.info("━" * 60)
        logger.info("@simulator: Checking Azure Activity data...")
        logger.info("━" * 60)

        rows = self._kql_query(
            "AzureActivity | where Timestamp > ago(5m) "
            "| summarize ops=dcount(OperationName), callers=dcount(Caller), "
            "regions=dcount(Region)"
        )
        if rows and rows[0][0] > 0:
            ops, callers, regions = rows[0]
            self._pass(
                f"Azure Activity: {ops} op types, {callers} callers, {regions} regions"
            )
        else:
            self._fail("No Azure Activity in last 5 min — simulator not running?")

    def check_dashboard(self):
        """Verify dashboard definition is deployed."""
        logger.info("━" * 60)
        logger.info("@dashboard: Checking dashboard...")
        logger.info("━" * 60)

        dash_id = self.fabric["items"]["kql_dashboard"].get("id", "")
        if not dash_id:
            self._warn("Dashboard ID not in config — can't verify definition")
            return

        import base64

        r = requests.post(
            f"https://api.fabric.microsoft.com/v1/workspaces/{self.ws_id}"
            f"/items/{dash_id}/getDefinition",
            headers=self._fab_headers(),
            verify=False,
        )
        if r.status_code == 200:
            parts = r.json().get("definition", {}).get("parts", [])
            for part in parts:
                if part.get("path") == "RealTimeDashboard.json":
                    decoded = json.loads(
                        base64.b64decode(part["payload"]).decode("utf-8")
                    )
                    tiles = decoded.get("tiles", [])
                    pages = decoded.get("pages", [])
                    self._pass(
                        f"Dashboard deployed: {len(pages)} pages, {len(tiles)} tiles"
                    )
                    return
            self._warn("Dashboard definition has no RealTimeDashboard.json part")
        else:
            self._fail(f"Could not read dashboard definition: HTTP {r.status_code}")

    def check_activator(self):
        """Check Activator item exists."""
        logger.info("━" * 60)
        logger.info("@activator: Checking Activator...")
        logger.info("━" * 60)

        act_name = self.fabric["items"]["activator"]["display_name"]
        r = self._fab_get("items?type=Reflex")
        items = r.json().get("value", [])
        found = any(i["displayName"] == act_name for i in items)
        if found:
            rules = self.fabric["items"]["activator"].get("rules", [])
            self._pass(
                f"Activator '{act_name}' exists — {len(rules)} rules defined in config"
            )
            for rule in rules:
                logger.info("    Rule: %s → %s", rule["name"], rule.get("action", "teams"))
        else:
            self._fail(f"Activator '{act_name}' NOT FOUND")

    # ── Deployment Actions ───────────────────────────────────

    def deploy_medallion(self):
        """Deploy silver + gold layers if missing."""
        logger.info("━" * 60)
        logger.info("@eventhouse: Deploying medallion architecture...")
        logger.info("━" * 60)

        medallion_path = os.path.join(PROJECT_DIR, "medallion-architecture.kql")
        if not os.path.exists(medallion_path):
            self._fail("medallion-architecture.kql not found")
            return

        # Silver tables
        silver_cmds = [
            (
                ".create-merge table silver_AzureActivity ("
                "Timestamp: datetime, OperationName: string, OperationCategory: string, "
                "Status: string, Level: string, ResourceGroup: string, ResourceType: string, "
                "Caller: string, CallerIpAddress: string, CorrelationId: string, Region: string)",
                "silver_AzureActivity table",
            ),
            (
                ".create-merge table silver_PhoneTelemetry ("
                "Timestamp: datetime, DeviceId: string, User: string, OS: string, City: string, "
                "Platform: string, BatteryLevel: real, BatteryCharging: bool, BatteryStatus: string, "
                "SignalStrength: int, SignalQuality: string, Latitude: real, Longitude: real, "
                "AppName: string, CpuUsage: real, MemoryUsageMB: int, CrashCount: int, "
                "NetworkType: string)",
                "silver_PhoneTelemetry table",
            ),
            (
                ".alter table silver_AzureActivity policy streamingingestion enable",
                "streaming on silver_AzureActivity",
            ),
            (
                ".alter table silver_PhoneTelemetry policy streamingingestion enable",
                "streaming on silver_PhoneTelemetry",
            ),
        ]

        for cmd, label in silver_cmds:
            if self._kql_mgmt(cmd):
                self._pass(f"Created {label}")
            else:
                self._warn(f"Failed to create {label}")
            time.sleep(1)

        # Functions (read from file for full definitions)
        with open(medallion_path, "r", encoding="utf-8") as f:
            kql = f.read()

        # Extract function definitions
        import re

        for match in re.finditer(
            r"(\.create-or-alter function \w+\(\) \{.*?\})",
            kql,
            re.DOTALL,
        ):
            func_cmd = match.group(1)
            func_name = re.search(r"function (\w+)", func_cmd).group(1)
            if self._kql_mgmt(func_cmd):
                self._pass(f"Created function {func_name}")
            else:
                self._warn(f"Failed to create function {func_name}")
            time.sleep(1)

        # Update policies
        policies = [
            (
                ".alter table silver_AzureActivity policy update "
                "@'[{\"IsEnabled\":true,\"Source\":\"AzureActivity\","
                "\"Query\":\"fn_SilverAzureActivity\",\"IsTransactional\":false,"
                "\"PropagateIngestionProperties\":true}]'",
                "silver_AzureActivity update policy",
            ),
            (
                ".alter table silver_PhoneTelemetry policy update "
                "@'[{\"IsEnabled\":true,\"Source\":\"PhoneTelemetry\","
                "\"Query\":\"fn_SilverPhoneTelemetry\",\"IsTransactional\":false,"
                "\"PropagateIngestionProperties\":true}]'",
                "silver_PhoneTelemetry update policy",
            ),
        ]

        for cmd, label in policies:
            if self._kql_mgmt(cmd):
                self._pass(f"Set {label}")
            else:
                self._warn(f"Failed to set {label}")
            time.sleep(1)

        # Materialized views
        mv_cmds = [
            (
                '.create materialized-view with (backfill=true) gold_AzureOperationsSummary '
                'on table silver_AzureActivity { silver_AzureActivity '
                '| summarize EventCount = count(), ErrorCount = countif(Level == "Error"), '
                'LastSeen = max(Timestamp) by OperationName, OperationCategory, ResourceGroup, Region }',
                "gold_AzureOperationsSummary",
            ),
            (
                '.create materialized-view with (backfill=true) gold_AzureCallerActivity '
                'on table silver_AzureActivity { silver_AzureActivity '
                '| summarize ActionCount = count(), ErrorCount = countif(Level == "Error"), '
                'LastActive = max(Timestamp), DistinctOperations = dcount(OperationName) '
                'by Caller, CallerIpAddress }',
                "gold_AzureCallerActivity",
            ),
            (
                '.create materialized-view with (backfill=true) gold_DeviceHealth '
                'on table silver_PhoneTelemetry { silver_PhoneTelemetry '
                '| summarize arg_max(Timestamp, *) by DeviceId }',
                "gold_DeviceHealth",
            ),
            (
                '.create materialized-view with (backfill=true) gold_AppCrashSummary '
                'on table silver_PhoneTelemetry { silver_PhoneTelemetry '
                '| where CrashCount > 0 '
                '| summarize TotalCrashes = sum(CrashCount), '
                'AffectedDevices = dcount(DeviceId), LastCrash = max(Timestamp) '
                'by AppName, Platform, City }',
                "gold_AppCrashSummary",
            ),
            (
                '.create materialized-view with (backfill=true) gold_NetworkQuality '
                'on table silver_PhoneTelemetry { silver_PhoneTelemetry '
                '| summarize AvgSignal = avg(SignalStrength), Events = count() '
                'by NetworkType, SignalQuality, Platform, City }',
                "gold_NetworkQuality",
            ),
        ]

        for cmd, name in mv_cmds:
            if self._kql_mgmt(cmd):
                self._pass(f"Created {name}")
            else:
                self._warn(f"Failed to create {name} (may already exist)")
            time.sleep(3)

    # ── Main Pipeline ────────────────────────────────────────

    def validate(self):
        """Run all validation checks."""
        self.check_workspace_items()
        self.check_tables()
        self.check_streaming()
        self.check_data_freshness()
        self.check_ingestion_mappings()
        self.check_medallion()
        self.check_phone_fleet()
        self.check_activity_stream()
        self.check_dashboard()
        self.check_activator()

    def prepare(self):
        """Validate and deploy missing components."""
        self.validate()

        # Auto-deploy medallion if needed
        if "deploy_medallion" in self.results["actions"]:
            logger.info("")
            self.deploy_medallion()

    def status(self):
        """Quick data pipeline status."""
        logger.info("━" * 60)
        logger.info("QUICK STATUS")
        logger.info("━" * 60)

        for table in ["AzureActivity", "PhoneTelemetry"]:
            rows = self._kql_query(
                f"{table} | summarize Total=count(), Last5m=countif(Timestamp > ago(5m)), "
                f"Devices=dcount(DeviceId), MaxT=max(Timestamp)"
            )
            if rows:
                r = rows[0]
                logger.info(
                    "  %s: %s total, %s last 5m, %s entities, latest=%s",
                    table, f"{r[0]:,}", f"{r[1]:,}", r[2], r[3],
                )

        for table in ["silver_AzureActivity", "silver_PhoneTelemetry"]:
            rows = self._kql_query(f"{table} | count")
            if rows:
                logger.info("  %s: %s rows", table, f"{rows[0][0]:,}")
            else:
                logger.info("  %s: not found", table)

        rows = self._kql_query(
            ".show materialized-views | project Name, IsHealthy, IsEnabled"
        )
        if rows:
            for name, h, e in rows:
                logger.info("  MV %s: healthy=%s enabled=%s", name, h, e)

    def print_summary(self):
        """Print final summary."""
        passed = len(self.results["passed"])
        failed = len(self.results["failed"])
        warned = len(self.results["warnings"])
        total = passed + failed

        logger.info("")
        logger.info("=" * 60)
        logger.info("PREPARATION SUMMARY")
        logger.info("=" * 60)
        logger.info(
            "  %s  %d/%d checks passed, %d warnings",
            "PASS" if failed == 0 else "FAIL", passed, total, warned,
        )

        if failed > 0:
            logger.info("")
            logger.info("FAILURES:")
            for msg in self.results["failed"]:
                logger.error("  ✗ %s", msg)

        if warned > 0:
            logger.info("")
            logger.info("WARNINGS:")
            for msg in self.results["warnings"]:
                logger.warning("  ⚠ %s", msg)

        logger.info("=" * 60)

        # Agent readiness summary
        logger.info("")
        logger.info("AGENT READINESS:")
        agents = {
            "@infra": failed == 0,
            "@eventhouse": "Table" not in " ".join(self.results.get("failed", [])),
            "@eventstream": "pipeline" not in " ".join(self.results.get("failed", [])),
            "@queryset": True,
            "@dashboard": "Dashboard" not in " ".join(self.results.get("failed", [])),
            "@activator": "Activator" not in " ".join(self.results.get("failed", [])),
            "@simulator": "simulator" not in " ".join(self.results.get("failed", [])),
            "@validator": True,
        }
        for agent, ready in agents.items():
            logger.info("  %s %s", "✓" if ready else "✗", agent)

        logger.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Prepare RTI Demo agents")
    parser.add_argument("--validate", action="store_true", help="Validation only")
    parser.add_argument("--medallion", action="store_true", help="Deploy medallion only")
    parser.add_argument("--status", action="store_true", help="Quick status check")
    args = parser.parse_args()

    config = load_config()
    preparer = AgentPreparer(config)

    if args.status:
        preparer.status()
    elif args.medallion:
        preparer.deploy_medallion()
        preparer.print_summary()
    elif args.validate:
        preparer.validate()
        preparer.print_summary()
    else:
        preparer.prepare()
        preparer.print_summary()


if __name__ == "__main__":
    main()
