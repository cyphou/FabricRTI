"""@validator agent — Post-deployment health check and verification."""

import logging
import time

from .fabric_client import FabricClient

logger = logging.getLogger(__name__)

# Expected item types and minimum counts
EXPECTED_ITEMS = {
    "Eventhouse": 1,
    "KQLDatabase": 1,
    "Eventstream": 2,
    "KQLQueryset": 1,
    "KQLDashboard": 1,
    "Reflex": 1,
}


class ValidatorAgent:
    """Validates that all RTI Demo items were deployed correctly."""

    def __init__(self, config: dict, client: FabricClient):
        self.cfg = config["fabric"]["items"]
        self.client = client

    def _check_item_exists(self, item_type: str, display_name: str) -> dict:
        """Check if a specific item exists in the workspace."""
        items = self.client.list_items(item_type)
        for item in items:
            if item["displayName"] == display_name:
                return {"found": True, "id": item["id"], "name": display_name}
        return {"found": False, "id": None, "name": display_name}

    def validate(self) -> dict:
        """Run full post-deployment validation."""
        logger.info("=== @validator: Running post-deployment validation ===")
        results = {"passed": [], "failed": [], "warnings": []}

        # 1. Check Eventhouse
        eh = self._check_item_exists("Eventhouse", self.cfg["eventhouse"]["display_name"])
        if eh["found"]:
            results["passed"].append(f"Eventhouse '{eh['name']}' exists (id={eh['id']})")
        else:
            results["failed"].append(f"Eventhouse '{eh['name']}' NOT FOUND")

        # 2. Check KQL Database
        db = self._check_item_exists("KQLDatabase", self.cfg["kql_database"]["display_name"])
        if db["found"]:
            results["passed"].append(f"KQL Database '{db['name']}' exists (id={db['id']})")
        else:
            results["failed"].append(f"KQL Database '{db['name']}' NOT FOUND")

        # 3. Check Eventstreams
        for es_cfg in self.cfg["eventstreams"]:
            es = self._check_item_exists("Eventstream", es_cfg["display_name"])
            if es["found"]:
                results["passed"].append(f"Eventstream '{es['name']}' exists (id={es['id']})")
            else:
                results["failed"].append(f"Eventstream '{es['name']}' NOT FOUND")

        # 4. Check KQL Queryset
        qs = self._check_item_exists("KQLQueryset", self.cfg["kql_queryset"]["display_name"])
        if qs["found"]:
            results["passed"].append(f"KQL Queryset '{qs['name']}' exists (id={qs['id']})")
        else:
            results["failed"].append(f"KQL Queryset '{qs['name']}' NOT FOUND")

        # 5. Check KQL Dashboard
        dash = self._check_item_exists("KQLDashboard", self.cfg["kql_dashboard"]["display_name"])
        if dash["found"]:
            results["passed"].append(f"KQL Dashboard '{dash['name']}' exists (id={dash['id']})")
        else:
            results["failed"].append(f"KQL Dashboard '{dash['name']}' NOT FOUND")

        # 6. Check Activator
        act = self._check_item_exists("Reflex", self.cfg["activator"]["display_name"])
        if act["found"]:
            results["passed"].append(f"Activator '{act['name']}' exists (id={act['id']})")
        else:
            results["failed"].append(f"Activator '{act['name']}' NOT FOUND")

        # 7. Check AI Agents
        for key in ("data_agent", "ops_agent", "anomaly_detector"):
            ai_cfg = self.cfg.get(key)
            if ai_cfg:
                ai = self._check_item_exists(ai_cfg["type"], ai_cfg["display_name"])
                if ai["found"]:
                    results["passed"].append(f"{ai_cfg['type']} '{ai['name']}' exists (id={ai['id']})")
                else:
                    results["failed"].append(f"{ai_cfg['type']} '{ai['name']}' NOT FOUND")

        # Summary
        total = len(results["passed"]) + len(results["failed"])
        passed = len(results["passed"])
        failed = len(results["failed"])

        results["summary"] = {
            "total_checks": total,
            "passed": passed,
            "failed": failed,
            "status": "PASS" if failed == 0 else "FAIL",
        }

        # Log results
        for msg in results["passed"]:
            logger.info("  ✓ %s", msg)
        for msg in results["failed"]:
            logger.error("  ✗ %s", msg)
        for msg in results["warnings"]:
            logger.warning("  ⚠ %s", msg)

        logger.info(
            "=== @validator: %s — %d/%d checks passed ===",
            results["summary"]["status"], passed, total,
        )

        # Manual steps reminder
        if failed == 0:
            logger.info("Post-deployment manual steps:")
            logger.info("  1. Wire Eventstream sources/destinations in Fabric UI")
            logger.info("  2. Connect Dashboard data source to KQL Database")
            logger.info("  3. Configure Activator trigger rules")
            logger.info("  4. Start phone simulator (phone-telemetry.html)")

        return results
