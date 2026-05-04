"""AI Agents — Creates Data Agent, Operational Agent, and Anomaly Detector.

These Fabric items are created via the generic Items API and must be
configured through the Fabric UI after creation (connect to KQL Database,
set up anomaly detection rules, etc.).
"""

import logging

logger = logging.getLogger(__name__)


class AIAgentsAgent:
    """Creates AI-powered Fabric items for the RTI Demo."""

    def __init__(self, config: dict, client):
        self.config = config
        self.client = client
        self.items_config = config["fabric"]["items"]

    def deploy(self) -> dict:
        """Deploy all AI agent items."""
        results = {}

        for key in ("data_agent", "ops_agent", "anomaly_detector"):
            item_cfg = self.items_config.get(key)
            if not item_cfg:
                logger.warning("No config for %s — skipping", key)
                continue

            item_type = item_cfg["type"]
            display_name = item_cfg["display_name"]
            description = item_cfg.get("description", "")

            existing = self.client.find_item(item_type, display_name)
            if existing:
                results[key] = existing
                continue

            result = self.client.create_item(item_type, display_name, description)
            results[key] = result
            logger.info("Created %s: %s", item_type, display_name)

        # Summary
        created = [k for k, v in results.items() if v.get("id")]
        logger.info("AI Agents: %d items ready", len(created))

        return {
            "status": "success",
            "items": results,
            "post_deployment": [
                "Connect RTI-Demo-DataAgent to KQL Database in Fabric UI",
                "Connect RTI-Ops-Agent to KQL Database in Fabric UI",
                "Configure RTI-Anomaly-Detector data source and rules in Fabric UI",
            ],
        }
