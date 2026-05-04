"""@activator agent — Creates Activator (Reflex) for automated alerting."""

import logging

from .fabric_client import FabricClient

logger = logging.getLogger(__name__)


class ActivatorAgent:
    """Creates an Activator item with alert rules from config.

    Note: The Activator/Reflex API creates the item container. Rule
    configuration (trigger conditions, actions, object tracking) must be
    done via the Fabric UI after creation, as the Reflex definition API
    does not yet support full rule specification.
    """

    def __init__(self, config: dict, client: FabricClient):
        self.cfg = config["fabric"]["items"]["activator"]
        self.client = client

    def deploy(self) -> dict:
        """Create the Activator item."""
        logger.info("=== @activator: Creating Activator ===")

        activator = self.client.find_or_create(
            "Reflex", self.cfg["display_name"],
            self.client.create_activator,
            description=self.cfg["description"],
        )
        act_id = activator.get("id", activator.get("operation_id", "pending"))

        if act_id == "pending" or "operation_id" in activator:
            items = self.client.list_items("Reflex")
            for item in items:
                if item["displayName"] == self.cfg["display_name"]:
                    act_id = item["id"]
                    break

        # Log the rules that need to be configured in UI
        rules = self.cfg.get("rules", [])
        logger.info(
            "Activator '%s' created (id=%s) — %d rules to configure in UI:",
            self.cfg["display_name"], act_id, len(rules),
        )
        for rule in rules:
            logger.info(
                "  - %s: %s WHERE %s → %s",
                rule["name"],
                rule["source_table"],
                rule.get("condition", ""),
                rule.get("action", "teams"),
            )

        logger.info("=== @activator: Deployment complete ===")
        logger.info(
            "ACTION REQUIRED: Open Activator in Fabric UI to configure "
            "trigger rules, data connections, and action endpoints."
        )
        return {
            "activator_id": act_id,
            "display_name": self.cfg["display_name"],
            "rules_count": len(rules),
            "rules": [r["name"] for r in rules],
        }
