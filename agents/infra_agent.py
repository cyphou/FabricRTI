"""@infra agent — Provisions Azure resources (Event Hubs, Diagnostic Settings)."""

import json
import logging
import subprocess

logger = logging.getLogger(__name__)


class InfraAgent:
    """Provisions Azure infrastructure for the RTI demo."""

    def __init__(self, config: dict):
        self.cfg = config["azure"]
        self.rg = self.cfg["resource_group"]
        self.location = self.cfg["location"]
        self.eh = self.cfg["event_hub"]

    def _run_az(self, args: list[str]) -> str:
        """Run an Azure CLI command and return stdout."""
        cmd = ["az"] + args + ["--output", "json"]
        logger.info("az %s", " ".join(args[:4]))
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return result.stdout.strip()

    def create_resource_group(self) -> dict:
        """Create the resource group."""
        out = self._run_az([
            "group", "create",
            "--name", self.rg,
            "--location", self.location,
        ])
        logger.info("Resource group '%s' created", self.rg)
        return json.loads(out)

    def create_event_hub_namespace(self) -> dict:
        """Create Event Hub namespace."""
        out = self._run_az([
            "eventhubs", "namespace", "create",
            "--name", self.eh["namespace"],
            "--resource-group", self.rg,
            "--location", self.location,
            "--sku", self.eh["sku"],
        ])
        logger.info("Event Hub namespace '%s' created", self.eh["namespace"])
        return json.loads(out)

    def create_event_hub(self, hub_config: dict) -> dict:
        """Create a single Event Hub."""
        out = self._run_az([
            "eventhubs", "eventhub", "create",
            "--name", hub_config["name"],
            "--namespace-name", self.eh["namespace"],
            "--resource-group", self.rg,
            "--partition-count", str(hub_config["partition_count"]),
            "--message-retention", str(hub_config["message_retention"]),
        ])
        logger.info("Event Hub '%s' created", hub_config["name"])
        return json.loads(out)

    def get_connection_string(self) -> str:
        """Get the namespace connection string."""
        out = self._run_az([
            "eventhubs", "namespace", "authorization-rule", "keys", "list",
            "--resource-group", self.rg,
            "--namespace-name", self.eh["namespace"],
            "--name", "RootManageSharedAccessKey",
            "--query", "primaryConnectionString",
            "--output", "tsv",
        ])
        return out

    def create_diagnostic_settings(self) -> dict:
        """Route Azure Activity Log to Event Hub."""
        diag = self.cfg["diagnostic_settings"]
        sub_id = self._run_az(["account", "show", "--query", "id", "--output", "tsv"])

        auth_rule = (
            f"/subscriptions/{sub_id}/resourceGroups/{self.rg}"
            f"/providers/Microsoft.EventHub/namespaces/{self.eh['namespace']}"
            f"/authorizationRules/RootManageSharedAccessKey"
        )

        logs = json.dumps([
            {"category": cat, "enabled": True} for cat in diag["categories"]
        ])

        out = self._run_az([
            "monitor", "diagnostic-settings", "subscription", "create",
            "--name", diag["name"],
            "--location", self.location,
            "--event-hub-name", self.eh["hubs"]["azure_activity"]["name"],
            "--event-hub-auth-rule", auth_rule,
            "--logs", logs,
        ])
        logger.info("Diagnostic settings '%s' created", diag["name"])
        return json.loads(out) if out else {}

    def deploy(self) -> dict:
        """Run full infrastructure deployment."""
        logger.info("=== @infra: Starting Azure infrastructure deployment ===")

        self.create_resource_group()
        self.create_event_hub_namespace()

        for hub_key, hub_config in self.eh["hubs"].items():
            self.create_event_hub(hub_config)

        conn_str = self.get_connection_string()
        logger.info("Connection string retrieved")

        self.create_diagnostic_settings()

        logger.info("=== @infra: Azure infrastructure deployment complete ===")
        return {
            "resource_group": self.rg,
            "namespace": self.eh["namespace"],
            "connection_string": conn_str,
            "hubs": list(self.eh["hubs"].keys()),
        }

    def cleanup(self):
        """Delete all Azure resources."""
        logger.info("Cleaning up Azure resources...")
        try:
            self._run_az([
                "monitor", "diagnostic-settings", "subscription", "delete",
                "--name", self.cfg["diagnostic_settings"]["name"],
            ])
        except subprocess.CalledProcessError:
            logger.warning("Diagnostic settings already deleted or not found")

        self._run_az([
            "group", "delete",
            "--name", self.rg,
            "--yes", "--no-wait",
        ])
        logger.info("Cleanup initiated (async)")
