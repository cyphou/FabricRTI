"""@eventstream agent — Creates Eventstreams for Azure Activity + Phone Telemetry.

Supports two ingestion modes:
  Mode A (custom_endpoint): Eventstream exposes a built-in Event Hub endpoint.
    - No separate Azure Event Hub needed.
    - Point Azure Diagnostic Settings directly to the Eventstream endpoint.
    - Simpler, fewer Azure resources, lower cost.
    - Connection string format: sb://eventstream-xxx.servicebus.windows.net/

  Mode B (azure_event_hub): Eventstream pulls from an existing Azure Event Hub.
    - Requires Azure Event Hub Namespace + hub (created by @infra agent).
    - Azure Diagnostic Settings → Azure Event Hub → Eventstream → Eventhouse.
    - More control over partitioning, retention, consumer groups.
"""

import logging

from .fabric_client import FabricClient

logger = logging.getLogger(__name__)


class EventstreamAgent:
    """Creates Eventstream items via Fabric REST API.

    After creation, source/destination wiring is done in Fabric UI:
    - Mode A: Add Custom Endpoint source → copy connection string → use in Azure
    - Mode B: Add Azure Event Hubs source → provide EH connection string
    Then add Eventhouse destination → select KQL DB + table.
    """

    def __init__(self, config: dict, client: FabricClient):
        self.cfg = config["fabric"]["items"]["eventstreams"]
        self.ingestion_mode = config["fabric"].get("ingestion_mode", "custom_endpoint")
        self.client = client

    def deploy(self) -> dict:
        """Create all configured Eventstreams."""
        logger.info("=== @eventstream: Creating Eventstreams (mode=%s) ===", self.ingestion_mode)
        results = []

        for es_cfg in self.cfg:
            eventstream = self.client.find_or_create(
                "Eventstream", es_cfg["display_name"],
                self.client.create_eventstream,
                description=es_cfg["description"],
            )
            es_id = eventstream.get("id", eventstream.get("operation_id", "pending"))

            if es_id == "pending" or "operation_id" in eventstream:
                items = self.client.list_items("Eventstream")
                for item in items:
                    if item["displayName"] == es_cfg["display_name"]:
                        es_id = item["id"]
                        break

            results.append({
                "id": es_id,
                "display_name": es_cfg["display_name"],
                "source_type": es_cfg["source"]["type"],
                "source_hub": es_cfg["source"]["event_hub_name"],
                "dest_table": es_cfg["destination"]["table_name"],
            })
            logger.info(
                "Eventstream '%s' created (id=%s)",
                es_cfg["display_name"], es_id,
            )

        logger.info("=== @eventstream: %d Eventstreams created ===", len(results))
        self._log_wiring_instructions(results)
        return {"eventstreams": results, "ingestion_mode": self.ingestion_mode}

    def _log_wiring_instructions(self, results: list):
        """Log UI wiring instructions based on ingestion mode."""
        logger.info("")
        logger.info("=" * 60)
        logger.info("WIRING INSTRUCTIONS (Fabric UI)")
        logger.info("=" * 60)

        if self.ingestion_mode == "custom_endpoint":
            logger.info("Mode A: Custom Endpoint (no Azure Event Hub needed)")
            logger.info("")
            logger.info("For EACH Eventstream:")
            logger.info("  1. Open Eventstream in Fabric UI → Edit mode")
            logger.info("  2. Add source → Custom Endpoint → name it → Publish")
            logger.info("  3. Click the Custom Endpoint source node")
            logger.info("  4. In Details pane → SAS Key Authentication tab:")
            logger.info("     Copy the 'Connection string-primary key'")
            logger.info("     Format: sb://eventstream-xxx.servicebus.windows.net/...")
            logger.info("")
            logger.info("For Azure Activity stream ('%s'):", results[0]["display_name"])
            logger.info("  5. Go to Azure Portal → Monitor → Diagnostic Settings")
            logger.info("  6. Create subscription diagnostic setting:")
            logger.info("     - Event Hub namespace = the sb://eventstream-xxx... hostname")
            logger.info("     - Event Hub name = the EntityPath value from connection string")
            logger.info("     - Policy = the SharedAccessKeyName from connection string")
            logger.info("     NOTE: Azure Diagnostic Settings require an Azure Event Hub")
            logger.info("     target, so Mode A works for phone telemetry (custom app)")
            logger.info("     but NOT directly for Azure Diagnostic Settings.")
            logger.info("     → For Azure Activity: use Mode B or stream via custom app.")
            logger.info("")
            logger.info("For Phone Telemetry stream ('%s'):", results[1]["display_name"] if len(results) > 1 else "N/A")
            logger.info("  5. Use the connection string in phone-telemetry.html")
            logger.info("     or phone_simulator.py (set EVENT_HUB_CONNECTION_STRING)")
            logger.info("")

        else:  # azure_event_hub mode
            logger.info("Mode B: Azure Event Hub → Eventstream")
            logger.info("")
            logger.info("For EACH Eventstream:")
            logger.info("  1. Open Eventstream in Fabric UI → Edit mode")
            logger.info("  2. Add source → Azure Event Hubs → Connect")
            logger.info("  3. Enter Event Hub namespace: %s", results[0].get("source_hub", "eh-rti-demo"))
            logger.info("  4. Enter Event Hub name (e.g., 'azure-activity')")
            logger.info("  5. Auth: Shared Access Key")
            logger.info("     - Key Name: RootManageSharedAccessKey")
            logger.info("     - Key: (from setup-azure.ps1 output)")
            logger.info("  6. Consumer group: $Default")
            logger.info("  7. Data format: JSON")
            logger.info("  8. Click Next → Review + Connect → Add")
            logger.info("")

        logger.info("Then for ALL Eventstreams, add destination:")
        logger.info("  1. Edit mode → Add destination → Eventhouse")
        logger.info("  2. Select workspace → Eventhouse → KQL Database")
        logger.info("  3. Select target table (AzureActivity / PhoneTelemetry)")
        logger.info("  4. Input data format: JSON")
        logger.info("  5. Publish")
        logger.info("=" * 60)
