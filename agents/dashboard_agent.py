"""@dashboard agent — Creates KQL Dashboard with RealTimeDashboard.json definition."""

import json
import logging
import uuid

from .fabric_client import FabricClient

logger = logging.getLogger(__name__)


class DashboardAgent:
    """Generates and deploys a KQL Dashboard with tiles from config."""

    def __init__(self, config: dict, client: FabricClient, database_id: str = ""):
        self.cfg = config["fabric"]["items"]["kql_dashboard"]
        self.db_name = config["fabric"]["items"]["kql_database"]["display_name"]
        self.eh_name = config["fabric"]["items"]["eventhouse"]["display_name"]
        self.client = client
        self.database_id = database_id

    def _make_datasource(self) -> dict:
        """Create the data source reference for the KQL database."""
        return {
            "id": str(uuid.uuid4()),
            "name": self.db_name,
            "clusterUri": "",  # Auto-resolved by Fabric when dashboard opens
            "database": self.db_name,
            "kind": "kusto",
            "scopeId": "cross",
        }

    def _make_tile(self, tile_cfg: dict, datasource_id: str, page_id: str) -> dict:
        """Convert a config tile definition to RealTimeDashboard tile format."""
        tile_id = str(uuid.uuid4())

        visual_type_map = {
            "timechart": "line",
            "piechart": "pie",
            "columnchart": "bar",
            "table": "table",
            "map": "map",
            "stat": "stat",
            "barchart": "bar",
            "areachart": "area",
        }

        return {
            "id": tile_id,
            "title": tile_cfg["title"],
            "visualType": visual_type_map.get(tile_cfg["visual_type"], tile_cfg["visual_type"]),
            "pageId": page_id,
            "layout": {
                "x": 0,
                "y": 0,
                "width": tile_cfg.get("width", 6),
                "height": tile_cfg.get("height", 5),
            },
            "queryRef": {
                "kind": "inline",
                "text": tile_cfg["query"],
                "dataSourceId": datasource_id,
            },
        }

    def build_dashboard_definition(self) -> dict:
        """Build the full RealTimeDashboard.json structure."""
        datasource = self._make_datasource()
        pages = []
        tiles = []

        for page_cfg in self.cfg["pages"]:
            page_id = str(uuid.uuid4())
            pages.append({
                "id": page_id,
                "name": page_cfg["name"],
            })

            # Layout tiles in a grid
            y_offset = 0
            for tile_cfg in page_cfg["tiles"]:
                tile = self._make_tile(tile_cfg, datasource["id"], page_id)
                tile["layout"]["y"] = y_offset
                tiles.append(tile)
                y_offset += tile["layout"]["height"]

        auto_refresh = self.cfg.get("auto_refresh", {})

        dashboard_def = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
            "dataSources": [datasource],
            "pages": pages,
            "tiles": tiles,
            "parameters": [],
        }

        if auto_refresh.get("enabled"):
            dashboard_def["autoRefresh"] = {
                "enabled": True,
                "defaultInterval": auto_refresh.get("default_interval", "30s"),
                "minInterval": auto_refresh.get("min_interval", "15s"),
            }

        return dashboard_def

    def deploy(self) -> dict:
        """Create the KQL Dashboard with full definition."""
        logger.info("=== @dashboard: Creating KQL Dashboard ===")

        dashboard_json = self.build_dashboard_definition()
        tile_count = len(dashboard_json["tiles"])
        page_count = len(dashboard_json["pages"])
        logger.info("Dashboard definition: %d pages, %d tiles", page_count, tile_count)

        existing = self.client.find_item("KQLDashboard", self.cfg["display_name"])
        if existing:
            dash_id = existing["id"]
            # Update existing dashboard definition
            self.client.update_item_definition(
                "KQLDashboard", dash_id,
                {"parts": [{"path": "RealTimeDashboard.json",
                            "payload": self.client.encode_payload(json.dumps(dashboard_json)),
                            "payloadType": "InlineBase64"}]},
            )
            logger.info("Updated existing dashboard '%s' (id=%s)", self.cfg["display_name"], dash_id)
        else:
            dashboard = self.client.create_kql_dashboard(
                display_name=self.cfg["display_name"],
                dashboard_json=dashboard_json,
                description=self.cfg["description"],
            )
            dash_id = dashboard.get("id", dashboard.get("operation_id", "pending"))

            if dash_id == "pending" or "operation_id" in dashboard:
                items = self.client.list_items("KQLDashboard")
                for item in items:
                    if item["displayName"] == self.cfg["display_name"]:
                        dash_id = item["id"]
                        break

        logger.info("KQL Dashboard '%s' created (id=%s)", self.cfg["display_name"], dash_id)
        logger.info(
            "NOTE: Open dashboard in Fabric UI and connect data source to '%s'",
            self.db_name,
        )

        logger.info("=== @dashboard: Deployment complete ===")
        return {
            "dashboard_id": dash_id,
            "display_name": self.cfg["display_name"],
            "pages": page_count,
            "tiles": tile_count,
        }

    def export_definition(self, output_path: str = None) -> str:
        """Export the dashboard definition JSON to a file."""
        import os
        dashboard_json = self.build_dashboard_definition()
        if not output_path:
            output_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "RealTimeDashboard.json",
            )
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(dashboard_json, f, indent=2)
        logger.info("Dashboard definition exported to %s", output_path)
        return output_path
