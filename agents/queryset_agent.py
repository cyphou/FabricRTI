"""@queryset agent — Creates KQL Queryset with demo queries."""

import logging
import os

from .fabric_client import FabricClient

logger = logging.getLogger(__name__)


class QuerysetAgent:
    """Creates a KQL Queryset item with pre-built demo queries."""

    def __init__(self, config: dict, client: FabricClient, project_dir: str = None):
        self.cfg = config["fabric"]["items"]["kql_queryset"]
        self.client = client
        self.project_dir = project_dir or os.path.dirname(os.path.dirname(__file__))

    def _load_queries(self) -> str:
        """Load KQL queries from the demo-queries.kql file."""
        queries_file = os.path.join(self.project_dir, self.cfg["queries_file"])
        with open(queries_file, "r", encoding="utf-8") as f:
            return f.read()

    def deploy(self) -> dict:
        """Create the KQL Queryset."""
        logger.info("=== @queryset: Creating KQL Queryset ===")

        queryset = self.client.find_or_create(
            "KQLQueryset", self.cfg["display_name"],
            self.client.create_kql_queryset,
            description=self.cfg["description"],
        )
        qs_id = queryset.get("id", queryset.get("operation_id", "pending"))

        if qs_id == "pending" or "operation_id" in queryset:
            items = self.client.list_items("KQLQueryset")
            for item in items:
                if item["displayName"] == self.cfg["display_name"]:
                    qs_id = item["id"]
                    break

        logger.info("KQL Queryset '%s' created (id=%s)", self.cfg["display_name"], qs_id)

        # Load queries for reference (queryset content is typically edited in UI)
        try:
            queries = self._load_queries()
            query_count = queries.count("//") // 2  # rough count
            logger.info("Loaded %d queries from %s", query_count, self.cfg["queries_file"])
        except FileNotFoundError:
            logger.warning("Queries file not found: %s", self.cfg["queries_file"])

        logger.info("=== @queryset: Deployment complete ===")
        return {
            "queryset_id": qs_id,
            "display_name": self.cfg["display_name"],
        }
