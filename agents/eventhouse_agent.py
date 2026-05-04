"""@eventhouse agent — Creates Eventhouse + KQL Database with table schema.

Supports medallion architecture (bronze → silver → gold) using KQL update
policies and materialized views, inspired by:
https://github.com/chakras/github-audit-log-analytics
"""

import logging
import os

from .fabric_client import FabricClient

logger = logging.getLogger(__name__)

# KQL schema to create tables for both data streams
DATABASE_SCHEMA = """\
// KQL management commands for RTI Demo database

// Azure Activity Log table
.create-merge table AzureActivity (
    Timestamp: datetime,
    OperationName: string,
    OperationId: string,
    Status: string,
    Level: string,
    ResourceGroup: string,
    ResourceType: string,
    ResourceId: string,
    Caller: string,
    CallerIpAddress: string,
    Category: string,
    ResultType: string,
    ResultSignature: string,
    CorrelationId: string,
    Region: string,
    Properties: dynamic
)

// Phone Telemetry table
.create-merge table PhoneTelemetry (
    Timestamp: datetime,
    DeviceId: string,
    User: string,
    OS: string,
    City: string,
    BatteryLevel: real,
    BatteryCharging: bool,
    SignalStrength: int,
    Latitude: real,
    Longitude: real,
    AppName: string,
    CpuUsage: real,
    MemoryUsageMB: int,
    CrashCount: int,
    NetworkType: string,
    ScreenWidth: int,
    ScreenHeight: int,
    UserAgent: string
)

// Enable streaming ingestion on both tables
.alter table AzureActivity policy streamingingestion enable
.alter table PhoneTelemetry policy streamingingestion enable

// Note: Retention policy can be set via Fabric UI or KQL queryset after creation

// Auto-mapping for JSON ingestion
.create-or-alter table AzureActivity ingestion json mapping 'AzureActivityMapping'
'['
'  {"column": "Timestamp", "path": "$.Timestamp", "datatype": "datetime"},'
'  {"column": "OperationName", "path": "$.operationName", "datatype": "string"},'
'  {"column": "OperationId", "path": "$.operationId", "datatype": "string"},'
'  {"column": "Status", "path": "$.status.value", "datatype": "string"},'
'  {"column": "Level", "path": "$.level", "datatype": "string"},'
'  {"column": "ResourceGroup", "path": "$.resourceGroupName", "datatype": "string"},'
'  {"column": "ResourceType", "path": "$.resourceType.value", "datatype": "string"},'
'  {"column": "ResourceId", "path": "$.resourceId", "datatype": "string"},'
'  {"column": "Caller", "path": "$.caller", "datatype": "string"},'
'  {"column": "CallerIpAddress", "path": "$.callerIpAddress", "datatype": "string"},'
'  {"column": "Category", "path": "$.category.value", "datatype": "string"},'
'  {"column": "ResultType", "path": "$.resultType.value", "datatype": "string"},'
'  {"column": "ResultSignature", "path": "$.resultSignature.value", "datatype": "string"},'
'  {"column": "CorrelationId", "path": "$.correlationId", "datatype": "string"},'
'  {"column": "Region", "path": "$.Region", "datatype": "string"},'
'  {"column": "Properties", "path": "$.properties", "datatype": "dynamic"}'
']'

.create-or-alter table PhoneTelemetry ingestion json mapping 'PhoneTelemetryMapping'
'['
'  {"column": "Timestamp", "path": "$.Timestamp", "datatype": "datetime"},'
'  {"column": "DeviceId", "path": "$.DeviceId", "datatype": "string"},'
'  {"column": "User", "path": "$.User", "datatype": "string"},'
'  {"column": "OS", "path": "$.OS", "datatype": "string"},'
'  {"column": "City", "path": "$.City", "datatype": "string"},'
'  {"column": "BatteryLevel", "path": "$.BatteryLevel", "datatype": "real"},
'  {"column": "BatteryCharging", "path": "$.BatteryCharging", "datatype": "bool"},'
'  {"column": "SignalStrength", "path": "$.SignalStrength", "datatype": "int"},'
'  {"column": "Latitude", "path": "$.Latitude", "datatype": "real"},'
'  {"column": "Longitude", "path": "$.Longitude", "datatype": "real"},'
'  {"column": "AppName", "path": "$.AppName", "datatype": "string"},'
'  {"column": "CpuUsage", "path": "$.CpuUsage", "datatype": "real"},'
'  {"column": "MemoryUsageMB", "path": "$.MemoryUsageMB", "datatype": "int"},'
'  {"column": "CrashCount", "path": "$.CrashCount", "datatype": "int"},'
'  {"column": "NetworkType", "path": "$.NetworkType", "datatype": "string"},'
'  {"column": "ScreenWidth", "path": "$.ScreenWidth", "datatype": "int"},'
'  {"column": "ScreenHeight", "path": "$.ScreenHeight", "datatype": "int"},'
'  {"column": "UserAgent", "path": "$.UserAgent", "datatype": "string"}'
']'
"""


class EventhouseAgent:
    """Creates Eventhouse and KQL Database with table definitions.

    Supports optional medallion architecture deployment (silver/gold layers)
    that runs after data starts flowing.
    """

    def __init__(self, config: dict, client: FabricClient, project_dir: str = None):
        self.cfg = config["fabric"]["items"]
        self.client = client
        self.project_dir = project_dir or os.path.dirname(os.path.dirname(__file__))

    def _load_medallion_schema(self) -> str | None:
        """Load the medallion architecture KQL from file."""
        path = os.path.join(self.project_dir, "medallion-architecture.kql")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return f.read()
        return None

    def deploy(self) -> dict:
        """Create Eventhouse + KQL Database."""
        logger.info("=== @eventhouse: Creating Eventhouse + KQL Database ===")

        # 1. Find or create Eventhouse
        eh_cfg = self.cfg["eventhouse"]
        eventhouse = self.client.find_or_create(
            "Eventhouse", eh_cfg["display_name"],
            self.client.create_eventhouse,
            description=eh_cfg["description"],
        )
        eventhouse_id = eventhouse.get("id", eventhouse.get("operation_id", "pending"))
        logger.info("Eventhouse ID: %s", eventhouse_id)

        # If LRO, we need to list items to get the actual ID
        if eventhouse_id == "pending" or "operation_id" in eventhouse:
            items = self.client.list_items("Eventhouse")
            for item in items:
                if item["displayName"] == eh_cfg["display_name"]:
                    eventhouse_id = item["id"]
                    break

        # 2. Find or create KQL Database with schema
        db_cfg = self.cfg["kql_database"]
        existing_db = self.client.find_item("KQLDatabase", db_cfg["display_name"])
        if existing_db:
            database = existing_db
        else:
            database = self.client.create_kql_database(
                display_name=db_cfg["display_name"],
                parent_eventhouse_id=eventhouse_id,
                schema_kql=DATABASE_SCHEMA,
                description=db_cfg["description"],
            )
        database_id = database.get("id", database.get("operation_id", "pending"))

        if database_id == "pending" or "operation_id" in database:
            items = self.client.list_items("KQLDatabase")
            for item in items:
                if item["displayName"] == db_cfg["display_name"]:
                    database_id = item["id"]
                    break

        logger.info("=== @eventhouse: Deployment complete ===")

        # Check if medallion architecture script is available
        medallion = self._load_medallion_schema()
        if medallion:
            logger.info(
                "Medallion architecture script found (medallion-architecture.kql). "
                "Run it after data starts flowing to create silver/gold layers."
            )

        return {
            "eventhouse_id": eventhouse_id,
            "database_id": database_id,
            "eventhouse_name": eh_cfg["display_name"],
            "database_name": db_cfg["display_name"],
            "medallion_ready": medallion is not None,
        }
