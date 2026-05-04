"""Fabric REST API client for RTI Demo deployment.

Handles authentication, item CRUD, long-running operations (LRO) polling,
and base64 definition encoding for all Fabric item types.
"""

import base64
import json
import logging
import time

import requests
from azure.identity import AzureCliCredential, InteractiveBrowserCredential

logger = logging.getLogger(__name__)

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
AZURE_MGMT_SCOPE = "https://management.azure.com/.default"

LRO_POLL_INTERVAL = 5  # seconds
LRO_MAX_WAIT = 300  # seconds


class FabricClient:
    """Client for Fabric REST API operations."""

    def __init__(self, workspace_id: str, credential=None):
        self.workspace_id = workspace_id
        self.credential = credential or self._get_credential()
        self._token_cache = {}

    @staticmethod
    def _get_credential():
        """Try AzureCliCredential first, fall back to interactive browser."""
        try:
            cred = AzureCliCredential(process_timeout=30)
            cred.get_token(FABRIC_SCOPE)
            logger.info("Using AzureCliCredential")
            return cred
        except Exception:
            logger.info("Falling back to InteractiveBrowserCredential")
            return InteractiveBrowserCredential()

    def _get_token(self, scope: str = FABRIC_SCOPE) -> str:
        """Get an access token, caching it until near expiry."""
        cached = self._token_cache.get(scope)
        if cached and cached.expires_on > time.time() + 60:
            return cached.token
        token = self.credential.get_token(scope)
        self._token_cache[scope] = token
        return token.token

    def _headers(self, scope: str = FABRIC_SCOPE) -> dict:
        return {
            "Authorization": f"Bearer {self._get_token(scope)}",
            "Content-Type": "application/json",
        }

    def _url(self, path: str) -> str:
        return f"{FABRIC_API_BASE}/workspaces/{self.workspace_id}/{path}"

    # ----------------------------------------------------------------
    # LRO polling
    # ----------------------------------------------------------------
    def _poll_operation(self, operation_url: str, operation_id: str = None) -> dict:
        """Poll a long-running operation until completion."""
        start = time.time()
        while time.time() - start < LRO_MAX_WAIT:
            resp = requests.get(operation_url, headers=self._headers())
            if resp.status_code == 200:
                body = resp.json()
                status = body.get("status", "Unknown")
                if status in ("Succeeded", "Completed"):
                    logger.info("Operation %s completed", operation_id or "")
                    return body
                if status in ("Failed", "Cancelled"):
                    raise RuntimeError(
                        f"Operation {operation_id} {status}: {body.get('error', {})}"
                    )
                logger.debug("Operation %s status: %s", operation_id, status)
            time.sleep(LRO_POLL_INTERVAL)
        raise TimeoutError(f"Operation {operation_id} timed out after {LRO_MAX_WAIT}s")

    def _handle_response(self, resp: requests.Response, item_type: str) -> dict:
        """Handle 201 (created) or 202 (LRO) responses."""
        if resp.status_code == 201:
            result = resp.json()
            logger.info("Created %s: %s (id=%s)", item_type, result["displayName"], result["id"])
            return result
        if resp.status_code == 202:
            op_url = resp.headers.get("Location", "")
            op_id = resp.headers.get("x-ms-operation-id", "")
            logger.info("Creating %s (LRO: %s)...", item_type, op_id)
            self._poll_operation(op_url, op_id)
            # After LRO completes, the item might need to be fetched separately
            return {"status": "created_async", "operation_id": op_id}
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 30))
            logger.warning("Rate limited, retrying in %ds", retry_after)
            time.sleep(retry_after)
            return None  # caller should retry
        resp.raise_for_status()
        return resp.json()

    # ----------------------------------------------------------------
    # Find existing items (idempotent deployment support)
    # ----------------------------------------------------------------
    def find_item(self, item_type: str, display_name: str) -> dict | None:
        """Find an existing item by type and display name. Returns None if not found."""
        items = self.list_items(item_type)
        for item in items:
            if item["displayName"] == display_name:
                logger.info("Found existing %s: %s (id=%s)", item_type, display_name, item["id"])
                return item
        return None

    def find_or_create(self, item_type: str, display_name: str,
                       create_fn, **create_kwargs) -> dict:
        """Find an existing item or create it. Returns the item dict with at least 'id'."""
        existing = self.find_item(item_type, display_name)
        if existing:
            return existing
        return create_fn(display_name=display_name, **create_kwargs)

    # ----------------------------------------------------------------
    # Base64 encoding helpers
    # ----------------------------------------------------------------
    @staticmethod
    def encode_payload(content: str) -> str:
        """Encode string content to Base64 for Fabric definitions."""
        return base64.b64encode(content.encode("utf-8")).decode("utf-8")

    @staticmethod
    def decode_payload(b64: str) -> str:
        """Decode Base64 Fabric definition payload."""
        return base64.b64decode(b64).decode("utf-8")

    # ----------------------------------------------------------------
    # Item CRUD
    # ----------------------------------------------------------------
    def create_eventhouse(self, display_name: str, description: str = "") -> dict:
        """Create an Eventhouse."""
        body = {"displayName": display_name, "description": description}
        resp = requests.post(self._url("eventhouses"), headers=self._headers(), json=body)
        return self._handle_response(resp, "Eventhouse")

    def create_kql_database(
        self, display_name: str, parent_eventhouse_id: str,
        schema_kql: str = None, description: str = ""
    ) -> dict:
        """Create a KQL Database, optionally with schema definition."""
        if schema_kql:
            db_props = json.dumps({
                "databaseType": "ReadWrite",
                "parentEventhouseItemId": parent_eventhouse_id,
                "oneLakeCachingPeriod": "P36500D",
                "oneLakeStandardStoragePeriod": "P36500D",
            })
            body = {
                "displayName": display_name,
                "description": description,
                "definition": {
                    "parts": [
                        {
                            "path": "DatabaseProperties.json",
                            "payload": self.encode_payload(db_props),
                            "payloadType": "InlineBase64",
                        },
                        {
                            "path": "DatabaseSchema.kql",
                            "payload": self.encode_payload(schema_kql),
                            "payloadType": "InlineBase64",
                        },
                    ]
                },
            }
        else:
            body = {
                "displayName": display_name,
                "description": description,
                "creationPayload": {
                    "databaseType": "ReadWrite",
                    "parentEventhouseItemId": parent_eventhouse_id,
                },
            }
        resp = requests.post(self._url("kqlDatabases"), headers=self._headers(), json=body)
        return self._handle_response(resp, "KQLDatabase")

    def create_eventstream(self, display_name: str, description: str = "") -> dict:
        """Create an Eventstream (empty, configure via UI or definition update)."""
        body = {"displayName": display_name, "description": description}
        resp = requests.post(self._url("eventstreams"), headers=self._headers(), json=body)
        return self._handle_response(resp, "Eventstream")

    def create_kql_queryset(
        self, display_name: str, description: str = ""
    ) -> dict:
        """Create a KQL Queryset."""
        body = {"displayName": display_name, "description": description}
        resp = requests.post(self._url("kqlQuerysets"), headers=self._headers(), json=body)
        return self._handle_response(resp, "KQLQueryset")

    def create_kql_dashboard(
        self, display_name: str, dashboard_json: dict = None, description: str = ""
    ) -> dict:
        """Create a KQL Dashboard, optionally with full definition."""
        if dashboard_json:
            body = {
                "displayName": display_name,
                "description": description,
                "definition": {
                    "parts": [
                        {
                            "path": "RealTimeDashboard.json",
                            "payload": self.encode_payload(json.dumps(dashboard_json)),
                            "payloadType": "InlineBase64",
                        }
                    ]
                },
            }
        else:
            body = {"displayName": display_name, "description": description}
        resp = requests.post(self._url("kqlDashboards"), headers=self._headers(), json=body)
        return self._handle_response(resp, "KQLDashboard")

    def create_activator(self, display_name: str, description: str = "") -> dict:
        """Create an Activator (Reflex)."""
        body = {"displayName": display_name, "description": description}
        resp = requests.post(self._url("reflexes"), headers=self._headers(), json=body)
        return self._handle_response(resp, "Activator")

    def create_item(self, item_type: str, display_name: str, description: str = "") -> dict:
        """Create a generic Fabric item (DataAgent, AnomalyDetector, etc.)."""
        body = {"displayName": display_name, "type": item_type, "description": description}
        resp = requests.post(
            f"{FABRIC_API_BASE}/workspaces/{self.workspace_id}/items",
            headers=self._headers(),
            json=body,
        )
        return self._handle_response(resp, item_type)

    # ----------------------------------------------------------------
    # Read / List
    # ----------------------------------------------------------------
    def list_items(self, item_type: str = None) -> list:
        """List items in workspace, optionally filtered by type."""
        url = f"{FABRIC_API_BASE}/workspaces/{self.workspace_id}/items"
        if item_type:
            url += f"?type={item_type}"
        resp = requests.get(url, headers=self._headers())
        resp.raise_for_status()
        return resp.json().get("value", [])

    def get_item(self, item_type: str, item_id: str) -> dict:
        """Get a specific item."""
        type_map = {
            "Eventhouse": "eventhouses",
            "KQLDatabase": "kqlDatabases",
            "Eventstream": "eventstreams",
            "KQLQueryset": "kqlQuerysets",
            "KQLDashboard": "kqlDashboards",
            "Reflex": "reflexes",
            "DataAgent": "dataAgents",
            "AnomalyDetector": "anomalyDetectors",
            "OperationsAgent": "items",
        }
        path = type_map.get(item_type, "items")
        resp = requests.get(self._url(f"{path}/{item_id}"), headers=self._headers())
        resp.raise_for_status()
        return resp.json()

    def get_item_definition(self, item_type: str, item_id: str) -> dict:
        """Get item definition."""
        type_map = {
            "Eventhouse": "eventhouses",
            "KQLDatabase": "kqlDatabases",
            "Eventstream": "eventstreams",
            "KQLQueryset": "kqlQuerysets",
            "KQLDashboard": "kqlDashboards",
        }
        path = type_map[item_type]
        resp = requests.post(
            self._url(f"{path}/{item_id}/getDefinition"), headers=self._headers()
        )
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 202:
            op_url = resp.headers.get("Location", "")
            op_id = resp.headers.get("x-ms-operation-id", "")
            return self._poll_operation(op_url, op_id)
        resp.raise_for_status()

    def update_item_definition(self, item_type: str, item_id: str, definition: dict):
        """Update item definition."""
        type_map = {
            "Eventhouse": "eventhouses",
            "KQLDatabase": "kqlDatabases",
            "Eventstream": "eventstreams",
            "KQLQueryset": "kqlQuerysets",
            "KQLDashboard": "kqlDashboards",
        }
        path = type_map[item_type]
        resp = requests.post(
            self._url(f"{path}/{item_id}/updateDefinition"),
            headers=self._headers(),
            json={"definition": definition},
        )
        if resp.status_code in (200, 202):
            logger.info("Updated definition for %s/%s", item_type, item_id)
            return True
        resp.raise_for_status()

    # ----------------------------------------------------------------
    # Delete
    # ----------------------------------------------------------------
    def delete_item(self, item_type: str, item_id: str):
        """Delete an item."""
        type_map = {
            "Eventhouse": "eventhouses",
            "KQLDatabase": "kqlDatabases",
            "Eventstream": "eventstreams",
            "KQLQueryset": "kqlQuerysets",
            "KQLDashboard": "kqlDashboards",
            "Reflex": "reflexes",
            "DataAgent": "dataAgents",
            "AnomalyDetector": "anomalyDetectors",
            "OperationsAgent": "items",
        }
        path = type_map.get(item_type, "items")
        resp = requests.delete(self._url(f"{path}/{item_id}"), headers=self._headers())
        resp.raise_for_status()
        logger.info("Deleted %s/%s", item_type, item_id)

    # ----------------------------------------------------------------
    # Azure Management API (for infra agent)
    # ----------------------------------------------------------------
    def azure_mgmt_request(self, method: str, url: str, body: dict = None) -> dict:
        """Make an Azure Management API request."""
        headers = self._headers(AZURE_MGMT_SCOPE)
        resp = requests.request(method, url, headers=headers, json=body)
        resp.raise_for_status()
        return resp.json() if resp.content else {}
