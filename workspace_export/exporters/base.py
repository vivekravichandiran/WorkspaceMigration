"""Shared HTTP base for all workspace exporters."""
from __future__ import annotations

import json
import logging
import os
import time
import urllib3
from typing import Any, Dict, Iterator, List, Optional

import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

_LOG = logging.getLogger(__name__)

# Warehouse / cluster field keys to strip before saving (runtime state only)
_WAREHOUSE_RUNTIME_KEYS = {
    "id", "state", "health", "creator_name", "create_time", "update_time",
    "num_clusters", "cluster_id", "odbc_params", "num_active_sessions",
}
_PIPELINE_RUNTIME_KEYS = {
    "pipeline_id", "state", "cluster_id", "create_time", "update_time",
    "last_modified", "creator_user_name", "last_restart_window",
}
_ENDPOINT_RUNTIME_KEYS = {
    "id", "state", "creator", "creation_timestamp", "last_updated_timestamp",
    "pending_config", "permission_level",
}
_REPO_RUNTIME_KEYS = {"id", "creator"}
_GENIE_RUNTIME_KEYS = {"space_id", "create_time", "update_time", "creator"}


def _strip(obj: Dict, keys: set) -> Dict:
    return {k: v for k, v in obj.items() if k not in keys}


class BaseExporter:
    """Lightweight REST client shared by all new-component exporters."""

    def __init__(
        self,
        workspace_url: str,
        token: str,
        export_dir: str,
        verify_ssl: bool = True,
    ) -> None:
        self.workspace_url = workspace_url.rstrip("/")
        self.token = token
        self.export_dir = export_dir
        self.verify_ssl = verify_ssl
        self._session = requests.Session()
        self._session.headers.update(
            {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        )

    # ------------------------------------------------------------------
    # Low-level HTTP helpers
    # ------------------------------------------------------------------

    def _get(self, path: str, params: Optional[Dict] = None) -> Any:
        url = f"{self.workspace_url}{path}"
        r = self._session.get(url, params=params, verify=self.verify_ssl, timeout=60)
        r.raise_for_status()
        return r.json()

    def _post(self, path: str, data: Optional[Dict] = None) -> Any:
        url = f"{self.workspace_url}{path}"
        r = self._session.post(url, json=data, verify=self.verify_ssl, timeout=60)
        r.raise_for_status()
        return r.json()

    def _paginated_get(
        self,
        path: str,
        results_key: str,
        params: Optional[Dict] = None,
        token_key: str = "next_page_token",
    ) -> List[Dict]:
        """Collect all pages for REST endpoints that use next_page_token."""
        out: List[Dict] = []
        params = dict(params or {})
        while True:
            data = self._get(path, params=params)
            items = data.get(results_key) or []
            out.extend(items)
            nxt = data.get(token_key)
            if not nxt:
                break
            params[token_key] = nxt
        return out

    # ------------------------------------------------------------------
    # SQL Statement Execution (used by UC exporter to query system tables)
    # ------------------------------------------------------------------

    def _execute_sql(
        self,
        warehouse_id: str,
        statement: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        timeout_s: int = 60,
    ) -> List[Dict]:
        """Run SQL via statement execution API; return list-of-dicts rows."""
        payload: Dict = {
            "statement": statement,
            "warehouse_id": warehouse_id,
            "wait_timeout": f"{min(timeout_s, 50)}s",
            "on_wait_timeout": "CONTINUE",
        }
        if catalog:
            payload["catalog"] = catalog
        if schema:
            payload["schema"] = schema
        resp = self._post("/api/2.0/sql/statements", payload)
        stmt_id = resp["statement_id"]
        status = resp.get("status", {}).get("state", "PENDING")

        deadline = time.time() + timeout_s
        while status in ("PENDING", "RUNNING") and time.time() < deadline:
            time.sleep(2)
            resp = self._get(f"/api/2.0/sql/statements/{stmt_id}")
            status = resp.get("status", {}).get("state", "PENDING")

        if status != "SUCCEEDED":
            err = resp.get("status", {}).get("error", {}).get("message", "unknown")
            raise RuntimeError(f"SQL failed ({status}): {err}")

        manifest = resp.get("manifest", {})
        result = resp.get("result", {})
        columns = [c["name"] for c in manifest.get("schema", {}).get("columns", [])]
        rows = result.get("data_array") or []
        return [dict(zip(columns, row)) for row in rows]

    # ------------------------------------------------------------------
    # File helpers
    # ------------------------------------------------------------------

    def _save_json(self, rel_path: str, data: Any) -> str:
        full = os.path.join(self.export_dir, rel_path)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, default=str)
        return full

    def _save_text(self, rel_path: str, text: str) -> str:
        full = os.path.join(self.export_dir, rel_path)
        os.makedirs(os.path.dirname(full) or ".", exist_ok=True)
        with open(full, "w", encoding="utf-8") as fh:
            fh.write(text)
        return full

    def _append_text(self, rel_path: str, text: str) -> None:
        full = os.path.join(self.export_dir, rel_path)
        os.makedirs(os.path.dirname(full) or ".", exist_ok=True)
        with open(full, "a", encoding="utf-8") as fh:
            fh.write(text)

    def _mkdir(self, rel_path: str) -> str:
        full = os.path.join(self.export_dir, rel_path)
        os.makedirs(full, exist_ok=True)
        return full
