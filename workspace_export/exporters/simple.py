"""
Simple list-and-save exporters for:
  • SQL Warehouses
  • DLT Pipelines
  • Git Repos
  • AI/BI (Lakeview) Dashboards
  • Genie AI Spaces
  • Model Serving Endpoints
"""
from __future__ import annotations

import json
import logging
import os
from typing import Dict

from .base import (
    BaseExporter,
    _strip,
    _WAREHOUSE_RUNTIME_KEYS,
    _PIPELINE_RUNTIME_KEYS,
    _ENDPOINT_RUNTIME_KEYS,
    _REPO_RUNTIME_KEYS,
    _GENIE_RUNTIME_KEYS,
)

_LOG = logging.getLogger(__name__)


class SimpleExporter(BaseExporter):
    """Handles all components that follow the list → strip → save pattern."""

    # ------------------------------------------------------------------
    # SQL Warehouses
    # ------------------------------------------------------------------

    def export_sql_warehouses(self) -> Dict:
        raw = self._get("/api/2.0/sql/warehouses").get("warehouses") or []
        importable = [_strip(w, _WAREHOUSE_RUNTIME_KEYS) for w in raw]
        self._save_json("sql_warehouses.json", {"warehouses": importable})
        _LOG.info("[sql_warehouses] exported %d warehouse(s)", len(importable))
        return {"count": len(importable), "items": importable}

    # ------------------------------------------------------------------
    # DLT Pipelines
    # ------------------------------------------------------------------

    def export_dlt_pipelines(self) -> Dict:
        raw = self._paginated_get(
            "/api/2.0/pipelines", "statuses",
            params={"max_results": 100},
            token_key="next_page_token",
        )
        importable = []
        for p in raw:
            pid = p.get("pipeline_id", "")
            try:
                detail = self._get(f"/api/2.0/pipelines/{pid}")
                spec = detail.get("spec") or {}
                importable.append(_strip(spec, _PIPELINE_RUNTIME_KEYS))
            except Exception as exc:  # noqa: BLE001
                _LOG.warning("[dlt_pipelines] Could not fetch %s: %s", pid, exc)
        self._save_json("dlt_pipelines.json", {"pipelines": importable})
        _LOG.info("[dlt_pipelines] exported %d pipeline(s)", len(importable))
        return {"count": len(importable), "items": importable}

    # ------------------------------------------------------------------
    # Git Repos
    # ------------------------------------------------------------------

    def export_repos(self) -> Dict:
        raw = self._paginated_get(
            "/api/2.0/repos", "repos",
            params={"page_size": 100},
        )
        importable = [_strip(r, _REPO_RUNTIME_KEYS) for r in raw]
        self._save_json("repos.json", {"repos": importable})
        _LOG.info("[repos] exported %d repo(s)", len(importable))
        return {"count": len(importable), "items": importable}

    # ------------------------------------------------------------------
    # AI/BI (Lakeview) Dashboards
    # ------------------------------------------------------------------

    def export_lakeview_dashboards(self) -> Dict:
        data = self._get("/api/2.0/lakeview/dashboards")
        dashboards = data.get("dashboards") or []
        out_dir = self._mkdir("lakeview_dashboards")
        imported = []
        for d in dashboards:
            did = d.get("dashboard_id", "")
            try:
                full = self._get(f"/api/2.0/lakeview/dashboards/{did}")
                safe_name = (
                    full.get("display_name", did)
                    .replace("/", "_")
                    .replace(" ", "_")[:80]
                )
                fname = f"{safe_name}_{did[:8]}.json"
                exportable = {
                    "display_name": full.get("display_name"),
                    "serialized_dashboard": full.get("serialized_dashboard"),
                    "warehouse_id": full.get("warehouse_id"),
                    "parent_path": full.get("parent_path"),
                }
                self._save_json(
                    os.path.join("lakeview_dashboards", fname), exportable
                )
                imported.append(exportable)
            except Exception as exc:  # noqa: BLE001
                _LOG.warning("[lakeview_dashboards] Skipped %s: %s", did, exc)
        self._save_json(
            "lakeview_dashboards_index.json",
            {"count": len(imported), "dashboards": [d.get("display_name") for d in imported]},
        )
        _LOG.info("[lakeview_dashboards] exported %d dashboard(s)", len(imported))
        return {"count": len(imported), "items": imported}

    # ------------------------------------------------------------------
    # Genie AI Spaces
    # ------------------------------------------------------------------

    def export_genie_spaces(self) -> Dict:
        """
        Export Genie AI Spaces metadata.

        NOTE: The Databricks Genie API does not expose the ``serialized_space``
        field required by the CREATE endpoint.  To maximise fidelity we also
        fetch the backing AI/BI Lakeview dashboard (via
        ``/api/2.0/lakeview/dashboards?genie_space_id=<id>``) and embed its
        ``serialized_dashboard`` content.  This allows a best-effort re-import
        once the API supports the field.

        All 5 fields exported per space:
          title, description, warehouse_id,
          backing_dashboard_id, backing_serialized_dashboard
        """
        data = self._get("/api/2.0/genie/spaces")
        spaces = data.get("spaces") or []
        exportable = []

        for s in spaces:
            sid   = s.get("space_id", "")
            entry = {k: v for k, v in s.items() if k not in _GENIE_RUNTIME_KEYS}

            # Fetch backing Lakeview dashboard for this Genie space
            try:
                dash_data = self._get(
                    "/api/2.0/lakeview/dashboards",
                    params={"genie_space_id": sid},
                )
                dashboards = dash_data.get("dashboards", [])
                if dashboards:
                    did = dashboards[0].get("dashboard_id", "")
                    full = self._get(f"/api/2.0/lakeview/dashboards/{did}")
                    entry["backing_dashboard_id"]             = did
                    entry["backing_serialized_dashboard"]     = full.get("serialized_dashboard", "")
                    entry["backing_dashboard_display_name"]   = full.get("display_name", "")
            except Exception as exc:
                _LOG.warning("[genie_spaces] Could not fetch backing dashboard for %s: %s", sid, exc)

            exportable.append(entry)

        self._save_json("genie_spaces.json", {
            "spaces": exportable,
            "_note": (
                "Genie Spaces cannot be automatically imported via the public API. "
                "The 'serialized_space' field required by POST /api/2.0/genie/spaces "
                "is an internal protobuf not exposed by GET endpoints. "
                "backing_serialized_dashboard contains the space content for manual recreation."
            ),
        })
        _LOG.info("[genie_spaces] exported %d space(s) (backing dashboard content included)", len(exportable))
        return {"count": len(exportable), "items": exportable}

    # ------------------------------------------------------------------
    # Model Serving Endpoints
    # ------------------------------------------------------------------

    def export_serving_endpoints(self) -> Dict:
        data = self._get("/api/2.0/serving-endpoints")
        endpoints = data.get("endpoints") or []
        # Skip platform-managed (databricks-*) endpoints; they're not user-owned
        user_endpoints = [
            e for e in endpoints if not e.get("name", "").startswith("databricks-")
        ]
        importable = [_strip(e, _ENDPOINT_RUNTIME_KEYS) for e in user_endpoints]
        self._save_json("serving_endpoints.json", {"endpoints": importable})
        skipped = len(endpoints) - len(user_endpoints)
        _LOG.info(
            "[serving_endpoints] exported %d endpoint(s) (skipped %d platform-managed)",
            len(importable), skipped,
        )
        return {"count": len(importable), "items": importable, "platform_skipped": skipped}
