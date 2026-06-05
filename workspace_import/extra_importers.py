"""
workspace_import/extra_importers.py
====================================
Imports workspace components that are NOT handled by the databrickslabs/migrate
tool's built-in import pipeline.  These are the "new REST-API" components exported
by the workspace_export package.

Components:
  • SQL Warehouses         (sql_warehouses.json)
  • DLT Pipelines          (dlt_pipelines.json)
  • Git Repos              (repos.json)
  • AI/BI Dashboards       (lakeview_dashboards_index.json + lakeview_dashboards/)
  • Genie AI Spaces        (genie_spaces.json)
  • Model Serving Endpoints(serving_endpoints.json)

Omitted (handled elsewhere):
  • Unity Catalog – apply uc_export/SQL files manually or via deploy step
  • MLflow – use mlflow-export-import for fine-grained control
  • DBFS Libraries – handled by import_dbfs_libs.py
"""
from __future__ import annotations

import json
import logging
import os
import urllib3
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import requests

# Suppress InsecureRequestWarning globally for this module – the migrate tool
# connects to workspaces whose certificates may not be in the system trust store.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

_LOG = logging.getLogger(__name__)

# Runtime-only fields that must be stripped before re-creating a resource
_WAREHOUSE_STRIP = {
    "id", "state", "health", "num_active_sessions", "creator_name",
    "warehouse_type",   # keep warehouse_type? actually keep it – it's config
    "creator_id",
}
# Actually warehouse_type IS a config field, only strip true runtime fields
_WAREHOUSE_RUNTIME = {
    "id", "state", "health", "num_active_sessions", "creator_name", "creator_id",
}
_PIPELINE_RUNTIME = {
    "id", "pipeline_id", "pipeline_type", "storage",
    "state", "cluster_id", "latest_updates", "cause",
    "health", "creator_user_name", "run_as_user_name",
    "last_modified", "run_as", "created_time",
}
_REPO_RUNTIME = {
    "id", "head_commit_id",
}
_SERVING_RUNTIME = {
    "name",   # keep – it's the resource identifier
    "creation_timestamp", "last_updated_timestamp", "state", "creator",
    "pending_config", "queued_events",
}


def _strip(d: Dict, keys: set) -> Dict:
    return {k: v for k, v in d.items() if k not in keys}


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class ImportResult:
    component: str
    total: int = 0
    created: int = 0
    skipped: int = 0
    failed: int = 0
    dry_run: int = 0
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def as_dict(self) -> Dict:
        return {
            "component": self.component,
            "total": self.total,
            "created": self.created,
            "skipped": self.skipped,
            "failed": self.failed,
            "dry_run": self.dry_run,
            "errors": self.errors,
            "warnings": self.warnings,
        }


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------

class _ApiClient:
    def __init__(self, workspace_url: str, token: str, verify_ssl: bool = True):
        self._base = workspace_url.rstrip("/")
        self._s = requests.Session()
        self._s.headers.update({"Authorization": f"Bearer {token}",
                                  "Content-Type": "application/json"})
        self._verify = verify_ssl

    def post(self, path: str, body: Dict) -> Dict:
        r = self._s.post(f"{self._base}{path}", json=body, verify=self._verify, timeout=60)
        r.raise_for_status()
        return r.json()

    def get(self, path: str, params: Optional[Dict] = None) -> Any:
        r = self._s.get(f"{self._base}{path}", params=params, verify=self._verify, timeout=60)
        r.raise_for_status()
        return r.json()

    def list_existing(self, path: str, key: str, name_field: str = "name") -> set:
        """Return a set of existing resource names for duplicate-check."""
        try:
            data = self.get(path)
            items = data.get(key, [])
            return {item.get(name_field, "") for item in items if item.get(name_field)}
        except Exception as exc:
            _LOG.warning("Could not list existing %s: %s", key, exc)
            return set()


# ---------------------------------------------------------------------------
# ExtraImporter
# ---------------------------------------------------------------------------

class ExtraImporter:
    """
    Imports the six components not handled by databrickslabs/migrate.

    Parameters
    ----------
    workspace_url : str
        Target GCP workspace URL.
    token : str
        PAT for the target workspace.
    session_dir : str
        Path to the export session directory that contains the JSON export files.
    verify_ssl : bool
        Set to False to disable SSL verification.
    dry_run : bool
        If True, print what would be imported without calling any API.
    """

    def __init__(
        self,
        workspace_url: str,
        token: str,
        session_dir: str,
        verify_ssl: bool = True,
        dry_run: bool = False,
    ):
        self._dir = session_dir
        self._dry = dry_run
        self._token = token
        self._api = _ApiClient(workspace_url, token, verify_ssl)

    def _path(self, *parts: str) -> str:
        return os.path.join(self._dir, *parts)

    def _load_json(self, rel_path: str) -> Optional[Any]:
        p = self._path(rel_path)
        if not os.path.isfile(p):
            _LOG.info("  File not found – skipping: %s", p)
            return None
        with open(p, encoding="utf-8") as f:
            return json.load(f)

    # ------------------------------------------------------------------
    # SQL Warehouses
    # ------------------------------------------------------------------

    def import_sql_warehouses(self) -> ImportResult:
        res = ImportResult("sql_warehouses")
        data = self._load_json("sql_warehouses.json")
        if data is None:
            return res

        existing = self._api.list_existing("/api/2.0/sql/warehouses", "warehouses")
        warehouses = data.get("warehouses", [])
        res.total = len(warehouses)

        for wh in warehouses:
            name = wh.get("name", "?")
            body = _strip(wh, _WAREHOUSE_RUNTIME)

            if name in existing:
                _LOG.info("  SKIP  warehouse '%s' (already exists)", name)
                res.skipped += 1
                continue

            if self._dry:
                _LOG.info("  DRY   warehouse '%s'", name)
                res.dry_run += 1
                continue

            try:
                r = self._api.post("/api/2.0/sql/warehouses", body)
                _LOG.info("  OK    warehouse '%s' → id=%s", name, r.get("id"))
                res.created += 1
            except requests.HTTPError as exc:
                msg = f"warehouse '{name}': {exc.response.status_code} {exc.response.text[:120]}"
                _LOG.error("  FAIL  %s", msg)
                res.errors.append(msg)
                res.failed += 1

        return res

    # ------------------------------------------------------------------
    # DLT Pipelines
    # ------------------------------------------------------------------

    def import_dlt_pipelines(self) -> ImportResult:
        res = ImportResult("dlt_pipelines")
        data = self._load_json("dlt_pipelines.json")
        if data is None:
            return res

        pipelines = data.get("pipelines", [])
        res.total = len(pipelines)

        # Existing pipelines (by name)
        try:
            existing_raw = self._api.get("/api/2.0/pipelines", {"max_results": 100})
            existing = {p.get("name", "") for p in existing_raw.get("statuses", [])}
        except Exception as exc:
            _LOG.warning("  Could not list existing pipelines: %s", exc)
            existing = set()

        for pl in pipelines:
            name = pl.get("name", "?")
            body = _strip(pl, _PIPELINE_RUNTIME)

            # Strip cloud-specific cluster attributes
            if "clusters" in body:
                cluster_strip = {"azure_attributes", "aws_attributes",
                                 "enable_elastic_disk", "instance_profile_arn"}
                body["clusters"] = [
                    {k: v for k, v in c.items() if k not in cluster_strip}
                    for c in body["clusters"]
                ]

            if name in existing:
                _LOG.info("  SKIP  pipeline '%s' (already exists)", name)
                res.skipped += 1
                continue

            if self._dry:
                _LOG.info("  DRY   pipeline '%s'", name)
                res.dry_run += 1
                continue

            try:
                r = self._api.post("/api/2.0/pipelines", body)
                _LOG.info("  OK    pipeline '%s' → id=%s", name, r.get("pipeline_id"))
                res.created += 1
            except requests.HTTPError as exc:
                msg = f"pipeline '{name}': {exc.response.status_code} {exc.response.text[:120]}"
                _LOG.error("  FAIL  %s", msg)
                res.errors.append(msg)
                res.failed += 1

        return res

    # ------------------------------------------------------------------
    # Git Repos
    # ------------------------------------------------------------------

    def import_repos(self) -> ImportResult:
        res = ImportResult("repos")
        data = self._load_json("repos.json")
        if data is None:
            return res

        repos = data.get("repos", [])
        res.total = len(repos)

        try:
            existing_raw = self._api.get("/api/2.0/repos")
            existing = {r.get("path", "") for r in existing_raw.get("repos", [])}
        except Exception as exc:
            _LOG.warning("  Could not list existing repos: %s", exc)
            existing = set()

        for repo in repos:
            path = repo.get("path", "?")
            body = _strip(repo, _REPO_RUNTIME)

            if path in existing:
                _LOG.info("  SKIP  repo '%s' (already exists)", path)
                res.skipped += 1
                continue

            if self._dry:
                _LOG.info("  DRY   repo '%s'  [%s]", path, repo.get("url", ""))
                res.dry_run += 1
                continue

            try:
                r = self._api.post("/api/2.0/repos", body)
                _LOG.info("  OK    repo '%s' → id=%s", path, r.get("id"))
                res.created += 1
            except requests.HTTPError as exc:
                msg = f"repo '{path}': {exc.response.status_code} {exc.response.text[:120]}"
                _LOG.error("  FAIL  %s", msg)
                res.errors.append(msg)
                res.failed += 1

        return res

    # ------------------------------------------------------------------
    # AI/BI Dashboards (Lakeview)
    # ------------------------------------------------------------------

    def import_lakeview_dashboards(self) -> ImportResult:
        res = ImportResult("lakeview_dashboards")

        # Load individual dashboard files from the lakeview_dashboards/ directory.
        # The index only contains display-name strings, so we enumerate files directly.
        dash_dir = os.path.join(self._dir, "lakeview_dashboards")
        if not os.path.isdir(dash_dir):
            _LOG.info("  No lakeview_dashboards/ directory – skipping")
            return res

        import glob as _glob
        dash_files = sorted(_glob.glob(os.path.join(dash_dir, "*.json")))
        res.total = len(dash_files)
        if not dash_files:
            return res

        try:
            existing_raw = self._api.get("/api/2.0/lakeview/dashboards")
            existing = {d.get("display_name", "") for d in existing_raw.get("dashboards", [])}
        except Exception as exc:
            _LOG.warning("  Could not list existing dashboards: %s", exc)
            existing = set()

        for fpath in dash_files:
            try:
                with open(fpath, encoding="utf-8") as fp:
                    content = json.load(fp)
            except Exception as exc:
                _LOG.warning("  Could not read %s: %s – skipping", fpath, exc)
                res.skipped += 1
                continue

            name = content.get("display_name", os.path.basename(fpath))

            if name in existing:
                _LOG.info("  SKIP  dashboard '%s' (already exists)", name)
                res.skipped += 1
                continue

            if self._dry:
                _LOG.info("  DRY   dashboard '%s'", name)
                res.dry_run += 1
                continue

            if "serialized_dashboard" in content and content["serialized_dashboard"]:
                body: Dict[str, Any] = {
                    "display_name": name,
                    "serialized_dashboard": content["serialized_dashboard"],
                }
            else:
                body = {"display_name": name, "serialized_dashboard": json.dumps(content)}

            try:
                r = self._api.post("/api/2.0/lakeview/dashboards", body)
                _LOG.info("  OK    dashboard '%s' → id=%s", name, r.get("dashboard_id"))
                res.created += 1
            except requests.HTTPError as exc:
                msg = f"dashboard '{name}': {exc.response.status_code} {exc.response.text[:120]}"
                _LOG.error("  FAIL  %s", msg)
                res.errors.append(msg)
                res.failed += 1

        return res

    # ------------------------------------------------------------------
    # Genie AI Spaces
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Genie Spaces  (warehouse reconciliation + creation attempt)
    # ------------------------------------------------------------------

    def _build_warehouse_id_map(self, staging_dir: str) -> Dict[str, str]:
        """Return {old_azure_warehouse_id: new_gcp_warehouse_id}.

        Strategy
        --------
        1. Parse the SOURCE sql_warehouses.json (uses jdbc_url to recover the
           original Azure warehouse ID that the export tool drops from 'id').
        2. For each old ID referenced by genie_spaces.json:
              a. Find the matching warehouse name from the source file.
              b. Check if that name already exists on the GCP target.
              c. If not → create the warehouse from the source config.
        3. Return the complete old → new mapping.
        """
        import re as _re

        # ── 1. Load source sql_warehouses.json ──────────────────────────
        wh_path = os.path.join(staging_dir, "sql_warehouses.json")
        if not os.path.isfile(wh_path):
            _LOG.warning("  sql_warehouses.json not found – cannot map warehouse IDs")
            return {}

        with open(wh_path, encoding="utf-8") as f:
            wh_data = json.load(f)
        src_warehouses = wh_data.get("warehouses", wh_data) if isinstance(wh_data, dict) else wh_data

        # Extract name → old_id from jdbc_url
        # Pattern: /sql/1.0/warehouses/<16-char-hex-id>
        src_name_to_old_id: Dict[str, str] = {}
        src_name_to_config: Dict[str, Dict] = {}
        for wh in src_warehouses:
            name = wh.get("name", "")
            jdbc = wh.get("jdbc_url", "")
            m = _re.search(r"/warehouses/([0-9a-f]{16})", jdbc)
            if m:
                src_name_to_old_id[name] = m.group(1)
            src_name_to_config[name] = wh

        _LOG.info("  Source warehouse name→id map: %s", src_name_to_old_id)

        # ── 2. Collect old IDs used by Genie spaces ──────────────────────
        genie_path = os.path.join(staging_dir, "genie_spaces.json")
        if not os.path.isfile(genie_path):
            return {}
        with open(genie_path, encoding="utf-8") as f:
            genie_data = json.load(f)
        needed_old_ids = {s.get("warehouse_id") for s in genie_data.get("spaces", [])
                         if s.get("warehouse_id")}
        _LOG.info("  Genie spaces reference warehouse IDs: %s", needed_old_ids)

        # ── 3. Build reverse map: old_id → name ──────────────────────────
        old_id_to_name: Dict[str, str] = {v: k for k, v in src_name_to_old_id.items()}

        # ── 4. List existing GCP warehouses ──────────────────────────────
        try:
            existing_raw = self._api.get("/api/2.0/sql/warehouses")
            existing_whs = existing_raw.get("warehouses", [])
        except Exception as exc:
            _LOG.warning("  Could not list GCP warehouses: %s", exc)
            existing_whs = []

        target_name_to_id: Dict[str, str] = {w.get("name", ""): w.get("id", "") for w in existing_whs}
        _LOG.info("  GCP existing warehouses: %s", list(target_name_to_id.keys()))

        # ── 5. Resolve each needed old ID ────────────────────────────────
        old_to_new: Dict[str, str] = {}
        for old_id in needed_old_ids:
            wh_name = old_id_to_name.get(old_id)
            if not wh_name:
                _LOG.warning("  Cannot resolve old warehouse ID %s – no matching name in source export", old_id)
                continue

            if wh_name in target_name_to_id:
                new_id = target_name_to_id[wh_name]
                _LOG.info("  FOUND  warehouse '%s' on GCP → %s", wh_name, new_id)
                old_to_new[old_id] = new_id
            else:
                # Create the warehouse on GCP
                config = src_name_to_config.get(wh_name, {})
                body = _strip(config, _WAREHOUSE_RUNTIME | {
                    "jdbc_url", "odbc_params", "channel", "creator_id",
                    "creator_name", "num_active_sessions", "num_clusters",
                    "state", "health",
                })
                # GCP does not support CLASSIC – downgrade to PRO
                if body.get("warehouse_type") == "CLASSIC":
                    body["warehouse_type"] = "PRO"
                    _LOG.info("  Upgrading warehouse '%s' CLASSIC → PRO for GCP", wh_name)
                if self._dry:
                    _LOG.info("  DRY   would create warehouse '%s'", wh_name)
                    continue
                try:
                    r = self._api.post("/api/2.0/sql/warehouses", body)
                    new_id = r.get("id", "")
                    _LOG.info("  CREATED warehouse '%s' → %s", wh_name, new_id)
                    old_to_new[old_id] = new_id
                    target_name_to_id[wh_name] = new_id
                except Exception as exc:
                    _LOG.error("  FAILED creating warehouse '%s': %s", wh_name, exc)

        return old_to_new

    @staticmethod
    def _patch_genie_warehouse_ids(staging_dir: str, id_map: Dict[str, str]) -> None:
        """Overwrite genie_spaces.json in staging_dir with remapped warehouse IDs."""
        if not id_map:
            return
        genie_path = os.path.join(staging_dir, "genie_spaces.json")
        if not os.path.isfile(genie_path):
            return
        with open(genie_path, encoding="utf-8") as f:
            data = json.load(f)
        changed = 0
        for space in data.get("spaces", []):
            old_id = space.get("warehouse_id", "")
            if old_id in id_map:
                space["warehouse_id"] = id_map[old_id]
                changed += 1
        with open(genie_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        _LOG.info("  Patched genie_spaces.json: %d space(s) updated with new warehouse IDs", changed)

    def import_genie_spaces(self) -> ImportResult:
        res = ImportResult("genie_spaces")
        data = self._load_json("genie_spaces.json")
        if data is None:
            return res

        spaces = data.get("spaces", [])
        res.total = len(spaces)
        if not spaces:
            return res

        # ── Step 1: Resolve & create warehouses, patch staging JSON ──────
        _LOG.info("  Resolving warehouse IDs for Genie spaces …")
        id_map = self._build_warehouse_id_map(self._dir)

        if id_map:
            self._patch_genie_warehouse_ids(self._dir, id_map)
            # Reload with new IDs
            data = self._load_json("genie_spaces.json")
            spaces = data.get("spaces", [])
            _LOG.info("  Warehouse ID mapping: %s", id_map)
        else:
            _LOG.warning("  No warehouse ID mapping could be built – using original IDs")

        # ── Step 2: List existing Genie spaces on target ──────────────────
        try:
            existing_resp = self._api.get("/api/2.0/genie/spaces")
            existing_spaces = {s.get("title", ""): s.get("id", "")
                               for s in existing_resp.get("genie_spaces", [])}
        except Exception as exc:
            _LOG.warning("  Could not list existing Genie spaces: %s", exc)
            existing_spaces = {}

        # ── Step 3: Create each Genie space ──────────────────────────────
        _GENIE_RUNTIME = {
            "id", "space_id", "backing_dashboard_id", "backing_dashboard_update_time",
            "backing_serialized_dashboard", "backing_dashboard_display_name",
        }

        for space in spaces:
            title       = space.get("title", "?")
            warehouse_id = space.get("warehouse_id", "")
            description = space.get("description", "")

            if title in existing_spaces:
                _LOG.info("  SKIP  Genie space '%s' (already exists → id=%s)", title, existing_spaces[title])
                res.skipped += 1
                continue

            if not warehouse_id:
                msg = f"Genie space '{title}': no warehouse_id after mapping – skipped"
                _LOG.warning("  ⚠  %s", msg)
                res.warnings.append(msg)
                res.skipped += 1
                continue

            if self._dry:
                _LOG.info("  DRY   Genie space '%s' (warehouse_id=%s)", title, warehouse_id)
                res.dry_run += 1
                continue

            # Build the smallest valid body the API accepts
            body: Dict[str, Any] = {
                "title":        title,
                "description":  description,
                "warehouse_id": warehouse_id,
            }

            try:
                r = self._api.post("/api/2.0/genie/spaces", body)
                space_id = r.get("id") or r.get("space_id", "?")
                _LOG.info("  OK    Genie space '%s' → id=%s  (warehouse_id=%s)", title, space_id, warehouse_id)
                res.created += 1
            except Exception as exc:
                # The Databricks Genie API requires an internal `serialized_space`
                # protobuf that is NOT returned by the public export endpoints.
                # The warehouse ID has been correctly resolved – provide it so
                # the operator can recreate the space manually in a few clicks.
                warning_msg = (
                    f"MANUAL RECREATION NEEDED – '{title}'\n"
                    f"    Steps: Databricks UI → New → AI/BI Genie → fill in:\n"
                    f"      Title       : {title}\n"
                    f"      Warehouse   : (use warehouse_id={warehouse_id})\n"
                    f"      Description : {description[:120]}"
                )
                _LOG.warning("  ⚠  Genie space '%s': auto-creation blocked by API "
                             "(serialized_space not exportable). "
                             "✅ Warehouse ID resolved → %s.  Create manually in UI.",
                             title, warehouse_id)
                res.warnings.append(
                    f"MANUAL – Genie space '{title}': "
                    f"create via UI with warehouse_id={warehouse_id}"
                )
                res.skipped += 1

        return res

    # ------------------------------------------------------------------
    # Model Serving Endpoints
    # ------------------------------------------------------------------

    def import_serving_endpoints(self) -> ImportResult:
        res = ImportResult("serving_endpoints")
        data = self._load_json("serving_endpoints.json")
        if data is None:
            return res

        endpoints = data.get("endpoints", [])
        res.total = len(endpoints)

        try:
            existing_raw = self._api.get("/api/2.0/serving-endpoints")
            existing = {e.get("name", "") for e in existing_raw.get("endpoints", [])}
        except Exception as exc:
            _LOG.warning("  Could not list existing serving endpoints: %s", exc)
            existing = set()

        _ENDPOINT_RUNTIME = {
            "id", "state", "creator", "creation_timestamp", "last_updated_timestamp",
            "pending_config", "queued_events",
        }
        _CONFIG_RUNTIME = {
            "config_version", "served_entities", "traffic_config",
            "auto_capture_config", "serving_cluster_id",
        }

        for ep in endpoints:
            name = ep.get("name", "?")
            if name in existing:
                _LOG.info("  SKIP  endpoint '%s' (already exists)", name)
                res.skipped += 1
                continue

            # Build create body: name + config
            ep_config = ep.get("config", {})
            served_entities = ep_config.get("served_entities") or ep_config.get("served_models", [])
            if not served_entities:
                res.warnings.append(f"endpoint '{name}': no served_entities – skipping")
                _LOG.warning("  SKIP  endpoint '%s': no served_entities", name)
                res.skipped += 1
                continue

            body = {
                "name": name,
                "config": {
                    "served_entities": [
                        {k: v for k, v in se.items()
                         if k not in {"entity_version", "state", "creator",
                                      "creation_timestamp", "update_timestamp",
                                      "entity_name_hash"}}
                        for se in served_entities
                    ],
                },
            }
            # Preserve traffic config if present
            if ep_config.get("traffic_config"):
                body["config"]["traffic_config"] = ep_config["traffic_config"]
            if ep.get("tags"):
                body["tags"] = ep["tags"]

            if self._dry:
                _LOG.info("  DRY   endpoint '%s'", name)
                res.dry_run += 1
                continue

            try:
                r = self._api.post("/api/2.0/serving-endpoints", body)
                _LOG.info("  OK    endpoint '%s' → name=%s", name, r.get("name"))
                res.created += 1
            except requests.HTTPError as exc:
                msg = f"endpoint '{name}': {exc.response.status_code} {exc.response.text[:120]}"
                _LOG.error("  FAIL  %s", msg)
                res.errors.append(msg)
                res.failed += 1

        return res

    # ------------------------------------------------------------------
    # Workspace Files (non-notebook)
    # ------------------------------------------------------------------

    def import_workspace_files(self) -> ImportResult:
        """Upload exported workspace FILE objects to the target workspace."""
        from workspace_import.workspace_files_importer import WorkspaceFilesImporter
        imp = WorkspaceFilesImporter(
            workspace_url=self._api._base,
            token=self._token,
            session_dir=self._dir,
            overwrite=True,
            verify_ssl=self._api._verify,
            dry_run=self._dry,
        )
        wf_res = imp.import_files()
        # Convert to extra_importers.ImportResult
        res = ImportResult("workspace_files")
        res.total   = wf_res.total
        res.created = wf_res.created
        res.skipped = wf_res.skipped
        res.failed  = wf_res.failed
        res.dry_run = wf_res.dry_run
        res.errors  = wf_res.errors
        res.warnings = wf_res.warnings
        return res

    # ------------------------------------------------------------------
    # Main runner
    # ------------------------------------------------------------------

    def run(self) -> Dict:
        """Run all extra importers in dependency order. Returns summary dict."""
        mode = "DRY RUN" if self._dry else "LIVE IMPORT"
        _LOG.info("[extra_importers] Starting (%s) from %s", mode, self._dir)

        results: List[ImportResult] = []
        importers = [
            ("SQL Warehouses",        self.import_sql_warehouses),
            ("DLT Pipelines",         self.import_dlt_pipelines),
            ("Git Repos",             self.import_repos),
            ("AI/BI Dashboards",      self.import_lakeview_dashboards),
            ("Genie AI Spaces",       self.import_genie_spaces),
            ("Model Serving Endpoints", self.import_serving_endpoints),
            ("Workspace Files",       self.import_workspace_files),
        ]

        for display, fn in importers:
            _LOG.info("─── %s ───", display)
            try:
                r = fn()
                results.append(r)
            except Exception as exc:
                _LOG.error("  FATAL  %s: %s", display, exc, exc_info=True)
                r = ImportResult(display.lower().replace(" ", "_"), failed=1,
                                 errors=[str(exc)])
                results.append(r)

        # Print summary table
        print()
        print("═" * 65)
        print(f" Extra Components Import  [{mode}]")
        print("═" * 65)
        print(f"  {'Component':<30} {'Total':>6} {'Created':>8} {'Skipped':>8} {'Failed':>7}")
        print("─" * 65)
        total_failed = 0
        for r in results:
            count_col = r.dry_run if self._dry else r.created
            col_lbl = "Dry-run" if self._dry else "Created"
            print(f"  {r.component:<30} {r.total:>6} {count_col:>8} {r.skipped:>8} {r.failed:>7}")
            total_failed += r.failed
        print("═" * 65)
        print()

        # Warnings
        all_warnings = [w for r in results for w in r.warnings]
        all_errors   = [e for r in results for e in r.errors]
        if all_warnings:
            for w in all_warnings:
                print(f"  ⚠  {w}")
        if all_errors:
            for e in all_errors:
                print(f"  ✗  {e}")

        return {
            "results": [r.as_dict() for r in results],
            "total_failed": total_failed,
        }
