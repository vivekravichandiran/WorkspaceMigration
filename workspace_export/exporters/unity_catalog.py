"""
Unity Catalog exporter.

Exports ALL UC objects (catalogs, schemas, tables, views, functions, volumes,
external locations, storage credentials) as importable SQL DDL files, plus
GRANT statements for ACLs — in correct deployment order.

Skips:
  • DELTASHARING / FOREIGN / SYSTEM / INTERNAL / MANAGED_ONLINE catalogs
  • system.* and samples.* schemas (read-only platform catalogs)
  • MATERIALIZED_VIEW and STREAMING_TABLE table types (identified via REST API
    table_type field, which mirrors system.information_schema.tables)

Deployment order written inside uc_export/:
  01_storage_credentials.sql
  02_external_locations.sql
  03_catalogs.sql
  04_schemas.sql
  05_volumes.sql
  tables/   <catalog>.<schema>.<table>.sql   (external first, then managed)
  views/    <catalog>.<schema>.<view>.sql    (topologically sorted by deps)
  06_functions.sql
  07_grants.sql                              (catalog → schema → table/view/vol/fn)
  import_manifest.json                      (ordered deployment plan)
  skipped_objects.json                      (MVs, streaming tables, errors)
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from .base import BaseExporter

_LOG = logging.getLogger(__name__)

# Catalog types we export (source-of-truth is what can be re-created in a new ws)
_EXPORTABLE_CATALOG_TYPES = {"MANAGED_CATALOG"}

# Table types we skip
_SKIP_TABLE_TYPES = {"MATERIALIZED_VIEW", "STREAMING_TABLE"}

# Securable type → GRANT keyword mapping
_GRANT_KEYWORD: Dict[str, str] = {
    "catalog": "CATALOG",
    "schema": "SCHEMA",
    "table": "TABLE",
    "function": "FUNCTION",
    "volume": "VOLUME",
    "external_location": "EXTERNAL LOCATION",
    "storage_credential": "STORAGE CREDENTIAL",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _esc(name: str) -> str:
    """Backtick-quote a single identifier part."""
    return f"`{name}`"


def _fqn(*parts: str) -> str:
    """Build a fully-qualified name like `cat`.`schema`.`table`."""
    return ".".join(_esc(p) for p in parts)


def _safe_filename(*parts: str) -> str:
    """Convert UC name parts to a safe filesystem name."""
    return ".".join(p.replace("/", "_").replace(" ", "_") for p in parts)


def _topo_sort_views(
    views: List[Dict], all_view_fqns: Set[str]
) -> List[Dict]:
    """
    Topologically sort views so that a view is always emitted after the views
    it depends on.  Cycles (invalid SQL anyway) are handled by appending
    remaining nodes at the end.
    """
    fqn_to_view = {v["fqn"]: v for v in views}
    in_degree: Dict[str, int] = {v["fqn"]: 0 for v in views}
    children: Dict[str, List[str]] = {v["fqn"]: [] for v in views}

    for v in views:
        for dep in _view_deps(v.get("view_definition", ""), all_view_fqns):
            if dep in fqn_to_view and dep != v["fqn"]:
                children[dep].append(v["fqn"])
                in_degree[v["fqn"]] += 1

    queue: deque = deque(fqn for fqn, deg in in_degree.items() if deg == 0)
    sorted_fqns: List[str] = []
    while queue:
        cur = queue.popleft()
        sorted_fqns.append(cur)
        for child in children[cur]:
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)

    # Append any remaining (cyclic deps) at the end
    remaining = [v["fqn"] for v in views if v["fqn"] not in sorted_fqns]
    if remaining:
        _LOG.warning("UC: %d view(s) have circular dependencies – appended as-is", len(remaining))
    sorted_fqns.extend(remaining)
    return [fqn_to_view[f] for f in sorted_fqns if f in fqn_to_view]


def _view_deps(view_def: str, all_view_fqns: Set[str]) -> Set[str]:
    """
    Extract other-view names referenced in a view's SQL definition.
    Uses a simple 3-part identifier regex (backticks optional).
    """
    deps: Set[str] = set()
    pattern = re.compile(
        r"`?(?P<c>[\w$]+)`?\.`?(?P<s>[\w$]+)`?\.`?(?P<t>[\w$]+)`?"
    )
    for m in pattern.finditer(view_def or ""):
        candidate = f"{m.group('c')}.{m.group('s')}.{m.group('t')}".lower()
        if candidate in all_view_fqns:
            deps.add(candidate)
    return deps


# ---------------------------------------------------------------------------
# DDL builders
# ---------------------------------------------------------------------------


def _catalog_ddl(info: Dict) -> str:
    lines = [f"CREATE CATALOG IF NOT EXISTS {_esc(info['name'])}"]
    if info.get("comment"):
        lines.append(f"  COMMENT '{_sql_escape(info['comment'])}'")
    return "\n".join(lines) + ";\n"


def _schema_ddl(info: Dict) -> str:
    fqname = _fqn(info["catalog_name"], info["name"])
    lines = [f"CREATE SCHEMA IF NOT EXISTS {fqname}"]
    if info.get("comment"):
        lines.append(f"  COMMENT '{_sql_escape(info['comment'])}'")
    return "\n".join(lines) + ";\n"


def _table_ddl(info: Dict) -> str:
    """Generate CREATE TABLE DDL from a UC table metadata dict."""
    cat, sch, tbl = info["catalog_name"], info["schema_name"], info["name"]
    fqname = _fqn(cat, sch, tbl)

    col_lines: List[str] = []
    for col in info.get("columns") or []:
        cname = _esc(col.get("name", ""))
        ctype = col.get("type_text", "STRING")
        nullable = "" if col.get("nullable", True) else " NOT NULL"
        cmt = (
            f" COMMENT '{_sql_escape(col['comment'])}'"
            if col.get("comment")
            else ""
        )
        col_lines.append(f"  {cname} {ctype}{nullable}{cmt}")

    ddl = f"CREATE TABLE IF NOT EXISTS {fqname}"
    if col_lines:
        ddl += " (\n" + ",\n".join(col_lines) + "\n)"

    fmt = info.get("data_source_format")
    if fmt:
        ddl += f"\nUSING {fmt}"

    if info.get("table_type") == "EXTERNAL" and info.get("storage_location"):
        ddl += f"\nLOCATION '{info['storage_location']}'"

    if info.get("comment"):
        ddl += f"\nCOMMENT '{_sql_escape(info['comment'])}'"

    props = info.get("properties") or {}
    # Strip internal/stats properties that don't belong in target DDL
    _NOISE_PREFIXES = (
        "spark.sql.statistics.", "delta.rowTracking.", "delta.dropFeature",
        "delta.checkpointPolicy", "delta.lastCommit", "delta.lastUpdate",
        "clusterByAuto", "clusteringColumns",
    )
    clean_props = {
        k: v for k, v in props.items()
        if not any(k.startswith(p) for p in _NOISE_PREFIXES)
    }
    if clean_props:
        prop_kv = ", ".join(f"'{k}'='{v}'" for k, v in clean_props.items())
        ddl += f"\nTBLPROPERTIES ({prop_kv})"

    return ddl + ";\n"


def _view_ddl(info: Dict) -> str:
    cat, sch, vw = info["catalog_name"], info["schema_name"], info["name"]
    fqname = _fqn(cat, sch, vw)
    view_def = info.get("view_definition", "SELECT 1 /* view definition unavailable */")
    ddl = f"CREATE OR REPLACE VIEW {fqname}"
    if info.get("comment"):
        ddl += f"\n  COMMENT '{_sql_escape(info['comment'])}'"
    ddl += f"\nAS\n{view_def};\n"
    return ddl


def _volume_ddl(info: Dict) -> str:
    cat, sch, vol = info["catalog_name"], info["schema_name"], info["name"]
    fqname = _fqn(cat, sch, vol)
    vtype = info.get("volume_type", "MANAGED")
    if vtype == "EXTERNAL" and info.get("storage_location"):
        ddl = f"CREATE EXTERNAL VOLUME IF NOT EXISTS {fqname}\n  LOCATION '{info['storage_location']}'"
    else:
        ddl = f"CREATE VOLUME IF NOT EXISTS {fqname}"
    if info.get("comment"):
        ddl += f"\n  COMMENT '{_sql_escape(info['comment'])}'"
    return ddl + ";\n"


def _function_ddl(info: Dict) -> str:
    """Best-effort function DDL from UC metadata."""
    cat, sch, fn = info["catalog_name"], info["schema_name"], info["name"]
    fqname = _fqn(cat, sch, fn)
    params = ", ".join(
        f"{_esc(p['name'])} {p.get('type_text', 'STRING')}"
        for p in (info.get("input_params", {}).get("parameters") or [])
    )
    ret = info.get("data_type", "STRING")
    body = info.get("routine_definition", "")
    lang = info.get("routine_body", "SQL")
    ddl = (
        f"CREATE OR REPLACE FUNCTION {fqname}({params})\n"
        f"RETURNS {ret}\n"
        f"LANGUAGE {lang}\n"
        f"AS $$\n{body}\n$$;\n"
    )
    return ddl


def _external_location_ddl(info: Dict) -> str:
    name = _esc(info["name"])
    url = info.get("url", "")
    cred = _esc(info.get("credential_name", ""))
    ddl = f"CREATE EXTERNAL LOCATION IF NOT EXISTS {name}\n  URL '{url}'\n  WITH (STORAGE CREDENTIAL {cred})"
    if info.get("comment"):
        ddl += f"\n  COMMENT '{_sql_escape(info['comment'])}'"
    return ddl + ";\n"


def _storage_credential_ddl(info: Dict) -> str:
    name = _esc(info["name"])
    comment = f"\n  COMMENT '{_sql_escape(info['comment'])}'" if info.get("comment") else ""
    # We emit as a comment block — credentials require cloud-specific config
    return (
        f"-- STORAGE CREDENTIAL: {info['name']}\n"
        f"-- Recreate manually with appropriate cloud config, then run:\n"
        f"-- CREATE STORAGE CREDENTIAL IF NOT EXISTS {name}{comment};\n"
    )


def _grants_to_sql(
    securable_type: str, full_name: str, privilege_assignments: List[Dict]
) -> List[str]:
    keyword = _GRANT_KEYWORD.get(securable_type, "TABLE")
    stmts: List[str] = []
    for pa in privilege_assignments:
        principal = pa.get("principal", "")
        privs = pa.get("privileges") or []
        if privs and principal:
            priv_str = ", ".join(privs)
            # Build quoted full name
            parts = full_name.split(".")
            quoted = ".".join(f"`{p}`" for p in parts)
            stmts.append(
                f"GRANT {priv_str} ON {keyword} {quoted} TO `{principal}`;"
            )
    return stmts


def _sql_escape(s: str) -> str:
    return s.replace("'", "''")


# ---------------------------------------------------------------------------
# Main exporter class
# ---------------------------------------------------------------------------


class UnityCatalogExporter(BaseExporter):
    """
    Exports Unity Catalog objects in a deployable hierarchy.

    Parameters
    ----------
    workspace_url, token, export_dir, verify_ssl:
        Passed to BaseExporter.
    warehouse_id:
        Optional SQL warehouse id for system-table queries.
        If None the exporter uses only the UC REST API (recommended).
    """

    def __init__(
        self,
        workspace_url: str,
        token: str,
        export_dir: str,
        verify_ssl: bool = True,
        warehouse_id: Optional[str] = None,
        include_catalogs: Optional[List[str]] = None,
        include_schemas: Optional[List[str]] = None,
        owned_only: bool = True,
    ) -> None:
        super().__init__(workspace_url, token, export_dir, verify_ssl)
        self._warehouse_id = warehouse_id
        # Explicit catalog/schema allow-lists (override owned_only)
        self._include_catalogs: Optional[Set[str]] = (
            set(include_catalogs) if include_catalogs else None
        )
        self._include_schemas: Optional[Set[str]] = (
            set(include_schemas) if include_schemas else None
        )
        # Only export catalogs owned by the current token user
        self._owned_only = owned_only
        # Populated during export, used for view-dependency resolution
        self._all_view_fqns: Set[str] = set()
        self._skipped: List[Dict] = []
        self._manifest_steps: List[Dict] = []
        self._step = 0
        self._current_user: Optional[str] = None

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def export(self) -> Dict:
        """
        Run the full UC export.  Returns a summary dict with counts.
        """
        base = self._mkdir("uc_export")
        self._mkdir("uc_export/tables")
        self._mkdir("uc_export/views")
        raw_dir = self._mkdir("uc_export/raw")

        stats: Dict[str, int] = defaultdict(int)

        # ── 1. Storage credentials (admin-only; emit as comments) ──────
        self._export_storage_credentials(stats)

        # ── 2. External locations ───────────────────────────────────────
        self._export_external_locations(stats)

        # ── 3. Catalogs ─────────────────────────────────────────────────
        catalogs = self._export_catalogs(stats)
        self._save_json("uc_export/raw/catalogs.json", catalogs)

        # ── 4. Schemas, volumes, tables, views, functions per catalog ──
        all_schemas: List[Dict] = []
        all_volumes: List[Dict] = []
        all_tables: List[Dict] = []  # MANAGED + EXTERNAL only
        all_views: List[Dict] = []   # VIEW only, will be topo-sorted
        all_functions: List[Dict] = []
        all_grants: List[str] = []

        for cat in catalogs:
            cat_name = cat["name"]
            schemas = self._list_schemas(cat_name)
            all_schemas.extend(schemas)
            self._collect_perms("catalog", cat_name, all_grants)

            for sch in schemas:
                sch_name = sch["name"]
                self._collect_perms("schema", f"{cat_name}.{sch_name}", all_grants)

                # Volumes
                vols = self._list_volumes(cat_name, sch_name)
                all_volumes.extend(vols)
                for v in vols:
                    self._collect_perms(
                        "volume", f"{cat_name}.{sch_name}.{v['name']}", all_grants
                    )

                # Tables + views
                tbl_list = self._list_tables(cat_name, sch_name, stats)
                for t in tbl_list:
                    full = f"{t['catalog_name']}.{t['schema_name']}.{t['name']}".lower()
                    if t.get("table_type") == "VIEW":
                        t["fqn"] = full
                        self._all_view_fqns.add(full)
                        all_views.append(t)
                    else:
                        all_tables.append(t)
                    self._collect_perms("table", full, all_grants)

                # Functions
                fns = self._list_functions(cat_name, sch_name)
                all_functions.extend(fns)
                for fn in fns:
                    self._collect_perms(
                        "function",
                        f"{cat_name}.{sch_name}.{fn['name']}",
                        all_grants,
                    )

        self._save_json("uc_export/raw/schemas.json", all_schemas)
        self._save_json("uc_export/raw/tables.json", all_tables + all_views)
        self._save_json("uc_export/raw/skipped.json", self._skipped)

        # ── 5. Write schemas SQL ────────────────────────────────────────
        schemas_sql = "\n".join(_schema_ddl(s) for s in all_schemas)
        self._write_numbered("04_schemas.sql", schemas_sql, "schemas")
        stats["schemas"] += len(all_schemas)

        # ── 6. Write volumes SQL ────────────────────────────────────────
        vols_sql = "\n".join(_volume_ddl(v) for v in all_volumes)
        self._write_numbered("05_volumes.sql", vols_sql, "volumes")
        stats["volumes"] += len(all_volumes)

        # ── 7. Write per-table SQL files (external first, then managed) ─
        ext_tables = [t for t in all_tables if t.get("table_type") == "EXTERNAL"]
        mgd_tables = [t for t in all_tables if t.get("table_type") != "EXTERNAL"]
        for t in ext_tables + mgd_tables:
            fname = _safe_filename(t["catalog_name"], t["schema_name"], t["name"])
            ddl = _table_ddl(t)
            self._save_text(f"uc_export/tables/{fname}.sql", ddl)
            self._manifest_steps.append(
                {"step": self._next_step(), "type": "table",
                 "file": f"uc_export/tables/{fname}.sql",
                 "full_name": f"{t['catalog_name']}.{t['schema_name']}.{t['name']}"}
            )
        stats["tables"] += len(all_tables)
        stats["external_tables"] += len(ext_tables)

        # ── 8. Write functions SQL ──────────────────────────────────────
        fn_sql = "\n".join(_function_ddl(f) for f in all_functions)
        self._write_numbered("06_functions.sql", fn_sql, "functions")
        stats["functions"] += len(all_functions)

        # ── 9. Write view SQL files (topologically sorted) ─────────────
        sorted_views = _topo_sort_views(all_views, self._all_view_fqns)
        for v in sorted_views:
            fname = _safe_filename(v["catalog_name"], v["schema_name"], v["name"])
            self._save_text(f"uc_export/views/{fname}.sql", _view_ddl(v))
            self._manifest_steps.append(
                {"step": self._next_step(), "type": "view",
                 "file": f"uc_export/views/{fname}.sql",
                 "full_name": f"{v['catalog_name']}.{v['schema_name']}.{v['name']}"}
            )
        stats["views"] += len(all_views)

        # ── 10. Write grants SQL ────────────────────────────────────────
        grants_sql = "\n".join(all_grants) + "\n"
        self._write_numbered("07_grants.sql", grants_sql, "grants")
        stats["acl_entries"] += len(all_grants)

        # ── 11. Write skipped objects log ──────────────────────────────
        stats["skipped_mv"] = sum(
            1 for s in self._skipped if s.get("reason") == "MATERIALIZED_VIEW"
        )
        stats["skipped_streaming"] = sum(
            1 for s in self._skipped if s.get("reason") == "STREAMING_TABLE"
        )

        # ── 12. Write import manifest ───────────────────────────────────
        manifest = {
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "source_workspace": self.workspace_url,
            "deployment_order": self._manifest_steps,
            "summary": dict(stats),
            "notes": [
                "Run steps in manifest order for a clean deployment.",
                "Storage credentials (01) must be created manually first.",
                "External locations (02) require storage credentials to exist.",
                "Tables are split: external before managed.",
                "Views are sorted by dependency – deploy after all tables.",
                "Grants (07) should be applied last.",
            ],
        }
        self._save_json("uc_export/import_manifest.json", manifest)
        _LOG.info("[unity_catalog] Export complete: %s", dict(stats))
        return dict(stats)

    # ------------------------------------------------------------------
    # Per-object export helpers
    # ------------------------------------------------------------------

    def _export_storage_credentials(self, stats: Dict) -> None:
        try:
            creds = self._get("/api/2.1/unity-catalog/storage-credentials").get(
                "storage_credentials"
            ) or []
        except Exception as exc:  # noqa: BLE001
            _LOG.warning("[uc] storage-credentials: %s (may need admin)", exc)
            creds = []
        sql = "\n".join(_storage_credential_ddl(c) for c in creds)
        self._write_numbered("01_storage_credentials.sql", sql, "storage_credentials")
        stats["storage_credentials"] += len(creds)

    def _export_external_locations(self, stats: Dict) -> None:
        try:
            locs = self._get("/api/2.1/unity-catalog/external-locations").get(
                "external_locations"
            ) or []
        except Exception as exc:  # noqa: BLE001
            _LOG.warning("[uc] external-locations: %s (may need admin)", exc)
            locs = []
        sql = "\n".join(_external_location_ddl(loc) for loc in locs)
        self._write_numbered("02_external_locations.sql", sql, "external_locations")
        stats["external_locations"] += len(locs)

    def _resolve_current_user(self) -> Optional[str]:
        """Return the userName of the token owner (cached)."""
        if self._current_user is None:
            try:
                data = self._get("/api/2.0/preview/scim/v2/Me")
                self._current_user = data.get("userName", "")
            except Exception:  # noqa: BLE001
                self._current_user = ""
        return self._current_user or None

    def _export_catalogs(self, stats: Dict) -> List[Dict]:
        try:
            all_cats = self._paginated_get(
                "/api/2.1/unity-catalog/catalogs",
                "catalogs",
                params={"page_size": 200},
            )
        except Exception as exc:  # noqa: BLE001
            _LOG.error("[uc] Cannot list catalogs: %s", exc)
            return []

        # Step 1: keep only exportable catalog types
        managed = [c for c in all_cats if c.get("catalog_type") in _EXPORTABLE_CATALOG_TYPES]

        # Step 2: explicit include list overrides everything
        if self._include_catalogs:
            exportable = [c for c in managed if c["name"] in self._include_catalogs]
            missing = self._include_catalogs - {c["name"] for c in exportable}
            if missing:
                _LOG.warning("[uc] Requested catalogs not found/accessible: %s", missing)
        elif self._owned_only:
            # Step 3: filter to catalogs owned by the current token user
            owner = self._resolve_current_user()
            exportable = [c for c in managed if c.get("owner") == owner]
            if not exportable:
                _LOG.warning(
                    "[uc] No catalogs owned by %s found. "
                    "Use --uc-all-catalogs to export all accessible catalogs.",
                    owner,
                )
            else:
                _LOG.info("[uc] Exporting %d catalog(s) owned by %s", len(exportable), owner)
        else:
            exportable = managed

        sql = "\n".join(_catalog_ddl(c) for c in exportable)
        self._write_numbered("03_catalogs.sql", sql, "catalogs")
        stats["catalogs"] += len(exportable)
        _LOG.info(
            "[uc] %d/%d managed catalog(s) selected (skipped %d non-exportable type)",
            len(exportable), len(managed), len(all_cats) - len(managed),
        )
        return exportable

    def _list_schemas(self, catalog_name: str) -> List[Dict]:
        try:
            schemas = self._paginated_get(
                "/api/2.1/unity-catalog/schemas",
                "schemas",
                params={"catalog_name": catalog_name, "page_size": 200},
            )
        except Exception as exc:  # noqa: BLE001
            _LOG.warning("[uc] schemas in %s: %s", catalog_name, exc)
            return []
        if self._include_schemas:
            schemas = [
                s for s in schemas
                if s["name"] in self._include_schemas
                or f"{catalog_name}.{s['name']}" in self._include_schemas
            ]
        return schemas

    def _list_volumes(self, catalog_name: str, schema_name: str) -> List[Dict]:
        try:
            return self._paginated_get(
                "/api/2.1/unity-catalog/volumes",
                "volumes",
                params={
                    "catalog_name": catalog_name,
                    "schema_name": schema_name,
                    "page_size": 200,
                },
            )
        except Exception as exc:  # noqa: BLE001
            _LOG.debug("[uc] volumes in %s.%s: %s", catalog_name, schema_name, exc)
            return []

    def _list_tables(
        self, catalog_name: str, schema_name: str, stats: Dict
    ) -> List[Dict]:
        """List and fetch full metadata for tables/views, skipping MV/ST."""
        try:
            summaries = self._paginated_get(
                "/api/2.1/unity-catalog/tables",
                "tables",
                params={
                    "catalog_name": catalog_name,
                    "schema_name": schema_name,
                    "page_size": 200,
                },
            )
        except Exception as exc:  # noqa: BLE001
            _LOG.warning("[uc] tables in %s.%s: %s", catalog_name, schema_name, exc)
            return []

        results: List[Dict] = []
        for s in summaries:
            ttype = s.get("table_type", "")
            full = f"{catalog_name}.{schema_name}.{s.get('name','')}"
            if ttype in _SKIP_TABLE_TYPES:
                self._skipped.append({"full_name": full, "reason": ttype})
                stats[f"skipped_{ttype.lower()}"] = (
                    stats.get(f"skipped_{ttype.lower()}", 0) + 1
                )
                _LOG.debug("[uc] Skipped %s (%s)", full, ttype)
                continue
            # Fetch full table detail (includes column info and view_definition)
            try:
                detail = self._get(f"/api/2.1/unity-catalog/tables/{full}")
                results.append(detail)
            except Exception as exc:  # noqa: BLE001
                _LOG.warning("[uc] Could not fetch detail for %s: %s", full, exc)
        return results

    def _list_functions(self, catalog_name: str, schema_name: str) -> List[Dict]:
        try:
            return self._paginated_get(
                "/api/2.1/unity-catalog/functions",
                "functions",
                params={
                    "catalog_name": catalog_name,
                    "schema_name": schema_name,
                    "page_size": 200,
                },
            )
        except Exception as exc:  # noqa: BLE001
            _LOG.debug("[uc] functions in %s.%s: %s", catalog_name, schema_name, exc)
            return []

    def _collect_perms(
        self, securable_type: str, full_name: str, out: List[str]
    ) -> None:
        """Fetch privilege assignments and append GRANT SQL to out."""
        try:
            data = self._get(
                f"/api/2.1/unity-catalog/permissions/{securable_type}/{full_name}"
            )
            assignments = data.get("privilege_assignments") or []
            out.extend(_grants_to_sql(securable_type, full_name, assignments))
        except Exception as exc:  # noqa: BLE001
            _LOG.debug("[uc] permissions %s/%s: %s", securable_type, full_name, exc)

    # ------------------------------------------------------------------
    # Manifest / file helpers
    # ------------------------------------------------------------------

    def _next_step(self) -> int:
        self._step += 1
        return self._step

    def _write_numbered(self, rel_path: str, sql: str, label: str) -> None:
        """Write a numbered SQL file and register it in the manifest."""
        header = (
            f"-- ===========================================================\n"
            f"-- {label.upper()}\n"
            f"-- Generated by workspace_export on "
            f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
            f"-- ===========================================================\n\n"
        )
        self._save_text(f"uc_export/{rel_path}", header + sql)
        self._manifest_steps.append(
            {"step": self._next_step(), "type": label, "file": f"uc_export/{rel_path}"}
        )
