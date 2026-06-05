"""
workspace_import/compare_report.py
====================================
Generates a comprehensive HTML comparison report that contrasts every
exported component (source Azure workspace) against every imported
component (target GCP workspace).

Highlights:
  • Side-by-side component table: exported count vs imported count
  • Delta / coverage column with visual progress bars
  • Separate colour-coded error section (FATAL / ERROR / WARNING)
  • GCP transformation summary (node mapping, Photon stripping, SP patching)
  • SP mapping table
  • Timeline section (export duration, import duration per stage)
  • CLI entry-point: python3 -m workspace_import.compare_report <session_dir>

Usage (programmatic)::

    from workspace_import.compare_report import generate_compare_report
    path = generate_compare_report(
        session_dir   = "logs/EXPORT_202604291119",
        import_log    = "logs/import_run_202604291119.log",   # optional raw log
    )
    print("Report:", path)
"""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _db_icon_svg(size: int = 56) -> str:
    """Return the Databricks stacked-bricks icon as inline SVG (no text, transparent bg)."""
    icon_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             "..", "assets", "databricks_icon.svg")
    try:
        with open(icon_path) as f:
            svg = f.read()
        svg = svg.replace('width="64"', f'width="{size}"')
        svg = svg.replace('height="64"', f'height="{size}"')
        return svg
    except Exception:
        return ""

def _load_json(path: str) -> Optional[Dict]:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _fmt_dur(seconds: Optional[float]) -> str:
    if seconds is None:
        return "—"
    td = timedelta(seconds=int(seconds))
    h, rem = divmod(td.seconds, 3600)
    m, s = divmod(rem, 60)
    if td.days or h:
        return f"{td.days*24+h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def _fmt_dt(iso: Optional[str]) -> str:
    if not iso:
        return "—"
    try:
        dt = datetime.fromisoformat(iso)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return iso


def _pct(a: Optional[int], b: Optional[int]) -> Optional[float]:
    if b and a is not None:
        return min(100.0, round(a / b * 100, 1))
    return None


# ---------------------------------------------------------------------------
# Data collection
# ---------------------------------------------------------------------------

# Map the component names used by import pipeline to export component keys
_IMPORT_COMPONENT_MAP: Dict[str, str] = {
    "import_instance_profiles": "instance_profiles",
    "import_users":             "users",
    "import_groups":            "groups",
    "import_cluster_policies":  "clusters",
    "import_notebooks":         "notebooks",
    "import_workspace_acls":    "workspace_acls",
    "import_secrets":           "secrets",
    "import_instance_pools":    "instance_pools",
    "import_clusters":          "clusters",
    "import_jobs":              "jobs",
    "import_metastore":         "metastore",
    "import_metastore_table_acls": "metastore_table_acls",
    "sql_warehouses":           "sql_warehouses",
    "dlt_pipelines":            "dlt_pipelines",
    "repos":                    "repos",
    "ai/bi_dashboards":         "lakeview_dashboards",
    "genie_spaces":             "genie_spaces",
    "serving_endpoints":        "serving_endpoints",
    "workspace_files":          "workspace_files",
    "sp_migration":             None,  # synthetic
    "migrate_tool":             None,  # synthetic
}

# Display labels for components not in export_status.json
_EXTRA_DISPLAY: Dict[str, str] = {
    "sql_warehouses":    "SQL Warehouses",
    "dlt_pipelines":     "DLT Pipelines",
    "repos":             "Git Repos",
    "ai/bi_dashboards":  "AI/BI Dashboards",
    "genie_spaces":      "Genie AI Spaces",
    "serving_endpoints": "Model Serving Endpoints",
    "workspace_files":   "Workspace Files",
    "sp_migration":      "Service Principals (SP)",
}


def _parse_import_log(log_path: str) -> Dict[str, Any]:
    """
    Parse the raw import run log file to extract per-component events:
    timings (import_X Completed), errors, warnings, and Step 3 extra table.
    """
    if not os.path.isfile(log_path):
        return {}

    with open(log_path, encoding="utf-8", errors="replace") as f:
        lines = f.readlines()

    result: Dict[str, Any] = {
        "component_timings": {},   # name → duration string from "Completed" lines
        "errors":   [],            # list of {level, component, message}
        "warnings": [],
        "extra_table": {},         # component → {total, created, skipped, failed}
        "preprocess": {
            "jobs_transformed": 0,
            "clusters_transformed": 0,
            "pools_transformed": 0,
            "warnings": [],
        },
        "sp_info": {
            "found": 0,
            "files_patched": 0,
            "replacements": 0,
            "sp_list": [],
        },
    }

    # ---------- regex patterns ----------
    _re_completed  = re.compile(r"import_(\w+) Completed\. Total time taken: (\S+)")
    _re_start      = re.compile(r"Start (import_\w+)")
    _re_err_json   = re.compile(r";(ERROR|WARNING);(\{.*)")
    _re_err_fatal  = re.compile(r";(ERROR|WARNING);\s+FATAL\s+(.*)")
    _re_err_fail   = re.compile(r";(ERROR|WARNING);\s+FAIL\s+(.*)")
    _re_err_plain  = re.compile(r";ERROR;\s+(.*)")
    _re_extra_row  = re.compile(
        r"(sql_warehouses|dlt_pipelines|repos|ai/bi_dashboards|genie_spaces|serving_endpoints|workspace_files)"
        r"\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)"
    )
    _re_preprocess = re.compile(r"→ (\d+) (job|cluster|pool)\(s\) transformed, (\d+) warning")
    _re_sp_count   = re.compile(r"SP scan complete: (\d+) service principal")
    _re_sp_patch   = re.compile(r"Patch complete: (\d+) file.*?(\d+) replacement")
    _re_sp_detail  = re.compile(r"(REUSE|CREATE)\s+(\S+.*?)\s+app_id=(\S+)\s+scim_id=(\S+)")

    current_component = "unknown"

    for line in lines:
        # ── component started ──
        m = _re_start.search(line)
        if m:
            current_component = m.group(1)

        # ── component completed ──
        m = _re_completed.search(line)
        if m:
            name, dur = m.group(1), m.group(2)
            result["component_timings"][name] = dur

        # ── extra-components table row ──
        m = _re_extra_row.search(line)
        if m:
            result["extra_table"][m.group(1)] = {
                "total":   int(m.group(2)),
                "created": int(m.group(3)),
                "skipped": int(m.group(4)),
                "failed":  int(m.group(5)),
            }

        # ── GCP pre-process ──
        m = _re_preprocess.search(line)
        if m:
            n, kind = int(m.group(1)), m.group(2)
            w = int(m.group(3))
            if "job" in kind:
                result["preprocess"]["jobs_transformed"] = n
            elif "cluster" in kind:
                result["preprocess"]["clusters_transformed"] = n
            elif "pool" in kind:
                result["preprocess"]["pools_transformed"] = n
            if w:
                result["preprocess"]["warnings"].append(line.strip())

        # ── SP info ──
        m = _re_sp_count.search(line)
        if m:
            result["sp_info"]["found"] = int(m.group(1))
        m = _re_sp_patch.search(line)
        if m:
            result["sp_info"]["files_patched"] = int(m.group(1))
            result["sp_info"]["replacements"] = int(m.group(2))
        m = _re_sp_detail.search(line)
        if m:
            result["sp_info"]["sp_list"].append({
                "action": m.group(1),
                "display_name": m.group(2).strip(),
                "app_id": m.group(3),
                "scim_id": m.group(4),
            })

        # ── errors / warnings ──
        m = _re_err_fatal.search(line)
        if m:
            level = m.group(1)
            result["errors"].append({
                "level": "FATAL", "component": current_component,
                "message": m.group(2).strip()
            })
            continue
        m = _re_err_fail.search(line)
        if m:
            level = m.group(1)
            bucket = "errors" if level == "ERROR" else "warnings"
            result[bucket].append({
                "level": level, "component": current_component,
                "message": m.group(2).strip()
            })
            continue
        m = _re_err_json.search(line)
        if m:
            level = m.group(1)
            try:
                payload = json.loads(m.group(2))
                msg = payload.get("message", m.group(2)[:200])
                ec  = payload.get("error_code", "")
                full = f"[{ec}] {msg}" if ec else msg
            except Exception:
                full = m.group(2)[:300]
            bucket = "errors" if level == "ERROR" else "warnings"
            result[bucket].append({
                "level": level, "component": current_component, "message": full
            })
            continue
        m = _re_err_plain.search(line)
        if m:
            result["errors"].append({
                "level": "ERROR", "component": current_component,
                "message": m.group(1).strip()
            })

    return result


# ---------------------------------------------------------------------------
# Report builder
# ---------------------------------------------------------------------------

def _build_component_rows(
    export_status: Dict,
    import_log_data: Dict,
    extra_import_results: Optional[Dict] = None,
) -> List[Dict]:
    """
    Merge export + import data into one list of row dicts.
    """
    exp_comps = export_status.get("components", {})
    extra = import_log_data.get("extra_table", {})
    timings = import_log_data.get("component_timings", {})

    # Normalised import counts from extra table
    def _extra(key: str) -> Dict:
        return extra.get(key, {})

    rows = []
    seen_export_keys = set()

    # ── Core components (from export_status) ──
    for name, ec in exp_comps.items():
        display = ec.get("display_name", name)
        exp_status = ec.get("status", "unknown")
        exp_items  = ec.get("items_exported")
        exp_dur    = ec.get("duration_seconds")
        exp_notes  = ec.get("notes") or ""

        # Map to import timing
        timing_key = name.replace("metastore_table_acls", "metastore_table_acls") \
                         .replace("workspace_item_log", "workspace_acls")

        # Import duration from log
        imp_dur_str = timings.get(name, timings.get("import_" + name, None))

        # Import result (items)
        imp_items  = None
        imp_status = None
        imp_notes  = ""

        # Extra table overrides (sql_warehouses etc.)
        extra_key_map = {
            "sql_warehouses":    "sql_warehouses",
            "dlt_pipelines":     "dlt_pipelines",
            "repos":             "repos",
            "lakeview_dashboards": "ai/bi_dashboards",
            "genie_spaces":      "genie_spaces",
            "serving_endpoints": "serving_endpoints",
            "workspace_files":   "workspace_files",
        }
        ek = extra_key_map.get(name)
        if ek and ek in extra:
            erow = extra[ek]
            imp_items  = erow.get("created", 0) + erow.get("skipped", 0)
            imp_status = "failed" if erow.get("failed", 0) > 0 else (
                         "success" if erow.get("created", 0) > 0 else "skipped")
            imp_notes  = (
                f"created={erow.get('created',0)} "
                f"skipped={erow.get('skipped',0)} "
                f"failed={erow.get('failed',0)}"
            )
        elif exp_status == "success" and name not in extra_key_map:
            # Core component imported by migrate tool
            imp_status = "success"

        # Skip indicator: if export was skipped, import is not applicable
        if exp_status == "skipped":
            imp_status = "n/a"

        pct = _pct(imp_items, exp_items)

        rows.append({
            "name":       name,
            "display":    display,
            "exp_status": exp_status,
            "exp_items":  exp_items,
            "exp_dur":    _fmt_dur(exp_dur),
            "exp_notes":  exp_notes,
            "imp_status": imp_status,
            "imp_items":  imp_items,
            "imp_dur":    imp_dur_str or "—",
            "imp_notes":  imp_notes,
            "pct":        pct,
            "category":   "core",
        })
        seen_export_keys.add(name)

    return rows


# ---------------------------------------------------------------------------
# HTML generation
# ---------------------------------------------------------------------------

_CSS = """
:root {
  --bg: #0d1117;
  --surface: #161b22;
  --surface2: #21262d;
  --border: #30363d;
  --text: #c9d1d9;
  --text-muted: #8b949e;
  --green: #3fb950;
  --red: #f85149;
  --yellow: #d29922;
  --blue: #58a6ff;
  --purple: #bc8cff;
  --orange: #f0883e;
  --teal: #39d353;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", monospace;
       background: var(--bg); color: var(--text); font-size: 13px; line-height: 1.6; }
a { color: var(--blue); text-decoration: none; }
.page { max-width: 1400px; margin: 0 auto; padding: 24px 20px; }

/* Header */
.report-header { background: linear-gradient(135deg, #1a2744 0%, #0d1117 100%);
  border: 1px solid var(--border); border-radius: 12px; padding: 36px 48px; margin-bottom: 24px; }
.report-header-inner { display: flex; align-items: center; gap: 24px; }
.report-header-logo { flex-shrink: 0; opacity: .95; }
.report-header-content { flex: 1; }
.report-header h1 { font-size: 32px; font-weight: 800; color: #fff; margin-bottom: 8px; letter-spacing: -.3px; }
.report-header .sub { color: var(--text-muted); font-size: 13px; }
.report-header .badges { display: flex; gap: 12px; flex-wrap: wrap; margin-top: 16px; }
.badge { padding: 4px 12px; border-radius: 20px; font-size: 11px; font-weight: 600;
         letter-spacing: .4px; text-transform: uppercase; }
.badge-green  { background: rgba(63,185,80,.15);  color: var(--green);  border: 1px solid rgba(63,185,80,.3); }
.badge-blue   { background: rgba(88,166,255,.12); color: var(--blue);   border: 1px solid rgba(88,166,255,.3); }
.badge-yellow { background: rgba(210,153,34,.15); color: var(--yellow); border: 1px solid rgba(210,153,34,.3); }
.badge-red    { background: rgba(248,81,73,.12);  color: var(--red);    border: 1px solid rgba(248,81,73,.3); }
.badge-purple { background: rgba(188,140,255,.12);color: var(--purple); border: 1px solid rgba(188,140,255,.3); }
.badge-teal   { background: rgba(57,211,83,.12);  color: var(--teal);   border: 1px solid rgba(57,211,83,.3); }

/* Summary cards */
.cards { display: grid; grid-template-columns: repeat(auto-fill, minmax(160px,1fr)); gap: 12px; margin-bottom: 24px; }
.card { background: var(--surface); border: 1px solid var(--border); border-radius: 10px;
        padding: 18px 16px; text-align: center; }
.card .value { font-size: 32px; font-weight: 700; }
.card .label { color: var(--text-muted); font-size: 11px; text-transform: uppercase;
               letter-spacing: .5px; margin-top: 4px; }
.card.green .value { color: var(--green); }
.card.red   .value { color: var(--red); }
.card.yellow .value { color: var(--yellow); }
.card.blue  .value { color: var(--blue); }
.card.purple .value { color: var(--purple); }
.card.teal   .value { color: var(--teal); }

/* Sections */
section { margin-bottom: 28px; }
section h2 { font-size: 16px; font-weight: 700; color: #fff; padding: 14px 0 10px;
             border-bottom: 1px solid var(--border); margin-bottom: 14px; display: flex;
             align-items: center; gap: 8px; }
section h2 .icon { font-size: 18px; }

/* Tables */
.tbl-wrap { overflow-x: auto; border-radius: 10px; border: 1px solid var(--border); }
table { width: 100%; border-collapse: collapse; }
th { background: var(--surface2); color: var(--text-muted); font-size: 11px; font-weight: 600;
     text-transform: uppercase; letter-spacing: .4px; padding: 10px 14px; text-align: left;
     border-bottom: 1px solid var(--border); white-space: nowrap; }
td { padding: 9px 14px; border-bottom: 1px solid rgba(48,54,61,.6); vertical-align: middle; }
tr:last-child td { border-bottom: none; }
tr:hover td { background: rgba(33,38,45,.6); }
.mono { font-family: monospace; font-size: 12px; }
.nowrap { white-space: nowrap; }

/* Status chips */
.chip { display: inline-block; padding: 2px 9px; border-radius: 12px;
        font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: .3px; }
.chip-success { background: rgba(63,185,80,.15);  color: var(--green); }
.chip-failed  { background: rgba(248,81,73,.12);  color: var(--red); }
.chip-warning { background: rgba(210,153,34,.15); color: var(--yellow); }
.chip-skipped { background: rgba(139,148,158,.1); color: var(--text-muted); }
.chip-na      { background: rgba(139,148,158,.1); color: var(--text-muted); }
.chip-partial { background: rgba(240,136,62,.12); color: var(--orange); }
.chip-pending { background: rgba(88,166,255,.1);  color: var(--blue); }

/* Progress bar */
.bar-wrap { width: 90px; height: 6px; background: var(--surface2);
            border-radius: 3px; overflow: hidden; display: inline-block; vertical-align: middle; }
.bar-fill  { height: 100%; border-radius: 3px; }
.bar-green  { background: var(--green); }
.bar-yellow { background: var(--yellow); }
.bar-red    { background: var(--red); }
.pct-text   { font-size: 11px; color: var(--text-muted); margin-left: 6px; }

/* Error section */
.err-block { border: 1px solid var(--border); border-radius: 10px; overflow: hidden; margin-bottom: 10px; }
.err-header { padding: 10px 16px; font-weight: 600; font-size: 12px;
              display: flex; align-items: center; gap: 8px; }
.err-header.fatal  { background: rgba(248,81,73,.1);  color: var(--red);    border-bottom: 1px solid rgba(248,81,73,.2); }
.err-header.error  { background: rgba(248,81,73,.08); color: var(--red);    border-bottom: 1px solid rgba(248,81,73,.15); }
.err-header.warn   { background: rgba(210,153,34,.08);color: var(--yellow); border-bottom: 1px solid rgba(210,153,34,.15); }
.err-body  { padding: 0; }
.err-row   { padding: 8px 16px; border-bottom: 1px solid rgba(48,54,61,.4);
             font-size: 12px; font-family: monospace; }
.err-row:last-child { border-bottom: none; }
.err-comp  { color: var(--blue); font-size: 11px; margin-bottom: 2px; }

/* Timeline */
.timeline { display: flex; flex-direction: column; gap: 6px; }
.tl-row    { display: flex; align-items: center; gap: 12px; }
.tl-label  { width: 200px; font-size: 12px; color: var(--text-muted); flex-shrink: 0; }
.tl-bar-wrap { flex: 1; height: 20px; background: var(--surface2); border-radius: 4px;
               overflow: hidden; position: relative; }
.tl-bar    { height: 100%; border-radius: 4px; display: flex; align-items: center;
             padding: 0 8px; font-size: 11px; color: rgba(255,255,255,.8); }
.tl-dur    { width: 60px; font-size: 12px; color: var(--text-muted); text-align: right; flex-shrink: 0; }

/* SP table */
.sp-table td:first-child { font-weight: 600; }
.sp-action-reuse  { color: var(--teal); }
.sp-action-create { color: var(--blue); }

/* Footer */
.footer { text-align: center; color: var(--text-muted); font-size: 11px;
          padding: 16px 0 8px; border-top: 1px solid var(--border); margin-top: 32px; }
"""

_JS = """
function toggleSection(id) {
  var el = document.getElementById(id);
  el.style.display = el.style.display === 'none' ? '' : 'none';
}
"""


def _chip(status: Optional[str]) -> str:
    if not status:
        return '<span class="chip chip-pending">—</span>'
    s = status.lower()
    if s == "success":
        return '<span class="chip chip-success">✓ Success</span>'
    if s in ("failed", "error", "fatal"):
        return '<span class="chip chip-failed">✗ Failed</span>'
    if s == "warning":
        return '<span class="chip chip-warning">⚠ Warning</span>'
    if s == "skipped":
        return '<span class="chip chip-skipped">⊘ Skipped</span>'
    if s == "n/a":
        return '<span class="chip chip-na">N/A</span>'
    if s == "partial":
        return '<span class="chip chip-partial">~ Partial</span>'
    return f'<span class="chip chip-pending">{status}</span>'


def _bar(pct: Optional[float]) -> str:
    if pct is None:
        return "—"
    cls = "bar-green" if pct >= 90 else ("bar-yellow" if pct >= 50 else "bar-red")
    return (
        f'<div class="bar-wrap"><div class="bar-fill {cls}" style="width:{pct}%"></div></div>'
        f'<span class="pct-text">{pct}%</span>'
    )


def generate_compare_report(
    session_dir: str,
    import_log: Optional[str] = None,
    output_path: Optional[str] = None,
) -> str:
    """
    Main entry point.  Returns the path to the generated HTML file.

    Parameters
    ----------
    session_dir:  Path to the export session directory (contains export_status.json).
    import_log:   Path to the raw import run log file (e.g. import_run_XXXXXXXX.log).
                  If None, the function searches for import_run_*.log beside session_dir.
    output_path:  Where to write the HTML.  Defaults to session_dir/compare_report.html.
    """
    session_dir = os.path.abspath(session_dir)
    if output_path is None:
        output_path = os.path.join(session_dir, "compare_report.html")

    # ── Load export status ──────────────────────────────────────────────────
    export_status = _load_json(os.path.join(session_dir, "export_status.json")) or {}
    session_id = export_status.get("session", os.path.basename(session_dir))
    source_url = export_status.get("workspace_url", "Unknown source")
    exp_start  = export_status.get("export_start")
    exp_end    = export_status.get("export_end")
    exp_dur_s  = None
    if exp_start and exp_end:
        try:
            exp_dur_s = (datetime.fromisoformat(exp_end) -
                         datetime.fromisoformat(exp_start)).total_seconds()
        except Exception:
            pass

    # ── Load import log ─────────────────────────────────────────────────────
    import_log_json = _load_json(os.path.join(session_dir, "import_log.json")) or {}
    target_url   = import_log_json.get("target_workspace", "Unknown target")
    imp_start_iso = import_log_json.get("started_at")
    imp_end_iso   = import_log_json.get("ended_at")

    imp_dur_s = None
    if imp_start_iso and imp_end_iso:
        try:
            imp_dur_s = (datetime.fromisoformat(imp_end_iso) -
                         datetime.fromisoformat(imp_start_iso)).total_seconds()
        except Exception:
            pass

    # ── Find raw log ────────────────────────────────────────────────────────
    if import_log is None:
        base = os.path.dirname(session_dir)
        candidates = [
            f for f in os.listdir(base)
            if f.startswith("import_run_") and f.endswith(".log")
        ]
        if candidates:
            import_log = os.path.join(base, sorted(candidates)[-1])

    log_data = _parse_import_log(import_log) if import_log else {}

    # ── Load SP mapping ─────────────────────────────────────────────────────
    sp_mapping = _load_json(os.path.join(session_dir, "sp_mapping.json")) or {}

    # ── Load GCP transform manifest ─────────────────────────────────────────
    gcp_manifest = _load_json(os.path.join(session_dir, "gcp_transform_manifest.json")) or {}

    # ── Build component rows ─────────────────────────────────────────────────
    rows = _build_component_rows(export_status, log_data)

    # ── Aggregate totals ─────────────────────────────────────────────────────
    total_exported = sum(r["exp_items"] or 0 for r in rows if r["exp_status"] == "success")
    total_exp_comps  = sum(1 for r in rows if r["exp_status"] == "success")
    total_skipped    = sum(1 for r in rows if r["exp_status"] == "skipped")
    total_errors_imp = len(log_data.get("errors", []))
    total_warns_imp  = len(log_data.get("warnings", []))
    sp_count         = log_data.get("sp_info", {}).get("found", len(sp_mapping))
    replacements     = log_data.get("sp_info", {}).get("replacements", 0)

    # ── Calculate total import duration from migrate step ───────────────────
    migrate_step = next(
        (s for s in import_log_json.get("steps", []) if s.get("component") == "migrate_tool"),
        None
    )
    migrate_dur = migrate_step.get("duration_seconds") if migrate_step else None

    # ── HTML ─────────────────────────────────────────────────────────────────
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Decide overall status
    fatal_errors   = [e for e in log_data.get("errors", []) if e["level"] == "FATAL"]
    regular_errors = [e for e in log_data.get("errors", []) if e["level"] != "FATAL"]
    warnings       = log_data.get("warnings", [])

    overall = "SUCCESS" if not fatal_errors else "PARTIAL"
    overall_badge = "badge-green" if overall == "SUCCESS" else "badge-yellow"

    # ── Build HTML ───────────────────────────────────────────────────────────
    parts: List[str] = []
    w = parts.append  # shorthand

    w(f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Migration Comparison Report – {session_id}</title>
<style>{_CSS}</style>
<script>{_JS}</script>
</head>
<body>
<div class="page">
""")

    # ── Header ───────────────────────────────────────────────────────────────
    w(f"""
<div class="report-header">
  <div class="report-header-inner">
    <div class="report-header-logo">
      {_db_icon_svg(72)}
    </div>
    <div class="report-header-content">
      <h1>Migration Comparison Report</h1>
      <div class="sub">
        Session: <strong>{session_id}</strong> &nbsp;·&nbsp; Generated: {now}
      </div>
    </div>
  </div>
  <div class="badges" style="margin-top:14px;">
    <div><span class="badge badge-blue">☁ Source (Azure)</span>
         <span style="color:var(--text-muted);font-size:12px;margin-left:6px;">{source_url}</span></div>
    <div><span class="badge badge-teal">☁ Target (GCP)</span>
         <span style="color:var(--text-muted);font-size:12px;margin-left:6px;">{target_url}</span></div>
  </div>
  <div class="badges">
    <span class="badge {overall_badge}">Overall: {overall}</span>
    <span class="badge badge-blue">Export: {_fmt_dt(exp_start)} → {_fmt_dt(exp_end)}&nbsp; ({_fmt_dur(exp_dur_s)})</span>
    <span class="badge badge-purple">Import Duration: {_fmt_dur(imp_dur_s or migrate_dur)}</span>
    <span class="badge badge-red">{total_errors_imp} Error(s)</span>
    <span class="badge badge-yellow">{total_warns_imp} Warning(s)</span>
  </div>
</div>
""")

    # ── Summary cards ────────────────────────────────────────────────────────
    w('<div class="cards">')
    cards = [
        ("green",  str(total_exp_comps),  "Components Exported"),
        ("teal",   str(total_exported),   "Items Exported"),
        ("purple", str(sp_count),         "SPs Migrated"),
        ("yellow", str(replacements),     "UUID Patches"),
        ("red",    str(total_errors_imp), "Import Errors"),
        ("yellow", str(total_warns_imp),  "Warnings"),
        ("blue",   str(total_skipped),    "Skipped"),
    ]
    for cls, val, lbl in cards:
        w(f'<div class="card {cls}"><div class="value">{val}</div><div class="label">{lbl}</div></div>')
    w('</div>')

    # ── Component comparison table ───────────────────────────────────────────
    w("""
<section>
  <h2><span class="icon">📊</span> Component Export vs Import Comparison</h2>
  <div class="tbl-wrap">
  <table>
    <thead>
      <tr>
        <th>#</th>
        <th>Component</th>
        <th>Export Status</th>
        <th>Exported</th>
        <th>Export Duration</th>
        <th>Import Status</th>
        <th>Imported</th>
        <th>Coverage</th>
        <th>Notes</th>
      </tr>
    </thead>
    <tbody>
""")
    for i, r in enumerate(rows, 1):
        exp_s = r["exp_status"]
        imp_s = r["imp_status"]

        # Determine row highlight
        row_style = ""
        if imp_s == "failed" or (exp_s == "success" and imp_s is None):
            row_style = 'style="background:rgba(248,81,73,.04)"'
        elif imp_s == "n/a" or exp_s == "skipped":
            row_style = 'style="opacity:.65"'

        exp_items_str = str(r["exp_items"]) if r["exp_items"] is not None else "—"
        imp_items_str = str(r["imp_items"]) if r["imp_items"] is not None else "—"
        notes = r["imp_notes"] or r["exp_notes"] or ""
        notes_html = f'<span style="color:var(--text-muted);font-size:11px;">{notes[:80]}</span>' if notes else ""

        w(f"""      <tr {row_style}>
        <td class="mono" style="color:var(--text-muted)">{i:02d}</td>
        <td><strong>{r["display"]}</strong></td>
        <td>{_chip(exp_s)}</td>
        <td class="mono nowrap" style="text-align:right">{exp_items_str}</td>
        <td class="mono nowrap" style="color:var(--text-muted)">{r["exp_dur"]}</td>
        <td>{_chip(imp_s)}</td>
        <td class="mono nowrap" style="text-align:right">{imp_items_str}</td>
        <td class="nowrap">{_bar(r["pct"])}</td>
        <td>{notes_html}</td>
      </tr>
""")
    w("    </tbody>\n  </table>\n  </div>\n</section>")

    # ── GCP Transformation Summary ───────────────────────────────────────────
    pp = log_data.get("preprocess", {})
    tf_files = gcp_manifest.get("files_transformed", {})
    tf_warns = gcp_manifest.get("all_warnings", [])

    w("""
<section>
  <h2><span class="icon">⚙️</span> GCP Pre-Processing Transformations</h2>
  <div class="tbl-wrap">
  <table>
    <thead><tr><th>File</th><th>Records Transformed</th><th>Warnings</th><th>Details</th></tr></thead>
    <tbody>
""")
    for fname, finfo in tf_files.items():
        count = finfo.get("count", "—")
        fw = finfo.get("warnings", 0)
        warn_cls = 'style="color:var(--yellow)"' if fw else 'style="color:var(--text-muted)"'
        detail = ""
        if isinstance(finfo, dict):
            flags = []
            if finfo.get("photon_stripped"):
                flags.append("Photon stripped")
            if finfo.get("node_mapped"):
                flags.append(f"node mapped")
            if finfo.get("gcp_attrs_injected"):
                flags.append("GCP attrs injected")
            detail = ", ".join(flags)
        w(f"""      <tr>
        <td class="mono">{fname}</td>
        <td class="mono" style="text-align:right">{count}</td>
        <td class="mono" {warn_cls}>{fw}</td>
        <td style="color:var(--text-muted);font-size:11px">{detail}</td>
      </tr>
""")
    if not tf_files:
        # Fallback: use parsed log data
        for label, val in [
            ("jobs.log", pp.get("jobs_transformed", "—")),
            ("clusters.log", pp.get("clusters_transformed", "—")),
            ("instance_pools.log", pp.get("pools_transformed", "—")),
        ]:
            w(f'      <tr><td class="mono">{label}</td><td class="mono" style="text-align:right">{val}</td><td>—</td><td></td></tr>\n')
    w("    </tbody>\n  </table>\n  </div>")

    if tf_warns:
        w('<div style="margin-top:10px;">')
        for warn in tf_warns[:20]:
            w(f'<div class="err-row" style="background:rgba(210,153,34,.04);border:1px solid rgba(210,153,34,.1);border-radius:6px;margin-bottom:4px;">⚠ {warn}</div>')
        w('</div>')
    w("</section>")

    # ── Service Principal Mapping ─────────────────────────────────────────────
    sp_list = log_data.get("sp_info", {}).get("sp_list", [])
    if not sp_list and sp_mapping:
        sp_list = [
            {"action": "REUSE", "display_name": v.get("display_name", k),
             "app_id": v.get("src_app_id", ""), "scim_id": v.get("dst_scim_id", "")}
            for k, v in sp_mapping.items()
        ]

    w("""
<section>
  <h2><span class="icon">🔑</span> Service Principal Mapping (Azure → GCP)</h2>
  <div class="tbl-wrap">
  <table class="sp-table">
    <thead><tr><th>Action</th><th>Display Name</th><th>Source App ID (Azure)</th><th>GCP App ID / SCIM ID</th></tr></thead>
    <tbody>
""")
    if sp_list:
        for sp in sp_list:
            action = sp.get("action", "REUSE")
            acls = "sp-action-reuse" if action == "REUSE" else "sp-action-create"
            w(f"""      <tr>
        <td><span class="{acls}">{'♻ REUSE' if action=='REUSE' else '+ CREATE'}</span></td>
        <td><strong>{sp.get("display_name","")}</strong></td>
        <td class="mono" style="font-size:11px">{sp.get("app_id","")}</td>
        <td class="mono" style="font-size:11px">{sp.get("scim_id","")}</td>
      </tr>
""")
    else:
        w('      <tr><td colspan="4" style="color:var(--text-muted);text-align:center">No SP data found</td></tr>\n')
    files_patched = log_data.get("sp_info", {}).get("files_patched", 0)
    rep_count     = log_data.get("sp_info", {}).get("replacements", 0)
    w(f"""    </tbody>
  </table>
  </div>
  <div style="margin-top:8px;color:var(--text-muted);font-size:12px">
    {files_patched} file(s) patched &nbsp;·&nbsp; {rep_count} UUID replacement(s) in staging copy
  </div>
</section>
""")

    # ── Timeline ─────────────────────────────────────────────────────────────
    timings = log_data.get("component_timings", {})

    def _parse_dur(s: str) -> float:
        if not s or s == "—":
            return 0.0
        m = re.match(r"(?:(\d+):)?(\d+):(\d+)\.(\d+)", s)
        if m:
            h = int(m.group(1) or 0)
            mi = int(m.group(2))
            sc = int(m.group(3))
            return h * 3600 + mi * 60 + sc
        return 0.0

    timeline_steps = [
        ("Export (total)",    exp_dur_s or 0,       "#58a6ff"),
        ("  └ Hive Metastore", export_status.get("components", {}).get("metastore", {}).get("duration_seconds", 0) or 0, "#8b949e"),
        ("  └ Table ACLs",    export_status.get("components", {}).get("metastore_table_acls", {}).get("duration_seconds", 0) or 0, "#8b949e"),
        ("Import: migrate pipeline", migrate_dur or 0, "#3fb950"),
        ("  └ import_users",  _parse_dur(timings.get("users", "0:00:00.0")), "#8b949e"),
        ("  └ import_clusters", _parse_dur(timings.get("clusters", "0:00:00.0")), "#8b949e"),
        ("  └ import_jobs",   _parse_dur(timings.get("jobs", "0:00:00.0")), "#8b949e"),
        ("  └ import_metastore", _parse_dur(timings.get("metastore", "0:00:00.0")), "#8b949e"),
        ("Import: extra components", 60, "#bc8cff"),
    ]
    max_dur = max(d for _, d, _ in timeline_steps) or 1

    w('<section><h2><span class="icon">⏱</span> Duration Timeline</h2><div class="timeline">')
    for label, dur, color in timeline_steps:
        pct_tl = min(100, int(dur / max_dur * 100))
        w(f'''  <div class="tl-row">
    <div class="tl-label">{label}</div>
    <div class="tl-bar-wrap">
      <div class="tl-bar" style="width:{pct_tl}%;background:{color};">{_fmt_dur(dur) if dur>0 else ""}</div>
    </div>
    <div class="tl-dur">{_fmt_dur(dur)}</div>
  </div>
''')
    w('</div></section>')

    # ── Errors section ───────────────────────────────────────────────────────
    w('<section><h2><span class="icon">🚨</span> Issues &amp; Errors</h2>')

    def _render_errors(title: str, items: List[Dict], level_cls: str) -> str:
        if not items:
            return ""
        out = [f'<div class="err-block"><div class="err-header {level_cls}">'
               f'{"🔴" if level_cls in ("fatal","error") else "🟡"} {title} ({len(items)})</div>'
               f'<div class="err-body">']
        for item in items:
            out.append(f'<div class="err-row">'
                       f'<div class="err-comp">{item.get("component","?")}</div>'
                       f'{item.get("message","")}'
                       f'</div>')
        out.append('</div></div>')
        return "\n".join(out)

    # Group errors by component
    from collections import defaultdict
    fatal_by_comp: Dict[str, List] = defaultdict(list)
    error_by_comp: Dict[str, List] = defaultdict(list)
    warn_by_comp:  Dict[str, List] = defaultdict(list)

    for e in fatal_errors:
        fatal_by_comp[e["component"]].append(e)
    for e in regular_errors:
        error_by_comp[e["component"]].append(e)
    for e in warnings:
        warn_by_comp[e["component"]].append(e)

    if not fatal_errors and not regular_errors and not warnings:
        w('<div style="padding:20px;text-align:center;color:var(--green);">✅ No errors or warnings recorded.</div>')
    else:
        if fatal_errors:
            w(_render_errors("Fatal Errors (blocked import)", fatal_errors, "fatal"))
        if regular_errors:
            w(_render_errors("Errors (non-fatal, some items may not have imported)", regular_errors, "error"))
        if warnings:
            w(_render_errors("Warnings (informational)", warnings[:50], "warn"))
            if len(warnings) > 50:
                w(f'<div style="color:var(--text-muted);font-size:12px;padding:6px 0">… and {len(warnings)-50} more warnings (see raw import log)</div>')

    w('</section>')

    # ── Recommendations ──────────────────────────────────────────────────────
    w('<section><h2><span class="icon">💡</span> Recommendations &amp; Action Items</h2>')
    recs = []

    # Check for DLT notebooks that failed
    nb_errors = [e for e in regular_errors if "import_notebooks" in e.get("component","")]
    if nb_errors:
        recs.append(("yellow", "DLT Notebooks",
            f"{len(nb_errors)} DLT notebook(s) failed to upload (likely because the DLT folder paths didn't "
            "exist yet). These are covered by DLT Pipeline import — verify DLT pipelines are running."))

    # ACL resource not exist warnings
    acl_errors = [e for e in regular_errors if "workspace_acls" in e.get("component","")
                  and "RESOURCE_DOES_NOT_EXIST" in e.get("message","")]
    if acl_errors:
        recs.append(("yellow", "Workspace ACLs",
            f"{len(acl_errors)} ACL path(s) did not exist on GCP target (user home dirs / empty folders). "
            "These will auto-resolve once users log in and create their home directories."))

    # Instance pool warning
    pool_warns = [e for e in warnings if "instance_pool" in e.get("component","").lower()
                  or "n1-standard-4" in e.get("message","")]
    if pool_warns:
        recs.append(("red", "Instance Pool: n1-standard-4",
            "Instance type n1-standard-4 is not available in us-east1. Update the pool to use "
            "a node type available in your GCP region (e.g. n1-standard-2 or n2-standard-4)."))

    # Job node_type_id missing
    job_warns = [e for e in warnings if "node_type_id" in e.get("message","")]
    if job_warns:
        recs.append(("red", "Jobs: Missing node_type_id",
            f"{len(job_warns)} job(s) were created without node_type_id. Check jobs.log and add a "
            "valid GCP node type. This may cause job runs to fail."))

    # Genie spaces failure
    genie_err = [e for e in regular_errors if "genie" in e.get("component","").lower() or "Genie" in e.get("message","")]
    if genie_err:
        recs.append(("red", "Genie AI Spaces",
            f"{len(genie_err)} Genie Space(s) failed: the Databricks API requires the `serialized_space` field "
            "which is not available via public export APIs. These must be recreated manually."))

    # AI/BI dashboards
    dash_err = [e for e in fatal_errors if "dashboard" in e.get("message","").lower() or "lakeview" in e.get("component","")]
    if dash_err:
        recs.append(("yellow", "AI/BI Dashboards",
            "Dashboard import had a type error (now fixed in code). Re-run Step 3 to import dashboards."))

    # Workspace files
    wf_rows = [r for r in rows if r["name"] == "workspace_files"]
    if wf_rows and wf_rows[0]["exp_items"] == 0:
        recs.append(("blue", "Workspace Files",
            "No FILE-type workspace objects were found in the source. "
            "If you expect .py/.sql/.md files, check that they exist in the Azure workspace "
            "under /Shared or user home directories."))

    if not recs:
        recs.append(("green", "All Clear", "No action items identified. Migration looks complete."))

    w('<div style="display:flex;flex-direction:column;gap:8px;">')
    for sev, title, msg in recs:
        icon = {"green":"✅","yellow":"⚠️","red":"❌","blue":"ℹ️"}.get(sev,"•")
        bdr  = {"green":"var(--green)","yellow":"var(--yellow)","red":"var(--red)","blue":"var(--blue)"}.get(sev,"var(--border)")
        w(f'''<div style="border:1px solid {bdr};border-radius:8px;padding:12px 16px;
background:rgba(0,0,0,.2);">
  <div style="font-weight:600;margin-bottom:4px;">{icon} {title}</div>
  <div style="color:var(--text-muted);font-size:12px;">{msg}</div>
</div>''')
    w('</div></section>')

    # ── Footer ───────────────────────────────────────────────────────────────
    w(f"""
<div class="footer">
  Databricks Azure → GCP Migration &nbsp;·&nbsp; Session {session_id} &nbsp;·&nbsp;
  Report generated {now}
</div>
</div></body></html>""")

    html = "\n".join(parts)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    return output_path


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Generate export vs import comparison HTML report"
    )
    parser.add_argument("session_dir", help="Export session directory (contains export_status.json)")
    parser.add_argument("--import-log", default=None, help="Path to import_run_*.log file")
    parser.add_argument("--output",     default=None, help="Output HTML path (default: session_dir/compare_report.html)")
    args = parser.parse_args()

    path = generate_compare_report(
        session_dir=args.session_dir,
        import_log=args.import_log,
        output_path=args.output,
    )
    print(f"Compare report: {path}")


if __name__ == "__main__":
    main()
