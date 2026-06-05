"""
workspace_import/html_reporter.py
===================================
Generates a self-contained, single-file HTML report that covers both the
export phase and the import phase of a Databricks workspace migration.

Two entry points:
  • generate_export_html(session_dir)     – called at end of export
  • generate_import_html(session_dir)     – called at end of import
      (includes export data if export_status.json exists)

Writes to:
  <session_dir>/export_report.html   (export only)
  <session_dir>/import_report.html   (combined export + import)
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from html import escape
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Databricks logo helper
# ---------------------------------------------------------------------------
def _db_icon_svg(size: int = 48) -> str:
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


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _read_json(path: str) -> Optional[Dict]:
    if os.path.isfile(path):
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
    return None


def _fmt_dur(seconds: Optional[float]) -> str:
    if seconds is None:
        return "–"
    td = timedelta(seconds=int(seconds))
    h, rem = divmod(td.seconds, 3600)
    m, s = divmod(rem, 60)
    parts = []
    if td.days:
        parts.append(f"{td.days}d")
    if h:
        parts.append(f"{h}h")
    if m:
        parts.append(f"{m}m")
    parts.append(f"{s}s")
    return " ".join(parts)


def _fmt_bytes(n: Optional[int]) -> str:
    if not n:
        return ""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024  # type: ignore[assignment]
    return f"{n:.1f} PB"


def _fmt_dt(iso: Optional[str]) -> str:
    if not iso:
        return "–"
    try:
        dt = datetime.fromisoformat(iso)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return iso


def _duration_between(start: Optional[str], end: Optional[str]) -> Optional[float]:
    if not start or not end:
        return None
    try:
        s = datetime.fromisoformat(start)
        e = datetime.fromisoformat(end)
        return (e - s).total_seconds()
    except ValueError:
        return None


def _badge(status: str) -> str:
    """Return an HTML badge span for a status string."""
    color_map = {
        "success":     ("#1a7a3f", "#d4edda"),
        "failed":      ("#842029", "#f8d7da"),
        "skipped":     ("#856404", "#fff3cd"),
        "in_progress": ("#084298", "#cfe2ff"),
        "pending":     ("#6c757d", "#f8f9fa"),
        "created":     ("#1a7a3f", "#d4edda"),
        "dry_run":     ("#0d6efd", "#cfe2ff"),
        "error":       ("#842029", "#f8d7da"),
        "ok":          ("#1a7a3f", "#d4edda"),
    }
    fg, bg = color_map.get(status.lower(), ("#495057", "#e9ecef"))
    label = status.upper().replace("_", " ")
    return (f'<span style="display:inline-block;padding:2px 8px;border-radius:12px;'
            f'font-size:11px;font-weight:600;color:{fg};background:{bg}">{label}</span>')


# ---------------------------------------------------------------------------
# CSS / HTML skeleton
# ---------------------------------------------------------------------------

_CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
       background: #f4f6f9; color: #212529; font-size: 14px; }
header { background: linear-gradient(135deg, #1C3A5F 0%, #E8501A 100%);
         color: #fff; padding: 32px 48px; }
header .header-inner { display: flex; align-items: center; gap: 24px; }
header .db-logo-wrap { flex-shrink: 0; opacity: .95; }
header .header-text { flex: 1; }
header h1 { font-size: 32px; font-weight: 800; letter-spacing: -.3px; margin: 0; }
header .meta { margin-top: 10px; font-size: 13px; opacity: .8; }
.container { max-width: 1200px; margin: 0 auto; padding: 24px 32px; }
/* Tabs */
.tabs { display: flex; gap: 4px; margin-bottom: 0; }
.tab  { padding: 10px 22px; cursor: pointer; border-radius: 6px 6px 0 0;
        background: #dee2e6; color: #495057; font-weight: 600; font-size: 13px;
        border: 1px solid #dee2e6; border-bottom: none; user-select: none; }
.tab.active { background: #fff; color: #1C3A5F; border-color: #ced4da; }
.tab-content { display: none; background: #fff; border: 1px solid #ced4da;
               border-radius: 0 6px 6px 6px; padding: 28px; }
.tab-content.active { display: block; }
/* Cards */
.card { background: #fff; border: 1px solid #dee2e6; border-radius: 8px;
        padding: 20px 24px; margin-bottom: 20px; }
.card h2 { font-size: 15px; font-weight: 700; color: #1C3A5F;
           border-bottom: 2px solid #E8501A; padding-bottom: 8px; margin-bottom: 16px; }
/* KPI grid */
.kpi-grid { display: grid; grid-template-columns: repeat(auto-fit,minmax(150px,1fr)); gap: 12px; }
.kpi { background: #f8f9fa; border-radius: 8px; padding: 14px 16px; text-align: center;
       border: 1px solid #e9ecef; }
.kpi .value { font-size: 28px; font-weight: 700; color: #1C3A5F; line-height: 1.1; }
.kpi .label { font-size: 11px; color: #6c757d; margin-top: 4px; text-transform: uppercase; letter-spacing: .5px; }
/* Tables */
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th { background: #f1f3f5; text-align: left; padding: 9px 12px;
     font-size: 11px; font-weight: 700; color: #495057; text-transform: uppercase;
     letter-spacing: .5px; border-bottom: 2px solid #dee2e6; white-space: nowrap; }
td { padding: 9px 12px; border-bottom: 1px solid #f1f3f5; vertical-align: top; }
tr:last-child td { border-bottom: none; }
tr:hover td { background: #f8f9fa; }
.mono { font-family: 'SF Mono', Menlo, Consolas, monospace; font-size: 12px; }
.muted { color: #6c757d; }
.text-right { text-align: right; }
.text-center { text-align: center; }
/* Alerts */
.alert { padding: 12px 16px; border-radius: 6px; margin-bottom: 16px; font-size: 13px; }
.alert-danger  { background: #f8d7da; color: #842029; border: 1px solid #f5c2c7; }
.alert-warning { background: #fff3cd; color: #856404; border: 1px solid #ffecb5; }
.alert-success { background: #d1e7dd; color: #0f5132; border: 1px solid #badbcc; }
.alert-info    { background: #cff4fc; color: #055160; border: 1px solid #b6effb; }
/* Checklist */
.checklist li { list-style: none; padding: 5px 0; font-size: 13px; }
.checklist li::before { margin-right: 8px; font-size: 14px; }
.checklist li.ok::before   { content: '✅'; }
.checklist li.fail::before { content: '❌'; }
.checklist li.skip::before { content: '⏭'; }
.checklist li.todo::before { content: '📋'; }
/* Progress bar */
.progress { height: 8px; background: #e9ecef; border-radius: 4px; overflow: hidden; }
.progress-bar { height: 100%; background: #1a7a3f; border-radius: 4px; }
/* Mapping table */
.mapping-change { background: #fff8e1; }
/* File tree */
.file-tree { font-family: monospace; font-size: 12px; line-height: 1.8; }
.file-tree .dir  { color: #1C3A5F; font-weight: 600; }
.file-tree .file { color: #495057; }
.file-tree .size { color: #6c757d; margin-left: 8px; }
"""

_JS = """
function showTab(evt, id) {
  document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.getElementById(id).classList.add('active');
  evt.currentTarget.classList.add('active');
}
"""


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

def _load_export_data(session_dir: str) -> Optional[Dict]:
    return _read_json(os.path.join(session_dir, "export_status.json"))


def _load_transform_manifest(session_dir: str) -> Optional[Dict]:
    return _read_json(os.path.join(session_dir, "gcp_transform_manifest.json"))


def _load_import_log(session_dir: str) -> Optional[Dict]:
    return _read_json(os.path.join(session_dir, "import_log.json"))


def _session_dir_tree(session_dir: str, max_entries: int = 60) -> List[Dict]:
    """Return a flat list of {name, is_dir, size, rel_path} for display."""
    entries = []
    try:
        for name in sorted(os.listdir(session_dir)):
            full = os.path.join(session_dir, name)
            if os.path.isdir(full):
                sz = sum(
                    os.path.getsize(os.path.join(r, f))
                    for r, _, fs in os.walk(full) for f in fs
                    if not os.path.islink(os.path.join(r, f))
                )
                n = sum(1 for _, _, fs in os.walk(full) for _ in fs)
                entries.append({"name": name + "/", "is_dir": True, "size": sz, "files": n})
            else:
                try:
                    sz = os.path.getsize(full)
                except OSError:
                    sz = 0
                entries.append({"name": name, "is_dir": False, "size": sz, "files": 1})
            if len(entries) >= max_entries:
                break
    except OSError:
        pass
    return entries


# ---------------------------------------------------------------------------
# Section builders
# ---------------------------------------------------------------------------

def _export_section(export_data: Dict, session_dir: str) -> str:
    comps = export_data.get("components", {})
    workspace_url = escape(export_data.get("workspace_url", "–"))
    session       = escape(export_data.get("session", "–"))
    started       = _fmt_dt(export_data.get("export_start"))
    ended         = _fmt_dt(export_data.get("export_end"))
    duration_secs = _duration_between(export_data.get("export_start"), export_data.get("export_end"))

    succeeded = sum(1 for c in comps.values() if c["status"] == "success")
    failed    = sum(1 for c in comps.values() if c["status"] == "failed")
    skipped   = sum(1 for c in comps.values() if c["status"] == "skipped")
    total     = len(comps)
    total_items = sum(c.get("items_exported") or 0 for c in comps.values() if c["status"] == "success")
    total_dl    = sum(c.get("bytes_downloaded") or 0 for c in comps.values())

    pct = int(succeeded / total * 100) if total else 0

    html = f"""
<div class="card">
  <h2>📋 Export Overview</h2>
  <table style="width:auto;margin-bottom:16px">
    <tr><td style="padding:4px 16px 4px 0;color:#6c757d">Workspace</td>
        <td><strong class="mono">{workspace_url}</strong></td></tr>
    <tr><td style="padding:4px 16px 4px 0;color:#6c757d">Session</td>
        <td><strong class="mono">{session}</strong></td></tr>
    <tr><td style="padding:4px 16px 4px 0;color:#6c757d">Started</td>
        <td>{started}</td></tr>
    <tr><td style="padding:4px 16px 4px 0;color:#6c757d">Completed</td>
        <td>{ended}</td></tr>
    <tr><td style="padding:4px 16px 4px 0;color:#6c757d">Duration</td>
        <td><strong>{_fmt_dur(duration_secs)}</strong></td></tr>
  </table>
  <div class="kpi-grid">
    <div class="kpi"><div class="value">{total}</div><div class="label">Components</div></div>
    <div class="kpi"><div class="value" style="color:#1a7a3f">{succeeded}</div><div class="label">Succeeded</div></div>
    <div class="kpi"><div class="value" style="color:#842029">{failed}</div><div class="label">Failed</div></div>
    <div class="kpi"><div class="value" style="color:#856404">{skipped}</div><div class="label">Skipped</div></div>
    <div class="kpi"><div class="value">{total_items:,}</div><div class="label">Objects Exported</div></div>
    {f'<div class="kpi"><div class="value">{_fmt_bytes(int(total_dl))}</div><div class="label">DBFS Downloaded</div></div>' if total_dl else ""}
  </div>
  <div style="margin-top:16px">
    <div style="font-size:11px;color:#6c757d;margin-bottom:4px">{pct}% components succeeded</div>
    <div class="progress"><div class="progress-bar" style="width:{pct}%"></div></div>
  </div>
</div>

<div class="card">
  <h2>📦 Component Status</h2>
  <table>
    <thead><tr>
      <th>Component</th><th>Status</th>
      <th class="text-right">Items</th>
      <th class="text-right">Duration</th>
      <th>Notes / Files</th>
    </tr></thead>
    <tbody>"""

    for name, comp in comps.items():
        st     = comp.get("status", "pending")
        items  = f"{comp['items_exported']:,}" if comp.get("items_exported") is not None else "–"
        dur    = _fmt_dur(comp.get("duration_seconds"))
        dlstr  = f"<br><small class='muted'>{_fmt_bytes(comp['bytes_downloaded'])} downloaded</small>" if comp.get("bytes_downloaded") else ""
        note   = escape(comp.get("notes") or comp.get("error_message") or "")
        note_html = f"<br><small class='muted'>{note[:120]}</small>" if note else ""
        logs   = ", ".join(os.path.basename(lf) for lf in comp.get("log_files", []))
        html += f"""
      <tr>
        <td><strong>{escape(comp.get('display_name', name))}</strong></td>
        <td>{_badge(st)}</td>
        <td class="text-right mono">{items}{dlstr}</td>
        <td class="text-right mono">{dur}</td>
        <td class="muted mono" style="font-size:11px">{escape(logs)}{note_html}</td>
      </tr>"""

    html += """
    </tbody>
  </table>
</div>"""

    # Failed components alert
    failed_comps = [(n, c) for n, c in comps.items() if c["status"] == "failed"]
    if failed_comps:
        html += '<div class="alert alert-danger"><strong>❌ Failed Components – Action Required</strong><ul style="margin-top:8px;padding-left:20px">'
        for n, c in failed_comps:
            err = escape(c.get("error_message") or "unknown error")
            html += f"<li><strong>{escape(c.get('display_name', n))}</strong>: {err}</li>"
        html += "</ul></div>"

    # Artifacts tree
    tree = _session_dir_tree(session_dir)
    if tree:
        html += '<div class="card"><h2>📁 Export Artifacts</h2><div class="file-tree">'
        for e in tree:
            sz = _fmt_bytes(e["size"]) if e["size"] else ""
            if e["is_dir"]:
                html += f'<div><span class="dir">📂 {escape(e["name"])}</span><span class="size">{e["files"]} files, {sz}</span></div>'
            else:
                html += f'<div>&nbsp;&nbsp;<span class="file">📄 {escape(e["name"])}</span><span class="size">{sz}</span></div>'
        html += "</div></div>"

    return html


def _transform_section(manifest: Dict) -> str:
    transformed = manifest.get("transformed", {})
    warnings    = manifest.get("all_warnings", [])
    changes     = manifest.get("node_type_changes", [])
    processed   = _fmt_dt(manifest.get("processed_at"))
    gcp_dir     = escape(manifest.get("gcp_ready_dir", ""))

    total_recs = sum(v.get("count", 0) for v in transformed.values())
    total_warn = len(warnings)

    html = f"""
<div class="card">
  <h2>🔄 GCP Pre-processing Overview</h2>
  <table style="width:auto;margin-bottom:16px">
    <tr><td style="padding:4px 16px 4px 0;color:#6c757d">Processed at</td>
        <td>{processed}</td></tr>
    <tr><td style="padding:4px 16px 4px 0;color:#6c757d">GCP-ready files</td>
        <td class="mono">{gcp_dir}</td></tr>
  </table>
  <div class="kpi-grid">
    <div class="kpi"><div class="value">{len(transformed)}</div><div class="label">Files Transformed</div></div>
    <div class="kpi"><div class="value">{total_recs}</div><div class="label">Records Processed</div></div>
    <div class="kpi"><div class="value" style="color:{('#856404' if total_warn else '#1a7a3f')}">{total_warn}</div>
        <div class="label">Unmapped Node Types</div></div>
    <div class="kpi"><div class="value">{len(changes)}</div><div class="label">Node Type Changes</div></div>
  </div>
</div>

<div class="card">
  <h2>📝 Transformed Files</h2>
  <table>
    <thead><tr>
      <th>Source Log File</th><th class="text-right">Records</th>
      <th>GCP-Ready Output</th><th class="text-right">Warnings</th>
    </tr></thead>
    <tbody>"""

    for fname, info in transformed.items():
        w = info.get("warnings", 0)
        warn_str = f'<span style="color:#856404">⚠ {w}</span>' if w else "✓ 0"
        html += f"""
    <tr>
      <td class="mono">{escape(fname)}</td>
      <td class="text-right mono">{info.get('count', 0)}</td>
      <td class="mono muted">{escape(info.get('gcp_ready_file', ''))}</td>
      <td class="text-right">{warn_str}</td>
    </tr>"""

    html += "</tbody></table></div>"

    if changes:
        html += """
<div class="card">
  <h2>🗺️ Node Type Mappings Applied</h2>
  <table>
    <thead><tr>
      <th>Resource</th><th>Type</th><th>Field</th>
      <th>Source (Azure)</th><th></th><th>GCP Node Type</th>
    </tr></thead>
    <tbody>"""
        for ch in changes:
            html += f"""
    <tr class="mapping-change">
      <td>{escape(ch.get('name', ''))}</td>
      <td>{_badge(ch.get('kind', ''))}</td>
      <td class="mono muted">{escape(ch.get('field', ''))}</td>
      <td class="mono" style="color:#842029">{escape(ch.get('from', ''))}</td>
      <td style="color:#6c757d">→</td>
      <td class="mono" style="color:#1a7a3f"><strong>{escape(ch.get('to', ''))}</strong></td>
    </tr>"""
        html += "</tbody></table></div>"

    if warnings:
        html += '<div class="alert alert-warning"><strong>⚠ Unmapped Node Types (kept as-is)</strong><ul style="margin-top:8px;padding-left:20px">'
        for w in warnings:
            html += f"<li class='mono' style='font-size:12px'>{escape(w)}</li>"
        html += "</ul><small>Edit node_type_mapping.csv to add missing mappings.</small></div>"

    return html


def _import_section(import_log: Optional[Dict], transform_manifest: Optional[Dict]) -> str:
    if not import_log:
        return '<div class="alert alert-info">ℹ Import log not yet available. Run import_gcp.sh to generate.</div>'

    target    = escape(import_log.get("target_workspace", "–"))
    session   = escape(import_log.get("session", "–"))
    started   = _fmt_dt(import_log.get("started_at"))
    ended     = _fmt_dt(import_log.get("ended_at"))
    dur_secs  = _duration_between(import_log.get("started_at"), import_log.get("ended_at"))
    steps     = import_log.get("steps", [])
    dry_run   = import_log.get("dry_run", False)

    total_created = sum(s.get("created") or 0 for s in steps)
    total_skipped = sum(s.get("skipped") or 0 for s in steps)
    total_failed  = sum(s.get("failed")  or 0 for s in steps)

    html = f"""
<div class="card">
  <h2>🚀 Import Overview</h2>
  {'<div class="alert alert-info" style="margin-bottom:16px">🔵 <strong>DRY RUN MODE</strong> – No resources were actually created.</div>' if dry_run else ""}
  <table style="width:auto;margin-bottom:16px">
    <tr><td style="padding:4px 16px 4px 0;color:#6c757d">Target Workspace</td>
        <td><strong class="mono">{target}</strong></td></tr>
    <tr><td style="padding:4px 16px 4px 0;color:#6c757d">Session</td>
        <td><strong class="mono">{session}</strong></td></tr>
    <tr><td style="padding:4px 16px 4px 0;color:#6c757d">Started</td>
        <td>{started}</td></tr>
    <tr><td style="padding:4px 16px 4px 0;color:#6c757d">Completed</td>
        <td>{ended}</td></tr>
    <tr><td style="padding:4px 16px 4px 0;color:#6c757d">Duration</td>
        <td><strong>{_fmt_dur(dur_secs)}</strong></td></tr>
  </table>
  <div class="kpi-grid">
    <div class="kpi"><div class="value">{len(steps)}</div><div class="label">Steps Run</div></div>
    <div class="kpi"><div class="value" style="color:#1a7a3f">{total_created}</div>
        <div class="label">{'Would Create' if dry_run else 'Created'}</div></div>
    <div class="kpi"><div class="value" style="color:#856404">{total_skipped}</div>
        <div class="label">Skipped</div></div>
    <div class="kpi"><div class="value" style="color:#842029">{total_failed}</div>
        <div class="label">Failed</div></div>
  </div>
</div>

<div class="card">
  <h2>📋 Import Steps</h2>
  <table>
    <thead><tr>
      <th>Step</th><th>Component</th><th>Handler</th>
      <th class="text-right">Total</th>
      <th class="text-right">{'Dry-run' if dry_run else 'Created'}</th>
      <th class="text-right">Skipped</th>
      <th class="text-right">Failed</th>
      <th>Duration</th><th>Notes</th>
    </tr></thead>
    <tbody>"""

    for step in steps:
        num       = step.get("step", "")
        comp      = escape(step.get("component", ""))
        handler   = escape(step.get("handler", ""))
        st        = step.get("status", "pending")
        total_n   = step.get("total", "–")
        created   = step.get("created", step.get("dry_run", "–"))
        skipped   = step.get("skipped", "–")
        failed_n  = step.get("failed", 0)
        dur       = _fmt_dur(step.get("duration_seconds"))
        notes     = escape(step.get("notes") or step.get("error", "") or "")
        errs      = step.get("errors", [])
        err_html  = ""
        if errs:
            err_html = "<br>" + "<br>".join(
                f"<small style='color:#842029'>{escape(e[:100])}</small>" for e in errs[:3]
            )

        html += f"""
    <tr>
      <td class="text-center muted">{num}</td>
      <td><strong>{comp}</strong></td>
      <td class="muted" style="font-size:12px">{handler}</td>
      <td class="text-right mono">{total_n}</td>
      <td class="text-right mono" style="color:#1a7a3f">{created}</td>
      <td class="text-right mono" style="color:#856404">{skipped}</td>
      <td class="text-right mono" style="color:{'#842029' if failed_n else 'inherit'}">{failed_n}</td>
      <td class="mono muted">{dur}</td>
      <td class="muted" style="font-size:11px">{notes}{err_html}</td>
    </tr>"""

    html += "</tbody></table></div>"

    # Errors
    all_errors = [e for s in steps for e in s.get("errors", [])]
    if all_errors:
        html += '<div class="alert alert-danger"><strong>❌ Import Errors</strong><ul style="margin-top:8px;padding-left:20px">'
        for e in all_errors[:20]:
            html += f"<li class='mono' style='font-size:12px'>{escape(e)}</li>"
        html += "</ul></div>"

    return html


def _paths_section(session_dir: str, export_data: Optional[Dict], import_log: Optional[Dict]) -> str:
    """
    Generate a "Paths & Logs" tab listing every important file/directory in the
    session, its purpose, and a ⭐ priority indicator for the key files to check first.
    """
    # Priority groups: (relative_path, label, description, priority, category)
    # priority: "key" = ⭐ must-read, "info" = useful, "debug" = deep-dive only
    CATEGORIES = [
        ("Reports", [
            ("import_report.html",         "Migration Report (this file)",                "Combined export + GCP transform + import HTML dashboard",                   "key"),
            ("export_report.html",         "Export Report",                               "Export-only HTML dashboard with component status and artifact tree",        "key"),
            ("compare_report.html",        "Compare Report",                              "Side-by-side comparison of export vs import counts; errors highlighted",    "key"),
        ]),
        ("Import Execution Logs", [
            ("import_run_*.log",           "Raw Import Log ⭐",                           "Full stdout/stderr of the import run — search here for any error messages",  "key"),
            ("import_log.json",            "Structured Import Log",                       "Machine-readable per-step status, counts, durations, and errors",           "key"),
            ("sp_mapping.json",            "Service Principal Mapping",                   "Old SP application-id → new GCP SP application-id translation table",       "info"),
            ("sp_migration_report.json",   "SP Migration Report",                         "Detail of every SP created, reused, or failed during SP migration",         "info"),
        ]),
        ("Export Session Data", [
            ("export_status.json",         "Export Status",                               "Per-component export status, item counts, durations, and error messages",   "key"),
            ("source_info.txt",            "Source Workspace URL",                        "URL of the Azure source workspace (read by the import pipeline)",           "info"),
        ]),
        ("GCP Pre-processing", [
            ("gcp_transform_manifest.json","GCP Transform Manifest ⭐",                   "Full transformation summary: node type changes, warnings, and record counts","key"),
            ("gcp_ready/jobs.json",        "GCP-Ready Jobs",                              "Final job payloads after preprocessing — inspect before import",            "key"),
            ("gcp_ready/clusters.json",    "GCP-Ready Clusters",                          "Final cluster specs after preprocessing",                                   "info"),
            ("gcp_ready/instance_pools.json","GCP-Ready Instance Pools",                  "Final pool specs with GCP attributes and zone hints applied",               "info"),
            ("jobs.log",                   "Jobs Log (modified in-place)",                "Job definitions — modified by preprocessor; check .original for backup",    "info"),
            ("jobs.log.original",          "Jobs Log (original backup)",                  "Pre-transformation backup created before GCP preprocessing",                "info"),
            ("clusters.log",               "Clusters Log (modified in-place)",            "Cluster definitions after preprocessing",                                   "info"),
            ("instance_pools.log",         "Instance Pools Log (modified in-place)",      "Pool definitions after preprocessing",                                      "info"),
        ]),
        ("Core Export Artifacts", [
            ("users.log",                  "Users",                                       "SCIM user records from the source workspace",                               "info"),
            ("groups/",                    "Groups",                                       "One JSON file per group",                                                   "info"),
            ("artifacts/",                 "Notebooks",                                   "Downloaded notebook content (DBC / SOURCE / HTML format)",                  "info"),
            ("secret_scopes/",             "Secret Scopes",                               "Secret scope definitions and ACL files",                                    "info"),
            ("acl_notebooks.log",          "Notebook ACLs",                               "Permission objects for notebooks and directories",                          "info"),
            ("acl_jobs.log",               "Job ACLs",                                    "Permission objects for job resources",                                      "info"),
        ]),
        ("Extra Components", [
            ("lakeview_dashboards/",       "AI/BI Dashboards",                            "Lakeview dashboard content JSON files, one per dashboard",                  "info"),
            ("genie_spaces.json",          "Genie AI Spaces ⚠",                           "Space metadata + backing dashboard content — manual recreation required",   "info"),
            ("workspace_files/",           "Workspace Files",                             "Non-notebook files (.py .md .sql .yaml .toml .json .csv …) with folder structure","info"),
            ("workspace_files_manifest.json","Workspace Files Manifest",                  "Index of all exported FILE-type workspace objects with paths and sizes",     "info"),
            ("sql_warehouses.json",        "SQL Warehouses",                              "Warehouse configuration objects",                                           "info"),
            ("dlt_pipelines.json",         "DLT Pipelines",                              "Delta Live Tables pipeline definitions",                                    "info"),
            ("serving_endpoints.json",     "Serving Endpoints",                           "Model serving endpoint configs (user-managed only)",                        "info"),
        ]),
        ("Unity Catalog", [
            ("uc_export/import_manifest.json","UC Deployment Plan ⭐",                    "Ordered list of SQL files to run and object counts per catalog/schema",     "key"),
            ("uc_export/",                 "UC DDL Files",                                "Numbered SQL files (01–07) for storage credentials, catalogs, schemas, tables, grants","info"),
        ]),
        ("Debug & App Logs", [
            ("app_logs/wm_logs.log",       "Migrate Tool Full Log",                       "Complete DEBUG-level log from databrickslabs/migrate — use for deep debugging","debug"),
            ("app_logs/",                  "App Logs Directory",                          "All per-component error logs and the main migrate tool log",                "debug"),
            ("checkpoint/",               "Checkpoint Files",                             "Resume state — delete to force a full re-export from scratch",              "debug"),
        ]),
    ]

    # Priority styling
    PRIORITY_STYLE = {
        "key":   ("⭐", "#fff8e1", "#e65100", "Must review"),
        "info":  ("📄", "#f8f9fa", "#495057", "Reference"),
        "debug": ("🔧", "#f8f9fa", "#6c757d", "Debug only"),
    }

    abs_session = os.path.abspath(session_dir)

    html = f"""
<div class="card">
  <h2>📂 Session Directory</h2>
  <table style="width:auto;margin-bottom:16px">
    <tr><td style="padding:4px 16px 4px 0;color:#6c757d">Base path</td>
        <td><code class="mono" style="background:#f1f3f5;padding:3px 8px;border-radius:4px">{escape(abs_session)}</code></td></tr>
  </table>
  <div class="alert alert-info" style="margin-bottom:0">
    ⭐ = <strong>Key files to check first</strong> &nbsp;|&nbsp;
    📄 = Reference files &nbsp;|&nbsp;
    🔧 = Deep-debug / advanced
  </div>
</div>"""

    for category, entries in CATEGORIES:
        html += f"""
<div class="card">
  <h2>{'⭐ ' if any(p == 'key' for _, _, _, p in entries) else ''}{escape(category)}</h2>
  <table>
    <thead><tr>
      <th style="width:28px"></th>
      <th>File / Directory</th>
      <th>Label</th>
      <th>Purpose</th>
      <th style="width:80px">Exists</th>
    </tr></thead>
    <tbody>"""

        for rel_path, label, description, priority in entries:
            icon, row_bg, text_color, _ = PRIORITY_STYLE[priority]
            # Resolve wildcard paths (e.g. import_run_*.log)
            full_path = os.path.join(session_dir, rel_path)
            if "*" in rel_path:
                import glob as _glob
                matches = sorted(_glob.glob(full_path))
                exists = bool(matches)
                display_path = rel_path
                if matches:
                    display_path = os.path.basename(matches[-1])  # most recent match
                    full_path = matches[-1]
            else:
                exists = os.path.exists(full_path)
                display_path = rel_path

            exists_badge = (
                '<span style="color:#1a7a3f;font-weight:600">✓ exists</span>' if exists
                else '<span style="color:#adb5bd">— not yet</span>'
            )
            abs_path = os.path.join(abs_session, display_path)

            html += f"""
    <tr style="background:{row_bg}">
      <td class="text-center" style="font-size:14px">{icon}</td>
      <td>
        <code class="mono" style="color:{text_color};font-size:12px">{escape(display_path)}</code><br>
        <small class="muted" style="font-size:10px">{escape(abs_path)}</small>
      </td>
      <td style="font-weight:600;font-size:12px;color:{text_color}">{escape(label)}</td>
      <td style="font-size:12px;color:#495057">{escape(description)}</td>
      <td>{exists_badge}</td>
    </tr>"""

        html += "</tbody></table></div>"

    # Quick-copy commands block
    html += f"""
<div class="card">
  <h2>🖥️ Quick Commands</h2>
  <div style="font-size:12px;margin-bottom:8px;color:#6c757d">Copy-paste these commands to inspect key files directly from your terminal.</div>
  <table>
    <thead><tr><th>Task</th><th>Command</th></tr></thead>
    <tbody>
    <tr><td>Open the migration report in browser</td>
        <td><code class="mono">open "{escape(abs_session)}/import_report.html"</code></td></tr>
    <tr><td>Open the compare report</td>
        <td><code class="mono">open "{escape(abs_session)}/compare_report.html"</code></td></tr>
    <tr><td>Search import log for errors</td>
        <td><code class="mono">grep -i "error\\|failed\\|exception" "{escape(abs_session)}"/import_run_*.log | head -30</code></td></tr>
    <tr><td>Check GCP transform warnings</td>
        <td><code class="mono">python3 -c "import json; d=json.load(open('{escape(abs_session)}/gcp_transform_manifest.json')); [print(w) for w in d.get('all_warnings',[])]"</code></td></tr>
    <tr><td>List Genie spaces needing manual recreation</td>
        <td><code class="mono">python3 -c "import json; [print(s['title'], '|', s.get('warehouse_id','?')) for s in json.load(open('{escape(abs_session)}/genie_spaces.json')).get('spaces',[])]"</code></td></tr>
    <tr><td>Check which export components failed</td>
        <td><code class="mono">python3 -c "import json; [print(k,v['status']) for k,v in json.load(open('{escape(abs_session)}/export_status.json'))['components'].items() if v['status']!='success']"</code></td></tr>
    <tr><td>Check import step results</td>
        <td><code class="mono">python3 -c "import json; [print(s['component'],s['status'],s.get('failed',0)) for s in json.load(open('{escape(abs_session)}/import_log.json')).get('steps',[])]"</code></td></tr>
    <tr><td>List all GCP-ready jobs (names only)</td>
        <td><code class="mono">python3 -c "import json; [print(j.get('settings',j).get('name','?')) for j in json.load(open('{escape(abs_session)}/gcp_ready/jobs.json'))]"</code></td></tr>
    </tbody>
  </table>
</div>"""

    return html


def _checklist_section(export_data: Optional[Dict], import_log: Optional[Dict]) -> str:
    html = '<div class="card"><h2>✅ Migration Checklist</h2><ul class="checklist">'

    def _item(ok: bool, text: str, skip: bool = False) -> str:
        cls = "ok" if ok else ("skip" if skip else "todo")
        return f"<li class='{cls}'>{escape(text)}</li>"

    # Export checks
    if export_data:
        comps = export_data.get("components", {})
        def _ok(name: str) -> bool:
            return comps.get(name, {}).get("status") == "success"
        html += "<li style='font-weight:700;margin-top:8px;font-size:13px'>Export Phase</li>"
        html += _item(_ok("users"),            "Users and groups exported")
        html += _item(_ok("notebooks"),        "Notebook content downloaded")
        html += _item(_ok("clusters"),         "Cluster configurations exported")
        html += _item(_ok("jobs"),             "Job configurations exported")
        html += _item(_ok("metastore"),        "Hive metastore exported")
        html += _item(_ok("dbfs_libraries"),   "DBFS libraries downloaded")
        html += _item(_ok("sql_warehouses"),   "SQL warehouses exported")
        html += _item(_ok("dlt_pipelines"),    "DLT pipelines exported")
        html += _item(_ok("lakeview_dashboards"), "AI/BI dashboards exported")
        html += _item(_ok("genie_spaces"),     "Genie AI spaces exported")
        html += _item(_ok("serving_endpoints"),"Model serving endpoints exported")
        html += _item(_ok("unity_catalog"),    "Unity Catalog DDL exported")

    if import_log:
        html += "<li style='font-weight:700;margin-top:8px;font-size:13px'>Import Phase</li>"
        steps = {s.get("component", ""): s for s in import_log.get("steps", [])}
        def _imp_ok(name: str) -> bool:
            s = steps.get(name, {})
            return s.get("status") == "success" and s.get("failed", 0) == 0

        html += _item(steps.get("preprocess", {}).get("status") == "success",
                      "Logs pre-processed for GCP (jobs.log, clusters.log, instance_pools.log)")
        del_step = steps.get("delete_existing_jobs", {})
        if del_step:
            html += _item(del_step.get("status") in ("success", "dry_run"),
                          f"Existing jobs cleared before import "
                          f"({del_step.get('notes', '')})")
        html += _item(_imp_ok("migrate_tool"),
                      "Migrate tool import pipeline complete (users, notebooks, clusters, jobs, metastore)")
        html += _item(_imp_ok("sql_warehouses"),   "SQL warehouses imported")
        html += _item(_imp_ok("dlt_pipelines"),    "DLT pipelines imported")
        html += _item(_imp_ok("repos"),            "Git repos imported")
        html += _item(_imp_ok("lakeview_dashboards"), "AI/BI dashboards imported")
        html += _item(_imp_ok("genie_spaces"),     "Genie AI spaces imported")
        html += _item(_imp_ok("serving_endpoints"),"Model serving endpoints imported")
        html += _item(False, "Deploy Unity Catalog SQL files (gcp_ready/uc_export/ → target workspace)", skip=True)
        html += _item(False, "Validate import: compare source and target workspaces", skip=True)

    html += "</ul></div>"
    return html


# ---------------------------------------------------------------------------
# Main HTML assembler
# ---------------------------------------------------------------------------

def _build_html(
    title: str,
    session_dir: str,
    export_data: Optional[Dict],
    transform_manifest: Optional[Dict],
    import_log: Optional[Dict],
    include_import_tab: bool,
) -> str:
    session   = (export_data or import_log or {}).get("session", os.path.basename(session_dir))
    workspace = (export_data or {}).get("workspace_url") or (import_log or {}).get("target_workspace") or "–"
    generated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    tabs_html = '<div class="tabs">'
    tabs_html += f'<div class="tab active" onclick="showTab(event,\'tab-export\')">📤 Export</div>'
    if transform_manifest:
        tabs_html += f'<div class="tab" onclick="showTab(event,\'tab-transform\')">🔄 GCP Transform</div>'
    if include_import_tab:
        tabs_html += f'<div class="tab" onclick="showTab(event,\'tab-import\')">📥 Import</div>'
    tabs_html += f'<div class="tab" onclick="showTab(event,\'tab-paths\')">📂 Paths & Logs</div>'
    tabs_html += f'<div class="tab" onclick="showTab(event,\'tab-checklist\')">✅ Checklist</div>'
    tabs_html += '</div>'

    export_content = _export_section(export_data, session_dir) if export_data else \
        '<div class="alert alert-info">No export_status.json found.</div>'

    transform_content = _transform_section(transform_manifest) if transform_manifest else ""
    import_content    = _import_section(import_log, transform_manifest) if include_import_tab else ""
    paths_content     = _paths_section(session_dir, export_data, import_log)
    checklist_content = _checklist_section(export_data, import_log)

    body = f"""
{tabs_html}
<div id="tab-export" class="tab-content active">{export_content}</div>"""

    if transform_manifest:
        body += f'<div id="tab-transform" class="tab-content">{transform_content}</div>'
    if include_import_tab:
        body += f'<div id="tab-import" class="tab-content">{import_content}</div>'

    body += f'<div id="tab-paths" class="tab-content">{paths_content}</div>'
    body += f'<div id="tab-checklist" class="tab-content">{checklist_content}</div>'

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{escape(title)}</title>
<style>{_CSS}</style>
</head>
<body>
<header>
  <div class="header-inner">
    <div class="db-logo-wrap">
      {_db_icon_svg(64)}
    </div>
    <div class="header-text">
      <h1>{escape(title)}</h1>
      <div class="meta">
        Session: <strong>{escape(str(session))}</strong>
        &nbsp;|&nbsp; Workspace: <strong>{escape(str(workspace))}</strong>
        &nbsp;|&nbsp; Generated: {generated}
      </div>
    </div>
  </div>
</header>
<div class="container">
{body}
</div>
<script>{_JS}</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_export_html(session_dir: str) -> str:
    """
    Generate export_report.html from export_status.json in session_dir.
    Returns the path to the written HTML file.
    """
    export_data = _load_export_data(session_dir)
    transform_manifest = _load_transform_manifest(session_dir)

    html = _build_html(
        title="Databricks Workspace Export Report",
        session_dir=session_dir,
        export_data=export_data,
        transform_manifest=transform_manifest,
        import_log=None,
        include_import_tab=False,
    )
    out_path = os.path.join(session_dir, "export_report.html")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    return out_path


def generate_import_html(session_dir: str) -> str:
    """
    Generate import_report.html combining export + GCP transform + import data.
    Returns the path to the written HTML file.
    """
    export_data        = _load_export_data(session_dir)
    transform_manifest = _load_transform_manifest(session_dir)
    import_log         = _load_import_log(session_dir)

    html = _build_html(
        title="Databricks Workspace Migration Report",
        session_dir=session_dir,
        export_data=export_data,
        transform_manifest=transform_manifest,
        import_log=import_log,
        include_import_tab=True,
    )
    out_path = os.path.join(session_dir, "import_report.html")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    return out_path


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import sys

    p = argparse.ArgumentParser(description="Generate HTML migration report")
    p.add_argument("session_dir", help="Path to the export/import session directory")
    p.add_argument("--mode", choices=["export", "import", "both"], default="both")
    args = p.parse_args()

    sdir = args.session_dir
    if not os.path.isdir(sdir):
        print(f"Directory not found: {sdir}", file=sys.stderr)
        sys.exit(1)

    if args.mode in ("export", "both"):
        path = generate_export_html(sdir)
        print(f"Export HTML : {path}")
    if args.mode in ("import", "both"):
        path = generate_import_html(sdir)
        print(f"Import HTML : {path}")
