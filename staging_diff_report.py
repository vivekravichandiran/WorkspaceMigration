#!/usr/bin/env python3
"""
staging_diff_report.py
======================
Compares a raw Databricks export session with its staged (transformed) copy
and produces a full pre/post change report in HTML + Excel.

Usage:
    python3 staging_diff_report.py \
        --raw-dir   logs/EXPORT_20260607 \
        --stage-dir logs_staging/EXPORT_20260607 \
        [--html-output  staging_report.html] \
        [--excel-output staging_report.xlsx]

The script reads every paired file (clusters.log, jobs.log, acl_*.log,
users.log, groups/*, user_dirs.log, instance_pools.log …) and performs a
deep-diff to surface every field that changed.  Changes are tagged with a
human-readable category (User Remapping, Node Type Mapping, GCP Attributes,
Cluster Sanitisation, Job Transform, ACL Change, etc.).
"""

import argparse, copy, json, os, re, sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
    _EXCEL = True
except ImportError:
    _EXCEL = False


# ─────────────────────────────────────────────────────────────────────────────
# Change record
# ─────────────────────────────────────────────────────────────────────────────

CATEGORIES = {
    "user_remap":    ("👤 User Remapping",      "#dbeafe"),
    "node_type":     ("🖥 Node Type Mapping",    "#dcfce7"),
    "gcp_attr":      ("☁ GCP Attributes",       "#fef9c3"),
    "cluster_san":   ("✏ Cluster Sanitisation",  "#ede9fe"),
    "job_transform": ("⚙ Job Transform",         "#fce7f3"),
    "acl_change":    ("🔐 ACL Change",            "#fee2e2"),
    "library":       ("📦 Library Removed",       "#ffedd5"),
    "sas_token":     ("🔑 SAS/Token Removed",     "#f1f5f9"),
    "spark_config":  ("⚡ Spark Config Change",   "#ecfdf5"),
    "elastic_disk":  ("💾 Elastic Disk Added",    "#f0fdf4"),
    "path_remap":    ("📁 Path Remapped",          "#faf5ff"),
    "field_added":   ("➕ Field Added",            "#f0fdf4"),
    "field_removed": ("➖ Field Removed",          "#fff7ed"),
    "field_changed": ("🔄 Value Changed",          "#f8fafc"),
    "excluded":      ("⛔ Object Excluded",        "#fef2f2"),
}


class Change:
    __slots__ = ("file", "object_id", "object_label", "field_path", "category",
                 "category_label", "color", "before", "after")

    def __init__(self, file_: str, object_id: str, object_label: str,
                 field_path: str, category: str, before: Any, after: Any):
        self.file         = file_
        self.object_id    = object_id
        self.object_label = object_label
        self.field_path   = field_path
        self.category     = category
        info              = CATEGORIES.get(category, (category, "#f8fafc"))
        self.category_label = info[0]
        self.color        = info[1]
        self.before       = before
        self.after        = after


# ─────────────────────────────────────────────────────────────────────────────
# Deep-diff helpers
# ─────────────────────────────────────────────────────────────────────────────

def _flat_diff(before: Any, after: Any, path: str = "") -> List[Tuple[str, Any, Any]]:
    """Return list of (dotted.path, before_val, after_val) for every leaf diff."""
    diffs = []
    if isinstance(before, dict) and isinstance(after, dict):
        all_keys = set(before) | set(after)
        for k in sorted(all_keys):
            sub = f"{path}.{k}" if path else k
            if k not in before:
                diffs.append((sub, None, after[k]))
            elif k not in after:
                diffs.append((sub, before[k], None))
            else:
                diffs.extend(_flat_diff(before[k], after[k], sub))
    elif isinstance(before, list) and isinstance(after, list):
        # Summarise list changes at the parent level rather than item-by-item
        if before != after:
            diffs.append((path, before, after))
    else:
        if before != after:
            diffs.append((path, before, after))
    return diffs


def _classify(field_path: str, before: Any, after: Any) -> str:
    """Tag a diff with the most specific change category."""
    fp = field_path.lower()
    bstr = str(before).lower() if before is not None else ""
    astr = str(after).lower()  if after  is not None else ""

    if "gcp_attributes" in fp:
        return "gcp_attr"
    if fp == "enable_elastic_disk":
        return "elastic_disk"
    if "node_type_id" in fp or "driver_node_type_id" in fp:
        return "node_type"
    if any(x in fp for x in ("user_name", "creator_user_name", "run_as_user_name",
                               "username", "email", "principal", "display_name",
                               "owner", "author", "committer", "members")):
        return "user_remap"
    if "path" in fp and "/users/" in (bstr + astr).lower():
        return "path_remap"
    if "path" in fp:
        return "path_remap"
    if "libraries" in fp:
        return "library"
    if any(x in fp for x in ("azure", "wasb", "abfs", "s3", "sas", "account.key")):
        return "sas_token"
    if "spark_conf" in fp:
        return "spark_config"
    if "access_control_list" in fp or fp.endswith("acl"):
        return "acl_change"
    if any(x in fp for x in ("cluster_name", "instance_pool_name", "sanitise")):
        return "cluster_san"
    if any(x in fp for x in ("job_id", "schedule", "name", "email_notifications",
                               "run_as", "new_cluster")):
        return "job_transform"
    if after is None:
        return "field_removed"
    if before is None:
        return "field_added"
    return "field_changed"


# ─────────────────────────────────────────────────────────────────────────────
# File readers
# ─────────────────────────────────────────────────────────────────────────────

def _read_jsonl(path: str) -> List[Dict]:
    recs = []
    if not os.path.isfile(path):
        return recs
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    recs.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return recs


def _read_json(path: str) -> Any:
    if not os.path.isfile(path):
        return None
    with open(path, encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return None


def _fmt(v: Any, maxlen: int = 120) -> str:
    if v is None:
        return "(removed)"
    s = json.dumps(v, ensure_ascii=False) if not isinstance(v, str) else v
    return s[:maxlen] + ("…" if len(s) > maxlen else "")


# ─────────────────────────────────────────────────────────────────────────────
# Per-file diffing
# ─────────────────────────────────────────────────────────────────────────────

def _diff_jsonl_by_key(
    raw_path: str, stage_path: str, file_label: str,
    key_fn, label_fn, changes: List[Change],
) -> None:
    raw_recs   = _read_jsonl(raw_path)
    stage_recs = _read_jsonl(stage_path)

    raw_map   = {key_fn(r): r for r in raw_recs   if key_fn(r)}
    stage_map = {key_fn(r): r for r in stage_recs if key_fn(r)}

    for obj_id, raw_r in raw_map.items():
        label = label_fn(raw_r)
        if obj_id not in stage_map:
            changes.append(Change(file_label, obj_id, label,
                                  "(object)", "excluded", raw_r, None))
            continue
        stage_r = stage_map[obj_id]
        for fp, bv, av in _flat_diff(raw_r, stage_r):
            if fp.endswith(".original") or ".original" in fp:
                continue
            cat = _classify(fp, bv, av)
            changes.append(Change(file_label, obj_id, label, fp, cat, bv, av))


def _diff_groups(raw_dir: str, stage_dir: str, changes: List[Change]) -> None:
    rg = os.path.join(raw_dir,   "groups")
    sg = os.path.join(stage_dir, "groups")
    if not os.path.isdir(rg) or not os.path.isdir(sg):
        return
    for fname in sorted(os.listdir(rg)):
        rpath = os.path.join(rg, fname)
        spath = os.path.join(sg, fname)
        if not os.path.isfile(rpath):
            continue
        r = _read_json(rpath)
        s = _read_json(spath) if os.path.isfile(spath) else None
        if r is None:
            continue
        label = r.get("displayName", fname)
        obj_id = f"group:{fname}"
        if s is None:
            changes.append(Change("groups/", obj_id, label,
                                  "(object)", "excluded", r, None))
            continue
        for fp, bv, av in _flat_diff(r, s):
            cat = _classify(fp, bv, av)
            changes.append(Change("groups/", obj_id, label, fp, cat, bv, av))


# ─────────────────────────────────────────────────────────────────────────────
# Main diff runner
# ─────────────────────────────────────────────────────────────────────────────

def collect_all_changes(raw_dir: str, stage_dir: str) -> List[Change]:
    changes: List[Change] = []

    def jp(f):  return os.path.join(raw_dir,   f)
    def sp(f):  return os.path.join(stage_dir, f)

    # clusters.log
    _diff_jsonl_by_key(
        jp("clusters.log"), sp("clusters.log"), "clusters.log",
        lambda r: r.get("cluster_name") or r.get("cluster_id"),
        lambda r: r.get("cluster_name", r.get("cluster_id", "?")),
        changes,
    )
    # instance_pools.log
    _diff_jsonl_by_key(
        jp("instance_pools.log"), sp("instance_pools.log"), "instance_pools.log",
        lambda r: r.get("instance_pool_name") or r.get("instance_pool_id"),
        lambda r: r.get("instance_pool_name", "?"),
        changes,
    )
    # jobs.log
    def _job_key(r):
        name = r.get("settings", {}).get("name", "")
        return re.sub(r":::\d+$", "", name)
    _diff_jsonl_by_key(
        jp("jobs.log"), sp("jobs.log"), "jobs.log",
        _job_key,
        lambda r: _job_key(r) or f"job:{r.get('job_id','')}",
        changes,
    )
    # ACL files
    for fname in ("acl_clusters.log", "acl_jobs.log", "acl_notebooks.log",
                  "acl_directories.log", "acl_repos.log", "secret_scopes_acls.log"):
        _diff_jsonl_by_key(
            jp(fname), sp(fname), fname,
            lambda r: r.get("object_id"),
            lambda r: r.get("object_id", "?"),
            changes,
        )
    # users.log
    _diff_jsonl_by_key(
        jp("users.log"), sp("users.log"), "users.log",
        lambda r: r.get("userName") or r.get("id"),
        lambda r: r.get("userName", r.get("id", "?")),
        changes,
    )
    # user_dirs.log & user_workspace.log
    for fname in ("user_dirs.log", "user_workspace.log"):
        _diff_jsonl_by_key(
            jp(fname), sp(fname), fname,
            lambda r: r.get("object_id") or r.get("path"),
            lambda r: r.get("path", str(r.get("object_id", "?"))),
            changes,
        )
    # groups
    _diff_groups(raw_dir, stage_dir, changes)

    # Filter out noise: .original backup fields and zero-diff records
    return [c for c in changes if not (c.field_path.endswith(".original")
                                       or ".original" in c.field_path)]


# ─────────────────────────────────────────────────────────────────────────────
# Summary aggregation
# ─────────────────────────────────────────────────────────────────────────────

def build_summary(changes: List[Change]):
    from collections import defaultdict, Counter
    by_file  = defaultdict(list)
    by_cat   = Counter()
    for c in changes:
        by_file[c.file].append(c)
        by_cat[c.category] += 1

    summary_rows = []
    for fname in sorted(by_file):
        grp        = by_file[fname]
        obj_ids    = {c.object_id for c in grp}
        cat_counts = Counter(c.category for c in grp)
        cats       = ", ".join(f"{CATEGORIES.get(k,('',))[0]} ×{v}"
                               for k, v in cat_counts.most_common(5))
        summary_rows.append({
            "file":         fname,
            "objects":      len(obj_ids),
            "changes":      len(grp),
            "categories":   cats,
        })

    cat_summary = [{"category": CATEGORIES.get(k, (k,))[0],
                    "color":    CATEGORIES.get(k, ("","#fff"))[1],
                    "count":    v}
                   for k, v in by_cat.most_common()]
    return summary_rows, cat_summary, by_file


# ─────────────────────────────────────────────────────────────────────────────
# HTML report
# ─────────────────────────────────────────────────────────────────────────────

_DBX_LOGO = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 200 40" height="32">
  <text x="0" y="30" font-family="Arial,sans-serif" font-size="28" font-weight="bold"
        fill="#FF3621">Databricks</text></svg>"""

def _esc(s: str) -> str:
    return (str(s).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
                  .replace('"',"&quot;"))


def render_html(changes: List[Change], raw_dir: str, stage_dir: str,
                output_path: str) -> str:
    summary_rows, cat_summary, by_file = build_summary(changes)
    ts   = datetime.now().strftime("%Y-%m-%d %H:%M")
    sess = os.path.basename(raw_dir)

    # Build detail cards per file
    detail_cards = []
    for fname in sorted(by_file):
        grp = by_file[fname]
        from collections import defaultdict
        by_obj = defaultdict(list)
        for c in grp:
            by_obj[(c.object_id, c.object_label)].append(c)

        rows_html = []
        for (oid, olabel), obj_changes in sorted(by_obj.items()):
            rows_html.append(f"""
            <tr class="obj-row" onclick="toggleObj(this)">
              <td colspan="4" class="obj-header">
                <span class="caret">▶</span>
                <strong>{_esc(olabel)}</strong>
                <small class="obj-id"> — {_esc(str(oid))}</small>
                <span class="badge">{len(obj_changes)} change(s)</span>
              </td>
            </tr>""")
            for ch in obj_changes:
                bfmt = _esc(_fmt(ch.before))
                afmt = _esc(_fmt(ch.after))
                rows_html.append(f"""
            <tr class="change-row hidden">
              <td class="field-path">{_esc(ch.field_path)}</td>
              <td><span class="cat-badge" style="background:{ch.color}">{_esc(ch.category_label)}</span></td>
              <td class="before-val">{bfmt}</td>
              <td class="after-val">{afmt}</td>
            </tr>""")

        detail_cards.append(f"""
        <div class="file-card" id="card-{_esc(fname.replace('/','_').replace('.','_'))}">
          <div class="file-header" onclick="toggleCard(this)">
            <span class="file-caret">▶</span>
            <strong>{_esc(fname)}</strong>
            <span class="badge">{len({c.object_id for c in grp})} objects · {len(grp)} changes</span>
          </div>
          <div class="file-body hidden">
            <table class="diff-table">
              <thead><tr>
                <th>Field path</th><th>Change type</th>
                <th>Before</th><th>After</th>
              </tr></thead>
              <tbody>{''.join(rows_html)}</tbody>
            </table>
          </div>
        </div>""")

    # Summary table rows
    sum_rows_html = "".join(f"""
        <tr>
          <td>{_esc(r['file'])}</td>
          <td class="num">{r['objects']}</td>
          <td class="num">{r['changes']}</td>
          <td class="cats">{r['categories']}</td>
        </tr>""" for r in summary_rows)

    # Category breakdown pills
    cat_pills = "".join(
        f'<span class="cat-pill" style="background:{c["color"]}">'
        f'{_esc(c["category"])} <strong>{c["count"]}</strong></span>'
        for c in cat_summary
    )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Staging Diff Report — {_esc(sess)}</title>
<style>
:root {{
  --primary:#FF3621; --bg:#f8fafc; --card:#fff;
  --border:#e2e8f0; --text:#1e293b; --muted:#64748b;
  --green:#16a34a; --red:#dc2626;
}}
* {{ box-sizing:border-box; margin:0; padding:0; }}
body {{ font-family:'Segoe UI',Arial,sans-serif; background:var(--bg); color:var(--text); font-size:13px; }}
header {{ background:var(--primary); color:#fff; padding:14px 28px; display:flex; align-items:center; gap:16px; }}
header svg text {{ fill:#fff !important; }}
header .title {{ font-size:20px; font-weight:700; }}
header .meta  {{ font-size:12px; opacity:.85; margin-left:auto; text-align:right; }}
.tabs {{ display:flex; background:#fff; border-bottom:2px solid var(--border); padding:0 20px; }}
.tab {{ padding:12px 22px; cursor:pointer; font-weight:600; color:var(--muted); border-bottom:3px solid transparent; margin-bottom:-2px; }}
.tab.active {{ color:var(--primary); border-color:var(--primary); }}
.pane {{ display:none; padding:24px 28px; }}
.pane.active {{ display:block; }}

/* Summary */
.kpi-row {{ display:flex; gap:16px; margin-bottom:24px; flex-wrap:wrap; }}
.kpi {{ background:var(--card); border:1px solid var(--border); border-radius:10px;
         padding:16px 24px; min-width:160px; }}
.kpi .num {{ font-size:32px; font-weight:700; color:var(--primary); }}
.kpi .lbl {{ color:var(--muted); font-size:12px; margin-top:4px; }}
.cat-pills {{ display:flex; flex-wrap:wrap; gap:8px; margin-bottom:24px; }}
.cat-pill {{ border-radius:20px; padding:4px 12px; font-size:12px; border:1px solid #cbd5e1; }}
table.sum-table {{ width:100%; border-collapse:collapse; background:var(--card);
                  border:1px solid var(--border); border-radius:8px; overflow:hidden; }}
table.sum-table th {{ background:#f1f5f9; padding:10px 14px; text-align:left;
                      border-bottom:1px solid var(--border); font-size:12px; color:var(--muted); }}
table.sum-table td {{ padding:10px 14px; border-bottom:1px solid var(--border); }}
table.sum-table tr:last-child td {{ border-bottom:none; }}
.num {{ text-align:center; font-weight:600; }}
.cats {{ font-size:11px; color:var(--muted); }}

/* Detail */
.file-card {{ background:var(--card); border:1px solid var(--border); border-radius:8px;
              margin-bottom:12px; overflow:hidden; }}
.file-header {{ padding:12px 18px; cursor:pointer; display:flex; align-items:center;
                gap:10px; font-size:14px; user-select:none;
                background:#f8fafc; border-bottom:1px solid var(--border); }}
.file-header:hover {{ background:#f1f5f9; }}
.file-caret {{ transition:transform .2s; display:inline-block; color:var(--muted); }}
.file-card.open .file-caret {{ transform:rotate(90deg); }}
.file-body {{ padding:0; }}
.badge {{ margin-left:auto; background:#e2e8f0; border-radius:20px;
          padding:2px 10px; font-size:11px; color:var(--muted); }}

table.diff-table {{ width:100%; border-collapse:collapse; }}
table.diff-table th {{ background:#f8fafc; padding:8px 14px; text-align:left;
                       border-bottom:1px solid var(--border); font-size:11px;
                       color:var(--muted); position:sticky; top:0; z-index:1; }}
table.diff-table td {{ padding:7px 14px; border-bottom:1px solid #f1f5f9;
                       vertical-align:top; font-size:12px; }}
.obj-row {{ cursor:pointer; background:#f8fafc; }}
.obj-row:hover {{ background:#f1f5f9; }}
.obj-row td {{ padding:9px 14px; }}
.obj-header {{ font-size:13px; }}
.obj-id {{ color:var(--muted); font-size:11px; }}
.change-row td {{ background:#fff; }}
.field-path {{ font-family:monospace; font-size:11px; color:#6366f1; }}
.before-val {{ color:var(--red);   font-family:monospace; font-size:11px; max-width:280px; word-break:break-all; }}
.after-val  {{ color:var(--green); font-family:monospace; font-size:11px; max-width:280px; word-break:break-all; }}
.cat-badge {{ border-radius:4px; padding:2px 8px; font-size:11px; border:1px solid #e2e8f0; }}
.hidden {{ display:none; }}

/* Search */
.search-bar {{ display:flex; gap:12px; margin-bottom:18px; align-items:center; }}
.search-bar input {{ flex:1; padding:8px 14px; border:1px solid var(--border);
                      border-radius:6px; font-size:13px; }}
.search-bar select {{ padding:8px 12px; border:1px solid var(--border); border-radius:6px; font-size:13px; }}
</style>
</head>
<body>
<header>
  {_DBX_LOGO}
  <div class="title">Staging Transformation Report</div>
  <div class="meta">Session: <strong>{_esc(sess)}</strong><br>Generated: {ts}</div>
</header>

<div class="tabs">
  <div class="tab active" onclick="showTab('summary',this)">📋 Summary</div>
  <div class="tab" onclick="showTab('detail',this)">🔍 Detailed Changes</div>
</div>

<!-- SUMMARY PANE -->
<div class="pane active" id="pane-summary">
  <div class="kpi-row">
    <div class="kpi"><div class="num">{len({c.object_id+c.file for c in changes})}</div><div class="lbl">Objects Changed</div></div>
    <div class="kpi"><div class="num">{len(changes)}</div><div class="lbl">Total Changes</div></div>
    <div class="kpi"><div class="num">{len(by_file)}</div><div class="lbl">Files Affected</div></div>
    <div class="kpi"><div class="num">{len(cat_summary)}</div><div class="lbl">Change Categories</div></div>
  </div>

  <div class="cat-pills">{cat_pills}</div>

  <table class="sum-table">
    <thead><tr>
      <th>File</th><th>Objects</th><th>Changes</th><th>Categories</th>
    </tr></thead>
    <tbody>{sum_rows_html}</tbody>
  </table>
</div>

<!-- DETAIL PANE -->
<div class="pane" id="pane-detail">
  <div class="search-bar">
    <input id="search-input" type="text" placeholder="Search object name, field path, or value…"
           oninput="filterCards()">
    <select id="cat-filter" onchange="filterCards()">
      <option value="">All categories</option>
      {''.join(f'<option value="{_esc(c["category"])}">{_esc(c["category"])}</option>' for c in cat_summary)}
    </select>
  </div>
  {''.join(detail_cards)}
</div>

<script>
function showTab(name, el) {{
  document.querySelectorAll('.pane').forEach(p=>p.classList.remove('active'));
  document.querySelectorAll('.tab' ).forEach(t=>t.classList.remove('active'));
  document.getElementById('pane-'+name).classList.add('active');
  el.classList.add('active');
}}
function toggleCard(el) {{
  const card = el.closest('.file-card');
  card.classList.toggle('open');
  el.closest('.file-header').nextElementSibling.classList.toggle('hidden');
}}
function toggleObj(tr) {{
  tr.querySelector('.caret').textContent = tr.querySelector('.caret').textContent==='▶'?'▼':'▶';
  let next = tr.nextElementSibling;
  while(next && next.classList.contains('change-row')) {{
    next.classList.toggle('hidden');
    next = next.nextElementSibling;
  }}
}}
function filterCards() {{
  const q   = document.getElementById('search-input').value.toLowerCase();
  const cat = document.getElementById('cat-filter').value.toLowerCase();
  document.querySelectorAll('.file-card').forEach(card => {{
    const text = card.textContent.toLowerCase();
    const catMatch = !cat || text.includes(cat);
    const qMatch   = !q   || text.includes(q);
    card.style.display = (catMatch && qMatch) ? '' : 'none';
  }});
}}
// Expand all on load for files with ≤3 objects
document.addEventListener('DOMContentLoaded', ()=>{{
  document.querySelectorAll('.file-card').forEach(card=>{{
    const rows = card.querySelectorAll('.obj-row');
    if(rows.length<=3){{
      card.classList.add('open');
      card.querySelector('.file-body').classList.remove('hidden');
    }}
  }});
}});
</script>
</body>
</html>"""

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    return output_path


# ─────────────────────────────────────────────────────────────────────────────
# Excel report
# ─────────────────────────────────────────────────────────────────────────────

def render_excel(changes: List[Change], raw_dir: str, stage_dir: str,
                 output_path: str) -> str:
    if not _EXCEL:
        print("  ⚠  openpyxl not installed — skipping Excel output")
        return ""

    summary_rows, cat_summary, by_file = build_summary(changes)
    sess = os.path.basename(raw_dir)
    ts   = datetime.now().strftime("%Y-%m-%d %H:%M")

    wb = openpyxl.Workbook()

    # ── Helper styles ─────────────────────────────────────────────────────────
    def _hdr(ws, row, values, bg="1e293b", fg="FFFFFF"):
        for col, val in enumerate(values, 1):
            c = ws.cell(row=row, column=col, value=val)
            c.font      = Font(bold=True, color=fg, size=11)
            c.fill      = PatternFill("solid", fgColor=bg)
            c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            c.border    = Border(bottom=Side(style="thin", color="CCCCCC"))

    def _cell(ws, row, col, val, bg=None, bold=False, wrap=True, align="left"):
        c = ws.cell(row=row, column=col, value=str(val) if val is not None else "")
        c.font      = Font(bold=bold, size=10)
        c.alignment = Alignment(horizontal=align, vertical="top", wrap_text=wrap)
        if bg:
            c.fill = PatternFill("solid", fgColor=bg.lstrip("#"))
        c.border = Border(bottom=Side(style="thin", color="F1F5F9"))
        return c

    def _set_col_widths(ws, widths):
        for i, w in enumerate(widths, 1):
            ws.column_dimensions[get_column_letter(i)].width = w

    # ── Summary sheet ─────────────────────────────────────────────────────────
    ws = wb.active
    ws.title = "Summary"
    ws.sheet_view.showGridLines = False

    ws.merge_cells("A1:D1")
    c = ws.cell(1, 1, f"Staging Transformation Report — {sess}  ({ts})")
    c.font = Font(bold=True, size=14, color="FF3621")
    c.alignment = Alignment(horizontal="left", vertical="center")
    ws.row_dimensions[1].height = 30

    ws.cell(3, 1, "KPI").font = Font(bold=True, size=11)
    kpis = [
        ("Objects changed",   len({c.object_id+c.file for c in changes})),
        ("Total changes",     len(changes)),
        ("Files affected",    len(by_file)),
        ("Change categories", len(cat_summary)),
    ]
    for i, (lbl, val) in enumerate(kpis):
        ws.cell(3, i+1, lbl ).font = Font(bold=True, color="64748B", size=10)
        c = ws.cell(4, i+1, val)
        c.font = Font(bold=True, size=22, color="FF3621")
        c.alignment = Alignment(horizontal="center")
    ws.row_dimensions[4].height = 36

    # Category breakdown
    r = 6
    ws.cell(r, 1, "Change Category Breakdown").font = Font(bold=True, size=11)
    r += 1
    _hdr(ws, r, ["Category", "Count"], "334155")
    r += 1
    for cs in cat_summary:
        bg = cs["color"].lstrip("#")
        _cell(ws, r, 1, cs["category"], bg=bg)
        _cell(ws, r, 2, cs["count"],    bg=bg, align="center")
        r += 1

    # File summary table
    r += 1
    ws.cell(r, 1, "File-level Summary").font = Font(bold=True, size=11)
    r += 1
    _hdr(ws, r, ["File", "Objects", "Changes", "Top Change Categories"], "334155")
    r += 1
    for sr in summary_rows:
        _cell(ws, r, 1, sr["file"])
        _cell(ws, r, 2, sr["objects"],    align="center")
        _cell(ws, r, 3, sr["changes"],    align="center")
        _cell(ws, r, 4, sr["categories"])
        r += 1

    _set_col_widths(ws, [36, 12, 12, 60])

    # ── Per-file detail sheets ─────────────────────────────────────────────────
    from collections import defaultdict
    for fname in sorted(by_file):
        grp = by_file[fname]
        # Safe Excel sheet name
        sname = re.sub(r'[\\/*?\[\]:]', '-', fname.rstrip("/"))[:31]
        ws2   = wb.create_sheet(title=sname)
        ws2.sheet_view.showGridLines = False

        ws2.merge_cells("A1:F1")
        c2 = ws2.cell(1, 1, f"{fname}  —  {len({x.object_id for x in grp})} objects, {len(grp)} changes")
        c2.font = Font(bold=True, size=12, color="FF3621")
        c2.alignment = Alignment(horizontal="left", vertical="center")
        ws2.row_dimensions[1].height = 24

        _hdr(ws2, 2, ["Object", "Object ID", "Field Path", "Category",
                       "Before", "After"], "334155")

        by_obj = defaultdict(list)
        for ch in grp:
            by_obj[(ch.object_id, ch.object_label)].append(ch)

        row = 3
        for (oid, olabel), obj_changes in sorted(by_obj.items()):
            # Object header row
            for col in range(1, 7):
                c3 = ws2.cell(row, col, "")
                c3.fill   = PatternFill("solid", fgColor="E2E8F0")
                c3.border = Border(bottom=Side(style="thin", color="CBD5E1"))
            ws2.cell(row, 1, olabel).font = Font(bold=True, size=11)
            ws2.cell(row, 2, str(oid)).font = Font(color="64748B", size=10)
            ws2.cell(row, 4, f"{len(obj_changes)} change(s)").font = Font(color="64748B", size=10)
            ws2.row_dimensions[row].height = 18
            row += 1

            for ch in obj_changes:
                bg = ch.color.lstrip("#")
                _cell(ws2, row, 1, olabel)
                _cell(ws2, row, 2, str(oid),         bg=bg)
                _cell(ws2, row, 3, ch.field_path,    bg=bg)
                _cell(ws2, row, 4, ch.category_label, bg=bg, bold=True)
                _cell(ws2, row, 5, _fmt(ch.before, 200), bg="FECACA")
                _cell(ws2, row, 6, _fmt(ch.after,  200), bg="BBF7D0")
                ws2.row_dimensions[row].height = 16
                row += 1

        _set_col_widths(ws2, [28, 30, 38, 26, 44, 44])
        ws2.freeze_panes = "A3"

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    wb.save(output_path)
    return output_path


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Generate staging diff report (HTML + Excel)")
    ap.add_argument("--raw-dir",      required=True, help="Path to raw export session directory")
    ap.add_argument("--stage-dir",    required=True, help="Path to staged session directory")
    ap.add_argument("--html-output",  default=None,  help="HTML output path (default: auto)")
    ap.add_argument("--excel-output", default=None,  help="Excel output path (default: auto)")
    ap.add_argument("--no-html",      action="store_true")
    ap.add_argument("--no-excel",     action="store_true")
    args = ap.parse_args()

    sess = os.path.basename(args.raw_dir.rstrip("/\\"))

    html_out  = args.html_output  or f"staging_diff_{sess}.html"
    excel_out = args.excel_output or f"staging_diff_{sess}.xlsx"

    print(f"\n  Staging Diff Report")
    print(f"  Raw   : {args.raw_dir}")
    print(f"  Stage : {args.stage_dir}")
    print()

    changes = collect_all_changes(args.raw_dir, args.stage_dir)
    print(f"  Found {len(changes)} changes across "
          f"{len({c.file for c in changes})} file type(s)")

    if not args.no_html:
        p = render_html(changes, args.raw_dir, args.stage_dir, html_out)
        print(f"  HTML  : {p}")

    if not args.no_excel:
        p = render_excel(changes, args.raw_dir, args.stage_dir, excel_out)
        if p:
            print(f"  Excel : {p}")

    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
