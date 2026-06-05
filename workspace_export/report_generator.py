"""
workspace_export/report_generator.py
======================================
Generates a detailed, human-readable export report from an
``ExportStatusTracker`` instance (or directly from ``export_status.json``).

The report is both printed to stdout and written to
``{export_dir}/export_report.txt``.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Optional

from .status_tracker import (
    ExportStatusTracker,
    ComponentStatus,
    SUCCESS,
    FAILED,
    SKIPPED,
    IN_PROGRESS,
    PENDING,
)

# Visual status icons
_ICON = {
    SUCCESS: "✓",
    FAILED: "✗",
    SKIPPED: "⊘",
    IN_PROGRESS: "⟳",
    PENDING: "○",
}

_W = 74  # report column width


# ---------------------------------------------------------------------------
# Public helpers used by other modules
# ---------------------------------------------------------------------------

def count_log_lines(path: str) -> Optional[int]:
    """Count non-empty lines in a newline-delimited JSON log file."""
    if not os.path.exists(path):
        return None
    count = 0
    with open(path, "r", encoding="utf-8", errors="replace") as fp:
        for line in fp:
            if line.strip():
                count += 1
    return count or None


def count_dir_entries(path: str) -> Optional[int]:
    """Count files (non-hidden) directly inside a directory."""
    if not os.path.isdir(path):
        return None
    return sum(1 for f in os.listdir(path) if not f.startswith(".")) or None


def dir_size_bytes(path: str) -> int:
    """Recursively compute directory size in bytes."""
    total = 0
    if not os.path.exists(path):
        return total
    for dirpath, _, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(dirpath, f))
            except OSError:
                pass
    return total


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _fmt_duration(seconds: Optional[float]) -> str:
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


def _fmt_count(n: Optional[int]) -> str:
    return f"{n:,}" if n is not None else "–"


def _fmt_bytes(n: Optional[int]) -> str:
    if n is None or n == 0:
        return ""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def _sep(char: str = "═") -> str:
    return char * _W


def _bar(char: str = "─") -> str:
    return char * _W


# ---------------------------------------------------------------------------
# Core report builder
# ---------------------------------------------------------------------------

def generate_report(tracker: ExportStatusTracker, export_dir: str) -> str:
    """
    Build a complete export report string.

    Parameters
    ----------
    tracker:
        Populated ``ExportStatusTracker`` (loaded from file or in-memory).
    export_dir:
        Absolute path to the session export directory used for artifact stats.
    """
    comps = tracker.all()
    succeeded  = [c for c in comps if c.status == SUCCESS]
    failed     = [c for c in comps if c.status == FAILED]
    skipped    = [c for c in comps if c.status == SKIPPED]
    pending    = [c for c in comps if c.status == PENDING]
    in_prog    = [c for c in comps if c.status == IN_PROGRESS]

    total_items  = sum(c.items_exported or 0 for c in succeeded)
    total_dl     = sum(c.bytes_downloaded or 0 for c in comps)
    total_dir_sz = dir_size_bytes(export_dir)

    # Overall duration
    overall_secs = None
    if tracker.export_start and tracker.export_end:
        try:
            s = datetime.fromisoformat(tracker.export_start)
            e = datetime.fromisoformat(tracker.export_end)
            overall_secs = (e - s).total_seconds()
        except ValueError:
            pass

    lines = []

    def add(*parts):
        lines.append(" ".join(str(p) for p in parts))

    # ── Header ──────────────────────────────────────────────────────────────
    add(_sep())
    add(" DATABRICKS WORKSPACE EXPORT REPORT")
    add(_sep())
    add(f"  Workspace URL  : {tracker.workspace_url or '–'}")
    add(f"  Session ID     : {tracker.session or '–'}")
    add(f"  Export Dir     : {export_dir}")
    add(f"  Started        : {tracker.export_start or '–'}")
    add(f"  Completed      : {tracker.export_end or 'IN PROGRESS'}")
    add(f"  Total Duration : {_fmt_duration(overall_secs)}")
    add(_sep())
    add("")

    # ── Component table ──────────────────────────────────────────────────────
    C_W, S_W, N_W, D_W = 26, 14, 9, 9  # col widths
    add(_bar())
    add(f"  {'COMPONENT':<{C_W}} {'STATUS':<{S_W}} {'ITEMS':>{N_W}} {'DURATION':>{D_W}}  LOG FILES")
    add(_bar())

    for comp in comps:
        icon       = _ICON.get(comp.status, "?")
        status_str = f"{icon} {comp.status.upper()}"
        count_str  = _fmt_count(comp.items_exported)
        dur_str    = _fmt_duration(comp.duration_seconds)
        logs_str   = (
            ", ".join(os.path.basename(f) for f in comp.log_files)
            if comp.log_files else "–"
        )
        add(
            f"  {comp.display_name:<{C_W}} {status_str:<{S_W}}"
            f" {count_str:>{N_W}} {dur_str:>{D_W}}  {logs_str}"
        )
        # Extra line for DBFS download size
        if comp.bytes_downloaded:
            add(
                f"  {'':>{C_W}} {'':>{S_W}} {'':>{N_W}} {'':>{D_W}}"
                f"  ({_fmt_bytes(comp.bytes_downloaded)} downloaded)"
            )
        # Extra line for notes
        if comp.notes and comp.status == SKIPPED:
            add(f"  {'':>{C_W}} {'':>{S_W}} {'':>{N_W}} {'':>{D_W}}  ↳ {comp.notes}")

    add(_bar())
    add("")

    # ── Summary ──────────────────────────────────────────────────────────────
    add(_sep())
    add(" SUMMARY")
    add(_sep())
    add(f"  Total components     : {len(comps)}")
    add(f"  ✓ Succeeded          : {len(succeeded)}")
    add(f"  ✗ Failed             : {len(failed)}")
    add(f"  ⊘ Skipped            : {len(skipped)}")
    if in_prog:
        add(f"  ⟳ In Progress        : {len(in_prog)}")
    if pending:
        add(f"  ○ Pending            : {len(pending)}")
    add("")
    add(f"  Total objects exported   : {_fmt_count(total_items)}")
    if total_dl:
        add(f"  Total DBFS downloaded    : {_fmt_bytes(total_dl)}")
    if total_dir_sz:
        add(f"  Export directory size    : {_fmt_bytes(total_dir_sz)}")
    add(_sep())
    add("")

    # ── Failures ─────────────────────────────────────────────────────────────
    if failed:
        add(_bar("!"))
        add(" FAILURES — ACTION REQUIRED")
        add(_bar("!"))
        for comp in failed:
            add(f"  ✗  {comp.display_name}")
            add(f"       Error    : {comp.error_message or 'unknown error'}")
            add(f"       Started  : {comp.start_time or '–'}")
            add(f"       Ended    : {comp.end_time or '–'}")
            add("")
        add(_bar("!"))
        add("")

    # ── Per-component details ─────────────────────────────────────────────────
    add(_bar())
    add(" COMPONENT DETAILS")
    add(_bar())
    for comp in succeeded:
        add(f"  {comp.display_name}")
        add(f"    Status   : ✓ {comp.status}")
        add(f"    Items    : {_fmt_count(comp.items_exported)}")
        add(f"    Duration : {_fmt_duration(comp.duration_seconds)}")
        if comp.log_files:
            for lf in comp.log_files:
                full = os.path.join(export_dir, lf)
                size_str = _fmt_bytes(os.path.getsize(full)) if os.path.exists(full) else "file missing"
                add(f"    Log      : {lf}  ({size_str})")
        if comp.bytes_downloaded:
            add(f"    DBFS DL  : {_fmt_bytes(comp.bytes_downloaded)}")
        if comp.notes:
            add(f"    Notes    : {comp.notes}")
        add("")
    add(_bar())
    add("")

    # ── Artifact listing ─────────────────────────────────────────────────────
    add(_sep())
    add(" EXPORT ARTIFACTS")
    add(_sep())
    if os.path.exists(export_dir):
        add(f"  Root            : {export_dir}")
        add(f"  Total size      : {_fmt_bytes(total_dir_sz)}")
        add("")
        for entry in sorted(os.listdir(export_dir)):
            full = os.path.join(export_dir, entry)
            if os.path.isfile(full):
                add(f"  {entry:<48} {_fmt_bytes(os.path.getsize(full)):>10}")
            elif os.path.isdir(full):
                dsz = dir_size_bytes(full)
                count = sum(1 for _ in _walk_files(full))
                add(f"  {entry + '/':<48} {_fmt_bytes(dsz):>10}  ({count} files)")
    else:
        add(f"  [directory not found: {export_dir}]")
    add(_sep())
    add("")

    # ── Post-export checklist ─────────────────────────────────────────────────
    add(_bar())
    add(" POST-EXPORT CHECKLIST")
    add(_bar())

    def _check(condition: bool, text: str) -> str:
        return f"  [{'✓' if condition else '○'}] {text}"

    add(_check(len(failed) == 0, "All components exported without errors"))
    add(_check(
        any(c.name == "users" and c.status == SUCCESS for c in comps),
        "Users and groups exported"
    ))
    add(_check(
        any(c.name == "notebooks" and c.status == SUCCESS for c in comps),
        "Notebook content downloaded"
    ))
    add(_check(
        any(c.name == "metastore" and c.status == SUCCESS for c in comps),
        "Hive metastore definitions exported"
    ))
    add(_check(
        any(c.name == "jobs" and c.status == SUCCESS for c in comps),
        "Job configurations exported"
    ))
    add(_check(
        any(c.name == "dbfs_libraries" and c.status == SUCCESS for c in comps),
        "DBFS libraries downloaded (library_manifest.json)"
    ))
    add(_check(False, "Run import pipeline against destination workspace:"))
    add(f"       python3 migration_pipeline.py --profile DST --import-pipeline --session {tracker.session or '<session>'}")
    add(_check(False, "Re-import DBFS libraries:"))
    add(f"       python3 import_dbfs_libs.py --profile DST --session {tracker.session or '<session>'}")
    add(_check(False, "Validate exported vs imported:"))
    add(f"       ./validate_pipeline.sh {tracker.session or '<src_session>'} <dst_session>")

    add(_bar())
    add("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Standalone: generate report from a saved status file
# ---------------------------------------------------------------------------

def report_from_file(status_file: str) -> str:
    """Load a saved export_status.json and produce the report string."""
    tracker = ExportStatusTracker.load(status_file)
    export_dir = tracker.export_dir or os.path.dirname(status_file)
    return generate_report(tracker, export_dir)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _walk_files(path: str):
    for dirpath, _, files in os.walk(path):
        for f in files:
            yield os.path.join(dirpath, f)
