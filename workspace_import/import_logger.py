"""
workspace_import/import_logger.py
===================================
Writes a structured import_log.json to the session directory so the
HTML reporter can read it. Called by import_jobs_gcp.py and import_gcp.sh.
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional


class ImportLogger:
    """
    Tracks each import step and writes import_log.json after every update.

    Schema::

        {
          "session": "PROD_MIGRATION_2024",
          "target_workspace": "https://...",
          "started_at": "2026-04-28T...",
          "ended_at": "2026-04-28T...",
          "dry_run": false,
          "steps": [
            {
              "step": 1,
              "component": "preprocess",
              "handler": "import_jobs_gcp.py",
              "status": "success",
              "started_at": "...",
              "ended_at": "...",
              "duration_seconds": 2.3,
              "total": 9,
              "created": 9,
              "skipped": 0,
              "failed": 0,
              "notes": "...",
              "errors": []
            },
            ...
          ]
        }
    """

    def __init__(
        self,
        log_path: str,
        session: str,
        target_workspace: str,
        dry_run: bool = False,
    ):
        self._path    = log_path
        self._dry     = dry_run
        self._data: Dict[str, Any] = {
            "session": session,
            "target_workspace": target_workspace,
            "started_at": datetime.now().isoformat(),
            "ended_at": None,
            "dry_run": dry_run,
            "steps": [],
        }
        self._step_counter = 0
        self._current: Optional[Dict] = None
        self._flush()

    # ------------------------------------------------------------------
    # Step management
    # ------------------------------------------------------------------

    def begin_step(self, component: str, handler: str, notes: str = "") -> None:
        self._step_counter += 1
        self._current = {
            "step": self._step_counter,
            "component": component,
            "handler": handler,
            "status": "in_progress",
            "started_at": datetime.now().isoformat(),
            "ended_at": None,
            "duration_seconds": None,
            "total": None,
            "created": 0,
            "skipped": 0,
            "failed": 0,
            "dry_run": 0,
            "notes": notes,
            "errors": [],
            "warnings": [],
        }
        self._data["steps"].append(self._current)
        self._flush()

    def end_step(
        self,
        status: str = "success",
        total: Optional[int] = None,
        created: int = 0,
        skipped: int = 0,
        failed: int = 0,
        dry_run_count: int = 0,
        notes: str = "",
        errors: Optional[List[str]] = None,
        warnings: Optional[List[str]] = None,
    ) -> None:
        if self._current is None:
            return
        now = datetime.now().isoformat()
        self._current["status"] = status
        self._current["ended_at"] = now
        self._current["duration_seconds"] = _elapsed(self._current["started_at"], now)
        if total is not None:
            self._current["total"] = total
        self._current["created"]  = created
        self._current["skipped"]  = skipped
        self._current["failed"]   = failed
        self._current["dry_run"]  = dry_run_count
        if notes:
            self._current["notes"] = notes
        if errors:
            self._current["errors"] = errors
        if warnings:
            self._current["warnings"] = warnings
        self._current = None
        self._flush()

    def fail_step(self, error: str) -> None:
        self.end_step(status="failed", errors=[error])

    def complete(self) -> None:
        self._data["ended_at"] = datetime.now().isoformat()
        self._flush()

    # ------------------------------------------------------------------
    # Convenience: record from ExtraImporter result dict
    # ------------------------------------------------------------------

    def record_extra_results(self, results: List[Dict]) -> None:
        for r in results:
            comp = r.get("component", "unknown")
            self.begin_step(comp, "workspace_import.extra_importers.ExtraImporter")
            self.end_step(
                status="failed" if r.get("failed", 0) > 0 else "success",
                total=r.get("total"),
                created=r.get("created", 0),
                skipped=r.get("skipped", 0),
                failed=r.get("failed", 0),
                dry_run_count=r.get("dry_run", 0),
                errors=r.get("errors", []),
                warnings=r.get("warnings", []),
            )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _flush(self) -> None:
        os.makedirs(os.path.dirname(self._path) or ".", exist_ok=True)
        tmp = self._path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self._data, f, indent=2)
        os.replace(tmp, self._path)


def _elapsed(start: str, end: str) -> Optional[float]:
    try:
        s = datetime.fromisoformat(start)
        e = datetime.fromisoformat(end)
        return round((e - s).total_seconds(), 1)
    except ValueError:
        return None


def load_import_log(log_path: str) -> Optional[Dict]:
    if not os.path.isfile(log_path):
        return None
    with open(log_path, encoding="utf-8") as f:
        return json.load(f)
