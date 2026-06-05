"""
workspace_export/status_tracker.py
====================================
Tracks per-component export status and persists it atomically to
``export_status.json`` inside the session export directory.

The JSON is written with an atomic rename so a crash mid-write can never
leave the file in a corrupt state.

Schema of export_status.json
-----------------------------
{
  "workspace_url": "https://...",
  "session": "M202404281200",
  "export_dir": "logs/M202404281200/",
  "export_start": "2024-04-28T12:00:00.000000",
  "export_end": "2024-04-28T12:45:23.000000",
  "components": {
    "users": {
      "name": "users",
      "display_name": "Users",
      "status": "success",
      "start_time": "...",
      "end_time": "...",
      "duration_seconds": 83.4,
      "items_exported": 156,
      "log_files": ["users.log"],
      "bytes_downloaded": null,
      "error_message": null,
      "notes": null
    },
    ...
  }
}
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

# Status constants
PENDING = "pending"
IN_PROGRESS = "in_progress"
SUCCESS = "success"
FAILED = "failed"
SKIPPED = "skipped"


# ---------------------------------------------------------------------------
# ComponentStatus
# ---------------------------------------------------------------------------

@dataclass
class ComponentStatus:
    name: str           # machine key, e.g. "instance_profiles"
    display_name: str   # human label, e.g. "Instance Profiles"
    status: str = PENDING
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    duration_seconds: Optional[float] = None
    # count of exported objects (log file lines, directory entries, etc.)
    items_exported: Optional[int] = None
    # relative paths to generated log files / directories
    log_files: List[str] = field(default_factory=list)
    # bytes downloaded from DBFS (populated for dbfs_libraries component)
    bytes_downloaded: Optional[int] = None
    error_message: Optional[str] = None
    notes: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> ComponentStatus:
        return cls(**d)


# ---------------------------------------------------------------------------
# Ordered component registry
# ---------------------------------------------------------------------------

# Each entry: (machine_key, display_name)
ALL_COMPONENTS: List[tuple] = [
    # ── Legacy dbclient-based components ──────────────────────────────
    ("instance_profiles",    "Instance Profiles"),
    ("users",                "Users"),
    ("groups",               "Groups"),
    ("workspace_item_log",   "Workspace Item Log"),
    ("workspace_acls",       "Workspace ACLs"),
    ("notebooks",            "Notebooks"),
    ("secrets",              "Secrets"),
    ("clusters",             "Clusters"),
    ("instance_pools",       "Instance Pools"),
    ("jobs",                 "Jobs"),
    ("metastore",            "Hive Metastore"),
    ("metastore_table_acls", "Metastore Table ACLs"),
    ("mlflow_experiments",   "MLflow Experiments"),
    ("mlflow_runs",          "MLflow Runs"),
    ("dbfs_libraries",       "DBFS Libraries"),
    # ── New REST-API-based components ─────────────────────────────────
    ("sql_warehouses",       "SQL Warehouses"),
    ("dlt_pipelines",        "DLT Pipelines"),
    ("repos",                "Git Repos"),
    ("lakeview_dashboards",  "AI/BI Dashboards"),
    ("genie_spaces",         "Genie AI Spaces"),
    ("serving_endpoints",    "Model Serving Endpoints"),
    ("unity_catalog",        "Unity Catalog Objects"),
    ("workspace_files",      "Workspace Files (non-notebook)"),
]


# ---------------------------------------------------------------------------
# ExportStatusTracker
# ---------------------------------------------------------------------------

class ExportStatusTracker:
    """
    Tracks per-component export status and persists it to a JSON file.

    Typical usage::

        tracker = ExportStatusTracker(status_file)
        tracker.set_metadata(url, session, export_dir)

        tracker.start("users")
        try:
            scim_c.log_all_users()
            tracker.success("users", log_files=["users.log"], items_exported=156)
        except Exception as e:
            tracker.fail("users", str(e))

        tracker.mark_complete()
        tracker.load(status_file)  # re-load for report generation
    """

    def __init__(self, status_file: str):
        self._status_file = status_file
        self._workspace_url: str = ""
        self._session: str = ""
        self._export_dir: str = ""
        self._export_start: Optional[str] = None
        self._export_end: Optional[str] = None
        # Initialise all components as pending in declaration order
        self._components: Dict[str, ComponentStatus] = {
            name: ComponentStatus(name=name, display_name=display)
            for name, display in ALL_COMPONENTS
        }
        self._persist()

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    def set_metadata(self, workspace_url: str, session: str, export_dir: str) -> None:
        self._workspace_url = workspace_url
        self._session = session
        self._export_dir = export_dir
        self._export_start = datetime.now().isoformat()
        self._persist()

    def mark_complete(self) -> None:
        self._export_end = datetime.now().isoformat()
        self._persist()

    # ------------------------------------------------------------------
    # State transitions
    # ------------------------------------------------------------------

    def start(self, name: str) -> None:
        comp = self._components.get(name)
        if comp:
            comp.status = IN_PROGRESS
            comp.start_time = datetime.now().isoformat()
            self._persist()

    def success(
        self,
        name: str,
        log_files: Optional[List[str]] = None,
        items_exported: Optional[int] = None,
        bytes_downloaded: Optional[int] = None,
        notes: Optional[str] = None,
    ) -> None:
        comp = self._components.get(name)
        if not comp:
            return
        comp.status = SUCCESS
        comp.end_time = datetime.now().isoformat()
        comp.duration_seconds = self._elapsed(comp)
        if log_files is not None:
            comp.log_files = log_files
        if items_exported is not None:
            comp.items_exported = items_exported
        if bytes_downloaded is not None:
            comp.bytes_downloaded = bytes_downloaded
        if notes is not None:
            comp.notes = notes
        self._persist()

    def fail(self, name: str, error_message: str) -> None:
        comp = self._components.get(name)
        if not comp:
            return
        comp.status = FAILED
        comp.end_time = datetime.now().isoformat()
        comp.duration_seconds = self._elapsed(comp)
        comp.error_message = error_message
        self._persist()

    def skip(self, name: str, reason: Optional[str] = None) -> None:
        comp = self._components.get(name)
        if not comp:
            return
        comp.status = SKIPPED
        if reason:
            comp.notes = reason
        self._persist()

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    def get(self, name: str) -> Optional[ComponentStatus]:
        return self._components.get(name)

    def all(self) -> List[ComponentStatus]:
        """Return components in declaration order."""
        return [self._components[name] for name, _ in ALL_COMPONENTS if name in self._components]

    def counts(self) -> Dict[str, int]:
        comps = list(self._components.values())
        return {
            "total": len(comps),
            "succeeded": sum(1 for c in comps if c.status == SUCCESS),
            "failed": sum(1 for c in comps if c.status == FAILED),
            "skipped": sum(1 for c in comps if c.status == SKIPPED),
            "pending": sum(1 for c in comps if c.status == PENDING),
            "in_progress": sum(1 for c in comps if c.status == IN_PROGRESS),
        }

    @property
    def workspace_url(self) -> str:
        return self._workspace_url

    @property
    def session(self) -> str:
        return self._session

    @property
    def export_dir(self) -> str:
        return self._export_dir

    @property
    def export_start(self) -> Optional[str]:
        return self._export_start

    @property
    def export_end(self) -> Optional[str]:
        return self._export_end

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _persist(self) -> None:
        """Write status atomically (write to tmp then rename)."""
        payload = {
            "workspace_url": self._workspace_url,
            "session": self._session,
            "export_dir": self._export_dir,
            "export_start": self._export_start,
            "export_end": self._export_end,
            "components": {
                name: comp.to_dict()
                for name, comp in self._components.items()
            },
        }
        os.makedirs(os.path.dirname(self._status_file) or ".", exist_ok=True)
        tmp = self._status_file + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fp:
            json.dump(payload, fp, indent=2)
        os.replace(tmp, self._status_file)

    @classmethod
    def load(cls, status_file: str) -> ExportStatusTracker:
        """Reconstruct tracker from an existing status file."""
        tracker = cls.__new__(cls)
        tracker._status_file = status_file
        tracker._components = {}
        tracker._workspace_url = ""
        tracker._session = ""
        tracker._export_dir = ""
        tracker._export_start = None
        tracker._export_end = None

        with open(status_file, "r", encoding="utf-8") as fp:
            payload = json.load(fp)

        tracker._workspace_url = payload.get("workspace_url", "")
        tracker._session = payload.get("session", "")
        tracker._export_dir = payload.get("export_dir", "")
        tracker._export_start = payload.get("export_start")
        tracker._export_end = payload.get("export_end")

        for name, comp_dict in payload.get("components", {}).items():
            tracker._components[name] = ComponentStatus.from_dict(comp_dict)

        return tracker

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _elapsed(comp: ComponentStatus) -> Optional[float]:
        if not comp.start_time:
            return None
        try:
            start = datetime.fromisoformat(comp.start_time)
            return (datetime.now() - start).total_seconds()
        except ValueError:
            return None
