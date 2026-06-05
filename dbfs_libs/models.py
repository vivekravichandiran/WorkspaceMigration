"""
Data models for the DBFS / library migration manifest.

Each LibraryEntry captures:
  - lib_type       : jar | whl | egg | pypi | maven | cran
  - source fields  : dbfs_path  (file-based)
                     pypi_*     (PyPI)
                     maven_*    (Maven)
                     cran_*     (CRAN)
  - local_file     : relative path inside the export directory where the
                     binary was downloaded (file-based libs only)
  - used_by        : list of LibraryUsage objects (cluster / job / job_task)
  - raw            : original Databricks API library dict (for easy re-POST)
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# LibraryUsage
# ---------------------------------------------------------------------------

@dataclass
class LibraryUsage:
    """Records one entity (cluster or job/task) that uses a library."""

    # "cluster" | "cluster_runtime" | "job" | "job_task"
    entity_type: str
    entity_id: str
    entity_name: str
    # populated only when entity_type == "job_task"
    task_name: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> LibraryUsage:
        return cls(**d)


# ---------------------------------------------------------------------------
# LibraryEntry
# ---------------------------------------------------------------------------

@dataclass
class LibraryEntry:
    """Represents a single unique library and every place it is used."""

    lib_type: str  # jar | whl | egg | pypi | maven | cran

    # --- file-based (jar / whl / egg) ---
    dbfs_path: Optional[str] = None  # original DBFS path, e.g. dbfs:/FileStore/jars/foo.jar
    local_file: Optional[str] = None  # relative path inside export dir after download

    # --- PyPI ---
    pypi_package: Optional[str] = None  # e.g. "requests>=2.28"
    pypi_repo: Optional[str] = None     # optional custom repo URL

    # --- Maven ---
    maven_coordinates: Optional[str] = None  # e.g. "com.example:my-lib:1.0"
    maven_repo: Optional[str] = None
    maven_exclusions: Optional[List[str]] = None

    # --- CRAN ---
    cran_package: Optional[str] = None
    cran_repo: Optional[str] = None

    # --- usage tracking ---
    used_by: List[LibraryUsage] = field(default_factory=list)

    # Original Databricks API dict ({"jar": "dbfs:/..."} / {"pypi": {...}} etc.)
    # Stored as-is so it can be POSTed straight back to /libraries/install.
    raw: Optional[Dict[str, Any]] = None

    # ------------------------------------------------------------------
    # Serialisation helpers
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        return d

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> LibraryEntry:
        used_by_raw = d.pop("used_by", [])
        entry = cls(**d)
        entry.used_by = [LibraryUsage.from_dict(u) for u in used_by_raw]
        return entry

    # ------------------------------------------------------------------
    # Unique key for deduplication across sources
    # ------------------------------------------------------------------

    def key(self) -> str:
        if self.dbfs_path:
            return f"dbfs:{self.dbfs_path}"
        if self.pypi_package:
            return f"pypi:{self.pypi_package}"
        if self.maven_coordinates:
            return f"maven:{self.maven_coordinates}"
        if self.cran_package:
            return f"cran:{self.cran_package}"
        return f"unknown:{json.dumps(self.raw, sort_keys=True)}"

    # ------------------------------------------------------------------
    # Human-readable summary
    # ------------------------------------------------------------------

    def summary(self) -> str:
        if self.lib_type in ("jar", "whl", "egg"):
            loc = self.dbfs_path or "(unknown path)"
            downloaded = f" [local: {self.local_file}]" if self.local_file else " [NOT downloaded]"
            return f"[{self.lib_type.upper()}] {loc}{downloaded}"
        if self.lib_type == "pypi":
            return f"[PyPI] {self.pypi_package}" + (f"  repo={self.pypi_repo}" if self.pypi_repo else "")
        if self.lib_type == "maven":
            return f"[Maven] {self.maven_coordinates}" + (f"  repo={self.maven_repo}" if self.maven_repo else "")
        if self.lib_type == "cran":
            return f"[CRAN] {self.cran_package}" + (f"  repo={self.cran_repo}" if self.cran_repo else "")
        return f"[{self.lib_type}] {self.raw}"


# ---------------------------------------------------------------------------
# LibraryManifest
# ---------------------------------------------------------------------------

@dataclass
class LibraryManifest:
    """Top-level manifest written to library_manifest.json after export."""

    source_workspace_url: str
    libraries: List[LibraryEntry] = field(default_factory=list)

    # ------------------------------------------------------------------
    # JSON I/O
    # ------------------------------------------------------------------

    def to_json(self, fp) -> None:
        payload = {
            "source_workspace_url": self.source_workspace_url,
            "libraries": [lib.to_dict() for lib in self.libraries],
        }
        json.dump(payload, fp, indent=2, default=str)

    @classmethod
    def from_json(cls, fp) -> LibraryManifest:
        payload = json.load(fp)
        libs = [LibraryEntry.from_dict(l) for l in payload.get("libraries", [])]
        return cls(
            source_workspace_url=payload.get("source_workspace_url", ""),
            libraries=libs,
        )

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    def file_based(self) -> List[LibraryEntry]:
        """Libraries that need a binary file download/upload."""
        return [l for l in self.libraries if l.lib_type in ("jar", "whl", "egg")]

    def coordinate_based(self) -> List[LibraryEntry]:
        """Libraries expressed as coordinates (no binary transfer needed)."""
        return [l for l in self.libraries if l.lib_type in ("pypi", "maven", "cran")]

    def print_summary(self) -> None:
        print(f"\n{'='*70}")
        print(f"Library Manifest  |  source: {self.source_workspace_url}")
        print(f"{'='*70}")
        print(f"Total unique libraries : {len(self.libraries)}")
        print(f"  File-based (jar/whl/egg) : {len(self.file_based())}")
        print(f"  Coordinate-based         : {len(self.coordinate_based())}")
        print(f"{'='*70}")
        for lib in self.libraries:
            print(f"  {lib.summary()}")
            for u in lib.used_by:
                task_suffix = f" / task={u.task_name}" if u.task_name else ""
                print(f"    -> {u.entity_type}: [{u.entity_id}] {u.entity_name}{task_suffix}")
        print(f"{'='*70}\n")
