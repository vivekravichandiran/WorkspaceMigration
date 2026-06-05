"""
LibraryImporter
===============
Reads a library_manifest.json produced by LibraryExporter and:

  1. Uploads every file-based library (jar / whl / egg) back to the same DBFS
     path on the destination workspace.
  2. Re-installs cluster-attached libraries on matching clusters (matched by
     cluster name).
  3. Skips coordinate-based libraries (PyPI / Maven / CRAN) from cluster
     installation because those are typically re-attached via cluster policies
     or init scripts; a report is printed so the operator can review them.

Usage
-----
    from dbfs_libs.importer import LibraryImporter
    importer = LibraryImporter(client_config, export_dir="/path/to/session/dir")
    report = importer.import_all()
    print(report)
"""

from __future__ import annotations

import base64
import json
import logging
import os
from dataclasses import dataclass, field
from typing import Dict, List

from .models import LibraryEntry, LibraryManifest

# Maximum bytes per DBFS /add-block call (Databricks hard limit = 1 MB)
_CHUNK_SIZE = 1_048_576


# ---------------------------------------------------------------------------
# Import report
# ---------------------------------------------------------------------------

@dataclass
class ImportReport:
    uploaded_files: List[str] = field(default_factory=list)
    skipped_files: List[str] = field(default_factory=list)   # local file missing
    failed_uploads: List[str] = field(default_factory=list)
    installed_on_clusters: List[str] = field(default_factory=list)
    skipped_clusters: List[str] = field(default_factory=list)  # not found in dest
    failed_installs: List[str] = field(default_factory=list)
    coordinate_libs_to_review: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        lines = [
            "",
            "=" * 70,
            "Library Import Report",
            "=" * 70,
            f"DBFS uploads  : {len(self.uploaded_files)} succeeded, "
            f"{len(self.failed_uploads)} failed, {len(self.skipped_files)} skipped (file missing)",
            f"Cluster installs : {len(self.installed_on_clusters)} succeeded, "
            f"{len(self.failed_installs)} failed, {len(self.skipped_clusters)} skipped (cluster not found)",
            f"Coordinate libs  : {len(self.coordinate_libs_to_review)} to review (see below)",
            "=" * 70,
        ]
        if self.uploaded_files:
            lines.append("Uploaded:")
            lines += [f"  ✓ {p}" for p in self.uploaded_files]
        if self.failed_uploads:
            lines.append("Failed uploads:")
            lines += [f"  ✗ {p}" for p in self.failed_uploads]
        if self.skipped_files:
            lines.append("Skipped (local file not found):")
            lines += [f"  - {p}" for p in self.skipped_files]
        if self.installed_on_clusters:
            lines.append("Installed on clusters:")
            lines += [f"  ✓ {c}" for c in self.installed_on_clusters]
        if self.failed_installs:
            lines.append("Failed cluster installs:")
            lines += [f"  ✗ {c}" for c in self.failed_installs]
        if self.skipped_clusters:
            lines.append("Clusters not found in destination (skipped):")
            lines += [f"  - {c}" for c in self.skipped_clusters]
        if self.coordinate_libs_to_review:
            lines.append("Coordinate-based libs (review / attach manually if needed):")
            lines += [f"  ! {l}" for l in self.coordinate_libs_to_review]
        lines.append("=" * 70 + "\n")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# LibraryImporter
# ---------------------------------------------------------------------------

class LibraryImporter:
    """
    Imports libraries exported by :class:`~dbfs_libs.exporter.LibraryExporter`.

    Parameters
    ----------
    client_config:
        Standard ``client_config`` dict used throughout the migrate tool, pointing
        at the *destination* workspace.
    export_dir:
        Path to the session export directory produced by the export run, e.g.
        ``logs/M202401011200/``.  Must contain ``library_manifest.json``.
    """

    def __init__(self, client_config: dict, export_dir: str):
        from dbclient.dbclient import dbclient as DbClient
        self._client = DbClient(client_config)
        self._export_dir = export_dir
        self._logger = logging.getLogger(__name__)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def import_all(self) -> ImportReport:
        report = ImportReport()

        manifest_path = os.path.join(self._export_dir, "library_manifest.json")
        if not os.path.exists(manifest_path):
            raise FileNotFoundError(
                f"library_manifest.json not found at {manifest_path}. "
                "Please run the export step first."
            )

        with open(manifest_path, "r", encoding="utf-8") as fp:
            manifest = LibraryManifest.from_json(fp)

        self._logger.info(
            f"Loaded manifest with {len(manifest.libraries)} unique libraries "
            f"from {manifest.source_workspace_url}"
        )

        # Phase 1 – upload binaries
        self._logger.info("=== Phase 1: Uploading file-based libraries to DBFS ===")
        self._upload_files(manifest, report)

        # Phase 2 – install on clusters
        self._logger.info("=== Phase 2: Installing libraries on clusters ===")
        self._install_on_clusters(manifest, report)

        # Phase 3 – report coordinate-based libs for manual review
        for lib in manifest.coordinate_based():
            report.coordinate_libs_to_review.append(lib.summary())

        return report

    # ------------------------------------------------------------------
    # Phase 1 – upload files
    # ------------------------------------------------------------------

    def _upload_files(self, manifest: LibraryManifest, report: ImportReport) -> None:
        for entry in manifest.file_based():
            if not entry.local_file:
                self._logger.warning(
                    f"No local_file recorded for {entry.dbfs_path} – skipping upload."
                )
                report.skipped_files.append(entry.dbfs_path or "unknown")
                continue

            local_path = os.path.join(self._export_dir, entry.local_file)
            if not os.path.exists(local_path):
                self._logger.warning(f"Local file missing: {local_path}")
                report.skipped_files.append(entry.dbfs_path or local_path)
                continue

            target_dbfs_path = entry.dbfs_path
            self._logger.info(f"Uploading {local_path} -> {target_dbfs_path}")
            try:
                self._upload_file(local_path, target_dbfs_path)
                report.uploaded_files.append(target_dbfs_path)
                self._logger.info(f"  -> uploaded successfully")
            except Exception as exc:
                self._logger.error(f"  -> FAILED: {exc}")
                report.failed_uploads.append(f"{target_dbfs_path}: {exc}")

    def _upload_file(self, local_path: str, dbfs_path: str) -> None:
        """
        Stream-upload a local file to DBFS using the create / add-block / close
        three-step API to handle files larger than 1 MB.
        """
        # Step 1: open a write handle
        create_resp = self._client.post(
            "/dbfs/create", {"path": dbfs_path, "overwrite": True}
        )
        handle = create_resp["handle"]

        try:
            # Step 2: send chunks
            with open(local_path, "rb") as fp:
                while True:
                    chunk = fp.read(_CHUNK_SIZE)
                    if not chunk:
                        break
                    encoded = base64.b64encode(chunk).decode("utf-8")
                    self._client.post(
                        "/dbfs/add-block", {"handle": handle, "data": encoded}
                    )
        finally:
            # Step 3: always close the handle
            self._client.post("/dbfs/close", {"handle": handle})

    # ------------------------------------------------------------------
    # Phase 2 – install on clusters
    # ------------------------------------------------------------------

    def _install_on_clusters(
        self, manifest: LibraryManifest, report: ImportReport
    ) -> None:
        cluster_name_to_id = self._get_cluster_name_to_id()

        # Build a map: cluster_name -> [raw library dicts]
        libs_by_cluster: Dict[str, List[dict]] = {}
        for entry in manifest.libraries:
            if entry.raw is None:
                continue
            for usage in entry.used_by:
                if usage.entity_type in ("cluster", "cluster_runtime"):
                    cname = usage.entity_name
                    libs_by_cluster.setdefault(cname, [])
                    # Avoid duplicate entries
                    if entry.raw not in libs_by_cluster[cname]:
                        libs_by_cluster[cname].append(entry.raw)

        for cname, libs in libs_by_cluster.items():
            new_cid = cluster_name_to_id.get(cname)
            if not new_cid:
                self._logger.warning(
                    f"Cluster '{cname}' not found in destination workspace – skipped."
                )
                report.skipped_clusters.append(cname)
                continue

            self._logger.info(
                f"Installing {len(libs)} lib(s) on cluster '{cname}' ({new_cid})"
            )
            try:
                self._client.post(
                    "/libraries/install",
                    {"cluster_id": new_cid, "libraries": libs},
                )
                report.installed_on_clusters.append(
                    f"{cname} ({new_cid}) – {len(libs)} lib(s)"
                )
            except Exception as exc:
                self._logger.error(f"  -> FAILED for cluster '{cname}': {exc}")
                report.failed_installs.append(f"{cname}: {exc}")

    def _get_cluster_name_to_id(self) -> Dict[str, str]:
        try:
            clusters = self._client.get("/clusters/list").get("clusters", [])
            return {c["cluster_name"]: c["cluster_id"] for c in clusters}
        except Exception as exc:
            self._logger.warning(f"Could not list clusters in destination: {exc}")
            return {}
