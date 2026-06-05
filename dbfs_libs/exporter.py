"""
LibraryExporter
===============
Scans exported Databricks artifacts (clusters.log, jobs.log) and live cluster
library statuses to build a complete LibraryManifest.

For every file-based library (jar / whl / egg) found it downloads the binary
from DBFS using the streaming DBFS read API and stores it under:

    {export_dir}/dbfs_files/<path-relative-to-dbfs-root>

The final manifest is written to:

    {export_dir}/library_manifest.json

Usage
-----
    from dbfs_libs.exporter import LibraryExporter
    exporter = LibraryExporter(client_config)
    manifest = exporter.export()
    manifest.print_summary()
"""

from __future__ import annotations

import base64
import json
import logging
import os
from typing import Dict, List, Optional

from .models import LibraryEntry, LibraryManifest, LibraryUsage

# Maximum bytes per DBFS /read call (Databricks hard limit = 1 MB)
_CHUNK_SIZE = 1_048_576


class LibraryExporter:
    """
    Exports library metadata and binaries from a Databricks workspace.

    Sources queried (in order):
      1. clusters.log  – libraries embedded in stored cluster configs
      2. jobs.log      – libraries per job (single-task) and per task (multi-task)
      3. Live /libraries/cluster-status API – runtime-attached libraries not
         captured in the static log files
    """

    def __init__(self, client_config: dict):
        # Import here to avoid hard dependency when running tests without dbclient
        from dbclient.dbclient import dbclient as DbClient
        self._client = DbClient(client_config)
        self._export_dir = client_config["export_dir"]
        self._dbfs_download_dir = os.path.join(self._export_dir, "dbfs_files")
        os.makedirs(self._dbfs_download_dir, exist_ok=True)
        self._logger = logging.getLogger(__name__)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def export(self) -> LibraryManifest:
        """Run the full export and return the completed manifest."""
        manifest = LibraryManifest(source_workspace_url=self._client.get_url())

        self._logger.info("=== Phase 1: Scanning clusters.log ===")
        clusters_log = os.path.join(self._export_dir, "clusters.log")
        if os.path.exists(clusters_log):
            self._process_clusters_log(clusters_log, manifest)
        else:
            self._logger.warning("clusters.log not found – run export pipeline first.")

        self._logger.info("=== Phase 2: Scanning jobs.log ===")
        jobs_log = os.path.join(self._export_dir, "jobs.log")
        if os.path.exists(jobs_log):
            self._process_jobs_log(jobs_log, manifest)
        else:
            self._logger.warning("jobs.log not found – skipping job library scan.")

        self._logger.info("=== Phase 3: Live cluster library statuses ===")
        self._process_live_cluster_libs(manifest)

        self._logger.info("=== Phase 4: Downloading DBFS file-based libraries ===")
        self._download_dbfs_files(manifest)

        manifest_path = os.path.join(self._export_dir, "library_manifest.json")
        with open(manifest_path, "w", encoding="utf-8") as fp:
            manifest.to_json(fp)
        self._logger.info(f"Manifest written to: {manifest_path}")

        return manifest

    # ------------------------------------------------------------------
    # Phase 1 – clusters.log
    # ------------------------------------------------------------------

    def _process_clusters_log(self, path: str, manifest: LibraryManifest) -> None:
        with open(path, "r", encoding="utf-8") as fp:
            for line in fp:
                line = line.strip()
                if not line:
                    continue
                cluster = json.loads(line)
                cid = cluster.get("cluster_id", "")
                cname = cluster.get("cluster_name", "")
                for lib_dict in cluster.get("libraries", []):
                    entry = self._find_or_create(manifest, lib_dict)
                    if entry:
                        self._add_usage(
                            entry,
                            LibraryUsage(
                                entity_type="cluster",
                                entity_id=cid,
                                entity_name=cname,
                            ),
                        )

    # ------------------------------------------------------------------
    # Phase 2 – jobs.log
    # ------------------------------------------------------------------

    def _process_jobs_log(self, path: str, manifest: LibraryManifest) -> None:
        with open(path, "r", encoding="utf-8") as fp:
            for line in fp:
                line = line.strip()
                if not line:
                    continue
                job = json.loads(line)
                job_id = str(job.get("job_id", ""))
                settings = job.get("settings", {})
                # strip the :::job_id suffix that the migrate tool adds
                job_name = settings.get("name", "").split(":::")[0]

                job_format = settings.get("format", "SINGLE_TASK")

                if job_format == "SINGLE_TASK":
                    # Top-level libraries field
                    for lib_dict in settings.get("libraries", []):
                        entry = self._find_or_create(manifest, lib_dict)
                        if entry:
                            self._add_usage(
                                entry,
                                LibraryUsage(
                                    entity_type="job",
                                    entity_id=job_id,
                                    entity_name=job_name,
                                ),
                            )
                else:
                    # MULTI_TASK: libraries live on individual tasks
                    for task in settings.get("tasks", []):
                        task_key = task.get("task_key", "")
                        for lib_dict in task.get("libraries", []):
                            entry = self._find_or_create(manifest, lib_dict)
                            if entry:
                                self._add_usage(
                                    entry,
                                    LibraryUsage(
                                        entity_type="job_task",
                                        entity_id=job_id,
                                        entity_name=job_name,
                                        task_name=task_key,
                                    ),
                                )

    # ------------------------------------------------------------------
    # Phase 3 – live library statuses
    # ------------------------------------------------------------------

    def _process_live_cluster_libs(self, manifest: LibraryManifest) -> None:
        try:
            clusters = self._client.get("/clusters/list").get("clusters", [])
        except Exception as exc:
            self._logger.warning(f"Could not list clusters: {exc}")
            return

        for cluster in clusters:
            cid = cluster.get("cluster_id", "")
            cname = cluster.get("cluster_name", "")
            try:
                resp = self._client.get(
                    "/libraries/cluster-status", {"cluster_id": cid}
                )
                for lib_status in resp.get("library_statuses", []):
                    lib_dict = lib_status.get("library", {})
                    entry = self._find_or_create(manifest, lib_dict)
                    if entry:
                        self._add_usage(
                            entry,
                            LibraryUsage(
                                entity_type="cluster_runtime",
                                entity_id=cid,
                                entity_name=cname,
                            ),
                        )
            except Exception as exc:
                self._logger.warning(
                    f"Could not fetch library status for cluster {cid} ({cname}): {exc}"
                )

    # ------------------------------------------------------------------
    # Phase 4 – download DBFS binaries
    # ------------------------------------------------------------------

    def _download_dbfs_files(self, manifest: LibraryManifest) -> None:
        for entry in manifest.file_based():
            dbfs_path = entry.dbfs_path
            if not dbfs_path:
                continue
            local_path = self._dbfs_path_to_local(dbfs_path)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            self._logger.info(f"Downloading  {dbfs_path}")
            try:
                self._download_file(dbfs_path, local_path)
                # Store relative path so the manifest is portable
                entry.local_file = os.path.relpath(local_path, self._export_dir)
                self._logger.info(f"  -> saved to {entry.local_file}")
            except Exception as exc:
                self._logger.error(f"  -> FAILED: {exc}")

    def _dbfs_path_to_local(self, dbfs_path: str) -> str:
        """Convert dbfs:/some/path  ->  {export_dir}/dbfs_files/some/path"""
        relative = dbfs_path.replace("dbfs:/", "").replace("dbfs:", "")
        return os.path.join(self._dbfs_download_dir, relative)

    def _download_file(self, dbfs_path: str, local_path: str) -> None:
        """Stream-download a DBFS file in 1 MB chunks via /dbfs/read."""
        offset = 0
        with open(local_path, "wb") as fp:
            while True:
                resp = self._client.get(
                    "/dbfs/read",
                    {"path": dbfs_path, "offset": offset, "length": _CHUNK_SIZE},
                )
                encoded = resp.get("data")
                if not encoded:
                    break
                data = base64.b64decode(encoded)
                fp.write(data)
                bytes_read = resp.get("bytes_read", len(data))
                offset += bytes_read
                if bytes_read < _CHUNK_SIZE:
                    break  # last chunk

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _find_or_create(
        self, manifest: LibraryManifest, lib_dict: dict
    ) -> Optional[LibraryEntry]:
        """Return existing entry for this lib_dict or create and register a new one."""
        entry = _parse_library_dict(lib_dict)
        if entry is None:
            return None
        key = entry.key()
        for existing in manifest.libraries:
            if existing.key() == key:
                return existing
        manifest.libraries.append(entry)
        return entry

    @staticmethod
    def _add_usage(entry: LibraryEntry, usage: LibraryUsage) -> None:
        """Append usage only if the same entity is not already listed."""
        for u in entry.used_by:
            if (
                u.entity_type == usage.entity_type
                and u.entity_id == usage.entity_id
                and u.task_name == usage.task_name
            ):
                return
        entry.used_by.append(usage)


# ---------------------------------------------------------------------------
# Module-level helper (also used by tests)
# ---------------------------------------------------------------------------

def _parse_library_dict(lib_dict: dict) -> Optional[LibraryEntry]:
    """
    Convert a Databricks library dict such as::

        {"jar":   "dbfs:/FileStore/jars/lib.jar"}
        {"pypi":  {"package": "requests>=2.28"}}
        {"maven": {"coordinates": "com.example:foo:1.0"}}
        {"cran":  {"package": "ggplot2"}}

    into a :class:`LibraryEntry`.  Returns *None* for unknown formats.
    """
    if "jar" in lib_dict:
        return LibraryEntry(lib_type="jar", dbfs_path=lib_dict["jar"], raw=lib_dict)

    if "whl" in lib_dict:
        return LibraryEntry(lib_type="whl", dbfs_path=lib_dict["whl"], raw=lib_dict)

    if "egg" in lib_dict:
        return LibraryEntry(lib_type="egg", dbfs_path=lib_dict["egg"], raw=lib_dict)

    if "pypi" in lib_dict:
        p = lib_dict["pypi"]
        return LibraryEntry(
            lib_type="pypi",
            pypi_package=p.get("package"),
            pypi_repo=p.get("repo"),
            raw=lib_dict,
        )

    if "maven" in lib_dict:
        m = lib_dict["maven"]
        return LibraryEntry(
            lib_type="maven",
            maven_coordinates=m.get("coordinates"),
            maven_repo=m.get("repo"),
            maven_exclusions=m.get("exclusions", []),
            raw=lib_dict,
        )

    if "cran" in lib_dict:
        c = lib_dict["cran"]
        return LibraryEntry(
            lib_type="cran",
            cran_package=c.get("package"),
            cran_repo=c.get("repo"),
            raw=lib_dict,
        )

    logging.warning(f"Unknown library format – skipping: {lib_dict}")
    return None
