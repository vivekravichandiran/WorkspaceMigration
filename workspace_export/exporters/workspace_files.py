"""
workspace_export/exporters/workspace_files.py
=============================================
Exports all workspace **FILE** objects (non-notebook files) from a Databricks
workspace, preserving the original folder structure.

Supported extensions (all FILE-type objects are exported; extensions here are
used only for display/filtering when ``extensions`` is customised):

    .py  .md  .sql  .yaml  .yml  .toml  .json  .csv  .txt
    .sh  .mhtml  .ipynb  .html  .r  .scala  .js  .ts  .xml

The exporter stores downloaded files under::

    <export_dir>/workspace_files/<workspace_path>

and writes a manifest at::

    <export_dir>/workspace_files_manifest.json

Usage::

    from workspace_export.exporters.workspace_files import WorkspaceFilesExporter

    exp = WorkspaceFilesExporter(workspace_url, token, export_dir)
    stats = exp.export()        # {"exported": N, "skipped": N, "failed": N}
"""
from __future__ import annotations

import base64
import json
import logging
import os
import re
import time
import urllib3
from typing import Dict, List, Optional, Set, Tuple

import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

_LOG = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default target extensions (all FILE objects are downloaded; this set is used
# when the caller passes extensions=DEFAULT_EXTENSIONS to restrict scope).
# Pass extensions=None to export every FILE object regardless of extension.
# ---------------------------------------------------------------------------
DEFAULT_EXTENSIONS: Set[str] = {
    ".py", ".md", ".sql", ".yaml", ".yml", ".toml", ".json", ".csv",
    ".txt", ".sh", ".mhtml", ".ipynb", ".html", ".r", ".scala",
    ".js", ".ts", ".xml", ".env", ".cfg", ".ini", ".conf",
}

# Workspace paths to skip during the recursive walk
_SKIP_PREFIXES = ("/Trash",)


class WorkspaceFilesExporter:
    """
    Downloads all FILE-type objects from a Databricks workspace.

    Parameters
    ----------
    workspace_url : str
        Source workspace HTTPS URL.
    token : str
        Personal Access Token.
    export_dir : str
        Root export directory (files saved under ``workspace_files/``).
    extensions : set[str] | None
        If provided, only export files whose extension (lower-cased) is in this
        set.  Pass ``None`` to export every FILE object.
    verify_ssl : bool
        Set False to disable SSL certificate verification.
    num_parallel : int
        Not currently used (downloads are sequential to avoid rate-limiting).
    """

    def __init__(
        self,
        workspace_url: str,
        token: str,
        export_dir: str,
        extensions: Optional[Set[str]] = None,
        verify_ssl: bool = True,
    ) -> None:
        self._base = workspace_url.rstrip("/")
        self._export_dir = export_dir
        self._extensions = extensions  # None → no filter (export all)
        self._verify = verify_ssl
        self._session = requests.Session()
        self._session.headers["Authorization"] = f"Bearer {token}"

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def export(self, root: str = "/") -> Dict:
        """
        Walk the workspace from *root* and download all FILE objects.

        Returns
        -------
        dict with keys ``exported``, ``skipped``, ``failed``, ``manifest``.
        """
        files_dir = os.path.join(self._export_dir, "workspace_files")
        os.makedirs(files_dir, exist_ok=True)

        all_files: List[Dict] = []
        self._walk(root, all_files)

        exported = 0
        skipped = 0
        failed = 0
        manifest_entries: List[Dict] = []

        for item in all_files:
            ws_path = item["path"]
            ext = os.path.splitext(ws_path)[1].lower()

            # Extension filter
            if self._extensions is not None and ext not in self._extensions:
                _LOG.debug("[workspace_files] skip (ext=%s): %s", ext, ws_path)
                skipped += 1
                continue

            # Determine local save path (mirror workspace path)
            # Remove leading "/" so os.path.join works correctly
            rel_path = ws_path.lstrip("/")
            local_path = os.path.join(files_dir, rel_path)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

            ok, size = self._download(ws_path, local_path)
            if ok:
                exported += 1
                manifest_entries.append({
                    "workspace_path": ws_path,
                    "local_path": os.path.relpath(local_path, self._export_dir),
                    "size_bytes": size,
                    "object_id": item.get("object_id"),
                })
                _LOG.info("[workspace_files] ✓ %s  (%d bytes)", ws_path, size)
            else:
                failed += 1

        # Write manifest
        manifest = {
            "total": len(all_files),
            "exported": exported,
            "skipped": skipped,
            "failed": failed,
            "files": manifest_entries,
        }
        manifest_path = os.path.join(self._export_dir, "workspace_files_manifest.json")
        with open(manifest_path, "w", encoding="utf-8") as fp:
            json.dump(manifest, fp, indent=2)

        _LOG.info(
            "[workspace_files] Done — exported=%d  skipped=%d  failed=%d",
            exported, skipped, failed,
        )
        return {"exported": exported, "skipped": skipped, "failed": failed,
                "manifest": manifest_path}

    # ------------------------------------------------------------------
    # Recursive workspace walker
    # ------------------------------------------------------------------

    def _walk(self, path: str, collector: List[Dict]) -> None:
        """Recursively list workspace directory and collect FILE objects."""
        if any(path.startswith(p) for p in _SKIP_PREFIXES):
            return

        try:
            resp = self._session.get(
                f"{self._base}/api/2.0/workspace/list",
                params={"path": path},
                verify=self._verify,
                timeout=30,
            )
        except requests.RequestException as exc:
            _LOG.warning("[workspace_files] list failed for %s: %s", path, exc)
            return

        if resp.status_code == 404:
            return
        if not resp.ok:
            _LOG.warning("[workspace_files] list %s → HTTP %s", path, resp.status_code)
            return

        items = resp.json().get("objects", [])
        for item in items:
            obj_type = item.get("object_type")
            obj_path = item.get("path", "")
            if obj_type == "DIRECTORY":
                self._walk(obj_path, collector)
            elif obj_type == "FILE":
                collector.append(item)

    # ------------------------------------------------------------------
    # File download
    # ------------------------------------------------------------------

    def _download(self, ws_path: str, local_path: str) -> Tuple[bool, int]:
        """
        Download a workspace FILE object to *local_path*.

        Tries two strategies:
        1. ``direct_download=true`` → raw bytes in response body
        2. ``direct_download=false`` → JSON ``{"content": "<base64>"}``

        Returns (success, size_bytes).
        """
        for direct in (True, False):
            try:
                resp = self._session.get(
                    f"{self._base}/api/2.0/workspace/export",
                    params={"path": ws_path, "format": "AUTO",
                            "direct_download": str(direct).lower()},
                    verify=self._verify,
                    timeout=60,
                    stream=direct,
                )
                if not resp.ok:
                    continue

                if direct:
                    data = resp.content
                else:
                    payload = resp.json()
                    b64 = payload.get("content", "")
                    data = base64.b64decode(b64) if b64 else b""

                with open(local_path, "wb") as fp:
                    fp.write(data)
                return True, len(data)

            except Exception as exc:
                _LOG.debug("[workspace_files] download attempt failed %s: %s", ws_path, exc)
                continue

        _LOG.error("[workspace_files] ✗ Failed to download: %s", ws_path)
        return False, 0
