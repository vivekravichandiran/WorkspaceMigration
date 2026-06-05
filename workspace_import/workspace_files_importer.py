"""
workspace_import/workspace_files_importer.py
============================================
Imports workspace FILE objects (non-notebook files) exported by
``workspace_export.exporters.workspace_files.WorkspaceFilesExporter``.

Reads ``workspace_files_manifest.json`` from the session directory and
uploads each file to the target Databricks workspace, preserving the
original path and creating parent directories as needed.

Supported upload strategy:
  - Primary  : ``POST /api/2.0/workspace/import`` (base64-encoded body, format=AUTO)
  - Fallback : ``PUT  /api/2.0/workspace-files/<path>`` (raw body, newer API)

Usage::

    from workspace_import.workspace_files_importer import WorkspaceFilesImporter

    imp = WorkspaceFilesImporter(
        workspace_url="https://...gcp.databricks.com",
        token="dapi...",
        session_dir="/path/to/export/session",
    )
    result = imp.import_files()   # returns ImportResult
"""
from __future__ import annotations

import base64
import json
import logging
import os
import urllib3
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from urllib.parse import quote

import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

_LOG = logging.getLogger(__name__)

MANIFEST_FILE = "workspace_files_manifest.json"


# ---------------------------------------------------------------------------
# Result dataclass (mirrors extra_importers.ImportResult interface)
# ---------------------------------------------------------------------------

@dataclass
class ImportResult:
    component: str = "workspace_files"
    total: int = 0
    created: int = 0
    skipped: int = 0
    failed: int = 0
    dry_run: int = 0
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def as_dict(self) -> Dict:
        return {
            "component": self.component,
            "total": self.total,
            "created": self.created,
            "skipped": self.skipped,
            "failed": self.failed,
            "dry_run": self.dry_run,
            "errors": self.errors,
            "warnings": self.warnings,
        }


# ---------------------------------------------------------------------------
# Importer
# ---------------------------------------------------------------------------

class WorkspaceFilesImporter:
    """
    Upload workspace files from an export session directory to a target
    Databricks workspace.

    Parameters
    ----------
    workspace_url : str
        Target Databricks workspace HTTPS URL.
    token : str
        Personal Access Token for the target workspace.
    session_dir : str
        Export session directory that contains ``workspace_files_manifest.json``
        and the ``workspace_files/`` subtree.
    overwrite : bool
        Overwrite files that already exist on the target.  Defaults to ``True``.
    verify_ssl : bool
        Set False to disable SSL certificate verification.
    dry_run : bool
        If True, print what would be uploaded without calling any API.
    skip_paths : list[str]
        Workspace path prefixes to skip (e.g. ``["/Users/old_user@company.com"]``).
    """

    def __init__(
        self,
        workspace_url: str,
        token: str,
        session_dir: str,
        overwrite: bool = True,
        verify_ssl: bool = True,
        dry_run: bool = False,
        skip_paths: Optional[List[str]] = None,
    ) -> None:
        self._base = workspace_url.rstrip("/")
        self._session_dir = session_dir
        self._overwrite = overwrite
        self._verify = verify_ssl
        self._dry = dry_run
        self._skip_paths = skip_paths or []
        self._http = requests.Session()
        self._http.headers["Authorization"] = f"Bearer {token}"

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def import_files(self) -> ImportResult:
        """Read manifest and upload all exported workspace files."""
        res = ImportResult()

        manifest_path = os.path.join(self._session_dir, MANIFEST_FILE)
        if not os.path.isfile(manifest_path):
            _LOG.info("[workspace_files] No manifest found at %s – skipping.", manifest_path)
            return res

        with open(manifest_path, encoding="utf-8") as fp:
            manifest = json.load(fp)

        files = manifest.get("files", [])
        res.total = len(files)

        if res.total == 0:
            _LOG.info("[workspace_files] Manifest is empty – nothing to import.")
            return res

        _LOG.info("[workspace_files] Importing %d file(s) …", res.total)

        for entry in files:
            ws_path = entry.get("workspace_path", "")
            local_rel = entry.get("local_path", "")
            local_abs = os.path.join(self._session_dir, local_rel)

            # Skip by path prefix
            if any(ws_path.startswith(p) for p in self._skip_paths):
                _LOG.info("[workspace_files] skip (path filter): %s", ws_path)
                res.skipped += 1
                continue

            if not os.path.isfile(local_abs):
                msg = f"local file missing: {local_abs}"
                _LOG.warning("[workspace_files] ✗ %s → %s", ws_path, msg)
                res.warnings.append(msg)
                res.skipped += 1
                continue

            if self._dry:
                _LOG.info("[workspace_files] DRY  %s", ws_path)
                res.dry_run += 1
                continue

            ok, err = self._upload(ws_path, local_abs)
            if ok:
                res.created += 1
                _LOG.info("[workspace_files] ✓ uploaded  %s", ws_path)
            else:
                res.failed += 1
                res.errors.append(f"{ws_path}: {err}")
                _LOG.error("[workspace_files] ✗ failed    %s  —  %s", ws_path, err)

        return res

    # ------------------------------------------------------------------
    # Upload helpers
    # ------------------------------------------------------------------

    def _ensure_parent_dir(self, ws_path: str) -> bool:
        """Create parent directory on the target workspace."""
        parent = "/".join(ws_path.rstrip("/").split("/")[:-1])
        if not parent or parent == "/":
            return True
        try:
            r = self._http.post(
                f"{self._base}/api/2.0/workspace/mkdirs",
                json={"path": parent},
                verify=self._verify,
                timeout=30,
            )
            if r.ok or r.status_code == 400:  # 400 = already exists
                return True
            _LOG.debug("[workspace_files] mkdirs %s → %s", parent, r.status_code)
            return False
        except Exception as exc:
            _LOG.debug("[workspace_files] mkdirs error: %s", exc)
            return False

    def _upload(self, ws_path: str, local_path: str) -> tuple[bool, str]:
        """
        Upload a file to *ws_path* on the target workspace.

        Strategy 1: workspace/import (base64, format=AUTO) — works on all versions.
        Strategy 2: workspace-files PUT (raw bytes) — newer API, better for large files.
        """
        with open(local_path, "rb") as fp:
            raw = fp.read()

        self._ensure_parent_dir(ws_path)

        # --- Strategy 1: workspace/import ---
        try:
            b64 = base64.b64encode(raw).decode()
            payload = {
                "path": ws_path,
                "format": "AUTO",
                "overwrite": self._overwrite,
                "content": b64,
            }
            r = self._http.post(
                f"{self._base}/api/2.0/workspace/import",
                json=payload,
                verify=self._verify,
                timeout=60,
            )
            if r.ok:
                return True, ""
            # 400 with "already exists" means it's there; treat as success if not overwriting
            body = r.text
            if r.status_code == 400 and "already exists" in body.lower() and not self._overwrite:
                return True, ""
            # Try fallback for non-200 responses
            _LOG.debug("[workspace_files] workspace/import %s → %s %s", ws_path, r.status_code, body[:120])
        except Exception as exc:
            _LOG.debug("[workspace_files] workspace/import exception: %s", exc)

        # --- Strategy 2: workspace-files PUT (newer API) ---
        try:
            encoded_path = quote(ws_path.lstrip("/"), safe="/")
            params = {"overwrite": "true"} if self._overwrite else {}
            headers = {"Content-Type": "application/octet-stream"}
            r = self._http.put(
                f"{self._base}/api/2.0/workspace-files/{encoded_path}",
                data=raw,
                params=params,
                headers=headers,
                verify=self._verify,
                timeout=60,
            )
            if r.ok or r.status_code == 409:  # 409 = conflict/already exists
                return True, ""
            return False, f"HTTP {r.status_code}: {r.text[:200]}"
        except Exception as exc:
            return False, str(exc)
