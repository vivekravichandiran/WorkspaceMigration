"""
workspace_import/sp_migrator.py
================================
Service-Principal migration helper for cross-cloud Databricks workspace imports.

Workflow
--------
1. **scan(session_dir)**
   Walk every artifact file in session_dir and collect every SP reference:
   - ``service_principal_name`` fields (UUID / applicationId) in ACL logs
   - ``sp_app_id`` / ``run_as_sp`` tags in jobs
   - Any UUID that also has a matching ``display_name`` nearby
   Build  ``sp_mapping.json``  in session_dir:
   {
     "<source_app_id>": {
       "display_name": "my-sp",
       "source_app_id": "<uuid>",
       "gcp_app_id": null,
       "gcp_scim_id": null,
       "found_in": ["acl_notebooks.log", ...]
     }, ...
   }

2. **create_on_target(session_dir, workspace_url, token)**
   For each SP in sp_mapping.json with ``gcp_app_id == null``:
   - POST /api/2.0/preview/scim/v2/ServicePrincipals {"displayName": ...}
   - Record returned ``applicationId`` and ``id`` (SCIM id) in the mapping.
   Writes updated sp_mapping.json.

3. **patch_staging(source_dir, staging_dir, session_dir)**
   Copy source_dir → staging_dir (fresh copy every time).
   For every file in staging_dir, replace every occurrence of
   ``source_app_id`` with ``gcp_app_id`` using the mapping.
   Writes a ``patch_summary.json`` into session_dir.

CLI usage (standalone):
   python3 workspace_import/sp_migrator.py \\
     --session      EXPORT_V2_202604281109 \
     --workspace-url https://gcp.databricks.com \\
     --token dapi... \\
     [--scan-only | --create-only | --patch-only]
"""
from __future__ import annotations

import argparse
import copy
import json
import logging
import os
import re
import shutil
import sys
import urllib3
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

_LOG = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# File sets to scan / patch
# ---------------------------------------------------------------------------

# NDJSON (one JSON object per line)
_NDJSON_FILES = [
    "jobs.log",
    "clusters.log",
    "instance_pools.log",
    "acl_notebooks.log",
    "acl_jobs.log",
    "acl_clusters.log",
    "acl_cluster_policies.log",
    "acl_directories.log",
    "acl_repos.log",
    "secret_scopes_acls.log",
    "users.log",
    "libraries.log",
]

# Regular JSON files
_JSON_FILES = [
    "sql_warehouses.json",
    "dlt_pipelines.json",
    "repos.json",
    "genie_spaces.json",
    "serving_endpoints.json",
]

# Glob patterns for directory trees (patch all .json files inside)
_JSON_DIRS = [
    "groups",
    "secret_scopes",
    "lakeview_dashboards",
]

# ---------------------------------------------------------------------------
# UUID pattern
# ---------------------------------------------------------------------------
_UUID_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
    re.IGNORECASE,
)

# Keys whose values are SP applicationIds (UUIDs or numeric strings)
_SP_APP_ID_KEYS = {"service_principal_name", "sp_app_id", "applicationId"}

# Keys whose values are SP display names (for correlation)
_SP_NAME_KEYS   = {"display_name", "run_as_sp", "displayName"}

# ---------------------------------------------------------------------------
# Mapping helpers
# ---------------------------------------------------------------------------

def _load_mapping(session_dir: str) -> Dict[str, Dict]:
    path = os.path.join(session_dir, "sp_mapping.json")
    if os.path.isfile(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {}


def _save_mapping(session_dir: str, mapping: Dict[str, Dict]) -> str:
    path = os.path.join(session_dir, "sp_mapping.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(mapping, f, indent=2)
    return path


# ---------------------------------------------------------------------------
# Phase 1 – Scan
# ---------------------------------------------------------------------------

def _iter_records(path: str):
    """Yield (record_dict, raw_line) from an NDJSON file."""
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            stripped = line.strip()
            if stripped:
                try:
                    yield json.loads(stripped), stripped
                except json.JSONDecodeError:
                    pass


def _extract_sp_pairs(obj: Any, pairs: List[Tuple[str, str]]) -> None:
    """
    Recursively walk a JSON object and collect (app_id, display_name) pairs
    wherever an SP applicationId key is adjacent to a display_name key in the
    same dict.
    """
    if isinstance(obj, dict):
        app_id   = None
        disp     = None
        for k, v in obj.items():
            if k in _SP_APP_ID_KEYS and isinstance(v, str) and _UUID_RE.fullmatch(v):
                app_id = v
            if k in _SP_NAME_KEYS and isinstance(v, str) and v:
                disp = v
        if app_id:
            pairs.append((app_id, disp or ""))
        for v in obj.values():
            _extract_sp_pairs(v, pairs)
    elif isinstance(obj, list):
        for item in obj:
            _extract_sp_pairs(item, pairs)


def _scan_file(path: str, mapping: Dict[str, Dict], fname: str, ndjson: bool) -> None:
    """Scan one file and update mapping with any SP references found."""
    try:
        if ndjson:
            records = [json.loads(l) for l in open(path, encoding="utf-8") if l.strip()]
        else:
            with open(path, encoding="utf-8") as f:
                records = [json.load(f)]
    except Exception as exc:
        _LOG.debug("Could not parse %s: %s", path, exc)
        return

    pairs: List[Tuple[str, str]] = []
    for rec in records:
        _extract_sp_pairs(rec, pairs)

    for app_id, disp in pairs:
        if app_id not in mapping:
            mapping[app_id] = {
                "display_name": disp or app_id,
                "source_app_id": app_id,
                "gcp_app_id": None,
                "gcp_scim_id": None,
                "found_in": [],
            }
        entry = mapping[app_id]
        if disp and not entry["display_name"]:
            entry["display_name"] = disp
        if fname not in entry["found_in"]:
            entry["found_in"].append(fname)


def scan(session_dir: str) -> Dict[str, Dict]:
    """
    Scan all artifact files in session_dir and build/update sp_mapping.json.
    Returns the mapping dict.
    """
    mapping = _load_mapping(session_dir)

    for fname in _NDJSON_FILES:
        path = os.path.join(session_dir, fname)
        if os.path.isfile(path):
            _scan_file(path, mapping, fname, ndjson=True)

    for fname in _JSON_FILES:
        path = os.path.join(session_dir, fname)
        if os.path.isfile(path):
            _scan_file(path, mapping, fname, ndjson=False)

    for dname in _JSON_DIRS:
        dpath = os.path.join(session_dir, dname)
        if os.path.isdir(dpath):
            for fn in os.listdir(dpath):
                if fn.endswith(".json"):
                    full = os.path.join(dpath, fn)
                    _scan_file(full, mapping, f"{dname}/{fn}", ndjson=False)

    path = _save_mapping(session_dir, mapping)
    _LOG.info("SP scan complete: %d service principal(s) found → %s", len(mapping), path)
    for app_id, entry in mapping.items():
        _LOG.info("  [%s]  display_name=%-35s  found_in=%s",
                  app_id, entry["display_name"], ", ".join(entry["found_in"]))

    return mapping


# ---------------------------------------------------------------------------
# Phase 2 – Create on target GCP workspace
# ---------------------------------------------------------------------------

def _scim_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/scim+json",
        "Accept": "application/scim+json",
    }


def _list_existing_sps(workspace_url: str, token: str, verify_ssl: bool) -> Dict[str, Dict]:
    """Return dict of displayName → {id, applicationId} for existing SPs."""
    url     = f"{workspace_url.rstrip('/')}/api/2.0/preview/scim/v2/ServicePrincipals"
    headers = _scim_headers(token)
    result: Dict[str, Dict] = {}
    start = 1
    while True:
        resp = requests.get(url, headers=headers, verify=verify_ssl,
                            params={"startIndex": start, "count": 100}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        for sp in data.get("Resources", []):
            dn = sp.get("displayName", "")
            result[dn] = {
                "scim_id":    str(sp.get("id", "")),
                "app_id":     str(sp.get("applicationId", "")),
            }
        total    = data.get("totalResults", 0)
        returned = data.get("itemsPerPage", 0)
        if start + returned > total:
            break
        start += returned
    return result


def _create_sp(workspace_url: str, token: str, display_name: str,
               verify_ssl: bool) -> Tuple[str, str]:
    """
    Create a service principal on the target workspace.
    Returns (scim_id, app_id).
    """
    url  = f"{workspace_url.rstrip('/')}/api/2.0/preview/scim/v2/ServicePrincipals"
    body = {
        "schemas":     ["urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal"],
        "displayName": display_name,
        "active":      True,
    }
    resp = requests.post(url, headers=_scim_headers(token),
                         json=body, verify=verify_ssl, timeout=30)
    resp.raise_for_status()
    data    = resp.json()
    scim_id = str(data.get("id", ""))
    app_id  = str(data.get("applicationId", ""))
    return scim_id, app_id


def create_on_target(
    session_dir: str,
    workspace_url: str,
    token: str,
    verify_ssl: bool = True,
    dry_run: bool = False,
) -> Dict[str, Dict]:
    """
    For each SP in sp_mapping.json, create it on the target workspace (if not
    already present), then update the mapping with gcp_scim_id and gcp_app_id.
    """
    mapping = _load_mapping(session_dir)
    if not mapping:
        _LOG.info("sp_mapping.json is empty – run scan() first.")
        return mapping

    _LOG.info("Checking existing service principals on target …")
    existing = _list_existing_sps(workspace_url, token, verify_ssl)
    _LOG.info("  Found %d existing SP(s) on target", len(existing))
    for dn, info in existing.items():
        _LOG.debug("  existing: %s  app_id=%s  scim_id=%s", dn, info["app_id"], info["scim_id"])

    for src_app_id, entry in mapping.items():
        display_name = entry["display_name"]
        if entry.get("gcp_app_id"):
            _LOG.info("  SKIP  %s (already mapped → %s)", display_name, entry["gcp_app_id"])
            continue

        if display_name in existing:
            info = existing[display_name]
            entry["gcp_scim_id"] = info["scim_id"]
            entry["gcp_app_id"]  = info["app_id"]
            _LOG.info("  REUSE %-40s  app_id=%s  scim_id=%s",
                      display_name, info["app_id"], info["scim_id"])
            continue

        if dry_run:
            _LOG.info("  DRY   would create SP: %s", display_name)
            continue

        try:
            scim_id, app_id = _create_sp(workspace_url, token, display_name, verify_ssl)
            entry["gcp_scim_id"] = scim_id
            entry["gcp_app_id"]  = app_id
            _LOG.info("  ✓ CREATED %-36s  app_id=%-36s  scim_id=%s",
                      display_name, app_id, scim_id)
        except requests.HTTPError as exc:
            _LOG.error("  ✗ FAILED to create SP '%s': %s", display_name, exc.response.text[:200])

    path = _save_mapping(session_dir, mapping)
    _LOG.info("SP mapping updated: %s", path)

    # Print table
    print("\n  Service Principal Mapping")
    print(f"  {'Display Name':<36} {'Source App-ID':<38} {'GCP App-ID':<38}")
    print("  " + "-" * 114)
    for entry in mapping.values():
        src  = entry.get("source_app_id", "?")
        gcp  = entry.get("gcp_app_id") or "NOT CREATED"
        name = entry.get("display_name", "?")
        print(f"  {name:<36} {src:<38} {gcp:<38}")

    return mapping


# ---------------------------------------------------------------------------
# Phase 3 – Patch staging copy
# ---------------------------------------------------------------------------

def _patch_text(content: str, mapping: Dict[str, Dict]) -> Tuple[str, int]:
    """Replace all source_app_id occurrences with gcp_app_id. Returns (new_content, change_count)."""
    count = 0
    for entry in mapping.values():
        src = entry.get("source_app_id")
        gcp = entry.get("gcp_app_id")
        if src and gcp and src != gcp:
            occurrences = content.count(src)
            if occurrences:
                content = content.replace(src, gcp)
                count  += occurrences
    return content, count


def _patch_file(path: str, mapping: Dict[str, Dict]) -> int:
    """Patch a single file in-place. Returns number of substitutions made."""
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            original = f.read()
    except OSError:
        return 0
    patched, count = _patch_text(original, mapping)
    if count:
        with open(path, "w", encoding="utf-8") as f:
            f.write(patched)
    return count


def patch_staging(
    source_dir: str,
    staging_dir: str,
    session_dir: str,
) -> Dict:
    """
    1. Copy source_dir → staging_dir (fresh copy, overwrites existing).
    2. Load sp_mapping.json from session_dir.
    3. Patch every text file in staging_dir replacing source UUIDs with GCP UUIDs.
    4. Write patch_summary.json to session_dir.

    Returns summary dict.
    """
    mapping = _load_mapping(session_dir)
    if not mapping:
        _LOG.warning("sp_mapping.json is empty – no patching to do.")
        return {"patched_files": 0, "total_replacements": 0}

    # Validate all SPs have been mapped
    unmapped = [e["display_name"] for e in mapping.values() if not e.get("gcp_app_id")]
    if unmapped:
        _LOG.warning("SP(s) not yet created on target (will be left as-is): %s", unmapped)

    # Fresh copy of source into staging
    # Guard: skip destructive copy when source and staging are the same directory
    if os.path.realpath(source_dir) == os.path.realpath(staging_dir):
        _LOG.info("Source and staging are the same directory – skipping copy, patching in-place.")
    else:
        _LOG.info("Copying %s → %s …", source_dir, staging_dir)
        if os.path.exists(staging_dir):
            shutil.rmtree(staging_dir)
        shutil.copytree(source_dir, staging_dir)
    _LOG.info("Copy complete.")

    # Ensure placeholder log files exist so the migrate tool does not fail
    # on missing optional files (cluster_policies, acl_cluster_policies, etc.)
    _ENSURE_EMPTY = [
        "cluster_policies.log",
        "acl_cluster_policies.log",
        "acl_token_policies.log",
        "token_policies.log",
        "mount_conf.log",
        "dbfs.log",
        "metastore.log",
        "table_acls.log",
        "mlflow.log",
    ]
    for fname in _ENSURE_EMPTY:
        p = os.path.join(staging_dir, fname)
        if not os.path.exists(p):
            open(p, "w").close()

    # Walk and patch every text file
    patched_files = 0
    total_replacements = 0
    file_details: List[Dict] = []

    for dirpath, _, files in os.walk(staging_dir):
        for fname in files:
            # Skip binary files
            if fname.endswith((".dbc", ".jar", ".whl", ".egg", ".zip", ".gz",
                               ".png", ".jpg", ".sqlite", ".db", ".original")):
                continue
            full = os.path.join(dirpath, fname)
            rel  = os.path.relpath(full, staging_dir)
            n    = _patch_file(full, mapping)
            if n:
                patched_files      += 1
                total_replacements += n
                file_details.append({"file": rel, "replacements": n})
                _LOG.info("  patched %-50s  %d replacement(s)", rel, n)

    summary = {
        "patched_at":          datetime.now().isoformat(),
        "source_dir":          source_dir,
        "staging_dir":         staging_dir,
        "patched_files":       patched_files,
        "total_replacements":  total_replacements,
        "sp_count":            len(mapping),
        "unmapped_sps":        unmapped,
        "files":               file_details,
    }
    summary_path = os.path.join(session_dir, "patch_summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    _LOG.info("Patch complete: %d file(s), %d replacement(s) → %s",
              patched_files, total_replacements, summary_path)

    print(f"\n  Staging patch complete")
    print(f"  Staging dir        : {staging_dir}")
    print(f"  Files patched      : {patched_files}")
    print(f"  Total replacements : {total_replacements}")
    if unmapped:
        print(f"  ⚠  Unmapped SPs (left as-is): {unmapped}")

    return summary


# ---------------------------------------------------------------------------
# Convenience: run all three phases in sequence
# ---------------------------------------------------------------------------

def migrate_service_principals(
    session_dir: str,
    staging_dir: str,
    workspace_url: str,
    token: str,
    verify_ssl: bool = True,
    dry_run: bool = False,
) -> Dict:
    """Run scan → create_on_target → patch_staging in one call."""
    _LOG.info("=== Phase 1: Scan for service principals ===")
    mapping = scan(session_dir)

    if not mapping:
        _LOG.info("No service principals found in exported artifacts – nothing to do.")
        return {"sp_count": 0, "patched_files": 0}

    _LOG.info("=== Phase 2: Create service principals on target ===")
    mapping = create_on_target(session_dir, workspace_url, token,
                               verify_ssl=verify_ssl, dry_run=dry_run)

    _LOG.info("=== Phase 3: Copy export to staging and patch UUIDs ===")
    summary = patch_staging(session_dir, staging_dir, session_dir)
    summary["mapping"] = mapping
    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="sp_migrator.py",
        description="Migrate service principals across clouds for Databricks workspace import.",
    )
    p.add_argument("-s", "--session",        default=None,  help="Session ID")
    p.add_argument("-d", "--export-dir",     default="logs", help="Base export directory")
    p.add_argument("--session-dir",          default=None,  help="Direct session directory path")
    p.add_argument("--staging-dir",          default=None,
                   help="Staging directory for patched copy (default: <session_dir>_staging)")
    p.add_argument("-u", "--workspace-url",  default=None,  help="Target workspace URL")
    p.add_argument("-t", "--token",          default=None,  help="Target workspace PAT")
    p.add_argument("--no-ssl-verification",  action="store_true")
    p.add_argument("--dry-run",              action="store_true",
                   help="Scan and show what would be created; do not call APIs or copy files")
    p.add_argument("--scan-only",   action="store_true", help="Only run Phase 1 (scan)")
    p.add_argument("--create-only", action="store_true", help="Only run Phase 2 (create on target)")
    p.add_argument("--patch-only",  action="store_true", help="Only run Phase 3 (patch staging)")
    p.add_argument("--debug",       action="store_true")
    return p


def main(argv=None):
    args = _build_parser().parse_args(argv)
    logging.basicConfig(
        format="%(asctime)s;%(levelname)s;%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.DEBUG if args.debug else logging.INFO,
    )

    # Resolve session dir
    if args.session_dir:
        session_dir = args.session_dir
    elif args.session:
        session_dir = os.path.join(args.export_dir, args.session)
    else:
        # Auto-pick newest
        base = args.export_dir
        dirs = sorted([d for d in os.listdir(base) if os.path.isdir(os.path.join(base, d))],
                      reverse=True) if os.path.isdir(base) else []
        if not dirs:
            print("No session directory found.", file=sys.stderr)
            return 1
        session_dir = os.path.join(base, dirs[0])

    if not os.path.isdir(session_dir):
        print(f"Session directory not found: {session_dir}", file=sys.stderr)
        return 1

    staging_dir = args.staging_dir or session_dir.rstrip("/\\") + "_staging"

    verify_ssl = not args.no_ssl_verification

    # Determine which phases to run
    run_scan   = not (args.create_only or args.patch_only)
    run_create = not (args.scan_only   or args.patch_only)
    run_patch  = not (args.scan_only   or args.create_only)

    if run_scan:
        mapping = scan(session_dir)
        if not mapping:
            print("\nNo service principals found – nothing to do.")
            return 0

    if run_create:
        if not args.workspace_url or not args.token:
            print("--workspace-url and --token are required for Phase 2.", file=sys.stderr)
            return 1
        create_on_target(session_dir, args.workspace_url, args.token,
                         verify_ssl=verify_ssl, dry_run=args.dry_run)

    if run_patch:
        if args.dry_run:
            print("\nDry run – staging copy skipped.")
            return 0
        patch_staging(session_dir, staging_dir, session_dir)

    print(f"\nDone. Staging directory: {staging_dir}")
    print(f"SP mapping            : {os.path.join(session_dir, 'sp_mapping.json')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
