#!/usr/bin/env python3
"""
import_jobs_gcp.py
==================
GCP Import Pre-processor for Databricks workspace migration.

This utility runs in two stages, designed to work alongside the
``databrickslabs/migrate`` tool:

┌──────────────────────────────────────────────────────────────────────────┐
│  Stage 1 – Pre-process (this script, default mode)                       │
│    Transforms exported log files in-place so the migrate tool's          │
│    import pipeline reads GCP-compatible definitions.                      │
│                                                                           │
│    Files transformed:                                                     │
│      jobs.log          – cluster specs inside job definitions             │
│      clusters.log      – interactive cluster definitions                  │
│      instance_pools.log – pool definitions                                │
│                                                                           │
│    Transformations applied to every cluster spec found:                   │
│      • Remove  azure_attributes / aws_attributes                          │
│      • Remove  enable_elastic_disk, instance_profile_arn, ebs_* fields   │
│      • Remove  azure_disk_volume_type from disk_spec                      │
│      • Inject  gcp_attributes  (from gcp_import_config.json)             │
│      • Map     node_type_id / driver_node_type_id  (node_type_mapping.csv)│
│      • Sanitise creator_user_name + cluster_name                         │
│          @ → _   apostrophes removed   spaces → _                        │
│      • Strip   :::JOB_ID suffix from job names                            │
│      • Force   schedule.pause_status = PAUSED                            │
│                                                                           │
│  Stage 2 – Import extra components (--import-extra)                      │
│    Imports components NOT covered by the migrate tool via REST API:       │
│      • SQL Warehouses       • AI/BI Dashboards                            │
│      • DLT Pipelines        • Genie AI Spaces                            │
│      • Git Repos            • Model Serving Endpoints                     │
└──────────────────────────────────────────────────────────────────────────┘

Typical usage (orchestrated by import_gcp.sh):

  # Step 1 – transform exported logs IN-PLACE
  python3 import_jobs_gcp.py \\
      --session PROD_MIGRATION_2024

  # Step 2 – run the migrate tool's built-in import pipeline
  python3 migration_pipeline.py \\
      --profile DST_PROFILE --import-pipeline --use-checkpoint \\
      --session PROD_MIGRATION_2024

  # Step 3 – import components the migrate tool doesn't cover
  python3 import_jobs_gcp.py \\
      --workspace-url https://XXXXXXXX.gcp.databricks.com \\
      --token dapi... \\
      --session PROD_MIGRATION_2024 \\
      --import-extra

Standalone (all in one command):
  python3 import_jobs_gcp.py \\
      --workspace-url https://XXXXXXXX.gcp.databricks.com \\
      --token dapi... \\
      --session PROD_MIGRATION_2024 \\
      --preprocess --import-extra
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
from typing import Any, Dict, List, Optional, Tuple

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

_LOG = logging.getLogger(__name__)

_HERE = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_CONFIG  = os.path.join(_HERE, "gcp_import_config.json")
_DEFAULT_MAPPING = os.path.join(_HERE, "node_type_mapping.csv")
_DEFAULT_EXPORT_DIR = os.path.join(_HERE, "logs")

# Log files that contain cluster specs and need transformation
_CLUSTER_LOG_FILES = ["clusters.log", "instance_pools.log"]
_JOB_LOG_FILES = ["jobs.log"]
_ALL_TRANSFORM_FILES = _JOB_LOG_FILES + _CLUSTER_LOG_FILES


# ---------------------------------------------------------------------------
# Config & mapping loaders
# ---------------------------------------------------------------------------

def load_config(path: str) -> Dict:
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)
    return {k: v for k, v in raw.items() if not k.startswith("_")}


def load_node_type_mapping(path: str) -> Dict[str, str]:
    """Return {source_node_type_lower → gcp_node_type}."""
    mapping: Dict[str, str] = {}
    with open(path, encoding="utf-8", newline="") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(",")
            if len(parts) >= 2:
                src, dst = parts[0].strip(), parts[1].strip()
                if src and dst and not src.startswith("#"):
                    mapping[src.lower()] = dst
    _LOG.info("Loaded %d node-type mappings", len(mapping))
    return mapping


def load_zone_hint_mapping(path: str) -> Dict[str, str]:
    """Return {gcp_node_type_lower → gcp_zone_hint} from the optional gcp_zone_hint column."""
    hints: Dict[str, str] = {}
    try:
        with open(path, encoding="utf-8", newline="") as f:
            header = None
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = [p.strip() for p in line.split(",")]
                if header is None:
                    header = parts
                    continue
                if len(parts) >= 7 and parts[6]:  # gcp_zone_hint column
                    gcp_type = parts[1].strip().lower()
                    if gcp_type:
                        hints[gcp_type] = parts[6]
    except Exception:
        pass
    return hints


# ---------------------------------------------------------------------------
# Text sanitisers
# ---------------------------------------------------------------------------

def _sanitise(name: str, cfg: Dict) -> str:
    if not name:
        return name
    t = cfg.get("cluster_name_transforms", {})
    if t.get("replace_at_with"):
        name = name.replace("@", t["replace_at_with"])
    if t.get("remove_apostrophes"):
        name = name.replace("'", "").replace("\u2019", "")
    if t.get("replace_spaces_with"):
        name = name.replace(" ", t["replace_spaces_with"])
    return name


def _strip_job_id_suffix(name: str, cfg: Dict) -> str:
    opts = cfg.get("job_name_suffix", {})
    if not opts.get("strip_id_suffix", True):
        return name
    sep = opts.get("suffix_separator", ":::")
    idx = name.find(sep)
    return name[:idx].rstrip() if idx != -1 else name


# ---------------------------------------------------------------------------
# Cluster spec transformer
# ---------------------------------------------------------------------------

# Azure availability → GCP availability mapping
_AZ_TO_GCP_AVAILABILITY: Dict[str, str] = {
    "ON_DEMAND_AZURE":           "ON_DEMAND_GCP",
    "SPOT_AZURE":                "PREEMPTIBLE_GCP",
    "SPOT_WITH_FALLBACK_AZURE":  "SPOT_WITH_FALLBACK_GCP",
}

# Azure use_preemptible_executors → GCP (legacy boolean field on some cluster types)
# Azure first_on_demand is 1-indexed: N means first N nodes on-demand, rest spot.
# GCP has the same semantics.


def _derive_availability(azure_attrs: Dict, cfg_default: str) -> tuple:
    """
    Derive GCP availability + first_on_demand from Azure cluster attributes.

    Returns (availability_str, first_on_demand_int_or_None).
    Falls back to cfg_default if azure_attrs has no explicit availability set.
    """
    az_av = azure_attrs.get("availability", "")
    gcp_av = _AZ_TO_GCP_AVAILABILITY.get(az_av, "")

    if not gcp_av:
        # No explicit Azure availability — use the config default
        gcp_av = cfg_default or "ON_DEMAND_GCP"

    # Carry over first_on_demand if set on the Azure side
    first_on_demand = azure_attrs.get("first_on_demand")

    return gcp_av, first_on_demand


def _build_gcp_attributes(cfg: Dict, source_cluster: Optional[Dict] = None) -> Dict:
    """
    Build the gcp_attributes dict for a cluster or pool.

    When *source_cluster* is provided the Azure availability settings are
    inspected and mapped to their GCP equivalents:

      Azure                      →  GCP
      ─────────────────────────────────────────────────────
      ON_DEMAND_AZURE            →  ON_DEMAND_GCP
      SPOT_AZURE                 →  PREEMPTIBLE_GCP
      SPOT_WITH_FALLBACK_AZURE   →  SPOT_WITH_FALLBACK_GCP
      (not set / default)        →  config default (ON_DEMAND_GCP)

    first_on_demand is carried over from azure_attributes when present.
    """
    # Start from the config-level defaults (zone_id, local_ssd_count, etc.)
    attrs = {k: v for k, v in cfg.get("gcp_attributes", {}).items()
             if not k.startswith("_")}
    for k, v in cfg.get("gcp_attributes_optional", {}).items():
        if v is not None and not k.startswith("_"):
            attrs[k] = v

    if source_cluster is not None:
        az = source_cluster.get("azure_attributes", {})
        cfg_default_av = attrs.get("availability", "ON_DEMAND_GCP")
        gcp_av, first_on_demand = _derive_availability(az, cfg_default_av)
        attrs["availability"] = gcp_av

        # Only override first_on_demand when the source explicitly set it
        if first_on_demand is not None:
            attrs["first_on_demand"] = first_on_demand

        if az:
            _LOG.debug(
                "  availability: azure=%s → gcp=%s  first_on_demand=%s",
                az.get("availability", "(default)"), gcp_av, first_on_demand,
            )

    return attrs


def _transform_disk_spec(disk_spec: Optional[Dict]) -> Optional[Dict]:
    """Strip Azure-specific disk type keys from disk_spec."""
    if not disk_spec:
        return disk_spec
    ds = copy.deepcopy(disk_spec)
    dt = ds.get("disk_type", {})
    dt.pop("azure_disk_volume_type", None)
    dt.pop("ebs_volume_type", None)
    if not dt:
        ds.pop("disk_type", None)
    else:
        ds["disk_type"] = dt
    # Remove zero-sized disk specs that are just placeholders
    if ds.get("disk_count", 0) == 0 and not dt:
        return None
    return ds or None


def transform_cluster_spec(
    cluster: Dict,
    cfg: Dict,
    node_map: Dict[str, str],
    warnings: List[str],
    label: str = "",
) -> Dict:
    """Return a new GCP-compatible cluster spec dict."""
    c = copy.deepcopy(cluster)

    # Capture azure_attributes BEFORE removing them so we can map availability
    azure_attrs = cluster.get("azure_attributes", {})

    # Remove cloud-specific fields
    for field in cfg.get("fields_to_remove_from_cluster", []):
        c.pop(field, None)

    # Fix disk_spec
    if "disk_spec" in c:
        cleaned = _transform_disk_spec(c["disk_spec"])
        if cleaned:
            c["disk_spec"] = cleaned
        else:
            c.pop("disk_spec", None)

    # Inject gcp_attributes — derive availability from source Azure attributes
    c["gcp_attributes"] = _build_gcp_attributes(cfg, source_cluster={"azure_attributes": azure_attrs})

    # Map node types
    for key in ("node_type_id", "driver_node_type_id"):
        original = c.get(key)
        if original:
            gcp = node_map.get(original.lower())
            if gcp:
                c[key] = gcp
                _LOG.debug("  %s %s: %s → %s", label, key, original, gcp)
            else:
                msg = f"{label}: no mapping for '{original}' ({key}) – kept as-is"

    # Strip Photon from spark_version for node types that don't support it on GCP.
    # n1-standard-8 and larger standard-N instances do not support Photon workers.
    _PHOTON_INCOMPATIBLE_PREFIXES = ("n1-standard-8", "n1-standard-16", "n1-standard-32",
                                     "n1-standard-64", "n1-standard-96")
    sv = c.get("spark_version", "")
    node = c.get("node_type_id", "")
    if "photon" in sv and any(node.startswith(p) for p in _PHOTON_INCOMPATIBLE_PREFIXES):
        original_sv = sv
        c["spark_version"] = sv.replace("-photon-scala", "-scala")
        warnings.append(
            f"{label}: stripped Photon from spark_version for node {node} "
            f"({original_sv} → {c['spark_version']})"
        )
        _LOG.info("  %s stripped Photon from spark_version: %s → %s (node=%s)",
                  label, original_sv, c["spark_version"], node)

    # Sanitise cluster_name
    if "cluster_name" in c:
        c["cluster_name"] = _sanitise(c["cluster_name"], cfg)

    # Sanitise creator_user_name inside cluster spec (some versions embed it)
    if "creator_user_name" in c:
        c["creator_user_name"] = _sanitise(c["creator_user_name"], cfg)

    return c


# ---------------------------------------------------------------------------
# Per-file transformers
# ---------------------------------------------------------------------------

def _transform_job(job: Dict, cfg: Dict, node_map: Dict[str, str], warnings: List[str]) -> Dict:
    j = copy.deepcopy(job)

    # Sanitise outer-level creator_user_name
    if "creator_user_name" in j:
        j["creator_user_name"] = _sanitise(j["creator_user_name"], cfg)

    # Remove unwanted outer fields.
    # NOTE: job_id is never stripped – the migrate tool uses it for checkpoint tracking
    # and building the old-id → new-id job mapping (job_id_map.log).
    for f in cfg.get("fields_to_remove_from_job", []):
        if f == "job_id":
            continue  # always preserve job_id
        j.pop(f, None)

    settings = j.get("settings", {})

    # Strip :::JOB_ID from name
    if "name" in settings:
        settings["name"] = _strip_job_id_suffix(settings["name"], cfg)

    # Pause schedules
    pause = cfg.get("schedule_behaviour", {}).get("set_pause_status", "PAUSED")
    if pause and "schedule" in settings:
        settings["schedule"]["pause_status"] = pause

    # Clear email notifications if requested
    if cfg.get("email_notifications", {}).get("clear_on_import"):
        settings.pop("email_notifications", None)

    label = settings.get("name", "?")

    # Transform clusters – SINGLE_TASK
    if "new_cluster" in settings:
        settings["new_cluster"] = transform_cluster_spec(
            settings["new_cluster"], cfg, node_map, warnings, label
        )

    # Transform clusters – MULTI_TASK tasks[]
    for task in settings.get("tasks", []):
        if "new_cluster" in task:
            task["new_cluster"] = transform_cluster_spec(
                task["new_cluster"], cfg, node_map, warnings,
                f"{label}/{task.get('task_key', '?')}"
            )

    # Transform job_clusters[]
    for jc in settings.get("job_clusters", []):
        if "new_cluster" in jc:
            jc["new_cluster"] = transform_cluster_spec(
                jc["new_cluster"], cfg, node_map, warnings,
                f"{label}/job_cluster:{jc.get('job_cluster_key', '?')}"
            )

    j["settings"] = settings
    return j


def _transform_cluster_record(cluster: Dict, cfg: Dict, node_map: Dict[str, str], warnings: List[str]) -> Dict:
    """Transform a standalone cluster record (from clusters.log)."""
    c = copy.deepcopy(cluster)

    # Sanitise outer fields
    if "creator_user_name" in c:
        c["creator_user_name"] = _sanitise(c["creator_user_name"], cfg)

    # Remove runtime-only outer fields irrelevant to recreation.
    # NOTE: cluster_id is preserved intentionally – the migrate tool's
    # get_cluster_id_mapping() function reads it to build old→new cluster ID
    # mappings for job references.
    for f in ["state", "state_message", "start_time", "terminated_time",
              "last_state_loss_time", "default_tags", "cluster_memory_mb", "cluster_cores",
              "cluster_source", "pinned_by_user_name", "last_activity_time",
              "init_scripts_safe_mode", "jdbc_port", "spark_context_id"]:
        c.pop(f, None)

    return transform_cluster_spec(c, cfg, node_map, warnings, c.get("cluster_name", "?"))


def _transform_pool_record(pool: Dict, cfg: Dict, node_map: Dict[str, str], warnings: List[str],
                            zone_hints: Optional[Dict[str, str]] = None) -> Dict:
    """Transform an instance pool record (from instance_pools.log)."""
    p = copy.deepcopy(pool)

    # Capture azure_attributes BEFORE removing them so we can map availability
    azure_attrs = pool.get("azure_attributes", {})

    # Remove runtime / read-only fields
    # NOTE: instance_pool_id is intentionally KEPT so that ClustersClient.get_instance_pool_id_mapping()
    # can build the old-Azure-ID → new-GCP-ID mapping after pool creation.
    # The create API ignores instance_pool_id in the POST body.
    for f in ["default_tags", "stats", "state",
              "status", "pending_used_count", "pending_idle_count",
              "used_count", "idle_count"]:
        p.pop(f, None)

    # Remove Azure cloud-specific attributes (azure_attributes, aws_attributes, etc.)
    for f in cfg.get("fields_to_remove_from_cluster", []):
        p.pop(f, None)

    # Fix disk_spec
    if "disk_spec" in p:
        cleaned = _transform_disk_spec(p["disk_spec"])
        if cleaned:
            p["disk_spec"] = cleaned
        else:
            p.pop("disk_spec", None)

    # Inject gcp_attributes for pools — derive availability from source Azure attributes
    p["gcp_attributes"] = _build_gcp_attributes(cfg, source_cluster={"azure_attributes": azure_attrs})

    # Map node type
    original = p.get("node_type_id")
    if original:
        gcp = node_map.get(original.lower(), original)  # keep as-is if no mapping (GCP→GCP identity)
        p["node_type_id"] = gcp
        if gcp != original:
            _LOG.debug("  pool '%s': node_type %s → %s", p.get("instance_pool_name", "?"), original, gcp)

        # Apply zone hint if defined (e.g. n1-standard-4 requires explicit zone in us-east1)
        if zone_hints:
            hint = zone_hints.get(gcp.lower(), "")
            if hint:
                p["gcp_attributes"]["zone_id"] = hint
                _LOG.info("  pool '%s': applied zone_hint=%s for node_type=%s",
                          p.get("instance_pool_name", "?"), hint, gcp)
    else:
        warnings.append(f"pool '{p.get('instance_pool_name','?')}': no mapping for '{original}'")

    # Sanitise pool name
    if "instance_pool_name" in p:
        p["instance_pool_name"] = _sanitise(p["instance_pool_name"], cfg)

    return p


# ---------------------------------------------------------------------------
# Log file processor
# ---------------------------------------------------------------------------

def _read_jsonl(path: str) -> List[Dict]:
    records = []
    with open(path, encoding="utf-8") as f:
        for lineno, raw in enumerate(f, 1):
            raw = raw.strip()
            if not raw:
                continue
            try:
                records.append(json.loads(raw))
            except json.JSONDecodeError as e:
                _LOG.warning("  %s line %d: JSON error – %s", path, lineno, e)
    return records


def _write_jsonl(path: str, records: List[Dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")


def _write_json(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def _backup(path: str) -> str:
    backup = path + ".original"
    if not os.path.exists(backup):
        shutil.copy2(path, backup)
        _LOG.info("  Backup created: %s", backup)
    return backup


def preprocess_session(
    session_dir: str,
    cfg: Dict,
    node_map: Dict[str, str],
    zone_hints: Optional[Dict[str, str]] = None,
) -> Dict:
    """
    Transform all log files in session_dir in-place AND write GCP-ready
    JSON bundles to <session_dir>/gcp_ready/ for inspection / manual import.
    Returns a summary dict.
    """
    gcp_ready_dir = os.path.join(session_dir, "gcp_ready")
    os.makedirs(gcp_ready_dir, exist_ok=True)

    summary: Dict = {
        "processed_at": datetime.now().isoformat(),
        "session_dir": session_dir,
        "gcp_ready_dir": gcp_ready_dir,
        "transformed": {},
        "all_warnings": [],
        "errors": [],
        "node_type_changes": [],
    }

    # ── jobs.log ──────────────────────────────────────────────────────────
    jobs_path = os.path.join(session_dir, "jobs.log")
    if os.path.isfile(jobs_path):
        _LOG.info("Transforming jobs.log …")
        _backup(jobs_path)
        jobs = _read_jsonl(jobs_path)
        out, warnings = [], []
        for job in jobs:
            transformed = _transform_job(job, cfg, node_map, warnings)
            out.append(transformed)
            # Track node type changes for reporting
            _record_node_changes(job, transformed, "job", summary["node_type_changes"])
        _write_jsonl(jobs_path, out)
        # Save GCP-ready bundle: just the settings payloads (ready for API create)
        gcp_jobs = [{"name": j.get("settings", {}).get("name", "?"),
                     "original_name": job.get("settings", {}).get("name", "?"),
                     "creator_user_name": j.get("creator_user_name"),
                     "settings": j.get("settings", {})}
                    for j, job in zip(out, jobs)]
        _write_json(os.path.join(gcp_ready_dir, "jobs.json"), gcp_jobs)
        summary["all_warnings"].extend(warnings)
        summary["transformed"]["jobs.log"] = {
            "count": len(out), "warnings": len(warnings),
            "gcp_ready_file": "gcp_ready/jobs.json",
        }
        _LOG.info("  → %d job(s) transformed, %d warning(s)", len(out), len(warnings))
    else:
        _LOG.warning("jobs.log not found in %s – skipping", session_dir)

    # ── acl_jobs.log – strip :::JOB_ID suffix from job_name field ────────
    acl_jobs_path = os.path.join(session_dir, "acl_jobs.log")
    if os.path.isfile(acl_jobs_path):
        acl_jobs = _read_jsonl(acl_jobs_path)
        changed = 0
        for acl in acl_jobs:
            jn = acl.get("job_name", "")
            if ":::" in jn:
                acl["job_name"] = jn.split(":::")[0].strip()
                changed += 1
        if changed:
            _backup(acl_jobs_path)
            _write_jsonl(acl_jobs_path, acl_jobs)
            _LOG.info("  acl_jobs.log: stripped :::JOB_ID suffix from %d job name(s)", changed)
            summary["transformed"]["acl_jobs.log"] = {"count": len(acl_jobs), "warnings": 0}

    # ── clusters.log ──────────────────────────────────────────────────────
    clusters_path = os.path.join(session_dir, "clusters.log")
    if os.path.isfile(clusters_path):
        _LOG.info("Transforming clusters.log …")
        _backup(clusters_path)
        clusters = _read_jsonl(clusters_path)
        out, warnings = [], []
        for c in clusters:
            transformed = _transform_cluster_record(c, cfg, node_map, warnings)
            out.append(transformed)
            _record_node_changes(c, transformed, "cluster", summary["node_type_changes"])
        _write_jsonl(clusters_path, out)
        _write_json(os.path.join(gcp_ready_dir, "clusters.json"), out)
        summary["all_warnings"].extend(warnings)
        summary["transformed"]["clusters.log"] = {
            "count": len(out), "warnings": len(warnings),
            "gcp_ready_file": "gcp_ready/clusters.json",
        }
        _LOG.info("  → %d cluster(s) transformed, %d warning(s)", len(out), len(warnings))
    else:
        _LOG.warning("clusters.log not found in %s – skipping", session_dir)

    # ── instance_pools.log ────────────────────────────────────────────────
    pools_path = os.path.join(session_dir, "instance_pools.log")
    if os.path.isfile(pools_path):
        _LOG.info("Transforming instance_pools.log …")
        _backup(pools_path)
        pools = _read_jsonl(pools_path)
        out, warnings = [], []
        for p in pools:
            transformed = _transform_pool_record(p, cfg, node_map, warnings, zone_hints=zone_hints)
            out.append(transformed)
            _record_node_changes(p, transformed, "pool", summary["node_type_changes"])
        _write_jsonl(pools_path, out)
        _write_json(os.path.join(gcp_ready_dir, "instance_pools.json"), out)
        summary["all_warnings"].extend(warnings)
        summary["transformed"]["instance_pools.log"] = {
            "count": len(out), "warnings": len(warnings),
            "gcp_ready_file": "gcp_ready/instance_pools.json",
        }
        _LOG.info("  → %d pool(s) transformed, %d warning(s)", len(out), len(warnings))
    else:
        _LOG.info("instance_pools.log not found – skipping")

    # ── Write transformation manifest ─────────────────────────────────────
    _write_json(os.path.join(session_dir, "gcp_transform_manifest.json"), summary)
    _LOG.info("Transformation manifest: %s/gcp_transform_manifest.json", session_dir)
    _LOG.info("GCP-ready JSON bundles : %s/", gcp_ready_dir)

    return summary


def _record_node_changes(original: Dict, transformed: Dict, kind: str, out: List) -> None:
    """Collect node_type_id changes for the HTML report."""
    for key in ("node_type_id", "driver_node_type_id"):
        orig_nc = _first_cluster(original)
        new_nc  = _first_cluster(transformed)
        if orig_nc and new_nc:
            before = orig_nc.get(key)
            after  = new_nc.get(key)
            if before and after and before != after:
                out.append({
                    "kind": kind,
                    "name": (original.get("settings", {}).get("name")
                             or original.get("cluster_name")
                             or original.get("instance_pool_name", "?")),
                    "field": key,
                    "from": before,
                    "to": after,
                })


def _first_cluster(obj: Dict) -> Optional[Dict]:
    """Return the first cluster spec found in a job, cluster, or pool record."""
    if "new_cluster" in obj.get("settings", {}):
        return obj["settings"]["new_cluster"]
    if "node_type_id" in obj:
        return obj
    tasks = obj.get("settings", {}).get("tasks", [])
    if tasks and "new_cluster" in tasks[0]:
        return tasks[0]["new_cluster"]
    return None


# ---------------------------------------------------------------------------
# Extra importers (components not covered by the migrate tool)
# ---------------------------------------------------------------------------
# Job deletion helper
# ---------------------------------------------------------------------------

def _list_all_jobs(workspace_url: str, token: str, verify_ssl: bool) -> List[Dict]:
    """Paginate through /api/2.1/jobs/list and return every job record."""
    headers = {"Authorization": f"Bearer {token}"}
    base    = workspace_url.rstrip("/")
    jobs: List[Dict] = []
    params: Dict[str, Any] = {"limit": 100, "expand_tasks": False}

    while True:
        resp = _req("GET", f"{base}/api/2.1/jobs/list", headers=headers,
                    params=params, verify=verify_ssl)
        resp.raise_for_status()
        data     = resp.json()
        batch    = data.get("jobs", [])
        jobs.extend(batch)
        token_next = data.get("next_page_token")
        if not token_next or not batch:
            break
        params["page_token"] = token_next

    return jobs


def _delete_job(workspace_url: str, token: str, job_id: int, verify_ssl: bool) -> None:
    headers = {"Authorization": f"Bearer {token}"}
    base    = workspace_url.rstrip("/")
    resp    = _req("POST", f"{base}/api/2.1/jobs/delete",
                   headers=headers, json={"job_id": job_id}, verify=verify_ssl)
    resp.raise_for_status()


def _req(method: str, url: str, **kwargs: Any):
    """Thin wrapper so tests can patch a single point."""
    import requests as _requests
    return _requests.request(method, url, timeout=30, **kwargs)


def delete_existing_jobs(
    workspace_url: str,
    token: str,
    dry_run: bool = False,
    force: bool = False,
    verify_ssl: bool = True,
) -> Dict:
    """
    List all jobs on the target workspace and delete them.

    Parameters
    ----------
    workspace_url : Target Databricks workspace URL.
    token         : Personal Access Token.
    dry_run       : If True, list jobs but do not delete.
    force         : If True, skip the interactive confirmation prompt.
    verify_ssl    : SSL certificate verification.

    Returns
    -------
    dict with keys: found, deleted, failed, errors, job_names
    """
    _LOG.info("Fetching existing jobs from %s …", workspace_url)
    try:
        jobs = _list_all_jobs(workspace_url, token, verify_ssl)
    except Exception as exc:
        return {"found": 0, "deleted": 0, "failed": 0,
                "errors": [f"Failed to list jobs: {exc}"], "job_names": []}

    if not jobs:
        _LOG.info("No existing jobs found – nothing to delete.")
        return {"found": 0, "deleted": 0, "failed": 0, "errors": [], "job_names": []}

    job_names = [
        f"[{j['job_id']}] {j.get('settings', {}).get('name', '(unnamed)')}"
        for j in jobs
    ]

    print(f"\n  Found {len(jobs)} existing job(s) on target workspace:")
    for name in job_names:
        print(f"    • {name}")
    print()

    if dry_run:
        print("  DRY RUN – jobs would be deleted (use without --dry-run to delete).")
        return {"found": len(jobs), "deleted": 0, "failed": 0,
                "errors": [], "job_names": job_names, "dry_run": True}

    if not force:
        try:
            answer = input(f"  ⚠  Delete all {len(jobs)} job(s)? [yes/No]: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            answer = ""
        if answer not in ("yes", "y"):
            print("  Deletion cancelled.")
            return {"found": len(jobs), "deleted": 0, "failed": 0,
                    "errors": ["Cancelled by user"], "job_names": job_names,
                    "cancelled": True}

    deleted, failed, errors = 0, 0, []
    for job in jobs:
        jid   = job["job_id"]
        jname = job.get("settings", {}).get("name", "(unnamed)")
        try:
            _delete_job(workspace_url, token, jid, verify_ssl)
            _LOG.info("  Deleted job [%d] %s", jid, jname)
            deleted += 1
        except Exception as exc:
            msg = f"Failed to delete job [{jid}] {jname}: {exc}"
            _LOG.warning(msg)
            errors.append(msg)
            failed += 1

    print(f"\n  ✓ Deleted {deleted} job(s)"
          + (f"  ✗ {failed} failed" if failed else ""))

    return {
        "found": len(jobs), "deleted": deleted, "failed": failed,
        "errors": errors, "job_names": job_names,
    }


# ---------------------------------------------------------------------------
# Extra components importer
# ---------------------------------------------------------------------------

def _import_extra_components(
    workspace_url: str,
    token: str,
    session_dir: str,
    cfg: Dict,
    verify_ssl: bool = True,
    dry_run: bool = False,
) -> Dict:
    """Import SQL warehouses, DLT pipelines, repos, dashboards, genie, serving endpoints."""
    # Import here to avoid hard dependency when only pre-processing
    try:
        from workspace_import.extra_importers import ExtraImporter
    except ImportError:
        _LOG.error("workspace_import.extra_importers not found. "
                   "Ensure workspace_import/ is in PYTHONPATH.")
        return {"error": "module not found"}

    importer = ExtraImporter(
        workspace_url=workspace_url,
        token=token,
        session_dir=session_dir,
        verify_ssl=verify_ssl,
        dry_run=dry_run,
    )
    return importer.run()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="import_jobs_gcp.py",
        description=(
            "GCP import pre-processor for Databricks workspace migration.\n\n"
            "Run this BEFORE migration_pipeline.py --import-pipeline to transform\n"
            "exported log files (jobs.log, clusters.log, instance_pools.log) to\n"
            "GCP-compatible definitions in-place."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    src = p.add_argument_group("source / session")
    src.add_argument("-s", "--session", default=None,
                     help="Export session ID (e.g. PROD_MIGRATION_2024). "
                          "Resolves to <export-dir>/<session>/")
    src.add_argument("-d", "--export-dir", default=_DEFAULT_EXPORT_DIR,
                     help=f"Base export directory (default: {_DEFAULT_EXPORT_DIR})")
    src.add_argument("--session-dir", default=None,
                     help="Direct path to session directory (overrides --session / --export-dir)")

    cfg_g = p.add_argument_group("configuration")
    cfg_g.add_argument("--config", default=_DEFAULT_CONFIG,
                       help=f"Path to gcp_import_config.json (default: {_DEFAULT_CONFIG})")
    cfg_g.add_argument("--mapping", default=_DEFAULT_MAPPING,
                       help=f"Path to node_type_mapping.csv (default: {_DEFAULT_MAPPING})")

    act = p.add_argument_group("actions")
    act.add_argument("--preprocess", action="store_true", default=True,
                     help="Transform logs in-place (default: on)")
    act.add_argument("--no-preprocess", dest="preprocess", action="store_false",
                     help="Skip pre-processing (only run --import-extra)")
    act.add_argument("--import-extra", action="store_true",
                     help="Import components not covered by the migrate tool "
                          "(SQL warehouses, DLT, repos, dashboards, genie, serving). "
                          "Requires --workspace-url and --token.")
    act.add_argument("--migrate-service-principals", action="store_true",
                     help="Scan exports for service principals, create them on the target "
                          "GCP workspace, and rewrite UUIDs in a staging copy before import. "
                          "Requires --workspace-url and --token.")
    act.add_argument("--delete-existing-jobs", action="store_true",
                     help="Delete ALL existing jobs on the target workspace before "
                          "importing. Presents a confirmation prompt unless --force is "
                          "also passed. Requires --workspace-url and --token.")
    act.add_argument("--force", action="store_true",
                     help="Skip the confirmation prompt when --delete-existing-jobs is used.")
    act.add_argument("--dry-run", action="store_true",
                     help="With --import-extra / --delete-existing-jobs: show what would "
                          "happen without calling the API")

    tgt = p.add_argument_group("target workspace (required for --import-extra and --migrate-service-principals)")
    tgt.add_argument("-u", "--workspace-url", default=None,
                     help="Target GCP workspace URL")
    tgt.add_argument("-t", "--token", default=None,
                     help="Personal Access Token for the target workspace")
    tgt.add_argument("--no-ssl-verification", action="store_true",
                     help="Disable SSL certificate verification")
    tgt.add_argument("--staging-dir", default=None,
                     help="Where to write the SP-patched copy of the session "
                          "(default: <session_dir>_staging). Used with "
                          "--migrate-service-principals.")

    p.add_argument("--debug", action="store_true", help="Enable DEBUG-level logging")
    return p


def _resolve_session_dir(args: argparse.Namespace) -> str:
    if args.session_dir:
        return args.session_dir
    if args.session:
        return os.path.join(args.export_dir, args.session)
    # Auto-pick newest session directory
    base = args.export_dir
    if os.path.isdir(base):
        sessions = sorted(
            [d for d in os.listdir(base) if os.path.isdir(os.path.join(base, d))],
            reverse=True,
        )
        if sessions:
            chosen = os.path.join(base, sessions[0])
            _LOG.info("Auto-selected session: %s", sessions[0])
            return chosen
    raise SystemExit(f"No session found in {base}. Use --session or --session-dir.")


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        format="%(asctime)s;%(levelname)s;%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.DEBUG if args.debug else logging.INFO,
    )

    if not os.path.isfile(args.config):
        _LOG.error("Config not found: %s", args.config)
        return 1
    if not os.path.isfile(args.mapping):
        _LOG.error("Node-type mapping not found: %s", args.mapping)
        return 1

    session_dir = _resolve_session_dir(args)
    if not os.path.isdir(session_dir):
        _LOG.error("Session directory not found: %s", session_dir)
        return 1

    cfg      = load_config(args.config)
    node_map = load_node_type_mapping(args.mapping)
    zone_hints = load_zone_hint_mapping(args.mapping)

    rc = 0

    # Initialise structured import log
    import_log_path = os.path.join(session_dir, "import_log.json")
    try:
        from workspace_import.import_logger import ImportLogger
        workspace_url = args.workspace_url or "(pre-process only)"
        ilog = ImportLogger(
            log_path=import_log_path,
            session=os.path.basename(session_dir),
            target_workspace=workspace_url,
            dry_run=args.dry_run,
        )
    except ImportError:
        ilog = None

    # ── Stage 1: Pre-process ──────────────────────────────────────────────
    if args.preprocess:
        _LOG.info("=" * 60)
        _LOG.info(" Stage 1 – GCP Pre-process: %s", session_dir)
        _LOG.info("=" * 60)
        if ilog:
            ilog.begin_step("preprocess", "import_jobs_gcp.py",
                            "Transform logs in-place + save gcp_ready/ JSON bundles")
        summary = preprocess_session(session_dir, cfg, node_map, zone_hints=zone_hints)

        total_recs = sum(v.get("count", 0) for v in summary["transformed"].values())
        total_warn = len(summary["all_warnings"])

        if ilog:
            ilog.end_step(
                status="success" if not summary.get("errors") else "failed",
                total=total_recs,
                created=total_recs,
                warnings=summary["all_warnings"],
            )

        print("\n" + "═" * 60)
        print(" GCP Pre-processing Complete")
        print("═" * 60)
        for fname, info in summary["transformed"].items():
            warn_str = f"  ⚠ {info['warnings']} warning(s)" if info["warnings"] else ""
            print(f"  ✓  {fname:<25}  {info['count']} record(s){warn_str}")
        print(f"\n  GCP-ready JSON bundles: {summary['gcp_ready_dir']}/")
        if summary["all_warnings"]:
            print(f"\n  {total_warn} unmapped node type(s) – kept as-is:")
            for w in summary["all_warnings"]:
                print(f"    ⚠  {w}")
        print("═" * 60)
        print()
        print("  ► Next step: run the migrate tool's import pipeline:")
        print("    python3 migration_pipeline.py \\")
        print("        --profile DST_PROFILE --import-pipeline --use-checkpoint \\")
        print(f"        --session {os.path.basename(session_dir)}")
        print()
        if summary["all_warnings"]:
            rc = 2

    # ── Stage 1.5: Service-Principal migration ────────────────────────────
    if getattr(args, "migrate_service_principals", False):
        if not args.workspace_url or not args.token:
            _LOG.error("--migrate-service-principals requires --workspace-url and --token")
            return 1
        staging_dir = getattr(args, "staging_dir", None) or (session_dir.rstrip("/\\") + "_staging")
        _LOG.info("=" * 60)
        _LOG.info(" Stage 1.5 – Service Principal Migration")
        _LOG.info("  Session dir  : %s", session_dir)
        _LOG.info("  Staging dir  : %s", staging_dir)
        _LOG.info("  Workspace    : %s", args.workspace_url)
        _LOG.info("  Dry run      : %s", args.dry_run)
        _LOG.info("=" * 60)
        if ilog:
            ilog.begin_step("sp_migration", "import_jobs_gcp.py --migrate-service-principals",
                            "Scan SPs, create on GCP, patch staging copy")
        try:
            from workspace_import.sp_migrator import migrate_service_principals
            sp_summary = migrate_service_principals(
                session_dir=session_dir,
                staging_dir=staging_dir,
                workspace_url=args.workspace_url,
                token=args.token,
                verify_ssl=not args.no_ssl_verification,
                dry_run=args.dry_run,
            )
            sp_errors = [
                f"SP '{e['display_name']}' not created: gcp_app_id missing"
                for e in sp_summary.get("mapping", {}).values()
                if not e.get("gcp_app_id")
            ] if not args.dry_run else []
            if ilog:
                ilog.end_step(
                    status="success" if not sp_errors else "failed",
                    total=sp_summary.get("sp_count", 0),
                    created=sum(1 for e in sp_summary.get("mapping", {}).values()
                                if e.get("gcp_app_id")),
                    notes=f"{sp_summary.get('patched_files', 0)} files patched in staging; "
                          f"{sp_summary.get('total_replacements', 0)} UUID replacements",
                    errors=sp_errors,
                )
            print(f"\n  SP mapping : {os.path.join(session_dir, 'sp_mapping.json')}")
            print(f"  Patch log  : {os.path.join(session_dir, 'patch_summary.json')}")
            print(f"  Staging    : {staging_dir}")
            print(f"  ► The migrate tool should now use --set-export-dir pointed at the "
                  f"PARENT of the staging directory.")
        except Exception as exc:
            _LOG.error("SP migration failed: %s", exc, exc_info=args.debug)
            if ilog:
                ilog.fail_step(str(exc))
            rc = 1

    # ── Stage 1.6: Delete existing jobs ──────────────────────────────────
    if getattr(args, "delete_existing_jobs", False):
        if not args.workspace_url or not args.token:
            _LOG.error("--delete-existing-jobs requires --workspace-url and --token")
            return 1
        _LOG.info("=" * 60)
        _LOG.info(" Stage 1.6 – Delete existing jobs on target workspace")
        _LOG.info("  Workspace : %s", args.workspace_url)
        _LOG.info("  Dry run   : %s", args.dry_run)
        _LOG.info("  Force     : %s", getattr(args, "force", False))
        _LOG.info("=" * 60)
        if ilog:
            ilog.begin_step("delete_existing_jobs", "import_jobs_gcp.py --delete-existing-jobs",
                            "Delete all jobs on target before re-import")
        del_result = delete_existing_jobs(
            workspace_url=args.workspace_url,
            token=args.token,
            dry_run=args.dry_run,
            force=getattr(args, "force", False),
            verify_ssl=not args.no_ssl_verification,
        )
        cancelled = del_result.get("cancelled", False)
        if ilog:
            status = "success"
            if del_result.get("dry_run"):
                status = "dry_run"
            elif cancelled:
                status = "skipped"
            elif del_result.get("failed", 0) > 0:
                status = "failed"
            ilog.end_step(
                status=status,
                total=del_result.get("found", 0),
                created=0,
                skipped=del_result.get("deleted", 0) if del_result.get("dry_run") else 0,
                failed=del_result.get("failed", 0),
                dry_run_count=del_result.get("found", 0) if del_result.get("dry_run") else 0,
                notes=f"Deleted {del_result.get('deleted', 0)} of {del_result.get('found', 0)} jobs"
                      + (" (dry run)" if del_result.get("dry_run") else "")
                      + (" (cancelled)" if cancelled else ""),
                errors=del_result.get("errors", []),
            )
        if cancelled:
            _LOG.info("Job deletion cancelled – aborting import to avoid duplicate jobs.")
            return 1
        if del_result.get("failed", 0) > 0 and not args.dry_run:
            _LOG.warning("%d job(s) could not be deleted – import may create duplicates",
                         del_result["failed"])
            rc = 2

    # ── Stage 2: Import extra components ─────────────────────────────────
    if args.import_extra:
        if not args.workspace_url or not args.token:
            _LOG.error("--import-extra requires --workspace-url and --token")
            return 1
        _LOG.info("=" * 60)
        _LOG.info(" Stage 2 – Import extra components")
        _LOG.info("  Workspace : %s", args.workspace_url)
        _LOG.info("  Dry run   : %s", args.dry_run)
        _LOG.info("=" * 60)
        result = _import_extra_components(
            workspace_url=args.workspace_url,
            token=args.token,
            session_dir=session_dir,
            cfg=cfg,
            verify_ssl=not args.no_ssl_verification,
            dry_run=args.dry_run,
        )
        if ilog and isinstance(result.get("results"), list):
            ilog.record_extra_results(result["results"])
        if result.get("error") or result.get("total_failed", 0) > 0:
            rc = 1

    # ── Generate HTML report ──────────────────────────────────────────────
    if ilog:
        ilog.complete()
    try:
        from workspace_import.html_reporter import generate_import_html, generate_export_html
        if args.import_extra or not args.preprocess:
            html_path = generate_import_html(session_dir)
        else:
            html_path = generate_export_html(session_dir)
        print(f"\n  HTML report: {html_path}")
    except Exception as exc:
        _LOG.warning("HTML report generation failed: %s", exc)

    return rc


if __name__ == "__main__":
    sys.exit(main())
