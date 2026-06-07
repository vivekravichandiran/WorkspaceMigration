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
                if src and dst and not src.startswith("#") and src.lower() != "source_node_type":
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
# User remapping
# ---------------------------------------------------------------------------

def remap_user(user: str, cfg: Dict) -> str:
    """Apply user_id_mapping (exact match) then user_domain_mapping (domain suffix).

    Examples (with config):
        user_id_mapping:    "3000818416@mynt.myntra.com" → "ashi.singhla@myntra.com"
        user_domain_mapping: "mynt.myntra.com" → "myntra.com"
            vivek@mynt.myntra.com → vivek@myntra.com
    """
    if not user:
        return user

    # 1. Exact ID mapping takes highest precedence
    id_map = cfg.get("user_id_mapping", {})
    if user in id_map:
        mapped = id_map[user]
        _LOG.debug("user_id_mapping: %s → %s", user, mapped)
        return mapped

    # 2. Domain-suffix mapping
    domain_map = cfg.get("user_domain_mapping", {})
    for src_domain, tgt_domain in domain_map.items():
        if user.lower().endswith("@" + src_domain.lower()):
            local_part = user[: -(len(src_domain) + 1)]
            mapped = f"{local_part}@{tgt_domain}"
            _LOG.debug("user_domain_mapping: %s → %s", user, mapped)
            return mapped

    return user


def _remap_users_in_obj(obj: Any, cfg: Dict) -> Any:
    """Recursively walk a JSON object and remap any string values that look like
    user emails / IDs across ALL known field names used by Databricks exports.

    Covers: jobs, clusters, ACLs, users, groups, repos, secrets, DLT, warehouses.
    Plain-string email items inside lists (e.g. email_notifications.on_failure)
    are also remapped — any bare string containing '@' and a domain is treated as
    a potential email and passed through remap_user().
    """
    _USER_KEYS = {
        # Jobs / clusters / pools
        "user_name", "creator_user_name", "run_as_user_name",
        "owner", "created_by",
        # Cloud resource tags (Azure/AWS → GCP carry-over)
        "Owner", "Creator",
        # SCIM users
        "userName", "value", "display",
        # Email notifications
        "email", "notification_email", "email_address",
        # ACLs
        "principal", "service_principal_name",
        # Repos / misc
        "author", "committer",
    }
    if isinstance(obj, dict):
        result = {}
        for k, v in obj.items():
            if k in _USER_KEYS and isinstance(v, str):
                result[k] = remap_user(v, cfg)
            elif k == "display_name" and isinstance(v, str) and "@" in v:
                # display_name often mirrors the email in ACL entries
                result[k] = remap_user(v, cfg)
            else:
                result[k] = _remap_users_in_obj(v, cfg)
        return result
    if isinstance(obj, list):
        return [_remap_users_in_obj(item, cfg) for item in obj]
    # Plain string — if it looks like an email remap it regardless of parent key.
    # This catches on_start/on_success/on_failure email arrays, webhook recipients,
    # and any other bare email string buried in the export.
    if isinstance(obj, str) and "@" in obj and "." in obj.split("@")[-1]:
        return remap_user(obj, cfg)
    return obj


def _remap_path_user(path: str, cfg: Dict) -> str:
    """Remap the user portion of workspace paths like /Users/<email>/...

    e.g. /Users/3000818416@mynt.myntra.com/nb  →  /Users/ashi.singhla@myntra.com/nb
    """
    if not path or not path.startswith("/Users/"):
        return path
    parts = path.split("/", 3)   # ['', 'Users', '<email>', rest...]
    if len(parts) < 3:
        return path
    old_user = parts[2]
    new_user = remap_user(old_user, cfg)
    if new_user != old_user:
        parts[2] = new_user
        return "/".join(parts)
    return path


# ---------------------------------------------------------------------------
# Exclude / skip filters
# ---------------------------------------------------------------------------

_EXCLUDE_PATTERN_CACHE: Dict[str, List[re.Pattern]] = {}

def _compile_patterns(patterns: List[str], key: str) -> List[re.Pattern]:
    if key not in _EXCLUDE_PATTERN_CACHE:
        compiled = []
        for p in patterns:
            try:
                compiled.append(re.compile(p, re.IGNORECASE))
            except re.error as e:
                _LOG.warning("Invalid exclude pattern %r: %s", p, e)
        _EXCLUDE_PATTERN_CACHE[key] = compiled
    return _EXCLUDE_PATTERN_CACHE[key]


def should_exclude(value: str, cfg: Dict, kind: str = "path") -> bool:
    """Return True if *value* matches any exclude pattern for *kind*.

    kind values:
        "path"    → workspace_excludes.path_patterns
        "job"     → workspace_excludes.job_name_patterns
        "cluster" → workspace_excludes.cluster_name_patterns
    """
    excludes = cfg.get("workspace_excludes", {})
    key_map  = {
        "path":    "path_patterns",
        "job":     "job_name_patterns",
        "cluster": "cluster_name_patterns",
    }
    pattern_list = excludes.get(key_map.get(kind, "path_patterns"), [])
    patterns = _compile_patterns(pattern_list, kind)
    for pat in patterns:
        if pat.search(value):
            _LOG.info("Excluding %s %r  (matched pattern %r)", kind, value, pat.pattern)
            return True
    return False


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


def _azure_has_ssd(node_type: str) -> bool:
    """Return True if the Azure VM size name indicates SSD or local NVMe disk support.

    Azure VM naming convention:
      Standard_{Family}{Size}{Features}_{Version}
      Features letters:
        's'  = Premium SSD storage supported   (e.g. D4s_v3, E8as_v5)
        'd'  = Local NVMe temp disk attached    (e.g. D4ds_v5, E8ads_v5)
        'a'  = AMD processor (not SSD-related)

    Examples that return True:
      Standard_D4s_v3   → 's'  in features → Premium SSD
      Standard_D4ds_v5  → 'd','s' in features → local NVMe + Premium SSD
      Standard_D4as_v5  → 'a','s' in features → AMD + Premium SSD
      Standard_D4ads_v5 → 'a','d','s' → all three

    Examples that return False:
      Standard_D4_v3    → no feature letters → HDD only
      Standard_D4a_v4   → only 'a' → AMD but no SSD
      Standard_D4_v3    → no suffix → HDD
    """
    import re as _re
    # Extract feature letters: lowercase group after the size digits,
    # before the optional _v{n} or end-of-string
    # e.g. "Standard_D4ds_v5"  → group(1) = "ds"
    #      "Standard_D4a_v4"   → group(1) = "a"
    #      "Standard_D4_v3"    → group(1) = ""
    m = _re.match(
        r"Standard_[A-Za-z]+\d+([a-z]*)(?:_v\d+)?$",
        node_type.strip(),
        _re.IGNORECASE,
    )
    if m:
        features = m.group(1).lower()
        return "s" in features or "d" in features
    return False


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

    local_ssd_count:
      "auto"   → inspect node_type_id from source_cluster; if the Azure VM
                  name has 's' or 'd' in its feature suffix (SSD / local NVMe)
                  use local_ssd_count_ssd_default (default 1), else 0.
      <int>    → use that fixed value for all clusters.

    enable_elastic_disk:
      Passed through as-is from config (default true).
    """
    gcp_cfg = cfg.get("gcp_attributes", {})

    # Start from the config-level defaults, skip comment keys
    attrs = {k: v for k, v in gcp_cfg.items() if not k.startswith("_")}
    for k, v in cfg.get("gcp_attributes_optional", {}).items():
        if v is not None and not k.startswith("_"):
            attrs[k] = v

    # ── local_ssd_count: resolve "auto" ──────────────────────────────────────
    ssd_cfg = attrs.get("local_ssd_count", 0)
    if ssd_cfg == "auto":
        node_type = (source_cluster or {}).get("node_type_id", "")
        ssd_default = int(gcp_cfg.get("local_ssd_count_ssd_default", 1))
        if node_type and _azure_has_ssd(node_type):
            attrs["local_ssd_count"] = ssd_default
            _LOG.debug("  local_ssd_count=auto: %s → has SSD → %d", node_type, ssd_default)
        else:
            attrs["local_ssd_count"] = 0
            _LOG.debug("  local_ssd_count=auto: %s → no SSD → 0", node_type)
    else:
        attrs["local_ssd_count"] = int(ssd_cfg) if ssd_cfg else 0

    # Drop the helper config key — not part of the GCP API payload
    attrs.pop("local_ssd_count_ssd_default", None)

    # ── availability + first_on_demand ────────────────────────────────────────
    if source_cluster is not None:
        az = source_cluster.get("azure_attributes", {})
        cfg_default_av = attrs.get("availability", "ON_DEMAND_GCP")
        gcp_av, first_on_demand = _derive_availability(az, cfg_default_av)
        attrs["availability"] = gcp_av

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


def _apply_apc_options(cluster: Dict, cfg: Dict, label: str = "") -> Dict:
    """Apply all_purpose_cluster_options from config to a standalone cluster record.

    Handles:
      • remove_libraries    — strips the 'libraries' array
      • remove_sas_tokens   — drops Azure SAS / account-key spark_conf keys
      • gcp_spark_config    — removes Azure/AWS storage spark_conf keys,
                              injects GCP-specific keys
    """
    opts = cfg.get("all_purpose_cluster_options", {})
    if not opts:
        return cluster

    c = cluster  # already a deep copy from caller

    # ── Remove libraries ─────────────────────────────────────────────────────
    if opts.get("remove_libraries"):
        if "libraries" in c:
            _LOG.info("  [APC] %s: removing libraries (%d)", label, len(c["libraries"]))
            c.pop("libraries")

    # ── Remove SAS tokens / Azure account keys from spark_conf ───────────────
    sc = c.get("spark_conf", {})
    if opts.get("remove_sas_tokens") and sc:
        _SAS_PATTERNS = (
            "azure", "wasb", "abfs", "abfss",
            "dfs.core.windows.net", "blob.core.windows.net",
            "adls", "adl.", "datalake",
        )
        removed = [k for k in list(sc.keys())
                   if any(p in k.lower() for p in _SAS_PATTERNS)]
        for k in removed:
            sc.pop(k)
            _LOG.info("  [APC] %s: removed SAS/key spark_conf key: %s", label, k)
        if removed:
            c["spark_conf"] = sc

    # ── GCP spark_conf rewrite ────────────────────────────────────────────────
    gcp_cfg = opts.get("gcp_spark_config", {})
    if gcp_cfg.get("enabled") and sc is not None:
        sc = c.get("spark_conf", {})

        if gcp_cfg.get("remove_azure_storage_configs"):
            _AZURE_PREFIXES = (
                "fs.azure", "spark.hadoop.fs.azure",
                "fs.adl", "spark.hadoop.fs.adl",
                "spark.hadoop.fs.abfs", "spark.hadoop.fs.abfss",
                "spark.hadoop.dfs.azure",
            )
            _AZURE_SUBSTRINGS = ("wasb://", "abfss://", "abfs://", ".dfs.core.windows.net",
                                 ".blob.core.windows.net", "azure.account")
            removed_az = []
            for k in list(sc.keys()):
                kl = k.lower()
                if (any(kl.startswith(p.lower()) for p in _AZURE_PREFIXES) or
                        any(s in kl for s in _AZURE_SUBSTRINGS)):
                    sc.pop(k)
                    removed_az.append(k)
            if removed_az:
                _LOG.info("  [APC] %s: removed %d Azure spark_conf key(s)", label, len(removed_az))

        if gcp_cfg.get("remove_aws_configs"):
            _AWS_PREFIXES = (
                "fs.s3", "spark.hadoop.fs.s3",
                "spark.hadoop.fs.s3a", "spark.hadoop.fs.s3n",
                "spark.hadoop.mapreduce.fileoutputcommitter",
                "fs.s3a", "fs.s3n",
            )
            removed_aws = []
            for k in list(sc.keys()):
                if any(k.lower().startswith(p.lower()) for p in _AWS_PREFIXES):
                    sc.pop(k)
                    removed_aws.append(k)
            if removed_aws:
                _LOG.info("  [APC] %s: removed %d AWS spark_conf key(s)", label, len(removed_aws))

        # Inject GCP-specific configs
        inject = {k: v for k, v in gcp_cfg.get("inject", {}).items()
                  if not k.startswith("_")}
        if inject:
            sc.update(inject)
            _LOG.info("  [APC] %s: injected %d GCP spark_conf key(s)", label, len(inject))

        c["spark_conf"] = sc

    return c


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
    c["gcp_attributes"] = _build_gcp_attributes(cfg, source_cluster={
        "azure_attributes": azure_attrs,
        "node_type_id": cluster.get("node_type_id", ""),  # needed for local_ssd_count=auto
    })

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

    # Remap first (preserves email for domain matching), then sanitise
    if "creator_user_name" in c:
        c["creator_user_name"] = _sanitise(
            remap_user(c["creator_user_name"], cfg), cfg)

    # ── enable_elastic_disk ───────────────────────────────────────────────────
    # Controlled by gcp_attributes.enable_elastic_disk; default true.
    # Placed at top-level of the cluster spec (not inside gcp_attributes).
    gcp_cfg = cfg.get("gcp_attributes", {})
    if "enable_elastic_disk" in gcp_cfg:
        c["enable_elastic_disk"] = bool(gcp_cfg["enable_elastic_disk"])
    else:
        c["enable_elastic_disk"] = True  # safe default

    return c


# ---------------------------------------------------------------------------
# Per-file transformers
# ---------------------------------------------------------------------------

def _transform_job(job: Dict, cfg: Dict, node_map: Dict[str, str], warnings: List[str]) -> Optional[Dict]:
    """Return transformed job dict, or None if the job should be excluded."""
    j = copy.deepcopy(job)

    settings = j.get("settings", {})
    job_name = settings.get("name", "")

    # ── Exclude check (regex patterns) ────────────────────────────────────────
    if job_name and should_exclude(job_name, cfg, "job"):
        warnings.append(f"EXCLUDED job: {job_name!r}")
        return None

    # ── job_import.skip_jobs (exact name match) ────────────────────────────
    # Strip the :::JOB_ID suffix first so config names match the display name
    _bare_name = job_name.split(":::")[0].strip() if ":::" in job_name else job_name
    ji_cfg           = cfg.get("job_import", {})
    skip_list        = {j.strip() for j in ji_cfg.get("skip_jobs", []) if j.strip()}
    force_recreate_s = {j.strip() for j in ji_cfg.get("force_recreate_jobs", []) if j.strip()}
    if _bare_name in skip_list and _bare_name not in force_recreate_s:
        warnings.append(f"SKIPPED (job_import.skip_jobs): {_bare_name!r}")
        return None

    # Remap first (preserves email for domain matching), then sanitise
    if "creator_user_name" in j:
        j["creator_user_name"] = _sanitise(
            remap_user(j["creator_user_name"], cfg), cfg)

    # Remove unwanted outer fields.
    # NOTE: job_id is never stripped – the migrate tool uses it for checkpoint tracking
    # and building the old-id → new-id job mapping (job_id_map.log).
    for f in cfg.get("fields_to_remove_from_job", []):
        if f == "job_id":
            continue  # always preserve job_id
        j.pop(f, None)

    # Strip :::JOB_ID from name
    if "name" in settings:
        settings["name"] = _strip_job_id_suffix(settings["name"], cfg)

    # Pause schedules
    pause = cfg.get("schedule_behaviour", {}).get("set_pause_status", "PAUSED")
    if pause and "schedule" in settings:
        settings["schedule"]["pause_status"] = pause

    # Remap all user references recursively
    j = _remap_users_in_obj(j, cfg)

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


def _transform_cluster_record(cluster: Dict, cfg: Dict, node_map: Dict[str, str],
                               warnings: List[str]) -> Optional[Dict]:
    """Transform a standalone cluster record (from clusters.log). Returns None if excluded."""
    c = copy.deepcopy(cluster)

    # ── Exclude check ────────────────────────────────────────────────────────
    cluster_name = c.get("cluster_name", "")
    if cluster_name and should_exclude(cluster_name, cfg, "cluster"):
        warnings.append(f"EXCLUDED cluster: {cluster_name!r}")
        return None

    # Remap first (preserves email for domain matching), then sanitise
    if "creator_user_name" in c:
        c["creator_user_name"] = _sanitise(
            remap_user(c["creator_user_name"], cfg), cfg)

    # Remove runtime-only outer fields irrelevant to recreation.
    # NOTE: cluster_id is preserved intentionally – the migrate tool's
    # get_cluster_id_mapping() function reads it to build old→new cluster ID
    # mappings for job references.
    for f in ["state", "state_message", "start_time", "terminated_time",
              "last_state_loss_time", "default_tags", "cluster_memory_mb", "cluster_cores",
              "cluster_source", "pinned_by_user_name", "last_activity_time",
              "init_scripts_safe_mode", "jdbc_port", "spark_context_id"]:
        c.pop(f, None)

    result = transform_cluster_spec(c, cfg, node_map, warnings, c.get("cluster_name", "?"))

    # Apply all-purpose-cluster-specific options (libraries, SAS tokens, GCP spark config)
    result = _apply_apc_options(result, cfg, label=c.get("cluster_name", "?"))
    return result


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
    p["gcp_attributes"] = _build_gcp_attributes(cfg, source_cluster={
        "azure_attributes": azure_attrs,
        "node_type_id": pool.get("node_type_id", ""),  # needed for local_ssd_count=auto
    })

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
        out, warnings, excluded_count = [], [], 0
        for job in jobs:
            transformed = _transform_job(job, cfg, node_map, warnings)
            if transformed is None:
                excluded_count += 1
                continue
            out.append(transformed)
            # Track node type changes for reporting
            _record_node_changes(job, transformed, "job", summary["node_type_changes"])
        if excluded_count:
            _LOG.info("  → %d job(s) excluded by workspace_excludes patterns", excluded_count)
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
        out, warnings, excluded_count = [], [], 0
        for c in clusters:
            transformed = _transform_cluster_record(c, cfg, node_map, warnings)
            if transformed is None:
                excluded_count += 1
                continue
            out.append(transformed)
            _record_node_changes(c, transformed, "cluster", summary["node_type_changes"])
        if excluded_count:
            _LOG.info("  → %d cluster(s) excluded by workspace_excludes patterns", excluded_count)
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


def build_staging(
    source_dir: str,
    staging_dir: str,
    cfg: Dict,
    node_map: Dict[str, str],
    zone_hints: Optional[Dict[str, str]] = None,
) -> Dict:
    """Copy the raw export directory to staging_dir, then apply ALL transforms
    (GCP cluster rewrite, user remapping, exclude filters) in the staging copy.

    The source export directory is left untouched so the user can compare
    raw vs. staged before triggering the import.

    Returns the preprocessing summary from the staged directory.
    """
    print(f"\n  ── Building staging directory ──────────────────────────────")
    print(f"  Source  (raw export) : {source_dir}")
    print(f"  Staging (transformed): {staging_dir}")

    if os.path.exists(staging_dir):
        print(f"  ⚠  Staging dir already exists — removing and recreating …")
        shutil.rmtree(staging_dir)

    print(f"  Copying …", end="", flush=True)
    shutil.copytree(source_dir, staging_dir)
    print(" done.")

    print(f"  Applying transforms (GCP rewrite + user remapping + excludes) …")
    summary = preprocess_session(staging_dir, cfg, node_map, zone_hints)

    # ── Comprehensive user remapping across ALL exported files ────────────────
    print(f"  Applying user remapping across ALL exported files …")
    remap_stats: Dict[str, int] = {}

    def _remap_jsonl_file(path: str, label: str):
        """Load a JSONL file, remap users in every record, write back."""
        if not os.path.isfile(path):
            return
        records = _read_jsonl(path)
        remapped = [_remap_users_in_obj(r, cfg) for r in records]
        _write_jsonl(path, remapped)
        remap_stats[label] = len(remapped)
        _LOG.info("  user-remapped %s: %d records", label, len(remapped))

    def _remap_json_file(path: str, label: str):
        """Load a JSON file (array or object), remap users, write back."""
        if not os.path.isfile(path):
            return
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        data = _remap_users_in_obj(data, cfg)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2)
        count = len(data) if isinstance(data, list) else 1
        remap_stats[label] = count
        _LOG.info("  user-remapped %s: %d item(s)", label, count)

    # ACL files
    for fname in ("acl_jobs.log", "acl_clusters.log", "acl_notebooks.log",
                  "acl_directories.log", "acl_repos.log", "secret_scopes_acls.log"):
        _remap_jsonl_file(os.path.join(staging_dir, fname), fname)

    # ── remove_acls: wipe cluster ACLs to clean slate ────────────────────────
    apc_opts = cfg.get("all_purpose_cluster_options", {})
    if apc_opts.get("remove_acls"):
        acl_path = os.path.join(staging_dir, "acl_clusters.log")
        if os.path.isfile(acl_path):
            records = _read_jsonl(acl_path)
            cleaned = []
            for rec in records:
                rec["access_control_list"] = []
                cleaned.append(rec)
            _write_jsonl(acl_path, cleaned)
            print(f"  remove_acls: wiped ACLs for {len(cleaned)} cluster(s) → clean slate")
            _LOG.info("  remove_acls: cleared acl_clusters.log (%d records)", len(cleaned))

    # Core export files (users, repos, pools, clusters)
    _remap_jsonl_file(os.path.join(staging_dir, "users.log"),           "users.log")
    _remap_jsonl_file(os.path.join(staging_dir, "repos.log"),           "repos.log")
    _remap_jsonl_file(os.path.join(staging_dir, "instance_pools.log"),  "instance_pools.log")
    _remap_jsonl_file(os.path.join(staging_dir, "libraries.log"),       "libraries.log")
    _remap_jsonl_file(os.path.join(staging_dir, "clusters.log"),        "clusters.log")
    _remap_jsonl_file(os.path.join(staging_dir, "skipped_clusters.log"),"skipped_clusters.log")
    _remap_jsonl_file(os.path.join(staging_dir, "jobs.log"),            "jobs.log")

    # Workspace path files — also remap /Users/<email>/ path segments
    for fname in ("user_dirs.log", "user_workspace.log"):
        path = os.path.join(staging_dir, fname)
        if os.path.isfile(path):
            records = _read_jsonl(path)
            out = []
            for r in records:
                r = _remap_users_in_obj(r, cfg)
                # Also remap path field
                if "path" in r and isinstance(r["path"], str):
                    r["path"] = _remap_path_user(r["path"], cfg)
                out.append(r)
            _write_jsonl(path, out)
            remap_stats[fname] = len(out)

    # Groups — each group is a separate file under groups/<name>
    groups_dir = os.path.join(staging_dir, "groups")
    if os.path.isdir(groups_dir):
        group_count = 0
        for gname in os.listdir(groups_dir):
            gpath = os.path.join(groups_dir, gname)
            if os.path.isfile(gpath):
                try:
                    with open(gpath, encoding="utf-8") as fh:
                        gdata = json.load(fh)
                    gdata = _remap_users_in_obj(gdata, cfg)
                    with open(gpath, "w", encoding="utf-8") as fh:
                        json.dump(gdata, fh, indent=2)
                    group_count += 1
                except Exception as e:
                    _LOG.warning("  could not remap group %s: %s", gname, e)
        remap_stats["groups/*"] = group_count

    # Extra component JSON files (written by export extras)
    for fname in ("sql_warehouses.json", "dlt_pipelines.json", "repos.json",
                  "genie_spaces.json", "serving_endpoints.json",
                  "lakeview_dashboards.json"):
        _remap_json_file(os.path.join(staging_dir, fname), fname)

    # Lakeview dashboard individual files
    dashboards_dir = os.path.join(staging_dir, "lakeview_dashboards")
    if os.path.isdir(dashboards_dir):
        db_count = 0
        for fname in os.listdir(dashboards_dir):
            if fname.endswith(".json"):
                _remap_json_file(os.path.join(dashboards_dir, fname), fname)
                db_count += 1
        if db_count:
            remap_stats["lakeview_dashboards/*"] = db_count

    # Log remap coverage summary
    if remap_stats:
        print(f"  User remapping applied to {len(remap_stats)} file type(s):")
        for fname, count in sorted(remap_stats.items()):
            print(f"    {fname:<40}  {count:>5} records")
    else:
        print(f"  No extra files found to remap (likely a dry-run or empty export).")

    # ── Final summary ─────────────────────────────────────────────────────────
    excluded_jobs     = sum(1 for w in summary.get("all_warnings", []) if w.startswith("EXCLUDED job:"))
    excluded_clusters = sum(1 for w in summary.get("all_warnings", []) if w.startswith("EXCLUDED cluster:"))
    total_warnings    = len(summary.get("all_warnings", []))

    print(f"\n  Staging build complete.")
    print(f"  ── Transform summary ────────────────────────────────────────")
    for fname, info in summary.get("transformed", {}).items():
        print(f"    {fname:<30}  {info.get('count', 0):>5} records"
              f"  {info.get('warnings', 0):>3} warnings")
    if excluded_jobs:
        print(f"  ⊘  {excluded_jobs} job(s) excluded by workspace_excludes.job_name_patterns")
    if excluded_clusters:
        print(f"  ⊘  {excluded_clusters} cluster(s) excluded by workspace_excludes.cluster_name_patterns")
    if total_warnings:
        print(f"  ⚠  {total_warnings} total warning(s) — see {staging_dir}/gcp_transform_manifest.json")
    print(f"\n  Review staged files at: {staging_dir}")
    print(f"  When ready, run import pointing --session to the staging dir.")
    print()

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


def force_delete_jobs_by_name(
    workspace_url: str,
    token: str,
    job_names: List[str],
    dry_run: bool = False,
    verify_ssl: bool = True,
) -> Dict:
    """
    Delete specific jobs from the target workspace by exact display name.

    Used for ``job_import.force_recreate_jobs``: removes the named jobs so
    migration_pipeline.py can re-create them from the staged export.

    Parameters
    ----------
    workspace_url : Target Databricks workspace URL.
    token         : Bearer token.
    job_names     : List of exact job display names to delete.
    dry_run       : Preview only — no API deletions.
    verify_ssl    : SSL certificate verification.

    Returns
    -------
    dict with keys: found, deleted, failed, not_found, errors
    """
    if not job_names:
        _LOG.info("force_delete_jobs_by_name: no job names supplied – nothing to do.")
        return {"found": 0, "deleted": 0, "failed": 0, "not_found": [], "errors": []}

    target_set = {n.strip() for n in job_names if n.strip()}
    _LOG.info("Force-recreate: looking for %d job(s) on target workspace …", len(target_set))

    try:
        all_jobs = _list_all_jobs(workspace_url, token, verify_ssl)
    except Exception as exc:
        return {"found": 0, "deleted": 0, "failed": 0,
                "not_found": list(target_set),
                "errors": [f"Failed to list jobs: {exc}"]}

    matched   = [j for j in all_jobs
                 if j.get("settings", {}).get("name", "").strip() in target_set]
    found_names = {j.get("settings", {}).get("name", "") for j in matched}
    not_found   = sorted(target_set - found_names)

    if not matched:
        _LOG.info("  None of the force_recreate_jobs found on target – "
                  "they will be created fresh.")
        return {"found": 0, "deleted": 0, "failed": 0,
                "not_found": not_found, "errors": []}

    print(f"\n  Force-recreate: found {len(matched)} job(s) to delete before re-import:")
    for j in matched:
        print(f"    • [{j['job_id']}] {j.get('settings', {}).get('name', '(unnamed)')}")
    if not_found:
        print(f"  Not found on target (will be created fresh): {not_found}")
    print()

    if dry_run:
        print("  DRY RUN – jobs would be deleted (no API calls made).")
        return {"found": len(matched), "deleted": 0, "failed": 0,
                "not_found": not_found, "errors": [], "dry_run": True}

    deleted, failed, errors = 0, 0, []
    for job in matched:
        jid   = job["job_id"]
        jname = job.get("settings", {}).get("name", "(unnamed)")
        try:
            _delete_job(workspace_url, token, jid, verify_ssl)
            _LOG.info("  Force-deleted job [%d] %s", jid, jname)
            deleted += 1
        except Exception as exc:
            msg = f"Failed to delete job [{jid}] {jname}: {exc}"
            _LOG.warning(msg)
            errors.append(msg)
            failed += 1

    print(f"  ✓ Force-deleted {deleted} job(s)"
          + (f"  ✗ {failed} failed" if failed else ""))

    return {
        "found": len(matched), "deleted": deleted, "failed": failed,
        "not_found": not_found, "errors": errors,
    }




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
# Folders-only import
# ---------------------------------------------------------------------------

def import_folders_only(
    session_dir: str,
    workspace_url: str,
    token: str,
    cfg: Dict,
    verify_ssl: bool = True,
    dry_run: bool = False,
) -> Dict:
    """Create workspace folder skeleton on the target without importing any
    notebook or file content.

    Reads directory paths from ``user_dirs.log`` and ``user_workspace.log``,
    applies user-path remapping and exclude filters from *cfg*, then calls
    ``POST /api/2.0/workspace/mkdirs`` for each surviving path.

    Returns a summary dict with keys: total, created, skipped, failed, errors.
    """
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    base_url = workspace_url.rstrip("/")
    mkdirs_url = f"{base_url}/api/2.0/workspace/mkdirs"

    # ── 1. Collect all directory paths ───────────────────────────────────────
    paths: List[str] = []
    for fname in ("user_dirs.log", "user_workspace.log"):
        fpath = os.path.join(session_dir, fname)
        if not os.path.isfile(fpath):
            continue
        for rec in _read_jsonl(fpath):
            p = rec.get("path", "")
            if p:
                paths.append(p)

    # De-duplicate, apply path remapping, apply exclude filter
    seen: set = set()
    filtered: List[str] = []
    for p in paths:
        p = _remap_path_user(p, cfg)
        if p in seen:
            continue
        seen.add(p)
        if should_exclude(p, cfg, kind="path"):
            _LOG.info("  EXCLUDED folder: %s", p)
            continue
        filtered.append(p)

    # Sort so parent directories are always created before children
    filtered.sort()

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Creating workspace folder skeleton")
    print(f"  Source session : {session_dir}")
    print(f"  Target         : {workspace_url}")
    print(f"  Total folders  : {len(filtered)}")
    print()

    created = skipped = failed = 0
    errors: List[str] = []

    for p in filtered:
        if dry_run:
            print(f"  [dry-run] mkdirs {p}")
            created += 1
            continue
        try:
            import requests as _req
            resp = _req.post(
                mkdirs_url,
                headers=headers,
                json={"path": p},
                verify=verify_ssl,
                timeout=30,
            )
            if resp.status_code == 200:
                created += 1
                _LOG.info("  ✓ mkdirs %s", p)
            elif resp.status_code in (400, 409):
                # RESOURCE_ALREADY_EXISTS or already a directory — not an error
                skipped += 1
                _LOG.debug("  already exists: %s", p)
            else:
                failed += 1
                msg = f"mkdirs {p} → HTTP {resp.status_code}: {resp.text[:120]}"
                errors.append(msg)
                _LOG.warning("  ✗ %s", msg)
        except Exception as exc:
            failed += 1
            msg = f"mkdirs {p} → {exc}"
            errors.append(msg)
            _LOG.warning("  ✗ %s", msg)

    total = len(filtered)
    print(f"\n  Folders-only import {'(dry run) ' if dry_run else ''}complete:")
    print(f"    Total   : {total}")
    print(f"    Created : {created}")
    print(f"    Skipped (already exist) : {skipped}")
    if failed:
        print(f"    Failed  : {failed}")
        for e in errors[:10]:
            print(f"      ✗ {e}")
        if len(errors) > 10:
            print(f"      … and {len(errors) - 10} more")

    return {
        "total": total,
        "created": created,
        "skipped": skipped,
        "failed": failed,
        "errors": errors,
    }


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
    act.add_argument("--build-staging", action="store_true",
                     help="Copy raw export → staging dir, apply ALL transforms "
                          "(GCP rewrite, user remapping, exclude filters), then stop. "
                          "Review staged files before running import. "
                          "Staging dir defaults to <session_dir>_staging or use --staging-dir.")
    act.add_argument("--import-folders-only", action="store_true",
                     help="Create workspace folder skeleton on target without importing "
                          "any notebook or file content. Reads user_dirs.log + "
                          "user_workspace.log, applies user-path remapping and exclude "
                          "filters, then calls workspace/mkdirs for every directory. "
                          "Requires --workspace-url and --token.")
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
    act.add_argument("--force-recreate-jobs", action="store_true",
                     help="Read job_import.force_recreate_jobs from --config and delete "
                          "those specific jobs on the target workspace by name, then let "
                          "migration_pipeline re-create them from the staged export. "
                          "Overrides job_import.skip_jobs for matching names. "
                          "Requires --workspace-url and --token.")
    act.add_argument("--force", action="store_true",
                     help="Skip the confirmation prompt when --delete-existing-jobs is used.")
    act.add_argument("--dry-run", action="store_true",
                     help="With --import-extra / --delete-existing-jobs / --force-recreate-jobs: "
                          "show what would happen without calling the API")

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

    cfg        = load_config(args.config)
    node_map   = load_node_type_mapping(args.mapping)
    zone_hints = load_zone_hint_mapping(args.mapping)

    rc = 0

    # ── --build-staging: copy raw → staging, transform, stop ─────────────────
    if getattr(args, "build_staging", False):
        staging_dir = (getattr(args, "staging_dir", None)
                       or session_dir.rstrip("/\\") + "_staging")
        build_staging(session_dir, staging_dir, cfg, node_map, zone_hints)
        return 0

    # ── --import-folders-only: mkdirs skeleton, no content ───────────────────
    if getattr(args, "import_folders_only", False):
        if not args.workspace_url or not args.token:
            _LOG.error("--import-folders-only requires --workspace-url and --token")
            return 1
        result = import_folders_only(
            session_dir=session_dir,
            workspace_url=args.workspace_url,
            token=args.token,
            cfg=cfg,
            verify_ssl=not args.no_ssl_verification,
            dry_run=args.dry_run,
        )
        return 1 if result.get("failed", 0) > 0 else 0

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

    # ── Stage 1.7: Force-recreate specific jobs by name ──────────────────
    if getattr(args, "force_recreate_jobs", False):
        if not args.workspace_url or not args.token:
            _LOG.error("--force-recreate-jobs requires --workspace-url and --token")
            return 1
        fr_names = [n.strip() for n in cfg.get("job_import", {}).get("force_recreate_jobs", [])
                    if n.strip()]
        _LOG.info("=" * 60)
        _LOG.info(" Stage 1.7 – Force-recreate jobs (delete then re-create)")
        _LOG.info("  Workspace  : %s", args.workspace_url)
        _LOG.info("  Jobs       : %s", fr_names if fr_names else "(none configured)")
        _LOG.info("  Dry run    : %s", args.dry_run)
        _LOG.info("=" * 60)
        if ilog:
            ilog.begin_step("force_recreate_jobs",
                            "import_jobs_gcp.py --force-recreate-jobs",
                            "Delete named jobs on target so migration_pipeline recreates them")
        fr_result = force_delete_jobs_by_name(
            workspace_url=args.workspace_url,
            token=args.token,
            job_names=fr_names,
            dry_run=args.dry_run,
            verify_ssl=not args.no_ssl_verification,
        )
        if ilog:
            ilog.end_step(
                status="dry_run" if fr_result.get("dry_run") else
                       ("failed" if fr_result.get("failed", 0) > 0 else "success"),
                total=fr_result.get("found", 0),
                created=0,
                skipped=len(fr_result.get("not_found", [])),
                failed=fr_result.get("failed", 0),
                notes=(f"Deleted {fr_result.get('deleted', 0)} of {fr_result.get('found', 0)} jobs"
                       f"; {len(fr_result.get('not_found',[]))} not found on target"),
                errors=fr_result.get("errors", []),
            )
        if fr_result.get("failed", 0) > 0:
            _LOG.warning("%d force-recreate deletion(s) failed – job(s) may already exist "
                         "on target and could be duplicated after import", fr_result["failed"])
            rc = 2


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
