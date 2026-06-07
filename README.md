# Databricks Workspace Migration Toolkit

A self-bootstrapping toolkit for migrating Databricks workspaces across clouds (Azure → GCP) with full component coverage, interactive HTML/Excel reports, staging review, and configurable user remapping.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Quick Start](#quick-start)
3. [Authentication](#authentication)
4. [Tools Overview](#tools-overview)
5. [Workspace Inventory](#workspace-inventory)
6. [Export](#export)
7. [Staging & Review](#staging--review)
8. [Import](#import)
9. [Configuration Reference (`gcp_import_config.json`)](#configuration-reference)
10. [Component Coverage](#component-coverage)
11. [Output Files](#output-files)
12. [Troubleshooting](#troubleshooting)

---

## Prerequisites

**System requirements:**
```bash
python3 --version   # 3.6+
git --version
```

**Python libraries:**
```bash
pip3 install requests urllib3 openpyxl
```

**Clone the repo:**
```bash
git clone https://github.com/vivekravichandiran/WorkspaceMigration.git
cd WorkspaceMigration
```

---

## Quick Start

```bash
# 1. Export Azure workspace (OAuth M2M — recommended)
./export_azure.sh \
  --workspace-url https://<workspace>.azuredatabricks.net \
  --client-id     <SERVICE_PRINCIPAL_CLIENT_ID>           \
  --client-secret <SERVICE_PRINCIPAL_CLIENT_SECRET>       \
  --azure

# 2. Review the 6 auto-generated reports inside logs/<SESSION>/
#    Then import into GCP:
./import_gcp.sh \
  --workspace-url https://<gcp-workspace>.gcp.databricks.com \
  --client-id     <GCP_SERVICE_PRINCIPAL_CLIENT_ID>          \
  --client-secret <GCP_SERVICE_PRINCIPAL_CLIENT_SECRET>      \
  --profile       <DATABRICKS_CLI_PROFILE>                   \
  --session       <SESSION_ID>
```

# 4. Import to target
./import_gcp.sh \
  --workspace-url https://<gcp-workspace>.gcp.databricks.com \
  --client-id     <GCP_SERVICE_PRINCIPAL_CLIENT_ID>          \
  --client-secret <GCP_SERVICE_PRINCIPAL_CLIENT_SECRET>      \
  --profile       <DATABRICKS_CLI_PROFILE>                   \
  --session       <SESSION_ID>
```

---

## Authentication

All tools support two authentication modes.

### OAuth M2M — Service Principal (recommended)
```bash
--client-id     <SERVICE_PRINCIPAL_CLIENT_ID> \
--client-secret <SERVICE_PRINCIPAL_CLIENT_SECRET>
```
The script exchanges credentials for a Bearer token via `POST /oidc/v1/token` automatically.  
No PAT token needs to be generated or stored.

> The service principal must have workspace **Admin** role (or sufficient entitlements) to export all components.

### PAT Token (legacy / fallback)
```bash
--token dapi<YOUR_PAT_TOKEN>
```

---

## Tools Overview

| Tool | Purpose |
|---|---|
| `workspace_inventory.py` | Standalone inventory — generates HTML + Excel reports |
| `export_azure.sh` | Full export orchestrator (inventory → export → staging) |
| `import_jobs_gcp.py` | GCP pre-processor + staging builder + extra component importer |
| `import_gcp.sh` | Full import orchestrator |

---

## Workspace Inventory

Generates an interactive HTML report and Excel workbook with a summary dashboard and per-component detail tabs.

### Usage
```bash
python3 workspace_inventory.py \
  --workspace-url https://<workspace>.azuredatabricks.net \
  --client-id     <SERVICE_PRINCIPAL_CLIENT_ID>           \
  --client-secret <SERVICE_PRINCIPAL_CLIENT_SECRET>
```

### PAT fallback
```bash
python3 workspace_inventory.py \
  --workspace-url https://<workspace>.azuredatabricks.net \
  --token         dapi<YOUR_PAT_TOKEN>
```

### Full options reference

| Flag | Default | Description |
|---|---|---|
| `--workspace-url` | required | Workspace URL (`https://...`) |
| `--token` | — | PAT token |
| `--client-id` | — | OAuth SP client ID |
| `--client-secret` | — | OAuth SP client secret |
| `--output` | auto | HTML output path |
| `--excel-output` | auto | Excel (`.xlsx`) output path |
| `--log-file` | auto | Log file path (mirrors terminal output) |
| `--no-excel` | off | Skip Excel generation |
| `--no-log-file` | off | Skip log file |
| `--max-scim` | 0 (unlimited) | Cap Users / Groups / Service Principals fetch |
| `--max-workspace-items` | 0 (unlimited) | Cap notebook/file scan item count |
| `--max-ws-api-calls` | 300 | Cap workspace directory API calls (prevents long scans on deep source trees) |
| `--verbose` / `-v` | off | Print every API call with URL, HTTP status, response time, and pagination detail |
| `--no-ssl-verification` | off | Disable SSL certificate verification |

### Output files
```
inventory_<workspace>_<timestamp>.html   # Interactive HTML dashboard
inventory_<workspace>_<timestamp>.xlsx   # Excel workbook (Summary + 16 tabs)
inventory_<workspace>_<timestamp>.log    # Full run log
```

### Excel workbook structure
- **Summary** sheet — component counts with Databricks branding
- **One sheet per component** — Users, Groups, Service Principals, Notebooks, Workspace Files, Jobs, All-Purpose Clusters, Instance Pools, Cluster Policies, SQL Warehouses, DLT Pipelines, AI-BI Dashboards, Genie AI Spaces, Secret Scopes, Git Repos, Model Serving Endpoints
- **Warnings** sheet — fetch errors (if any)

### Components inventoried
Users, Groups, Service Principals, Notebooks, Workspace Files, Jobs, All-Purpose Clusters, Instance Pools, Cluster Policies, SQL Warehouses, DLT Pipelines, AI/BI Dashboards, Genie AI Spaces, Secret Scopes, Git Repos, Model Serving Endpoints

---

## Export

`export_azure.sh` is a **single self-bootstrapping command** that:
- Installs `databrickslabs/migrate` automatically on first run
- Runs **3 steps** end-to-end
- Generates **6 output reports** ready for review before any import

```
Step 0  →  Pre-export workspace inventory   → inventory HTML + Excel
Step 1  →  Full component export            → raw logs + export HTML + text report
Step 2  →  Auto-staging (GCP transforms)    → staged files
Step 3  →  Staging diff report              → pre/post change HTML + Excel
```

### The command

```bash
./export_azure.sh \
  --workspace-url https://<workspace>.azuredatabricks.net \
  --client-id     <SERVICE_PRINCIPAL_CLIENT_ID>           \
  --client-secret <SERVICE_PRINCIPAL_CLIENT_SECRET>       \
  --azure
```

> **OAuth M2M is the recommended authentication method.** The script exchanges
> `--client-id` + `--client-secret` for a short-lived access token automatically via
> `POST /oidc/v1/token`. No PAT token needs to be generated or stored.
>
> PAT token fallback: replace `--client-id`/`--client-secret` with `--token dapi...`

### What gets generated

After the command completes, all 6 files land inside `logs/<SESSION>/`:

```
logs/
└── EXPORT_<TIMESTAMP>/
    ├── inventory_pre_export.html       ← 🗂  Workspace inventory (HTML dashboard)
    ├── inventory_pre_export.xlsx       ← 🗂  Workspace inventory (Excel, 16 tabs)
    ├── export_report.txt               ← 📋  Component-by-component export summary
    ├── export_report.html              ← 📊  Interactive HTML export report
    ├── staging_diff_<SESSION>.html     ← 🔍  Pre/post staging changes (HTML)
    ├── staging_diff_<SESSION>.xlsx     ← 🔍  Pre/post staging changes (Excel)
    └── <SESSION>_staging/             ← 📦  GCP-ready files (review before import)
```

Open the staging diff report to review every transformation applied before importing.

### Common variants

```bash
# Azure workspace (full export, OAuth)
./export_azure.sh \
  --workspace-url https://<workspace>.azuredatabricks.net \
  --client-id     <CLIENT_ID>     \
  --client-secret <CLIENT_SECRET> \
  --azure

# Azure workspace (PAT token fallback)
./export_azure.sh \
  --workspace-url https://<workspace>.azuredatabricks.net \
  --token         dapi<YOUR_PAT_TOKEN>                    \
  --azure

# Custom session name and export directory
./export_azure.sh \
  --workspace-url https://<workspace>.azuredatabricks.net \
  --client-id     <CLIENT_ID>     \
  --client-secret <CLIENT_SECRET> \
  --azure                         \
  --session PROD_MIGRATION_20260607 \
  --export-dir /data/exports

# Include MLflow (skipped by default — can be large)
./export_azure.sh \
  --workspace-url https://<workspace>.azuredatabricks.net \
  --client-id     <CLIENT_ID>     \
  --client-secret <CLIENT_SECRET> \
  --azure --include-mlflow

# Skip specific components
./export_azure.sh \
  --workspace-url https://<workspace>.azuredatabricks.net \
  --client-id     <CLIENT_ID>     \
  --client-secret <CLIENT_SECRET> \
  --azure \
  --skip metastore metastore_table_acls unity_catalog
```

### Full options reference

| Flag | Default | Description |
|---|---|---|
| `-u, --workspace-url` | required | Source workspace URL (`https://...`) |
| `--client-id` | — | **OAuth SP client ID (recommended)** |
| `--client-secret` | — | **OAuth SP client secret (recommended)** |
| `-t, --token` | — | PAT token (legacy / fallback) |
| `-s, --session` | auto-generated | Session ID (e.g. `PROD_MIGRATION_20260607`) |
| `-d, --export-dir` | `./logs/` | Base export directory |
| `--azure` | — | Flag for Azure source workspace |
| `--gcp` | — | Flag for GCP source workspace |
| `-p, --num-parallel` | 4 | Download thread count |
| `--notebook-format` | `DBC` | `DBC` / `SOURCE` / `HTML` |
| `--skip COMPONENT...` | — | Space-separated components to skip (see table below) |
| `--include-mlflow` | off | Export MLflow experiments and runs |
| `--skip-failed` | off | Skip metastore retries on failure |
| `--no-ssl-verification` | off | Disable SSL certificate verification |
| `--retry-total` | 10 | Total HTTP retries |
| `--retry-backoff` | 1.0 | Retry backoff factor |
| `--debug` | off | Enable debug-level logging |
| `--report-only` | off | Re-generate report from existing `export_status.json` without re-exporting |
| `--dry-run` | off | Print resolved config and exit without exporting |
| `--reinstall` | off | Force re-clone and re-install `databrickslabs/migrate` |

### Skippable components

| Component | `--skip` value |
|---|---|
| Users | `users` |
| Groups | `groups` |
| Notebooks | `notebooks` |
| Workspace ACLs | `workspace_acls` |
| Clusters | `clusters` |
| Jobs | `jobs` |
| Instance Pools | `instance_pools` |
| Secrets | `secrets` |
| Metastore (Hive) | `metastore` |
| Metastore Table ACLs | `metastore_table_acls` |
| DBFS Libraries | `dbfs_libraries` |
| SQL Warehouses | `sql_warehouses` |
| DLT Pipelines | `dlt_pipelines` |
| Git Repos | `repos` |
| AI/BI Dashboards | `lakeview_dashboards` |
| Genie Spaces | `genie_spaces` |
| Model Serving | `serving_endpoints` |
| Unity Catalog | `unity_catalog` |

---

## Staging & Review

After export, a staging directory is automatically created at `logs/<SESSION>_staging/`. All transforms from `gcp_import_config.json` are applied:

- GCP cluster rewrite (node types, `gcp_attributes`, availability mapping)
- User domain remapping (`user_domain_mapping`)
- User ID mapping (`user_id_mapping`)
- Exclude filters (`workspace_excludes` regex patterns)

The original export is left untouched so you can diff raw vs. staged.

**Files remapped in staging** (no old user IDs remain):
`acl_jobs.log`, `acl_clusters.log`, `acl_notebooks.log`, `acl_directories.log`, `acl_repos.log`, `secret_scopes_acls.log`, `users.log`, `groups/*`, `repos.log`, `user_dirs.log`, `user_workspace.log`, `instance_pools.log`, `sql_warehouses.json`, `dlt_pipelines.json`, `genie_spaces.json`, `serving_endpoints.json`, `lakeview_dashboards/**`

### Rebuild staging manually

```bash
python3 import_jobs_gcp.py \
  --session-dir  logs/<SESSION> \
  --staging-dir  logs/<SESSION>_staging \
  --build-staging
```

---

## Import

```bash
./import_gcp.sh \
  --workspace-url https://<gcp-workspace>.gcp.databricks.com \
  --client-id     <GCP_SERVICE_PRINCIPAL_CLIENT_ID>          \
  --client-secret <GCP_SERVICE_PRINCIPAL_CLIENT_SECRET>      \
  --profile       <DATABRICKS_CLI_PROFILE>                   \
  --session       <SESSION_ID>
```

---

## Configuration Reference

**`gcp_import_config.json`** — controls all transforms applied during staging and import.

### `workspace_excludes`

Regex patterns to skip objects during staging. Any object matching **any** pattern is excluded.

```json
"workspace_excludes": {
  "path_patterns": [
    "^/Users/svc-.*",
    ".*\\.bak$",
    "^/Shared/archive/.*"
  ],
  "job_name_patterns": [
    ".*\\[DEPRECATED\\].*",
    "^test_.*"
  ],
  "cluster_name_patterns": []
}
```

### `user_domain_mapping`

Rename email domains globally. Applied to **all** user references in **all** exported files.

```json
"user_domain_mapping": {
  "mynt.myntra.com": "myntra.com",
  "oldcompany.com":  "newcompany.com"
}
```

> `vivek@mynt.myntra.com` → `vivek@myntra.com`

### `user_id_mapping`

Explicit source → target user remapping. Takes **precedence** over `user_domain_mapping`.

```json
"user_id_mapping": {
  "3000818416@mynt.myntra.com": "ashi.singhla@myntra.com",
  "svc_account@oldcompany.com": "service@newcompany.com"
}
```

### `gcp_attributes`

GCP cluster attributes injected when source Azure cluster has no explicit availability.

```json
"gcp_attributes": {
  "availability":      "ON_DEMAND_GCP",
  "first_on_demand":   1,
  "local_ssd_count":   0,
  "zone_id":           "auto"
}
```

Azure → GCP availability mapping applied automatically:

| Azure | GCP |
|---|---|
| `ON_DEMAND_AZURE` | `ON_DEMAND_GCP` |
| `SPOT_AZURE` | `PREEMPTIBLE_GCP` |
| `SPOT_WITH_FALLBACK_AZURE` | `SPOT_WITH_FALLBACK_GCP` |

### `cluster_name_transforms`

```json
"cluster_name_transforms": {
  "replace_at_with":    "_",
  "remove_apostrophes": true,
  "replace_spaces_with": "_"
}
```

### `job_name_suffix`

```json
"job_name_suffix": {
  "strip_id_suffix":   true,
  "suffix_separator":  ":::"
}
```

### `schedule_behaviour`

```json
"schedule_behaviour": {
  "set_pause_status": "PAUSED"
}
```
All imported job schedules are paused by default to prevent unintended runs on the target.

### `email_notifications`

```json
"email_notifications": {
  "clear_on_import": false
}
```

---

## Component Coverage

| Component | Export | Staging Transform | Import |
|---|---|---|---|
| Users | ✅ | ✅ user remapping | ✅ |
| Groups | ✅ | ✅ member remapping | ✅ |
| Service Principals | ✅ | ✅ | ✅ auto-reconcile |
| Notebooks | ✅ | ✅ path remapping | ✅ |
| Workspace ACLs | ✅ | ✅ user remapping | ✅ |
| Jobs | ✅ | ✅ GCP rewrite + user remap + exclude filter | ✅ |
| All-Purpose Clusters | ✅ | ✅ GCP rewrite + exclude filter | ✅ |
| Instance Pools | ✅ | ✅ node type mapping | ✅ |
| Cluster Policies | ✅ | — | ✅ |
| SQL Warehouses | ✅ | ✅ user remapping | ✅ auto-create |
| DLT Pipelines | ✅ | ✅ user remapping | ✅ |
| AI/BI Dashboards | ✅ | ✅ | ✅ |
| Genie AI Spaces | ✅ | ✅ warehouse ID mapping | ⚠️ manual (API limitation) |
| Secret Scopes | ✅ | ✅ ACL remapping | ✅ |
| Git Repos | ✅ | ✅ user remapping | ✅ |
| Model Serving | ✅ | ✅ | ✅ |
| DBFS Libraries | ✅ | — | ✅ |
| Hive Metastore | ✅ | — | ✅ |
| Unity Catalog | ✅ | — | ✅ |
| MLflow | opt-in | — | opt-in |

---

## Output Files

After a full export + staging run, the following structure is created:

```
logs/
└── <SESSION>/
    ├── inventory_pre_export.html     # Pre-export inventory HTML
    ├── inventory_pre_export.xlsx     # Pre-export inventory Excel
    ├── export_status.json            # Machine-readable export status
    ├── export_report.html            # HTML export report
    ├── export_report.txt             # Text export report
    ├── jobs.log                      # Job definitions (JSONL)
    ├── clusters.log                  # Cluster definitions (JSONL)
    ├── users.log                     # SCIM user objects (JSONL)
    ├── groups/                       # SCIM group objects (one file per group)
    ├── acl_jobs.log                  # Job ACLs (JSONL)
    ├── acl_clusters.log              # Cluster ACLs (JSONL)
    ├── acl_notebooks.log             # Notebook ACLs (JSONL)
    ├── acl_directories.log           # Directory ACLs (JSONL)
    ├── acl_repos.log                 # Repo ACLs (JSONL)
    ├── secret_scopes/                # Secret scope definitions
    ├── secret_scopes_acls.log        # Secret scope ACLs (JSONL)
    ├── repos.log                     # Git repo definitions (JSONL)
    ├── sql_warehouses.json           # SQL warehouse configs
    ├── dlt_pipelines.json            # DLT pipeline definitions
    ├── genie_spaces.json             # Genie AI Space definitions
    ├── serving_endpoints.json        # Model serving endpoint configs
    ├── lakeview_dashboards/          # AI/BI dashboard content (one JSON each)
    ├── artifacts/                    # Notebook content (.dbc files)
    ├── gcp_ready/                    # GCP-ready JSON bundles (pre-import preview)
    └── gcp_transform_manifest.json   # Staging transform log

└── <SESSION>_staging/                # ← REVIEW HERE before import
    └── (same structure, all transforms applied)
```

---

## Troubleshooting

### SP gets 403 errors during export
The service principal needs **Admin** role on the source workspace.
Go to: **Settings → Identity & Access → Service Principals → [your SP] → Edit → Add Admin role**

### Inventory hangs on workspace scan
The workspace has deep source code trees (e.g. `node_modules` uploaded). Use:
```bash
--max-ws-api-calls 300   # default — limits directory traversal
```
The scanner automatically skips `.git`, `node_modules`, `dist`, `build`, `__pycache__`.

### `No module named urllib3 / openpyxl / requests`
```bash
pip3 install requests urllib3 openpyxl
```

### Genie Spaces show "MANUAL MIGRATION REQUIRED"
Genie Spaces require an internal `serialized_space` protobuf that cannot be extracted via the public API. The tool will map the warehouse IDs automatically and provide step-by-step instructions for manual recreation in the target workspace.

### Re-generate export report without re-running
```bash
./export_azure.sh \
  --workspace-url <URL>        \
  --client-id     <CLIENT_ID>  \
  --client-secret <CLIENT_SECRET> \
  --session       <SESSION>    \
  --report-only
```

### Rebuild staging without re-exporting
```bash
python3 import_jobs_gcp.py \
  --session-dir logs/<SESSION> \
  --build-staging
```
