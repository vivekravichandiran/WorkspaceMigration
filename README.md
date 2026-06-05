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
# 1. Inventory — see what's in the workspace before touching anything
python3 workspace_inventory.py \
  --workspace-url https://<workspace>.azuredatabricks.net \
  --token dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

# 2. Export + auto-stage
./migrate_workspace.sh \
  --workspace-url https://<workspace>.azuredatabricks.net \
  --token dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX \
  --azure

# 3. Review staged files
ls logs/<SESSION>_staging/

# 4. Import to target
./import_gcp.sh \
  --workspace-url https://<gcp-workspace>.gcp.databricks.com \
  --token dapiXXXX \
  --session <SESSION>
```

---

## Authentication

All tools support two authentication modes. Use **one** of the following per command.

### PAT Token (Personal Access Token)
```bash
--token dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
```

### OAuth M2M (Service Principal)
```bash
--client-id     <client-id-uuid> \
--client-secret <client-secret>
```
The tools exchange credentials for a Bearer token via `POST /oidc/v1/token` automatically. Token is auto-refreshed if the run exceeds 59 minutes.

> **Note:** The service principal must have workspace **Admin** role (or sufficient entitlements) to export all components. Limited SPs will get `403 PERMISSION_DENIED` errors on individual objects.

---

## Tools Overview

| Tool | Purpose |
|---|---|
| `workspace_inventory.py` | Standalone inventory — generates HTML + Excel reports |
| `migrate_workspace.sh` | Full export orchestrator (inventory → export → staging) |
| `import_jobs_gcp.py` | GCP pre-processor + staging builder + extra component importer |
| `import_gcp.sh` | Full import orchestrator |

---

## Workspace Inventory

Generates an interactive HTML report and Excel workbook with a summary dashboard and per-component detail tabs.

### Basic usage
```bash
python3 workspace_inventory.py \
  --workspace-url https://<workspace>.azuredatabricks.net \
  --token dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
```

### OAuth
```bash
python3 workspace_inventory.py \
  --workspace-url https://<workspace>.azuredatabricks.net \
  --client-id     <client-id> \
  --client-secret <client-secret>
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

`migrate_workspace.sh` orchestrates the full export in **3 steps**:

```
Step 0  →  Pre-export workspace inventory (HTML + Excel)
Step 1  →  Export all components via databrickslabs/migrate
Step 2  →  Auto-staging (copy → apply GCP transforms + user remapping)
```

### Basic usage
```bash
./migrate_workspace.sh \
  --workspace-url https://<workspace>.azuredatabricks.net \
  --token dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX \
  --azure
```

### OAuth
```bash
./migrate_workspace.sh \
  --workspace-url https://<workspace>.azuredatabricks.net \
  --client-id     <client-id> \
  --client-secret <client-secret> \
  --azure
```

### Full options reference

| Flag | Default | Description |
|---|---|---|
| `-u, --workspace-url` | required | Source workspace URL |
| `-t, --token` | — | PAT token |
| `--client-id` | — | OAuth SP client ID |
| `--client-secret` | — | OAuth SP client secret |
| `-s, --session` | auto-generated | Session ID (e.g. `PROD_MIGRATION_2024`) |
| `-d, --export-dir` | `./logs/` | Base export directory |
| `--azure` | — | Flag for Azure source workspace |
| `--gcp` | — | Flag for GCP source workspace |
| `-p, --num-parallel` | 4 | Download thread count |
| `--notebook-format` | `DBC` | `DBC` / `SOURCE` / `HTML` |
| `--skip COMPONENT...` | — | Space-separated components to skip (see below) |
| `--include-mlflow` | off | Export MLflow experiments and runs (skipped by default — can be very large) |
| `--skip-failed` | off | Skip metastore retries on failure |
| `--no-ssl-verification` | off | Disable SSL certificate verification |
| `--retry-total` | 10 | Total HTTP retries |
| `--retry-backoff` | 1.0 | Retry backoff factor |
| `--debug` | off | Enable debug-level logging |
| `--report-only` | off | Re-generate report from existing `export_status.json` |
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

### Component-by-component examples

```bash
# Jobs only
./migrate_workspace.sh --workspace-url <URL> --token <PAT> \
  --skip users groups notebooks clusters instance_pools metastore secrets \
         sql_warehouses dlt_pipelines repos lakeview_dashboards \
         genie_spaces serving_endpoints unity_catalog

# Notebooks only
./migrate_workspace.sh --workspace-url <URL> --token <PAT> \
  --skip users groups clusters jobs instance_pools metastore secrets \
         sql_warehouses dlt_pipelines repos lakeview_dashboards \
         genie_spaces serving_endpoints unity_catalog

# SQL Warehouses + DLT + Genie Spaces only
./migrate_workspace.sh --workspace-url <URL> --token <PAT> \
  --skip users groups notebooks clusters jobs instance_pools metastore \
         secrets repos lakeview_dashboards serving_endpoints unity_catalog
```

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
  --token dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX \
  --session <SESSION>
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
./migrate_workspace.sh \
  --workspace-url <URL> --token <PAT> \
  --session <SESSION> \
  --report-only
```

### Rebuild staging without re-exporting
```bash
python3 import_jobs_gcp.py \
  --session-dir logs/<SESSION> \
  --build-staging
```
