# Databricks Workspace Migration – Usage Guide

Complete reference for exporting a Databricks workspace (any cloud) and importing
it into a GCP workspace, including HTML reports, GCP pre-processing, clean-slate
job deletion, Unity Catalog DDL, and DBFS libraries.

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Scenario Examples](#scenario-examples)
  - [Scenario 1 – Azure → GCP full migration](#scenario-1--azure--gcp-full-migration)
  - [Scenario 2 – Export only (audit / inventory)](#scenario-2--export-only-audit--inventory)
  - [Scenario 3 – Re-import after changes (clean slate)](#scenario-3--re-import-after-changes-clean-slate)
- [Full Export – export_azure.sh](#full-export--export_azuresh)
  - [Minimal command](#minimal-command)
  - [Advanced export options](#advanced-export-options)
  - [Azure workspace](#azure-workspace)
  - [GCP workspace](#gcp-workspace)
  - [Skip specific components](#skip-specific-components)
  - [Export MLflow experiments and runs](#export-mlflow-experiments-and-runs)
  - [Force reinstall / update the migrate repo](#force-reinstall--update-the-migrate-repo)
  - [Resume / re-run after failure](#resume--re-run-after-failure)
  - [Regenerate report only](#regenerate-report-only)
  - [Dry run (validate config without exporting)](#dry-run-validate-config-without-exporting)
  - [HTML export report](#html-export-report)
- [Unity Catalog Export – export_unity_catalog.py](#unity-catalog-export--export_unity_catalogpy)
  - [Owned catalogs (default)](#owned-catalogs-default)
  - [Specific catalogs](#specific-catalogs)
  - [All accessible catalogs](#all-accessible-catalogs)
  - [Schema-level filtering](#schema-level-filtering)
  - [Preview mode (list-only)](#preview-mode-list-only)
  - [Deploying to the target workspace](#deploying-to-the-target-workspace)
- [DBFS & Library Export – export_dbfs_libs.py](#dbfs--library-export--export_dbfs_libspy)
- [GCP Import – import_gcp.sh](#gcp-import--import_gcpsh)
  - [Import workflow overview](#import-workflow-overview)
  - [Minimal command](#minimal-gcp-import-command)
  - [With clean-slate job deletion](#with-clean-slate-job-deletion)
  - [Dry run (preview all steps)](#dry-run-preview-all-steps)
  - [Skip individual steps](#skip-individual-steps)
  - [GCP pre-processor standalone – import_jobs_gcp.py](#gcp-pre-processor-standalone--import_jobs_gcppy)
- [Non-GCP Import (Standard Workflow)](#non-gcp-import-standard-workflow)
  - [Step 1 – Run the migrate import pipeline](#step-1--run-the-migrate-import-pipeline)
  - [Step 2 – Import DBFS libraries](#step-2--import-dbfs-libraries)
  - [Step 3 – Deploy Unity Catalog](#step-3--deploy-unity-catalog)
- [HTML Reports](#html-reports)
- [Validate Migration](#validate-migration)
- [Exported Components Reference](#exported-components-reference)
- [Outputs Reference](#outputs-reference)
  - [Export session directory](#export-session-directory)
  - [GCP import additions](#gcp-import-additions)
- [All CLI Flags Reference](#all-cli-flags-reference)
  - [export_azure.sh](#export_azuresh-flags)
  - [import_gcp.sh](#import_gcpsh-flags)
  - [import_jobs_gcp.py](#import_jobs_gcppy-flags)
  - [export_unity_catalog.py](#export_unity_catalogpy-flags)
  - [export_dbfs_libs.py](#export_dbfs_libspy-flags)
  - [import_dbfs_libs.py](#import_dbfs_libspy-flags)
- [Troubleshooting](#troubleshooting)
  - [SSL errors](#ssl-errors)
  - [Token expired mid-run](#token-expired-mid-run)
  - [API rate limits](#api-rate-limits)
  - [Imported jobs are duplicated](#imported-jobs-are-duplicated)
  - [Check which components failed](#check-which-components-failed)
  - [Genie AI Spaces are skipped during import](#genie-ai-spaces-are-skipped-during-import)
  - [Instance pool creation fails with 'not available in specified zone'](#instance-pool-creation-fails-with-not-available-in-specified-zone)
  - [GCP clusters created with wrong availability](#gcp-clusters-created-with-wrong-availability-always-on_demand)
  - [Unmapped node types after GCP pre-processing](#unmapped-node-types-after-gcp-pre-processing)
  - [Unity Catalog export takes very long](#unity-catalog-export-takes-very-long)
  - [Regenerate the HTML report after a partial run](#regenerate-the-html-report-after-a-partial-run)
  - [Metastore export fails for some tables](#metastore-export-fails-for-some-tables)
  - [DBFS library file too large / times out](#dbfs-library-file-too-large--times-out)
  - [MLflow runs are missing](#mlflow-runs-are-missing)

---

## Prerequisites

Only two system tools are required. Everything else is handled automatically.

| Tool | Purpose | Install |
|---|---|---|
| `git` | Clone the migrate repo | `brew install git` / `apt install git` |
| `python3` (3.6+) | Run the export and import | `brew install python3` / `apt install python3` |

The scripts automatically clone `databrickslabs/migrate`, install dependencies,
and copy utility modules on first run.

---

## Quick Start

```bash
# 1. Clone this repository
git clone https://github.com/your-org/workspace-migration.git
cd workspace-migration

# 2. Export from source (Azure in this example)
./export_azure.sh \
  --workspace-url https://adb-1234567890.7.azuredatabricks.net \
  --token dapi<SRC_TOKEN> \
  --session PROD_2024

# 3. Import into GCP target
./import_gcp.sh \
  --workspace-url https://your-gcp-workspace.gcp.databricks.com \
  --token dapi<DST_TOKEN> \
  --profile GCP_PROD \
  --session PROD_2024
```

After each command completes, an **HTML report** is written to the session directory:

| Report | Path |
|---|---|
| Export summary | `logs/PROD_2024/export_report.html` |
| Full migration (export + GCP transform + import) | `logs/PROD_2024/import_report.html` |

---

## Scenario Examples

### Scenario 1 – Azure → GCP full migration

Full production migration: export an Azure workspace, pre-process for GCP,
delete all existing GCP jobs, then import everything including AI/BI Dashboards,
DLT Pipelines, Repos, Genie Spaces, and Model Serving Endpoints.

```bash
# ── Step A: Export from Azure ─────────────────────────────────────────────────
./export_azure.sh \
  --workspace-url  https://adb-7405618685784929.9.azuredatabricks.net \
  --token          dapi<AZURE_TOKEN> \
  --azure \
  --session        PROD_MIGRATION_2024 \
  --num-parallel   8 \
  --notebook-format SOURCE

# At the end you get:
#   logs/PROD_MIGRATION_2024/export_report.txt    ← text summary
#   logs/PROD_MIGRATION_2024/export_report.html   ← rich HTML report (open in browser)

# ── Step B: Export Unity Catalog (owned catalogs only) ───────────────────────
python3 export_unity_catalog.py \
  --workspace-url  https://adb-7405618685784929.9.azuredatabricks.net \
  --token          dapi<AZURE_TOKEN> \
  --session        PROD_MIGRATION_2024

# ── Step C: Import into GCP – clean slate ────────────────────────────────────
./import_gcp.sh \
  --workspace-url  https://1234567890.7.gcp.databricks.com \
  --token          dapi<GCP_TOKEN> \
  --profile        GCP_PROD \
  --session        PROD_MIGRATION_2024 \
  --delete-existing-jobs \
  --force

# At the end you get:
#   logs/PROD_MIGRATION_2024/gcp_ready/           ← GCP-ready JSON bundles
#   logs/PROD_MIGRATION_2024/import_report.html   ← combined export+transform+import HTML
```

---

### Scenario 2 – Export only (audit / inventory)

Export the workspace for audit or inventory purposes without running an import.
Skip heavy components (DBFS downloads, MLflow) to run in under 5 minutes.

```bash
./export_azure.sh \
  --workspace-url  https://adb-7405618685784929.9.azuredatabricks.net \
  --token          dapi<TOKEN> \
  --session        AUDIT_APR_2026 \
  --skip           dbfs_libraries mlflow_experiments mlflow_runs \
                   metastore metastore_table_acls \
  --notebook-format DBC \
  --num-parallel   2

# Open the HTML report to review what was found
open logs/AUDIT_APR_2026/export_report.html    # macOS
# xdg-open logs/AUDIT_APR_2026/export_report.html  # Linux
```

**What you get:**

- All users, groups, jobs, clusters, instance pools
- Notebooks (DBC format)
- SQL Warehouses, DLT Pipelines, Repos, AI/BI Dashboards, Genie Spaces, Serving Endpoints
- Unity Catalog DDL for owned catalogs
- `export_report.html` — tabbed dashboard with per-component status, item counts,
  durations, and a post-export checklist

---

### Scenario 3 – Re-import after changes (clean slate)

Your first import completed, but you made config changes to `gcp_import_config.json`
or `node_type_mapping.csv` and need to re-run the import on a fresh GCP workspace.

```bash
# ── 1. Restore the original (pre-transformation) log files ───────────────────
cp logs/PROD_MIGRATION_2024/jobs.log.original          logs/PROD_MIGRATION_2024/jobs.log
cp logs/PROD_MIGRATION_2024/clusters.log.original      logs/PROD_MIGRATION_2024/clusters.log
cp logs/PROD_MIGRATION_2024/instance_pools.log.original logs/PROD_MIGRATION_2024/instance_pools.log

# ── 2. (Optional) Preview the transformation before applying ──────────────────
python3 import_jobs_gcp.py \
  --session        PROD_MIGRATION_2024 \
  --dry-run \
  --preprocess

# ── 3. Re-run import with clean-slate job deletion and skip UC (already deployed)
./import_gcp.sh \
  --workspace-url  https://1234567890.7.gcp.databricks.com \
  --token          dapi<GCP_TOKEN> \
  --profile        GCP_PROD \
  --session        PROD_MIGRATION_2024 \
  --delete-existing-jobs \
  --force \
  --skip-step 4

# ── 4. Review the updated HTML report ─────────────────────────────────────────
open logs/PROD_MIGRATION_2024/import_report.html
```

The import log (`import_log.json`) is updated after each run and the HTML report
is regenerated automatically at the end.

---

## Full Export – export_azure.sh

### Minimal command

```bash
./export_azure.sh \
  --workspace-url https://adb-1234567890123456.7.azuredatabricks.net \
  --token dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
```

> Session ID is auto-generated (e.g. `M202404281200`).
> All output goes to `logs/M202404281200/`.

---

### Advanced export options

```bash
./export_azure.sh \
  --workspace-url  https://adb-1234567890123456.7.azuredatabricks.net \
  --token          dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX \
  --azure \
  --session        AZURE_EXPORT_PROD \
  --export-dir     /data/migrations \
  --num-parallel   8 \
  --notebook-format SOURCE \
  --retry-total    30 \
  --retry-backoff  1.5 \
  --debug
```

---

### Azure workspace

```bash
./export_azure.sh \
  --workspace-url  https://adb-1234567890123456.7.azuredatabricks.net \
  --token          dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX \
  --azure \
  --session        AZURE_EXPORT_Q2 \
  --num-parallel   4
```

---

### GCP workspace

```bash
./export_azure.sh \
  --workspace-url  https://1234567890123456.7.gcp.databricks.com \
  --token          dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX \
  --gcp \
  --session        GCP_EXPORT_2024
```

---

### Skip specific components

```bash
# Skip DBFS library downloads (metadata only, runs much faster)
./export_azure.sh \
  --workspace-url https://my-workspace.azuredatabricks.net \
  --token         dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX \
  --skip          dbfs_libraries

# Skip metastore (very large workspaces)
./export_azure.sh \
  --workspace-url https://my-workspace.azuredatabricks.net \
  --token         dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX \
  --skip          metastore metastore_table_acls

# Export only users, groups, and notebooks (minimal snapshot)
./export_azure.sh \
  --workspace-url https://my-workspace.azuredatabricks.net \
  --token         dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX \
  --skip          secrets clusters instance_pools jobs \
                  metastore metastore_table_acls dbfs_libraries \
                  sql_warehouses dlt_pipelines repos lakeview_dashboards \
                  genie_spaces serving_endpoints unity_catalog
```

**All component names accepted by `--skip`:**

| Name | Description | Default |
|---|---|---|
| `instance_profiles` | AWS IAM instance profiles (auto-skipped; not applicable for Azure→GCP) | skipped |
| `users` | SCIM user records | exported |
| `groups` | SCIM group records | exported |
| `workspace_item_log` | Notebook & directory path listing | exported |
| `workspace_acls` | Notebook and directory permissions | exported |
| `notebooks` | Notebook content download | exported |
| `secrets` | Secret scope definitions + ACLs | exported |
| `clusters` | Cluster configurations + policies | exported |
| `instance_pools` | Instance pool definitions | exported |
| `jobs` | Job configurations + ACLs | exported |
| `metastore` | Legacy Hive metastore DDL | exported |
| `metastore_table_acls` | Legacy table GRANT/DENY statements | exported |
| `mlflow_experiments` | MLflow experiment definitions | **skipped by default** |
| `mlflow_runs` | MLflow run history | **skipped by default** |
| `dbfs_libraries` | DBFS file downloads + library manifest | exported |
| `sql_warehouses` | SQL warehouse configurations | exported |
| `dlt_pipelines` | Delta Live Tables pipeline definitions | exported |
| `repos` | Databricks Repos (Git) definitions | exported |
| `lakeview_dashboards` | AI/BI Dashboards (Lakeview) | exported |
| `genie_spaces` | Genie AI Space definitions (exported; **cannot be auto-imported — see platform limitation note**) | exported |
| `serving_endpoints` | Model Serving endpoint configs | exported |
| `unity_catalog` | Unity Catalog DDL + ACLs | exported |
| `workspace_files` | Non-notebook workspace files (.py .md .sql .yaml .toml .json .csv .txt .sh .ipynb …) | exported |

---

### Export MLflow experiments and runs

MLflow is **skipped by default** because runs can be very large. To include them:

```bash
./export_azure.sh \
  --workspace-url https://my-workspace.azuredatabricks.net \
  --token         dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX \
  --include-mlflow
```

> For large workspaces consider the dedicated
> [MLflow Export/Import](https://github.com/mlflow/mlflow-export-import) tool
> for date-range filtering and higher parallelism.

---

### Force reinstall / update the migrate repo

```bash
./export_azure.sh \
  --workspace-url https://my-workspace.azuredatabricks.net \
  --token         dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX \
  --reinstall

# Use a custom directory (instead of the default ~/.databricks-migrate)
./export_azure.sh \
  --workspace-url  https://my-workspace.azuredatabricks.net \
  --token          dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX \
  --migrate-dir    /opt/databricks/migrate

# Or via environment variable
DATABRICKS_MIGRATE_DIR=/opt/databricks/migrate ./export_azure.sh \
  --workspace-url https://my-workspace.azuredatabricks.net \
  --token         dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
```

---

### Resume / re-run after failure

The export is checkpointed. Rerun with the same `--session` — already-completed
components are skipped automatically:

```bash
# Original run (failed partway through)
./export_azure.sh \
  --workspace-url https://my-workspace.azuredatabricks.net \
  --token         dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX \
  --session       PROD_MIGRATION_2024

# Rerun with same --session → completed components are skipped
./export_azure.sh \
  --workspace-url https://my-workspace.azuredatabricks.net \
  --token         dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX \
  --session       PROD_MIGRATION_2024
```

---

### Regenerate report only

Re-read a saved report without re-exporting anything:

```bash
./export_azure.sh \
  --workspace-url https://my-workspace.azuredatabricks.net \
  --token         dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX \
  --session       PROD_MIGRATION_2024 \
  --report-only

# Or regenerate the HTML report directly from Python
python3 -c "
from workspace_import.html_reporter import generate_export_html
path = generate_export_html('logs/PROD_MIGRATION_2024')
print('HTML report:', path)
"
```

---

### Dry run (validate config without exporting)

Prints the resolved configuration and exits without touching Databricks:

```bash
./export_azure.sh \
  --workspace-url https://my-workspace.azuredatabricks.net \
  --token         dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX \
  --session       TEST_SESSION \
  --dry-run
```

---

### HTML export report

A self-contained `export_report.html` is automatically generated at the end of
every export run:

```
logs/PROD_MIGRATION_2024/export_report.html
```

Open it in any browser — no internet connection required. It contains:

- **Export Overview** — workspace URL, session ID, start/end times, duration
- **KPI cards** — total components, succeeded / failed / skipped counts, total objects
- **Component Status table** — per-component status badge, item count, duration, log file names
- **Export Artifacts** — full file tree of the session directory with sizes
- **Migration Checklist** — checkbox list of completed steps

To regenerate manually at any time:

```bash
python3 -c "
from workspace_import.html_reporter import generate_export_html
print(generate_export_html('logs/PROD_MIGRATION_2024'))
"
```

---

## Unity Catalog Export – export_unity_catalog.py

`export_unity_catalog.py` is a standalone CLI for exporting Unity Catalog objects
(catalogs, schemas, tables, views, functions, volumes, ACLs) as importable SQL DDL
files in correct deployment order.

It is also called automatically as the `unity_catalog` component in the full
migration pipeline. Run it standalone for more control over which catalogs or
schemas to export, or to re-run only the UC portion.

> **Materialized views and streaming tables are intentionally skipped** — they are
> identified from `information_schema.tables` (`table_type = 'MATERIALIZED_VIEW'`
> or `'STREAMING_TABLE'`) and must be recreated by running their DLT pipeline.

### Owned catalogs (default)

Exports only the catalogs owned by the token's user. Safe for shared workspaces.

```bash
python3 export_unity_catalog.py \
  --workspace-url https://adb-1234567890123456.7.azuredatabricks.net \
  --token dapi...
```

---

### Specific catalogs

```bash
python3 export_unity_catalog.py \
  --workspace-url https://adb-1234567890123456.7.azuredatabricks.net \
  --token dapi... \
  --catalogs sales_prod analytics_dev
```

---

### All accessible catalogs

Exports all `MANAGED_CATALOG` entries accessible to the token.
Use with care on large shared workspaces — can take 30–60 minutes.

```bash
python3 export_unity_catalog.py \
  --workspace-url https://adb-1234567890123456.7.azuredatabricks.net \
  --token dapi... \
  --all-catalogs
```

---

### Schema-level filtering

Restrict to specific schemas within the selected catalogs:

```bash
python3 export_unity_catalog.py \
  --workspace-url https://adb-1234567890123456.7.azuredatabricks.net \
  --token dapi... \
  --catalogs sales_prod \
  --schemas bronze silver gold
```

Both bare names (`bronze`) and qualified names (`sales_prod.bronze`) are accepted.

---

### Preview mode (list-only)

Print catalog/schema counts without writing any files:

```bash
python3 export_unity_catalog.py \
  --workspace-url https://adb-1234567890123456.7.azuredatabricks.net \
  --token dapi... \
  --all-catalogs \
  --list-only
```

Sample output:

```
Workspace : https://adb-1234567890123456.7.azuredatabricks.net
Token user: alice@company.com
Mode      : all-accessible

Catalog                                  Owner                               Schemas
------------------------------------------------------------------------------------------
  sales_prod                             alice@company.com                   12
  analytics_dev                          bob@company.com                     4
------------------------------------------------------------------------------------------
  2 catalog(s), 16 schema(s) would be exported
```

---

### Deploying to the target workspace

Apply the numbered SQL files in order. **Grants must always be applied last.**

```bash
UC_DIR="logs/PROD_MIGRATION_2024/uc_export"

# Storage credentials require manual creation (cloud-specific IAM/SP config)
# databricks sql execute --warehouse-id <wh-id> --file "${UC_DIR}/01_storage_credentials.sql"

databricks sql execute --warehouse-id <wh-id> --file "${UC_DIR}/02_external_locations.sql"
databricks sql execute --warehouse-id <wh-id> --file "${UC_DIR}/03_catalogs.sql"
databricks sql execute --warehouse-id <wh-id> --file "${UC_DIR}/04_schemas.sql"
databricks sql execute --warehouse-id <wh-id> --file "${UC_DIR}/05_volumes.sql"

for f in "${UC_DIR}/tables"/*.sql; do
    databricks sql execute --warehouse-id <wh-id> --file "$f"
done

for f in "${UC_DIR}/views"/*.sql; do
    databricks sql execute --warehouse-id <wh-id> --file "$f"
done

databricks sql execute --warehouse-id <wh-id> --file "${UC_DIR}/06_functions.sql"
databricks sql execute --warehouse-id <wh-id> --file "${UC_DIR}/07_grants.sql"  # always last
```

See [UC_EXPORT_GUIDE.md](UC_EXPORT_GUIDE.md) for the full deployment reference.

---

## DBFS & Library Export – export_dbfs_libs.py

Run this **after** the main export pipeline has produced `clusters.log` and
`jobs.log` in the session directory:

```bash
# Standard run
python3 export_dbfs_libs.py \
  --profile SRC_PROFILE \
  --session PROD_MIGRATION_2024

# Azure workspace
python3 export_dbfs_libs.py \
  --profile SRC_PROFILE \
  --session PROD_MIGRATION_2024 \
  --azure

# Custom export directory
python3 export_dbfs_libs.py \
  --profile SRC_PROFILE \
  --session PROD_MIGRATION_2024 \
  --set-export-dir /data/migrations

# Build manifest only – skip binary downloads (faster, metadata-only)
python3 export_dbfs_libs.py \
  --profile SRC_PROFILE \
  --session PROD_MIGRATION_2024 \
  --skip-download

# Debug mode
python3 export_dbfs_libs.py \
  --profile SRC_PROFILE \
  --session PROD_MIGRATION_2024 \
  --debug
```

> `export_azure.sh` calls the library exporter automatically as the
> `dbfs_libraries` component. Only run `export_dbfs_libs.py` directly when
> using the step-by-step workflow.

---

## GCP Import – import_gcp.sh

### Import workflow overview

`import_gcp.sh` orchestrates a **four-step** import pipeline:

| Step | What happens |
|---|---|
| **Step 1** – Pre-process | `import_jobs_gcp.py` transforms `jobs.log`, `clusters.log`, `instance_pools.log` in-place and writes GCP-ready JSON bundles to `gcp_ready/` |
| **Step 1.5** – Delete jobs *(optional)* | Deletes all existing jobs on the target workspace before importing. Prevents duplicate jobs. |
| **Step 2** – Migrate tool | Runs `migration_pipeline.py --import-pipeline` — imports users, groups, notebooks, secrets, clusters, instance pools, jobs, Hive metastore |
| **Step 3** – Extra components | Imports SQL Warehouses, DLT Pipelines, Repos, AI/BI Dashboards, Genie Spaces, Model Serving Endpoints, and **Workspace Files** (non-notebook files: .py .md .sql .yaml .toml .json .csv .txt .sh .ipynb …) via REST API |
| **Step 4** – Unity Catalog | Prints deployment instructions for the UC DDL files produced by the export |

At the end, a combined HTML report is written to `logs/<SESSION>/import_report.html`.

---

### Minimal GCP import command

```bash
./import_gcp.sh \
  --workspace-url https://your-gcp-workspace.gcp.databricks.com \
  --token         dapi<GCP_TOKEN> \
  --profile       GCP_PROD_PROFILE \
  --session       PROD_MIGRATION_2024
```

---

### With clean-slate job deletion

Before importing, delete all existing jobs on the GCP workspace to avoid
duplicates (the Jobs API creates new jobs on every import — it does not replace).

```bash
# With confirmation prompt (recommended for first run)
./import_gcp.sh \
  --workspace-url        https://your-gcp-workspace.gcp.databricks.com \
  --token                dapi<GCP_TOKEN> \
  --profile              GCP_PROD_PROFILE \
  --session              PROD_MIGRATION_2024 \
  --delete-existing-jobs

# Skip confirmation prompt (automated pipelines / CI)
./import_gcp.sh \
  --workspace-url        https://your-gcp-workspace.gcp.databricks.com \
  --token                dapi<GCP_TOKEN> \
  --profile              GCP_PROD_PROFILE \
  --session              PROD_MIGRATION_2024 \
  --delete-existing-jobs \
  --force
```

When `--delete-existing-jobs` is used without `--force`, the script will print
all found jobs and wait for `yes` confirmation before deleting:

```
  Found 9 existing job(s) on target workspace:
    • [101] Daily_ETL_Pipeline
    • [202] ML_Training_Job
    • [303] Reporting_Aggregation
    ...

  ⚠  Delete all 9 job(s)? [yes/No]:
```

Answering anything other than `yes` or `y` aborts the import.

---

### Dry run (preview all steps)

Preview the GCP transformation and import plan without touching any APIs:

```bash
./import_gcp.sh \
  --workspace-url https://your-gcp-workspace.gcp.databricks.com \
  --token         dapi<GCP_TOKEN> \
  --profile       GCP_PROD_PROFILE \
  --session       PROD_MIGRATION_2024 \
  --dry-run
```

In dry-run mode:

- **Step 1** still writes transformed JSON to `gcp_ready/` (safe, local only)
- **Step 1.5** lists jobs that would be deleted but does not call DELETE
- **Step 3** prints what would be created but makes no API calls

---

### Skip individual steps

Use `--skip-step` to bypass a step you've already run or want to skip:

```bash
# Re-run only Step 3 (extra components) – skipping preprocess and migrate tool
./import_gcp.sh \
  --workspace-url https://your-gcp-workspace.gcp.databricks.com \
  --token         dapi<GCP_TOKEN> \
  --profile       GCP_PROD_PROFILE \
  --session       PROD_MIGRATION_2024 \
  --no-preprocess \
  --skip-step 2 \
  --skip-step 4

# Run only the migrate tool (Step 2) – preprocess already done
./import_gcp.sh \
  --workspace-url https://your-gcp-workspace.gcp.databricks.com \
  --token         dapi<GCP_TOKEN> \
  --profile       GCP_PROD_PROFILE \
  --session       PROD_MIGRATION_2024 \
  --no-preprocess \
  --skip-step 3 \
  --skip-step 4
```

Allowed values for `--skip-step`: `1`, `2`, `3`, `4` (repeatable).

---

### GCP pre-processor standalone – import_jobs_gcp.py

Run the pre-processor independently for inspection or testing before the full import:

```bash
# Pre-process only (default mode)
python3 import_jobs_gcp.py \
  --session PROD_MIGRATION_2024

# Dry-run: show what would change without modifying any files
python3 import_jobs_gcp.py \
  --session  PROD_MIGRATION_2024 \
  --dry-run \
  --preprocess

# Custom config and mapping files
python3 import_jobs_gcp.py \
  --session  PROD_MIGRATION_2024 \
  --config   /etc/migration/gcp_import_config.json \
  --mapping  /etc/migration/node_type_mapping.csv

# Delete all jobs on target (standalone)
python3 import_jobs_gcp.py \
  --no-preprocess \
  --delete-existing-jobs \
  --workspace-url https://your-gcp-workspace.gcp.databricks.com \
  --token         dapi<GCP_TOKEN> \
  --force

# Import extra components only (SQL Warehouses, DLT, Repos, Dashboards, Genie, Serving)
python3 import_jobs_gcp.py \
  --no-preprocess \
  --import-extra \
  --workspace-url https://your-gcp-workspace.gcp.databricks.com \
  --token         dapi<GCP_TOKEN>

# Preview extra import (dry run)
python3 import_jobs_gcp.py \
  --no-preprocess \
  --import-extra \
  --dry-run \
  --workspace-url https://your-gcp-workspace.gcp.databricks.com \
  --token         dapi<GCP_TOKEN>
```

**What `--preprocess` does:**

1. Backs up `jobs.log`, `clusters.log`, `instance_pools.log` as `*.original`
2. Applies transformations from `gcp_import_config.json`:
   - Strips Azure cloud-specific fields (`azure_attributes`, `aws_attributes`,
     `enable_elastic_disk`, `instance_profile_arn`)
   - **Maps `azure_attributes.availability` → `gcp_attributes.availability`
     automatically** (see table below), rather than always defaulting to
     `ON_DEMAND_GCP`
   - Carries over `first_on_demand` from Azure when explicitly set; uses
     config default otherwise
   - Injects remaining `gcp_attributes` (`local_ssd_count`, `zone_id`,
     optionally `google_service_account`)
   - **Applies `gcp_zone_hint` from `node_type_mapping.csv`** for node types
     that require an explicit zone instead of `zone_id: auto` (e.g.
     `n1-standard-4` in `us-east1`)
   - Maps `node_type_id` / `driver_node_type_id` using `node_type_mapping.csv`
   - Sanitises `creator_user_name` and `cluster_name` (replaces `@` → `_`,
     removes apostrophes)
   - Strips `:::JOB_ID` suffixes from job names
   - Sets `schedule.pause_status` → `PAUSED` on all jobs
3. Writes modified files back in-place (so the migrate tool reads them directly)
4. Writes GCP-ready JSON bundles to `gcp_ready/` for inspection
5. Writes `gcp_transform_manifest.json` with full transformation summary

**Availability mapping (Azure → GCP):**

| Azure `azure_attributes.availability` | GCP `gcp_attributes.availability` |
|---|---|
| `ON_DEMAND_AZURE` | `ON_DEMAND_GCP` |
| `SPOT_AZURE` | `PREEMPTIBLE_GCP` |
| `SPOT_WITH_FALLBACK_AZURE` | `SPOT_WITH_FALLBACK_GCP` |
| *(not set / no `azure_attributes`)* | config default — `ON_DEMAND_GCP` |

This applies to all-purpose clusters, job clusters, task-level clusters, and
instance pools. The `availability` field in `gcp_import_config.json` is used
only as a fallback when the source has no `azure_attributes`.

---

## Non-GCP Import (Standard Workflow)

For importing into GCP, use the `import_gcp.sh` script which wraps the `databrickslabs/migrate` tool.

### Step 1 – Run the migrate import pipeline

```bash
# Standard import
python3 migration_pipeline.py \
  --profile DST_PROFILE \
  --import-pipeline \
  --use-checkpoint \
  --session PROD_MIGRATION_2024

# Azure destination
python3 migration_pipeline.py \
  --profile DST_PROFILE \
  --azure \
  --import-pipeline \
  --use-checkpoint \
  --session PROD_MIGRATION_2024

# With performance tuning
python3 migration_pipeline.py \
  --profile DST_PROFILE \
  --import-pipeline \
  --use-checkpoint \
  --session       PROD_MIGRATION_2024 \
  --num-parallel  8 \
  --retry-total   30 \
  --retry-backoff 1.0

# Archive notebooks belonging to users who no longer exist
python3 migration_pipeline.py \
  --profile DST_PROFILE \
  --import-pipeline \
  --use-checkpoint \
  --session        PROD_MIGRATION_2024 \
  --archive-missing
```

---

### Step 2 – Import DBFS libraries

```bash
# Standard import
python3 import_dbfs_libs.py \
  --profile DST_PROFILE \
  --session PROD_MIGRATION_2024

# Azure destination
python3 import_dbfs_libs.py \
  --profile DST_PROFILE \
  --session PROD_MIGRATION_2024 \
  --azure

# Upload DBFS files only (skip cluster library installs)
python3 import_dbfs_libs.py \
  --profile DST_PROFILE \
  --session PROD_MIGRATION_2024 \
  --skip-cluster-install

# Install cluster libraries only (DBFS files already copied separately)
python3 import_dbfs_libs.py \
  --profile DST_PROFILE \
  --session PROD_MIGRATION_2024 \
  --skip-upload

# Custom export dir
python3 import_dbfs_libs.py \
  --profile        DST_PROFILE \
  --session        PROD_MIGRATION_2024 \
  --set-export-dir /data/migrations
```

---

### Step 3 – Deploy Unity Catalog

Apply the UC DDL files in numbered order. Always apply grants last.

```bash
UC_DIR="logs/PROD_MIGRATION_2024/uc_export"

# Preview the deployment plan
cat "${UC_DIR}/import_manifest.json" | python3 -c "
import json, sys
m = json.load(sys.stdin)
for s in m['deployment_order']:
    print(s['step'], s['type'], s.get('full_name',''))
"

# Apply each file in order
databricks sql execute --warehouse-id <target-wh-id> --file "${UC_DIR}/02_external_locations.sql"
databricks sql execute --warehouse-id <target-wh-id> --file "${UC_DIR}/03_catalogs.sql"
databricks sql execute --warehouse-id <target-wh-id> --file "${UC_DIR}/04_schemas.sql"
databricks sql execute --warehouse-id <target-wh-id> --file "${UC_DIR}/05_volumes.sql"

for f in "${UC_DIR}/tables"/*.sql; do
    databricks sql execute --warehouse-id <target-wh-id> --file "$f"
done
for f in "${UC_DIR}/views"/*.sql; do
    databricks sql execute --warehouse-id <target-wh-id> --file "$f"
done

databricks sql execute --warehouse-id <target-wh-id> --file "${UC_DIR}/06_functions.sql"
databricks sql execute --warehouse-id <target-wh-id> --file "${UC_DIR}/07_grants.sql"
```

> `01_storage_credentials.sql` requires manual creation — storage credentials
> need cloud-specific IAM roles / service principals that differ by environment.

See [UC_EXPORT_GUIDE.md](UC_EXPORT_GUIDE.md) for the full deployment reference.

---

## HTML Reports

Two self-contained HTML reports are generated automatically. Both open in any
browser with no internet connection needed.

### Export report

Generated at the end of `export_azure.sh`:

```
logs/<SESSION>/export_report.html
```

**Tabs:**

| Tab | Contents |
|---|---|
| 📤 Export | KPI cards, component status table, artifact file tree |
| ✅ Checklist | End-to-end migration checklist |

### Import report (combined)

Generated at the end of `import_gcp.sh`:

```
logs/<SESSION>/import_report.html
```

**Tabs:**

| Tab | Contents |
|---|---|
| 📤 Export | All export component statuses from `export_status.json` |
| 🔄 GCP Transform | Files transformed, node type mapping table, unmapped warnings |
| 📥 Import | Per-step import results (created / skipped / failed) with errors |
| ✅ Checklist | Full export → import → UC deploy checklist |

### Manual generation

```bash
# Export-only report
python3 -c "
from workspace_import.html_reporter import generate_export_html
print(generate_export_html('logs/PROD_MIGRATION_2024'))
"

# Combined import report
python3 -c "
from workspace_import.html_reporter import generate_import_html
print(generate_import_html('logs/PROD_MIGRATION_2024'))
"

# Both at once (CLI)
python3 workspace_import/html_reporter.py logs/PROD_MIGRATION_2024 --mode both
```

---

## Validate Migration

Export the destination workspace into a separate session:

```bash
python3 migration_pipeline.py \
  --profile  DST_PROFILE \
  --export-pipeline \
  --use-checkpoint \
  --session  DST_EXPORT_2024
```

Then diff source and destination exports:

```bash
./validate_pipeline.sh PROD_MIGRATION_2024 DST_EXPORT_2024
```

---

## Exported Components Reference

The full pipeline exports **20 components** by default (MLflow is opt-in, total 22):

| # | Component | What is captured | Output |
|---|---|---|---|
| 1 | Instance Profiles | AWS IAM ARNs (auto-skipped for Azure source) | `instance_profiles.log` |
| 2 | Users | SCIM user records | `users.log` |
| 3 | Groups | SCIM group records | `groups/` |
| 4 | Workspace Item Log | All notebook/directory paths | `user_workspace.log` |
| 5 | Workspace ACLs | Notebook & directory permissions | `acl_notebooks.log` |
| 6 | Notebooks | Downloaded notebook content | `artifacts/` |
| 7 | Secrets | Secret scope definitions + ACLs | `secret_scopes/` |
| 8 | Clusters | Cluster configs + policies | `clusters.log` |
| 9 | Instance Pools | Pool definitions | `instance_pools.log` |
| 10 | Jobs | Job configs + ACLs | `jobs.log` |
| 11 | Hive Metastore | Legacy table DDL | `metastore/` |
| 12 | Metastore Table ACLs | Legacy table GRANT/DENY | `table_acls/` |
| 13 | MLflow Experiments | Experiment definitions | `mlflow_experiments.log` *(opt-in)* |
| 14 | MLflow Runs | Run history | `mlflow_runs.log` *(opt-in)* |
| 15 | DBFS Libraries | JAR/WHL/EGG files + manifest | `library_manifest.json` + `dbfs_files/` |
| 16 | SQL Warehouses | Warehouse configurations | `sql_warehouses.json` |
| 17 | DLT Pipelines | Delta Live Tables definitions | `dlt_pipelines.json` |
| 18 | Git Repos | Databricks Repos metadata | `repos.json` |
| 19 | AI/BI Dashboards | Lakeview dashboard content + index | `lakeview_dashboards/` |
| 20 | Genie AI Spaces | Space metadata + backing AI/BI dashboard content (`backing_serialized_dashboard`) — **cannot be auto-imported** (see note below) | `genie_spaces.json` |
| 21 | Model Serving Endpoints | User-managed endpoint configs | `serving_endpoints.json` |
| 22 | Unity Catalog Objects | DDL + ACLs for owned catalogs | `uc_export/` |
| 23 | **Workspace Files** | Non-notebook files (.py .md .sql .yaml .toml .json .csv .txt .sh .ipynb …) preserving full folder hierarchy | `workspace_files/` + `workspace_files_manifest.json` |

> **Platform-managed serving endpoints** (e.g. `databricks-meta-llama-*`,
> Foundation Model API) are automatically skipped — they cannot be manually
> recreated.

> **Genie AI Spaces — platform limitation:** Genie Spaces cannot be
> automatically imported via any public Databricks REST API. The
> `POST /api/2.0/genie/spaces` endpoint requires a `serialized_space` field
> (an internal `GenieSpaceExport` protobuf) that is not exposed by the Azure
> export API. The exporter captures the backing Lakeview dashboard's
> `serialized_dashboard` in `genie_spaces.json` for reference. During import,
> each space is counted as **skipped (not failed)** and a warning is logged with
> the space title, description, and warehouse ID so it can be **manually
> recreated** in the target workspace.

---

## Outputs Reference

### Export session directory

```
logs/PROD_MIGRATION_2024/
│
├── export_status.json            ← Machine-readable live status (per component)
├── export_report.txt             ← Human-readable text report
├── export_report.html            ← Self-contained HTML export report ✨
├── source_info.txt               ← Workspace URL of the source
│
├── instance_profiles.log         ← AWS instance profile ARNs (empty for Azure source)
├── users.log                     ← All SCIM user records
├── groups/                       ← One JSON file per group
│
├── user_workspace.log            ← All notebook paths
├── user_dirs.log                 ← All directory paths
├── libraries.log                 ← Library paths (metadata only)
├── acl_notebooks.log             ← Notebook permission objects
├── acl_directories.log           ← Directory permission objects
│
├── artifacts/                    ← Downloaded notebook binaries (DBC/SOURCE/HTML)
│   └── Users/
│       └── alice@example.com/
│
├── secret_scopes/                ← Secret scope definitions
├── secret_scopes_acls.log        ← Secret scope permission objects
│
├── clusters.log                  ← Cluster configuration objects
├── cluster_policies.log          ← Cluster policy definitions
├── instance_pools.log            ← Instance pool definitions
├── acl_clusters.log              ← Cluster permission objects
├── acl_cluster_policies.log      ← Policy permission objects
│
├── jobs.log                      ← Job configuration objects
├── acl_jobs.log                  ← Job permission objects
├── job_id_map.log                ← Old→new job ID mapping (post-import)
│
├── metastore/                    ← Per-database DDL files
│   └── default.sql
├── database_details.log          ← Database metadata
├── success_metastore.log         ← Successfully exported table list
│
├── table_acls/                   ← Per-database ACL JSON files
│
├── mlflow_experiments.log        ← MLflow experiments (only with --include-mlflow)
├── mlflow_runs.log               ← MLflow runs (only with --include-mlflow)
│
├── library_manifest.json         ← Full library manifest (all types, all usages)
├── dbfs_files/                   ← Downloaded DBFS binaries
│   └── FileStore/
│       └── jars/
│           └── my-lib.jar
│
├── sql_warehouses.json           ← SQL warehouse configurations
├── dlt_pipelines.json            ← DLT pipeline definitions
├── repos.json                    ← Git repo definitions
├── lakeview_dashboards/          ← AI/BI dashboard content
│   ├── <dashboard-name>.json
│   └── lakeview_dashboards_index.json
├── genie_spaces.json             ← Genie AI Space definitions
├── serving_endpoints.json        ← Model Serving endpoint configs (user-managed only)
├── workspace_files_manifest.json ← Index of all exported workspace FILE objects
├── workspace_files/              ← Non-notebook files (mirrors workspace folder structure)
│   ├── Shared/
│   │   └── <team-folder>/<file.py|.md|.sql|.yaml|…>
│   └── Users/
│       └── <user@email.com>/<file.ipynb|.json|.csv|…>
│
├── uc_export/                    ← Unity Catalog DDL + ACLs
│   ├── 01_storage_credentials.sql
│   ├── 02_external_locations.sql
│   ├── 03_catalogs.sql
│   ├── 04_schemas.sql
│   ├── 05_volumes.sql
│   ├── tables/                   ← One .sql per table
│   ├── views/                    ← One .sql per view (topologically sorted)
│   ├── 06_functions.sql
│   ├── 07_grants.sql             ← All GRANT statements, applied last
│   ├── import_manifest.json      ← Ordered deployment plan + statistics
│   └── raw/                      ← Raw API responses + skipped.json
│
├── app_logs/
│   ├── wm_logs.log               ← Full migration tool log
│   └── failed_export_*.log       ← Per-component error logs
│
└── checkpoint/
    └── export_*.log              ← Checkpoint files for resume support
```

---

### GCP import additions

After running `import_gcp.sh` or `import_jobs_gcp.py`, additional files appear
in the same session directory:

```
logs/PROD_MIGRATION_2024/
│
├── jobs.log.original             ← Original (pre-transformation) backup
├── clusters.log.original         ← Original backup
├── instance_pools.log.original   ← Original backup
│
├── gcp_transform_manifest.json   ← Full transformation summary:
│                                      • processed_at timestamp
│                                      • per-file record counts and warnings
│                                      • node_type_changes (from→to mapping log)
│                                      • all_warnings (unmapped node types)
│
├── gcp_ready/                    ← GCP-compatible JSON bundles (inspect before import)
│   ├── jobs.json                 ← Array of GCP-ready job settings payloads
│   ├── clusters.json             ← Array of GCP-ready cluster specs
│   └── instance_pools.json       ← Array of GCP-ready pool specs
│
├── import_log.json               ← Structured import log (per-step status, counts,
│                                      durations, errors) — updated live during import
│
└── import_report.html            ← Self-contained HTML report: ✨
                                       Export tab + GCP Transform tab +
                                       Import tab + Checklist tab
```

---

## All CLI Flags Reference

### export_azure.sh flags

```
REQUIRED
  -u, --workspace-url URL      Databricks workspace URL
                               Example: https://adb-123.7.azuredatabricks.net
  -t, --token PAT              Personal Access Token (dapi...)

CLOUD (default: --azure)
      --azure                  Source is an Azure Databricks workspace (default)
      --gcp                    Source is a GCP Databricks workspace

EXPORT OPTIONS
  -s, --session ID             Session ID (default: auto-generated M<YYYYMMDDHHmmss>)
                               Allowed: any alphanumeric string, underscores, hyphens
  -d, --export-dir DIR         Base export directory (default: ./logs/)
  -p, --num-parallel N         Download thread count (default: 4; allowed: 1–32)
      --notebook-format FMT    Notebook download format
                               Allowed: DBC | SOURCE | HTML  (default: DBC)
      --skip COMPONENT...      One or more component names to skip (space-separated)
                               See component table above for all allowed names
      --skip-failed            Do not retry failed metastore tables
      --include-mlflow         Export MLflow experiments and runs (skipped by default)
      --no-ssl-verification    Disable SSL certificate verification (use for self-signed certs)
      --retry-total N          Total HTTP retry attempts (default: 10; allowed: 0–100)
      --retry-backoff F        Retry backoff factor in seconds (default: 1.0; allowed: 0.0–10.0)
      --debug                  Enable DEBUG-level logging (very verbose)

UTILITY OPTIONS
      --report-only            Regenerate text + HTML report from existing status file; no export
      --dry-run                Print resolved config and exit; make no API calls
      --reinstall              Force re-clone and re-install the migrate repo
      --migrate-dir DIR        Override migrate repo location
                               (default: ~/.databricks-migrate or $DATABRICKS_MIGRATE_DIR)
  -h, --help                   Show help and exit
```

---

### import_gcp.sh flags

```
REQUIRED
  -u, --workspace-url URL      Target GCP workspace URL
                               Example: https://1234567890.7.gcp.databricks.com
  -t, --token PAT              Personal Access Token for the GCP workspace
  -p, --profile NAME           Databricks CLI profile for the migrate tool
                               (configured in ~/.databrickscfg)
  -s, --session ID             Export session ID (must match the export run)

OPTIONS
  -d, --export-dir DIR         Base export directory (default: ./logs/)
      --migrate-dir DIR        Override migrate repo location
                               (default: ~/.databricks-migrate)
      --config FILE            Path to gcp_import_config.json
                               (default: ./gcp_import_config.json)
      --mapping FILE           Path to node_type_mapping.csv
                               (default: ./node_type_mapping.csv)

STEP CONTROL
      --skip-step N            Skip a numbered step (repeatable)
                               Allowed values: 1 (preprocess) | 2 (migrate tool) |
                                              3 (extra components) | 4 (UC guidance)
                               Example: --skip-step 2 --skip-step 4
      --no-preprocess          Alias for --skip-step 1

JOB DELETION (step 1.5)
      --delete-existing-jobs   Delete ALL jobs on the target workspace before importing.
                               Requires --workspace-url and --token.
                               ⚠  Irreversible. Prevents duplicate job creation.
      --force                  Skip the confirmation prompt for --delete-existing-jobs
                               (required for non-interactive / CI use)

BEHAVIOUR
      --dry-run                Step 1.5: list jobs that would be deleted (no DELETE calls)
                               Step 3: print what would be imported (no POST calls)
      --include-mlflow         Step 2: include MLflow experiments + runs in the migrate
                               tool import (skipped by default)
      --no-ssl-verification    Disable SSL certificate verification
      --debug                  Enable DEBUG-level logging
  -h, --help                   Show help and exit
```

---

### import_jobs_gcp.py flags

```
SOURCE / SESSION
  -s, --session ID             Export session ID (resolves to <export-dir>/<session>/)
  -d, --export-dir DIR         Base export directory (default: ./logs/)
      --session-dir PATH       Direct path to session directory
                               (overrides --session / --export-dir)

CONFIGURATION
      --config FILE            Path to gcp_import_config.json
                               (default: ./gcp_import_config.json)
      --mapping FILE           Path to node_type_mapping.csv
                               (default: ./node_type_mapping.csv)

ACTIONS (all can be combined)
      --preprocess             Transform logs in-place + write gcp_ready/ bundles
                               (default: enabled)
      --no-preprocess          Skip pre-processing
      --import-extra           Import SQL Warehouses, DLT Pipelines, Repos,
                               AI/BI Dashboards, Genie Spaces, Serving Endpoints
                               via REST API. Requires --workspace-url and --token.
      --delete-existing-jobs   Delete all existing jobs on the target workspace.
                               Requires --workspace-url and --token.
                               Presents a confirmation prompt unless --force.
      --force                  Skip confirmation prompt for --delete-existing-jobs
      --dry-run                With --preprocess: logs what would change without
                               modifying any files.
                               With --import-extra / --delete-existing-jobs:
                               shows what would happen without calling the API.

TARGET WORKSPACE (required for --import-extra and --delete-existing-jobs)
  -u, --workspace-url URL      Target Databricks workspace URL
  -t, --token PAT              Personal Access Token for the target workspace
      --no-ssl-verification    Disable SSL certificate verification

OTHER
      --debug                  Enable DEBUG-level logging
  -h, --help                   Show help and exit

EXIT CODES
  0   Success (no warnings)
  1   Fatal error (missing config, API failure, cancelled by user)
  2   Success with warnings (e.g. unmapped node types — import continues)
```

**gcp_import_config.json structure:**

```json
{
  "gcp_attributes": {
    // FALLBACK values — used only when the source cluster has no azure_attributes.
    // The 'availability' field is AUTO-MAPPED from azure_attributes (see below).
    // Only set these as defaults for clusters that have no explicit Azure availability.
    "availability":    "ON_DEMAND_GCP",   // fallback: ON_DEMAND_GCP | PREEMPTIBLE_GCP | SPOT_WITH_FALLBACK_GCP
    "first_on_demand": 1,                 // fallback: integer ≥ 0 (1 = keep driver on-demand)
    "local_ssd_count": 0,                 // integer ≥ 0
    "zone_id":         "auto"             // "auto" or specific zone e.g. "us-central1-a"
  },
  "fields_to_remove_from_cluster": [
    "azure_attributes", "aws_attributes",
    "enable_elastic_disk", "instance_profile_arn"
  ],
  "fields_to_remove_from_job": [
    "job_id", "created_time", "run_as_user_name"
  ],
  "cluster_name_transforms": {
    "replace_at_with":    "_",   // replacement character for @ in names
    "remove_apostrophes": true,  // true | false
    "replace_spaces_with": "_"   // replacement character for spaces
  },
  "job_name_suffix": {
    "strip_id_suffix": true      // strips :::JOB_ID suffixes appended by some export tools
  },
  "schedule_behaviour": {
    "set_pause_status": "PAUSED" // PAUSED | UNPAUSED — applied to all imported job schedules
  },
  "email_notifications": {
    "clear_on_import": false     // true = remove all email_notifications blocks
  }
}
```

**node_type_mapping.csv format:**

```
source_node_type,gcp_node_type,source_cloud,vcpus,memory_gb,notes,gcp_zone_hint
Standard_DS3_v2,n1-standard-4,azure,4,14,4 vCPU 14 GB,us-east1-b
Standard_E8ds_v4,n2d-highmem-8,azure,8,64,8 vCPU 64 GB,
Standard_NC6s_v3,a2-highgpu-1g,azure,6,112,GPU: V100 → A100,
```

Column descriptions:

| Column | Required | Description |
|---|---|---|
| `source_node_type` | ✓ | Azure `Standard_*` instance type (case-insensitive match) |
| `gcp_node_type` | ✓ | GCP machine type to substitute |
| `source_cloud` | — | `azure` (informational only) |
| `vcpus` | — | vCPU count (informational) |
| `memory_gb` | — | Memory in GB (informational) |
| `notes` | — | Free text |
| `gcp_zone_hint` | — | Override `gcp_attributes.zone_id` for this specific node type. Useful when a node type is not available in all zones with `zone_id: auto` (e.g. `n1-standard-4` in `us-east1` requires `us-east1-b`). Leave empty to use the global `zone_id` from `gcp_import_config.json`. |

---

### export_unity_catalog.py flags

```
REQUIRED
  -u, --workspace-url URL      Databricks workspace URL
  -t, --token PAT              Personal Access Token

CATALOG / SCHEMA SELECTION (mutually exclusive)
      (default)                Export only catalogs owned by the token user
      --catalogs CAT [CAT...]  Export only the specified catalog(s)
                               Allowed: one or more catalog names, space-separated
      --all-catalogs           Export ALL accessible MANAGED_CATALOG entries
                               ⚠  Can be slow on large shared workspaces

  --schemas SCH [SCH...]       Restrict to these schema name(s)
                               Allowed bare names (bronze) or qualified (sales_prod.bronze)

OUTPUT
  -d, --export-dir DIR         Base export directory (default: ./logs/)
  -s, --session ID             Session sub-directory
                               (default: UC_<YYYYMMDDHHmmss>)

BEHAVIOUR
      --list-only              Print catalog/schema counts and exit; write no files
      --no-ssl-verification    Disable SSL certificate verification
      --debug                  Enable DEBUG-level logging
```

See [UC_EXPORT_GUIDE.md](UC_EXPORT_GUIDE.md) for full documentation.

---

### export_dbfs_libs.py flags

```
REQUIRED
  --profile PROFILE            Databricks CLI profile name (source workspace)

OPTIONS
  --session SESSION            Session ID (must match the export run)
  --set-export-dir DIR         Base export directory (default: ./logs/)
  --azure                      Source is an Azure workspace
  --gcp                        Source is a GCP workspace
  --no-ssl-verification        Disable SSL certificate verification
  --debug                      Enable DEBUG-level logging
  --skip-download              Build manifest only; skip DBFS binary downloads
```

---

### import_dbfs_libs.py flags

```
REQUIRED
  --profile PROFILE            Databricks CLI profile name (destination workspace)

OPTIONS
  --session SESSION            Session ID (must match the export run)
  --set-export-dir DIR         Base export directory (default: ./logs/)
  --azure                      Destination is an Azure workspace
  --gcp                        Destination is a GCP workspace
  --no-ssl-verification        Disable SSL certificate verification
  --debug                      Enable DEBUG-level logging
  --skip-upload                Skip DBFS file uploads (e.g. files already present)
  --skip-cluster-install       Skip cluster library installation step
```

---

## Troubleshooting

### SSL errors

```bash
# Pass the flag to either script
./export_azure.sh \
  --workspace-url https://... --token dapi... \
  --no-ssl-verification

./import_gcp.sh \
  --workspace-url https://... --token dapi... --profile GCP --session S \
  --no-ssl-verification

# Or unset bundle env vars in your shell
export REQUESTS_CA_BUNDLE=""
export CURL_CA_BUNDLE=""
```

---

### Token expired mid-run

The migrate tool polls `~/.databrickscfg` every 20 seconds when it receives a 403.
Renew the token in a separate terminal:

```bash
databricks configure --token --profile SRC_PROFILE
```

The running export detects the new token and resumes automatically.

---

### API rate limits

Reduce parallelism and increase retry backoff:

```bash
./export_azure.sh \
  --workspace-url https://... --token dapi... \
  --num-parallel  2 \
  --retry-total   50 \
  --retry-backoff 2.0
```

---

### Imported jobs are duplicated

The Databricks Jobs API always **creates** a new job on import — it does not
check for existing jobs with the same name.  Use `--delete-existing-jobs` before
importing to clear the workspace first:

```bash
./import_gcp.sh \
  --workspace-url https://... --token dapi... --profile GCP --session S \
  --delete-existing-jobs --force
```

For non-GCP workflows, delete jobs manually via the UI or:

```bash
# List all jobs and get their IDs
python3 -c "
import requests, json
url = 'https://your-workspace.gcp.databricks.com'
headers = {'Authorization': 'Bearer dapi...'}
resp = requests.get(f'{url}/api/2.1/jobs/list', headers=headers)
for j in resp.json().get('jobs', []):
    print(j['job_id'], j['settings']['name'])
"
```

---

### Check which components failed

```bash
python3 -c "
import json
with open('logs/PROD_MIGRATION_2024/export_status.json') as f:
    data = json.load(f)
for name, comp in data['components'].items():
    if comp['status'] in ('failed', 'in_progress', 'pending'):
        print(f\"{comp['status'].upper():12} {comp['display_name']} – {comp.get('error_message','')}\")
"
```

Or open `export_report.html` in a browser — failed components are highlighted in red.

---

### Genie AI Spaces are skipped during import

When `import_jobs_gcp.py` runs Step 3 you will see:

```
⚠  Genie AI Spaces cannot be migrated automatically. The Databricks Genie API
   requires a 'serialized_space' field that is not available via the public REST API.
   Please recreate the 5 space(s) manually in the target workspace.
  MANUAL: 'My Space'  warehouse=<id>  desc=<description>
```

This is a **known platform limitation** — the `serialized_space` field required
by `POST /api/2.0/genie/spaces` is an internal protobuf not exposed by any
public export API.  These spaces are counted as `skipped` (not `failed`) in the
import report.

**To recreate each Genie Space manually:**

1. Open the GCP workspace in a browser.
2. Navigate to **SQL** → **Genie** and click **New space**.
3. Use the details printed in the log (title, description, warehouse ID mapping)
   to configure each space.
4. The exported `genie_spaces.json` contains `backing_serialized_dashboard` —
   the underlying dashboard queries and datasets — which you can use as a
   reference when rebuilding the space's curated questions.

To get the list of spaces that need manual recreation:

```bash
python3 -c "
import json
with open('logs/PROD_MIGRATION_2024/genie_spaces.json') as f:
    data = json.load(f)
for s in data.get('spaces', []):
    print(f\"  Title      : {s['title']}\")
    print(f\"  Description: {s.get('description','')[:80]}\")
    print(f\"  Warehouse  : {s.get('warehouse_id','')}\")
    print()
"
```

---

### Instance pool creation fails with 'not available in specified zone'

When `import_jobs_gcp.py` or the migrate tool prints:

```
Instance type, n1-standard-4 is not available in specified zone: ZoneId(auto),
or the zone does not exist in region: us-east1
```

Some GCP machine types are not available in all zones within a region when
`zone_id` is set to `auto`.  Fix this by adding a `gcp_zone_hint` column to
`node_type_mapping.csv` for the affected machine type:

```csv
source_node_type,gcp_node_type,source_cloud,vcpus,memory_gb,notes,gcp_zone_hint
Standard_DS3_v2,n1-standard-4,azure,4,14,4 vCPU 14 GB,us-east1-b
```

The preprocessor will then set `gcp_attributes.zone_id = "us-east1-b"` for
any cluster or pool using that node type, overriding the global `zone_id: auto`.

To find which zones are available for a given machine type, check the
[GCP Machine types documentation](https://cloud.google.com/compute/docs/machine-resource)
or run:

```bash
gcloud compute machine-types list --filter="name=n1-standard-4" \
  --zones us-east1-a,us-east1-b,us-east1-c,us-east1-d
```

---

### GCP clusters created with wrong availability (always ON_DEMAND)

The pre-processor now **automatically maps** Azure availability settings to GCP.
If clusters appear as `ON_DEMAND_GCP` when you expected `PREEMPTIBLE_GCP`, the
source Azure cluster likely had no explicit `azure_attributes.availability` set
(the default on Azure is ON_DEMAND).

To verify what the source cluster was using, check `gcp_ready/clusters.json`:

```bash
python3 -c "
import json
with open('logs/PROD_MIGRATION_2024/gcp_ready/clusters.json') as f:
    for c in json.load(f):
        gcp = c.get('gcp_attributes', {})
        print(f\"{c.get('cluster_name','?'):35s}  {gcp.get('availability')}\")
"
```

To override availability for all clusters globally, change the fallback in
`gcp_import_config.json`:

```json
{
  "gcp_attributes": {
    "availability": "PREEMPTIBLE_GCP"
  }
}
```

Then restore the original log files and re-run preprocessing:

```bash
cp logs/PROD_MIGRATION_2024/clusters.log.original logs/PROD_MIGRATION_2024/clusters.log
cp logs/PROD_MIGRATION_2024/jobs.log.original     logs/PROD_MIGRATION_2024/jobs.log
python3 import_jobs_gcp.py --session PROD_MIGRATION_2024
```

---

### Unmapped node types after GCP pre-processing


```
⚠  Standard_D8_v3: no mapping for 'Standard_D8_v3' (node_type_id) – kept as-is
```

Add the missing mapping to `node_type_mapping.csv`:

```csv
Standard_D8_v3,n2-standard-8,azure,8,32,8 vCPU 32 GB
```

Then restore the original log files and re-run pre-processing:

```bash
cp logs/PROD_MIGRATION_2024/jobs.log.original     logs/PROD_MIGRATION_2024/jobs.log
cp logs/PROD_MIGRATION_2024/clusters.log.original logs/PROD_MIGRATION_2024/clusters.log
python3 import_jobs_gcp.py --session PROD_MIGRATION_2024
```

---

### Unity Catalog export takes very long

On a large shared workspace with many catalogs, use `--list-only` first, then
select only the catalogs you need:

```bash
# Preview scope first
python3 export_unity_catalog.py \
  --workspace-url ... --token ... \
  --all-catalogs --list-only

# Export only what you own
python3 export_unity_catalog.py \
  --workspace-url ... --token ... \
  --catalogs my_catalog_1 my_catalog_2
```

---

### Regenerate the HTML report after a partial run

```bash
# Export HTML only
python3 workspace_import/html_reporter.py logs/PROD_MIGRATION_2024 --mode export

# Import HTML (combined)
python3 workspace_import/html_reporter.py logs/PROD_MIGRATION_2024 --mode import

# Both
python3 workspace_import/html_reporter.py logs/PROD_MIGRATION_2024 --mode both
```

---

### Metastore export fails for some tables

The metastore exporter cycles through all IAM instance profiles to retry failed
tables. To skip retries and move on:

```bash
./export_azure.sh \
  --workspace-url https://... --token dapi... \
  --skip-failed
```

---

### DBFS library file too large / times out

Copy the file manually using the Databricks CLI, then place it at the expected
path in the session directory so the importer can find it:

```bash
databricks fs cp dbfs:/FileStore/jars/large-lib.jar ./local-backup/large-lib.jar \
  --profile SRC_PROFILE

mkdir -p logs/PROD_MIGRATION_2024/dbfs_files/FileStore/jars/
cp ./local-backup/large-lib.jar \
   logs/PROD_MIGRATION_2024/dbfs_files/FileStore/jars/large-lib.jar
```

---

### MLflow runs are missing

MLflow is **skipped by default**. Add `--include-mlflow` to include it:

```bash
./export_azure.sh \
  --workspace-url https://... --token dapi... \
  --include-mlflow
```

For large workspaces with many experiments, use the dedicated
[MLflow Export/Import](https://github.com/mlflow/mlflow-export-import) tool
which supports date-range filtering and incremental exports.
