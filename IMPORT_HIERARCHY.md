# Databricks Workspace Import — Hierarchy & Command Reference

> **One source of truth for component-by-component migration.**  
> Follow tiers in order. Each tier depends on the ones above it.

---

## Quick-start variables

Set these once before running any command below.

```bash
export SESSION="EXPORT_20260607"          # your export session ID
export STAGING="logs_staging"             # staging base dir (from build-staging step)
export PROFILE="ocm-gcp-dst"             # Databricks CLI profile for target GCP workspace
export GCP_URL="https://8259561172954120.0.gcp.databricks.com"
export TOKEN="dapi<YOUR_GCP_PAT_TOKEN>"
export MIGRATE_DIR="$HOME/.databricks-migrate"
```

The base migrate-tool command is always:

```bash
python3 $MIGRATE_DIR/migration_pipeline.py \
  --profile   $PROFILE    \
  --gcp                   \
  --import-pipeline       \
  --no-prompt             \
  --use-checkpoint        \
  --session   $SESSION    \
  --set-export-dir $STAGING
```

Add `--keep-tasks <name>` to run **one component at a time**.

---

## Dependency Hierarchy

```
┌─────────────────────────────────────────────────────────────────┐
│  Tier 1 · IDENTITY                                              │
│  Users  ──►  Groups                                             │
└───────────────────────┬─────────────────────────────────────────┘
                        │ (all tiers below require Tier 1)
          ┌─────────────┴──────────────┐
          ▼                            ▼
┌─────────────────┐         ┌──────────────────────────────────┐
│  Tier 2         │         │  Tier 3                          │
│  COMPUTE        │         │  WORKSPACE CONTENT               │
│  Instance Pools │         │  Item Log → ACLs → Notebooks     │
│  Clusters       │         └────────────────┬─────────────────┘
└────────┬────────┘                          │
         │                                   │
         └─────────────┬─────────────────────┘
                       ▼
          ┌────────────────────────┐
          │  Tier 4 · SECURITY     │
          │  Secrets & Scopes      │
          └────────────┬───────────┘
                       ▼
          ┌────────────────────────┐
          │  Tier 5 · JOBS         │
          │  Jobs                  │
          └────────────┬───────────┘
                       ▼
          ┌────────────────────────┐
          │  Tier 6 · METASTORE    │
          │  Hive → Table ACLs     │
          └────────────┬───────────┘
                       ▼
          ┌────────────────────────┐
          │  Tier 7 · EXTRA        │
          │  SQL Warehouses        │
          │  DLT Pipelines         │
          │  Git Repos             │
          │  AI/BI Dashboards      │
          │  Genie AI Spaces       │
          │  Model Serving         │
          └────────────┬───────────┘
                       ▼
          ┌────────────────────────┐
          │  Tier 8 · OPTIONAL     │
          │  MLflow                │
          └────────────────────────┘
```

---

## Tier 1 — Identity
> **No dependencies. Always run first.**

### T1-01 · Users

```bash
python3 $MIGRATE_DIR/migration_pipeline.py \
  --profile $PROFILE --gcp --import-pipeline \
  --no-prompt --use-checkpoint \
  --session $SESSION --set-export-dir $STAGING \
  --keep-tasks users
```

**What it does:** Creates all workspace users from `users.log` using SCIM API.  
**Prerequisite:** None.  
**Example output:** `Users: 42 created, 0 skipped, 0 failed`

---

### T1-02 · Groups

```bash
python3 $MIGRATE_DIR/migration_pipeline.py \
  --profile $PROFILE --gcp --import-pipeline \
  --no-prompt --use-checkpoint \
  --session $SESSION --set-export-dir $STAGING \
  --keep-tasks groups
```

**What it does:** Creates groups and assigns members from `groups/` directory.  
**Prerequisite:** Users (T1-01).

---

## Tier 2 — Compute
> **Depends on: Users**

### T2-01 · Instance Pools

```bash
python3 $MIGRATE_DIR/migration_pipeline.py \
  --profile $PROFILE --gcp --import-pipeline \
  --no-prompt --use-checkpoint \
  --session $SESSION --set-export-dir $STAGING \
  --keep-tasks instance_pools
```

**What it does:** Creates instance pools from `instance_pools.log`. Clusters reference pool IDs.  
**Prerequisite:** Users (T1-01).

---

### T2-02 · All-Purpose Clusters

```bash
python3 $MIGRATE_DIR/migration_pipeline.py \
  --profile $PROFILE --gcp --import-pipeline \
  --no-prompt --use-checkpoint \
  --session $SESSION --set-export-dir $STAGING \
  --keep-tasks clusters
```

**What it does:** Creates interactive clusters from `clusters.log`.  
GCP attributes (availability, local SSD, elastic disk) are injected by the pre-process step.  
**Prerequisite:** Instance Pools (T2-01), Users (T1-01).

---

## Tier 3 — Workspace Content
> **Depends on: Users**

### T3-01 · Workspace Item Log (Folder Structure)

```bash
python3 $MIGRATE_DIR/migration_pipeline.py \
  --profile $PROFILE --gcp --import-pipeline \
  --no-prompt --use-checkpoint \
  --session $SESSION --set-export-dir $STAGING \
  --keep-tasks workspace_item_log
```

**What it does:** Recreates the folder skeleton (`/Shared/…`, `/Users/…`).  
**Prerequisite:** Users (T1-01).  
**Tip:** Use `--folders-only` in `import_gcp.sh` to create only folders without notebook content.

---

### T3-02 · Workspace ACLs

```bash
python3 $MIGRATE_DIR/migration_pipeline.py \
  --profile $PROFILE --gcp --import-pipeline \
  --no-prompt --use-checkpoint \
  --session $SESSION --set-export-dir $STAGING \
  --keep-tasks workspace_acls
```

**What it does:** Sets permissions on workspace folders and notebooks.  
**Prerequisite:** Workspace Item Log (T3-01), Groups (T1-02).

---

### T3-03 · Notebooks

```bash
python3 $MIGRATE_DIR/migration_pipeline.py \
  --profile $PROFILE --gcp --import-pipeline \
  --no-prompt --use-checkpoint \
  --session $SESSION --set-export-dir $STAGING \
  --keep-tasks notebooks
```

**What it does:** Uploads all notebooks (`.dbc` / `SOURCE` format) from `user_artifacts/`.  
**Prerequisite:** Workspace Item Log (T3-01).

---

## Tier 4 — Security
> **Depends on: Users, Workspace**

### T4-01 · Secrets & Scopes

```bash
python3 $MIGRATE_DIR/migration_pipeline.py \
  --profile $PROFILE --gcp --import-pipeline \
  --no-prompt --use-checkpoint \
  --session $SESSION --set-export-dir $STAGING \
  --keep-tasks secrets
```

**What it does:** Creates secret scopes and sets ACLs from `secret_scopes.log`.  
**Note:** Secret *values* are not exported by Databricks API — values must be re-populated manually.  
**Prerequisite:** Users (T1-01), Groups (T1-02).

---

## Tier 5 — Jobs
> **Depends on: Clusters, Notebooks, Instance Pools**

### T5-01 · Jobs

```bash
python3 $MIGRATE_DIR/migration_pipeline.py \
  --profile $PROFILE --gcp --import-pipeline \
  --no-prompt --use-checkpoint \
  --session $SESSION --set-export-dir $STAGING \
  --keep-tasks jobs
```

**What it does:** Creates all jobs from `jobs.log`. Job cluster specs are already GCP-transformed.  
Schedules are set to **PAUSED** by default (controlled by `schedule_behaviour` in config).  
**Prerequisite:** Clusters (T2-02), Notebooks (T3-03), Instance Pools (T2-01).

---

## Tier 6 — Metastore
> **Depends on: Clusters**

### T6-01 · Hive Metastore

```bash
python3 $MIGRATE_DIR/migration_pipeline.py \
  --profile $PROFILE --gcp --import-pipeline \
  --no-prompt --use-checkpoint \
  --session $SESSION --set-export-dir $STAGING \
  --keep-tasks metastore \
  --cluster-name "ocm-dev-single-node"
```

**What it does:** Re-creates databases and tables in the Hive metastore.  
**Prerequisite:** An all-purpose cluster must be running (`--cluster-name`).

---

### T6-02 · Metastore Table ACLs

```bash
python3 $MIGRATE_DIR/migration_pipeline.py \
  --profile $PROFILE --gcp --import-pipeline \
  --no-prompt --use-checkpoint \
  --session $SESSION --set-export-dir $STAGING \
  --keep-tasks metastore_table_acls \
  --cluster-name "ocm-dev-single-node"
```

**What it does:** Sets GRANT/REVOKE permissions on metastore tables.  
**Prerequisite:** Hive Metastore (T6-01), Users (T1-01), Groups (T1-02).

---

## Tier 7 — Extra Components (REST API)
> **All six run in one command, in dependency order.**  
> Depends on: Users, SQL Warehouses (for Genie)

```bash
python3 import_jobs_gcp.py \
  --workspace-url $GCP_URL \
  --token         $TOKEN   \
  --session       $SESSION \
  --export-dir    $STAGING \
  --import-extra           \
  --no-preprocess
```

**Runs in this order internally:**

| Step | Component | Source file | GCP API |
|------|-----------|-------------|---------|
| 1 | SQL Warehouses | `sql_warehouses.json` | `POST /api/2.0/sql/warehouses` |
| 2 | DLT Pipelines | `dlt_pipelines.json` | `POST /api/2.0/pipelines` |
| 3 | Git Repos | `repos.log` | `POST /api/2.0/repos` |
| 4 | AI/BI Dashboards | `lakeview_dashboards/` | `POST /api/2.0/lakeview/dashboards` |
| 5 | Genie AI Spaces | `genie_spaces.json` | `POST /api/2.0/genie/spaces` |
| 6 | Model Serving Endpoints | `serving_endpoints.json` | `POST /api/2.1/serving-endpoints` |

Add `--dry-run` to preview without calling the API.

---

## Tier 8 — Optional: MLflow

```bash
python3 $MIGRATE_DIR/migration_pipeline.py \
  --profile $PROFILE --gcp --import-pipeline \
  --no-prompt --use-checkpoint \
  --session $SESSION --set-export-dir $STAGING \
  --keep-tasks mlflow_experiments \
  --keep-tasks mlflow_runs
```

**What it does:** Imports MLflow experiment metadata and run history.  
**Prerequisite:** Users (T1-01).

---

## Full pipeline in one shot

To run everything in one orchestrated command (all tiers, correct order):

```bash
./import_gcp.sh \
  --workspace-url $GCP_URL \
  --token         $TOKEN   \
  --profile       $PROFILE \
  --session       $SESSION
```

To skip a tier: add `--skip-step <N>` (1, 1.5, 2, 3, or 4).  
To run folders only (no notebook content): add `--folders-only`.

---

## Resuming after a failure

Every migrate-tool step is checkpointed. Re-run the exact same command — it will
skip already-imported items and continue from where it stopped.

```bash
# Example: resume jobs after a transient API failure
python3 $MIGRATE_DIR/migration_pipeline.py \
  --profile $PROFILE --gcp --import-pipeline \
  --no-prompt --use-checkpoint \          # <-- resumes from checkpoint
  --session $SESSION --set-export-dir $STAGING \
  --keep-tasks jobs
```

---

*Generated: 2026-06-07 | Source workspace: Azure (adb-984752964297111) | Target: GCP (8259561172954120)*
