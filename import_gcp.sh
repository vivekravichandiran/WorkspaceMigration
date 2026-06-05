#!/usr/bin/env bash
# =============================================================================
# import_gcp.sh – Full GCP Workspace Import Orchestrator
# =============================================================================
#
# Runs the complete import sequence for migrating an exported Databricks
# workspace into a GCP target workspace:
#
#   Step 1  Pre-process  – transform exported log files for GCP compatibility
#             (jobs.log, clusters.log, instance_pools.log)
#             Handles: azure→gcp attributes, node-type mapping,
#                      name sanitisation, schedule pausing
#
#   Step 1.5  SP Migration – copy exports to a staging folder, then scan,
#             create and UUID-patch all Service Principal references so that
#             ACLs and job run_as fields reference GCP identities.
#             Writes sp_mapping.json and patch_summary.json to the session dir.
#             All subsequent steps use the STAGING copy (original untouched).
#
#   Step 1.6  (Optional) Delete existing jobs on target before importing.
#
#   Step 2  Migrate tool – run databrickslabs/migrate --import-pipeline
#             Handles: users, groups, workspace/notebooks, secrets,
#                      clusters, instance pools, jobs, ACLs, Hive metastore,
#                      table ACLs, MLflow (if --include-mlflow)
#
#   Step 3  Extra import – import components NOT covered by the migrate tool
#             Handles: SQL Warehouses, DLT Pipelines, Git Repos,
#                      AI/BI Dashboards, Genie AI Spaces, Serving Endpoints
#
#   Step 4  UC deploy   – guidance for deploying Unity Catalog SQL files
#             (manual step; SQL files are in uc_export/)
#
# Usage:
#   ./import_gcp.sh \
#       --workspace-url https://XXXXXXXX.gcp.databricks.com \
#       --token dapi... \
#       --profile DST_GCP_PROFILE \
#       --session EXPORT_202604281109
#
# Quick reference:
#   --workspace-url  URL   Target GCP workspace URL (required)
#   --token          PAT   Personal Access Token for GCP workspace (required)
#   --profile        NAME  Databricks CLI profile for the migrate tool (required)
#   --session        ID    Export session ID (required)
#   --export-dir     DIR   Base export dir (default: ./logs)
#   --staging-dir    DIR   Override staging base dir (default: <export-dir>_staging)
#   --config         FILE  gcp_import_config.json (default: ./gcp_import_config.json)
#   --mapping        FILE  node_type_mapping.csv  (default: ./node_type_mapping.csv)
#   --skip-step      N     Skip step 1, 1.5, 2, 3, or 4 (repeatable)
#   --dry-run              Step 1.5/1.6/3: preview actions without calling API
#   --delete-existing-jobs Delete all jobs on target workspace before importing (step 1.6)
#   --force                Skip confirmation prompt for --delete-existing-jobs
#   --include-mlflow       Step 2: include MLflow in the migrate tool import
#   --no-ssl-verification  Disable SSL certificate verification
#   --debug                Enable verbose debug logging
#   -h, --help             Show this help
#
# Prerequisites:
#   • python3 and git installed
#   • ~/.databricks-migrate/  set up (run migrate_workspace.sh once to bootstrap)
#   • Databricks CLI configured with DST_GCP_PROFILE
# =============================================================================

set -euo pipefail

# ── Colour helpers ────────────────────────────────────────────────────────────
if [[ -t 1 ]]; then
    BOLD="\033[1m"; GREEN="\033[0;32m"; YELLOW="\033[0;33m"
    RED="\033[0;31m"; CYAN="\033[0;36m"; RESET="\033[0m"
else
    BOLD=""; GREEN=""; YELLOW=""; RED=""; CYAN=""; RESET=""
fi

log_info()  { echo -e "${GREEN}[INFO]${RESET}  $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${RESET}  $*"; }
log_error() { echo -e "${RED}[ERROR]${RESET} $*" >&2; }
log_step()  { echo -e "\n${BOLD}${CYAN}══════════════════════════════════════════════════════════${RESET}"; \
              echo -e "${BOLD}${CYAN}  $*${RESET}"; \
              echo -e "${BOLD}${CYAN}══════════════════════════════════════════════════════════${RESET}"; }

# ── Defaults ──────────────────────────────────────────────────────────────────
WORKSPACE_URL=""
PAT_TOKEN=""
CLI_PROFILE=""
SESSION_ID=""
EXPORT_DIR="logs"
STAGING_DIR_OVERRIDE=""
MIGRATE_REPO_DIR="${DATABRICKS_MIGRATE_DIR:-${HOME}/.databricks-migrate}"
GCP_CONFIG="gcp_import_config.json"
NODE_MAPPING="node_type_mapping.csv"
SKIP_STEPS=()
DRY_RUN=false
INCLUDE_MLFLOW=false
NO_SSL=false
DEBUG=false
DELETE_JOBS=false
FORCE_DELETE=false

# ── Argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        -u|--workspace-url)    WORKSPACE_URL="$2";        shift 2 ;;
        -t|--token)            PAT_TOKEN="$2";             shift 2 ;;
        -p|--profile)          CLI_PROFILE="$2";           shift 2 ;;
        -s|--session)          SESSION_ID="$2";             shift 2 ;;
        -d|--export-dir)       EXPORT_DIR="$2";             shift 2 ;;
        --staging-dir)         STAGING_DIR_OVERRIDE="$2";  shift 2 ;;
        --migrate-dir)         MIGRATE_REPO_DIR="$2";       shift 2 ;;
        --config)              GCP_CONFIG="$2";              shift 2 ;;
        --mapping)             NODE_MAPPING="$2";            shift 2 ;;
        --skip-step)           SKIP_STEPS+=("$2");           shift 2 ;;
        --dry-run)             DRY_RUN=true;                 shift   ;;
        --include-mlflow)      INCLUDE_MLFLOW=true;          shift   ;;
        --delete-existing-jobs) DELETE_JOBS=true;            shift   ;;
        --force)               FORCE_DELETE=true;            shift   ;;
        --no-ssl-verification) NO_SSL=true;                  shift   ;;
        --debug)               DEBUG=true;                   shift   ;;
        -h|--help)
            grep '^#' "$0" | sed 's/^# \?//' | sed '/^!\/usr\/bin\/env bash/d'
            exit 0 ;;
        *) log_error "Unknown argument: $1"; exit 1 ;;
    esac
done

# ── Validation ────────────────────────────────────────────────────────────────
_SKIP_STEP1=false; _SKIP_STEP1_5=false; _SKIP_STEP2=false; _SKIP_STEP3=false; _SKIP_STEP4=false
for s in "${SKIP_STEPS[@]+"${SKIP_STEPS[@]}"}"; do
    [[ "$s" == "1"   ]] && _SKIP_STEP1=true   || true
    [[ "$s" == "1.5" ]] && _SKIP_STEP1_5=true || true
    [[ "$s" == "2"   ]] && _SKIP_STEP2=true   || true
    [[ "$s" == "3"   ]] && _SKIP_STEP3=true   || true
    [[ "$s" == "4"   ]] && _SKIP_STEP4=true   || true
done

[[ -z "$WORKSPACE_URL" ]] && { log_error "--workspace-url is required"; exit 1; }
[[ -z "$PAT_TOKEN"     ]] && { log_error "--token is required"; exit 1; }
[[ -z "$SESSION_ID"    ]] && { log_error "--session is required"; exit 1; }

# Resolve EXPORT_DIR to absolute path so sub-processes run from other dirs work correctly
EXPORT_DIR="$(cd "$EXPORT_DIR" 2>/dev/null && pwd)" || { log_error "Export dir not found: ${EXPORT_DIR}"; exit 1; }

SESSION_DIR="${EXPORT_DIR}/${SESSION_ID}"
[[ -d "$SESSION_DIR" ]] || { log_error "Session directory not found: ${SESSION_DIR}"; exit 1; }

# Staging base: all imports run from the staging copy so the original stays clean
if [[ -n "$STAGING_DIR_OVERRIDE" ]]; then
    mkdir -p "$STAGING_DIR_OVERRIDE"
    STAGING_BASE="$(cd "$STAGING_DIR_OVERRIDE" && pwd)"
else
    STAGING_BASE="${EXPORT_DIR}_staging"
fi
STAGING_SESSION_DIR="${STAGING_BASE}/${SESSION_ID}"

# migrate tool is only needed if step 2 isn't skipped
if ! $_SKIP_STEP2; then
    [[ -z "$CLI_PROFILE" ]] && { log_error "--profile is required for Step 2 (migrate tool). Use --skip-step 2 to bypass."; exit 1; }
    [[ -d "$MIGRATE_REPO_DIR" ]] || { log_error "Migrate repo not found: ${MIGRATE_REPO_DIR}. Run migrate_workspace.sh once to bootstrap."; exit 1; }
fi

# Config/mapping only needed for step 1
if ! $_SKIP_STEP1; then
    [[ -f "$GCP_CONFIG"   ]] || { log_error "GCP config not found: ${GCP_CONFIG}"; exit 1; }
    [[ -f "$NODE_MAPPING" ]] || { log_error "Node mapping not found: ${NODE_MAPPING}"; exit 1; }
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON3="python3"

SSL_FLAG=""
$NO_SSL && SSL_FLAG="--no-ssl-verification"

DEBUG_FLAG=""
$DEBUG && DEBUG_FLAG="--debug"

START_TIME=$(date +%s)

# Shell-side import log helper – keeps import_log.json in sync for the migrate-tool step
_log_migrate_step() {
    local status="$1" duration="$2" notes="$3"
    local LOG_DIR="$4"
    PYTHONPATH="${SCRIPT_DIR}:${MIGRATE_REPO_DIR}" \
        "$PYTHON3" - "$LOG_DIR" "$status" "$duration" "$notes" <<'PYEOF'
import sys, json, os, datetime
sdir, status, dur, notes = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
log_path = os.path.join(sdir, "import_log.json")
if not os.path.isfile(log_path):
    sys.exit(0)
with open(log_path) as f:
    data = json.load(f)
step = {
    "step": len(data.get("steps", [])) + 1,
    "component": "migrate_tool",
    "handler": "migration_pipeline.py --import-pipeline",
    "status": status,
    "started_at": None,
    "ended_at": datetime.datetime.now().isoformat(),
    "duration_seconds": float(dur) if dur else None,
    "total": None, "created": None, "skipped": None, "failed": 0,
    "notes": notes, "errors": [],
}
data.setdefault("steps", []).append(step)
tmp = log_path + ".tmp"
with open(tmp, "w") as f:
    json.dump(data, f, indent=2)
os.replace(tmp, log_path)
PYEOF
}

echo ""
echo -e "${BOLD}Databricks GCP Workspace Import${RESET}"
echo "  Target workspace : ${WORKSPACE_URL}"
echo "  Session          : ${SESSION_ID}"
echo "  Export dir       : ${SESSION_DIR}"
echo "  Staging dir      : ${STAGING_SESSION_DIR}"
if ! $_SKIP_STEP2; then
    echo "  CLI profile      : ${CLI_PROFILE}"
    echo "  Migrate repo     : ${MIGRATE_REPO_DIR}"
fi
echo ""

# ── Step 1: Pre-process ───────────────────────────────────────────────────────
if $_SKIP_STEP1; then
    log_warn "Step 1 (Pre-process) skipped by --skip-step 1"
else
    log_step "Step 1 of 4 – GCP Pre-process"
    log_info "Transforming exported logs for GCP compatibility …"
    log_info "  jobs.log, clusters.log, instance_pools.log will be modified in-place."
    log_info "  Originals backed up as *.original"

    PYTHONPATH="${SCRIPT_DIR}:${MIGRATE_REPO_DIR}" \
        "$PYTHON3" "${SCRIPT_DIR}/import_jobs_gcp.py" \
            --session    "$SESSION_ID" \
            --export-dir "$EXPORT_DIR" \
            --config     "$GCP_CONFIG" \
            --mapping    "$NODE_MAPPING" \
            $DEBUG_FLAG && _preprocess_rc=0 || _preprocess_rc=$?
    # Exit code 2 = warnings only (non-fatal); 1 = actual error
    [[ $_preprocess_rc -le 2 ]] || { log_error "Pre-processing failed. Aborting."; exit 1; }

    log_info "Pre-processing complete. Logs are now GCP-compatible."
fi

# ── Step 1.5: Service Principal migration + staging copy ──────────────────────
if $_SKIP_STEP1_5; then
    log_warn "Step 1.5 (SP migration) skipped by --skip-step 1.5"
    # If skipped but staging doesn't exist yet, create a plain copy so later
    # steps always read from staging
    if [[ ! -d "$STAGING_SESSION_DIR" ]]; then
        log_info "Creating plain staging copy (no SP migration) …"
        mkdir -p "$STAGING_BASE"
        cp -r "$SESSION_DIR" "$STAGING_SESSION_DIR"
        log_info "Staging copy: ${STAGING_SESSION_DIR}"
    fi
else
    log_step "Step 1.5 – Service Principal Migration & Staging Copy"
    log_info "  Phase 1 : Scan exports for Service Principal references"
    log_info "  Phase 2 : Create SPs on target GCP workspace"
    log_info "  Phase 3 : Copy exports → staging, patch all SP UUIDs"
    log_info ""
    log_info "  Source   : ${SESSION_DIR}"
    log_info "  Staging  : ${STAGING_SESSION_DIR}"
    log_info "  Target   : ${WORKSPACE_URL}"

    SP_ARGS=(
        --workspace-url  "$WORKSPACE_URL"
        --token          "$PAT_TOKEN"
        --session        "$SESSION_ID"
        --export-dir     "$EXPORT_DIR"
        --staging-dir    "$STAGING_SESSION_DIR"
        --migrate-service-principals
        --no-preprocess
    )
    $DRY_RUN && SP_ARGS+=(--dry-run) || true
    $NO_SSL  && SP_ARGS+=($SSL_FLAG) || true
    $DEBUG   && SP_ARGS+=(--debug)   || true

    PYTHONPATH="${SCRIPT_DIR}:${MIGRATE_REPO_DIR}" \
        "$PYTHON3" "${SCRIPT_DIR}/import_jobs_gcp.py" "${SP_ARGS[@]}" && _sp_rc=0 || _sp_rc=$?
    [[ $_sp_rc -le 2 ]] || { log_error "SP migration failed. Aborting."; exit 1; }

    log_info "SP migration complete."
    log_info "  Mapping  : ${SESSION_DIR}/sp_mapping.json"
    log_info "  Patches  : ${SESSION_DIR}/patch_summary.json"
    log_info "  Staging  : ${STAGING_SESSION_DIR}"
fi

# From here all steps operate on the STAGING copy, not the original
ACTIVE_SESSION_DIR="$STAGING_SESSION_DIR"
ACTIVE_EXPORT_BASE="$STAGING_BASE"

# ── Step 1.6: Delete existing jobs (optional) ─────────────────────────────────
if $DELETE_JOBS; then
    log_step "Step 1.6 – Delete Existing Jobs on Target Workspace"
    log_warn "⚠  This will permanently delete ALL jobs on the target workspace!"
    log_info "  Workspace : ${WORKSPACE_URL}"
    log_info "  Dry run   : ${DRY_RUN}"

    DEL_ARGS=(
        --workspace-url "$WORKSPACE_URL"
        --token         "$PAT_TOKEN"
        --no-preprocess
        --delete-existing-jobs
    )
    $DRY_RUN     && DEL_ARGS+=(--dry-run) || true
    $FORCE_DELETE && DEL_ARGS+=(--force)  || true
    $NO_SSL      && DEL_ARGS+=($SSL_FLAG) || true
    $DEBUG       && DEL_ARGS+=(--debug)   || true

    PYTHONPATH="${SCRIPT_DIR}:${MIGRATE_REPO_DIR}" \
        "$PYTHON3" "${SCRIPT_DIR}/import_jobs_gcp.py" "${DEL_ARGS[@]}" \
        || { log_error "Job deletion step failed or was cancelled – aborting."; exit 1; }

    log_info "Job deletion step complete."
fi

# ── Step 2: Migrate tool import ───────────────────────────────────────────────
if $_SKIP_STEP2; then
    log_warn "Step 2 (Migrate tool import) skipped by --skip-step 2"
else
    log_step "Step 2 of 4 – databrickslabs/migrate Import Pipeline"
    log_info "Running migration_pipeline.py --import-pipeline …"
    log_info "  Profile      : ${CLI_PROFILE}"
    log_info "  Session      : ${SESSION_ID}"
    log_info "  Import from  : ${ACTIVE_SESSION_DIR}  (staging copy)"
    log_info ""
    log_info "  Components imported by migrate tool:"
    log_info "    Users, Groups, Workspace Items/ACLs, Notebooks, Secrets,"
    log_info "    Clusters, Instance Pools, Jobs, Hive Metastore, Table ACLs"
    if $INCLUDE_MLFLOW; then
        log_info "    MLflow Experiments + Runs (--include-mlflow)"
    fi

    MIGRATE_ARGS=(
        --profile "$CLI_PROFILE"
        --gcp
        --import-pipeline
        --use-checkpoint
        --no-prompt
        --session "$SESSION_ID"
        --set-export-dir "$ACTIVE_EXPORT_BASE"
    )
    $NO_SSL && MIGRATE_ARGS+=(--no-ssl-verification) || true
    $INCLUDE_MLFLOW && MIGRATE_ARGS+=(--include-mlflow) || true

    MIGRATE_STEP_START=$(date +%s)

    (
        cd "$MIGRATE_REPO_DIR"

        # Sync latest utility files into migrate repo
        for src_dir in workspace_export dbfs_libs stubs workspace_import; do
            if [[ -d "${SCRIPT_DIR}/${src_dir}" ]]; then
                rsync -a --delete "${SCRIPT_DIR}/${src_dir}/" "${MIGRATE_REPO_DIR}/${src_dir}/" 2>/dev/null || \
                    cp -r "${SCRIPT_DIR}/${src_dir}" "${MIGRATE_REPO_DIR}/" 2>/dev/null || true
            fi
        done

        PYTHONPATH="${MIGRATE_REPO_DIR}:${SCRIPT_DIR}/stubs" \
            "$PYTHON3" migration_pipeline.py "${MIGRATE_ARGS[@]}"
    ) || {
        MIGRATE_DUR=$(( $(date +%s) - MIGRATE_STEP_START ))
        _log_migrate_step "failed" "$MIGRATE_DUR" "migration_pipeline.py exited non-zero" "$SESSION_DIR"
        log_error "migration_pipeline.py --import-pipeline failed."
        log_warn  "Check logs in ${ACTIVE_SESSION_DIR}/app_logs/ for details."
        log_warn  "You can retry with --skip-step 1 --skip-step 1.5 --skip-step 3"
        exit 1
    }

    MIGRATE_DUR=$(( $(date +%s) - MIGRATE_STEP_START ))
    _log_migrate_step "success" "$MIGRATE_DUR" \
        "Users, Groups, Notebooks, Clusters, Jobs, ACLs, Metastore imported" "$SESSION_DIR"

    log_info "Migrate tool import complete."
fi

# ── Step 3: Extra components import ──────────────────────────────────────────
if $_SKIP_STEP3; then
    log_warn "Step 3 (Extra components import) skipped by --skip-step 3"
else
    log_step "Step 3 of 4 – Import Extra Components"
    log_info "Importing components not covered by the migrate tool:"
    log_info "  SQL Warehouses, DLT Pipelines, Git Repos,"
    log_info "  AI/BI Dashboards, Genie AI Spaces, Model Serving Endpoints,"
    log_info "  Workspace Files (.py .md .sql .yaml .toml .json .csv .txt .sh .ipynb …)"
    log_info "  Reading from: ${ACTIVE_SESSION_DIR}  (staging copy)"

    EXTRA_ARGS=(
        --workspace-url "$WORKSPACE_URL"
        --token         "$PAT_TOKEN"
        --session       "$SESSION_ID"
        --export-dir    "$ACTIVE_EXPORT_BASE"
        --import-extra
        --no-preprocess
    )
    $DRY_RUN && EXTRA_ARGS+=(--dry-run) || true
    $NO_SSL  && EXTRA_ARGS+=($SSL_FLAG) || true
    $DEBUG   && EXTRA_ARGS+=(--debug)   || true

    PYTHONPATH="${SCRIPT_DIR}:${MIGRATE_REPO_DIR}" \
        "$PYTHON3" "${SCRIPT_DIR}/import_jobs_gcp.py" \
            "${EXTRA_ARGS[@]}" \
        || log_warn "Some extra components failed to import (see output above)."

    log_info "Extra components import complete."
fi

# ── Step 4: Unity Catalog guidance ───────────────────────────────────────────
if $_SKIP_STEP4; then
    log_warn "Step 4 (Unity Catalog) skipped by --skip-step 4"
else
    log_step "Step 4 of 4 – Unity Catalog Deployment"
    UC_DIR="${SESSION_DIR}/uc_export"
    if [[ -d "$UC_DIR" ]]; then
        log_info "Unity Catalog SQL files are ready for deployment:"
        log_info "  ${UC_DIR}/"
        echo ""
        echo "  Deploy in numbered order on the target GCP workspace:"
        echo ""
        echo "  for f in ${UC_DIR}/01_*.sql ${UC_DIR}/02_*.sql ${UC_DIR}/03_*.sql \\"
        echo "           ${UC_DIR}/04_*.sql ${UC_DIR}/05_*.sql; do"
        echo "      databricks sql execute --warehouse-id <TARGET_WH_ID> --file \"\$f\""
        echo "  done"
        echo "  for f in ${UC_DIR}/tables/*.sql; do"
        echo "      databricks sql execute --warehouse-id <TARGET_WH_ID> --file \"\$f\""
        echo "  done"
        echo "  for f in ${UC_DIR}/views/*.sql; do"
        echo "      databricks sql execute --warehouse-id <TARGET_WH_ID> --file \"\$f\""
        echo "  done"
        if [[ -f "${UC_DIR}/import_manifest.json" ]]; then
            log_info "Deployment manifest: ${UC_DIR}/import_manifest.json"
        fi
        log_info "See UC_EXPORT_GUIDE.md for full deployment reference."
    else
        log_warn "No Unity Catalog export found at ${UC_DIR}"
        log_info "Run: python3 export_unity_catalog.py --workspace-url SRC_URL --token SRC_TOKEN"
    fi
fi

# ── Final summary ─────────────────────────────────────────────────────────────
END_TIME=$(date +%s)
ELAPSED=$(( END_TIME - START_TIME ))
ELAPSED_FMT=$(printf '%02d:%02d:%02d' $(( ELAPSED/3600 )) $(( (ELAPSED%3600)/60 )) $(( ELAPSED%60 )))

# ── Generate HTML import report ───────────────────────────────────────────────
log_step "Generating HTML Import Report …"
HTML_IMPORT_REPORT="${SESSION_DIR}/import_report.html"
PYTHONPATH="${SCRIPT_DIR}:${MIGRATE_REPO_DIR}" \
    "$PYTHON3" -c "
import sys
sys.path.insert(0, '${SCRIPT_DIR}')
from workspace_import.html_reporter import generate_import_html
path = generate_import_html('${SESSION_DIR}')
print(path)
" 2>/dev/null && log_info "HTML import report: ${HTML_IMPORT_REPORT}" \
             || log_warn  "HTML report generation failed (non-fatal)"

# ── Generate comparison report (export vs import) ─────────────────────────────
log_step "Generating Export vs Import Comparison Report …"
COMPARE_REPORT="${SESSION_DIR}/compare_report.html"
# Find the matching import run log
_IMPORT_LOG=$(ls -t "${EXPORT_DIR}"/import_run_*.log 2>/dev/null | head -1)
PYTHONPATH="${SCRIPT_DIR}:${MIGRATE_REPO_DIR}" \
    "$PYTHON3" -m workspace_import.compare_report \
        "${SESSION_DIR}" \
        ${_IMPORT_LOG:+--import-log "$_IMPORT_LOG"} \
        --output "${COMPARE_REPORT}" \
2>/dev/null && log_info "Comparison report:  ${COMPARE_REPORT}" \
            || { PYTHONPATH="${SCRIPT_DIR}:${MIGRATE_REPO_DIR}" "$PYTHON3" -c "
import sys; sys.path.insert(0,'${SCRIPT_DIR}')
from workspace_import.compare_report import generate_compare_report
path = generate_compare_report('${SESSION_DIR}', import_log='${_IMPORT_LOG:-}' or None, output_path='${COMPARE_REPORT}')
print(path)
" 2>/dev/null && log_info "Comparison report: ${COMPARE_REPORT}" \
             || log_warn "Comparison report generation failed (non-fatal)"; }

echo ""
echo -e "${BOLD}${GREEN}══════════════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}${GREEN}  GCP Import Complete  (${ELAPSED_FMT})${RESET}"
echo -e "${BOLD}${GREEN}══════════════════════════════════════════════════════════${RESET}"
echo ""
echo "  Session           : ${SESSION_ID}"
echo "  Original exports  : ${SESSION_DIR}"
echo "  Staging (amended) : ${STAGING_SESSION_DIR}"
echo "  Target workspace  : ${WORKSPACE_URL}"
echo ""
echo "  ── Reports ──────────────────────────────────────────────"
[[ -f "${HTML_IMPORT_REPORT}"                        ]] && echo "  HTML Report           : ${HTML_IMPORT_REPORT}"
[[ -f "${COMPARE_REPORT}"                            ]] && echo "  Comparison Report     : ${COMPARE_REPORT}"
[[ -f "${SESSION_DIR}/import_log.json"               ]] && echo "  Structured log        : ${SESSION_DIR}/import_log.json"
[[ -f "${SESSION_DIR}/sp_mapping.json"               ]] && echo "  SP mapping            : ${SESSION_DIR}/sp_mapping.json"
[[ -f "${SESSION_DIR}/patch_summary.json"            ]] && echo "  Staging patch log     : ${SESSION_DIR}/patch_summary.json"
[[ -f "${SESSION_DIR}/gcp_transform_manifest.json"   ]] && echo "  Transform manifest    : ${SESSION_DIR}/gcp_transform_manifest.json"
[[ -d "${SESSION_DIR}/gcp_ready"                     ]] && echo "  GCP-ready bundles     : ${SESSION_DIR}/gcp_ready/"
echo ""
echo "  To verify the import:"
echo "    • Open the target GCP workspace in a browser"
echo "    • Check Jobs, Clusters, Notebooks, SQL Warehouses, Pipelines"
echo ""
