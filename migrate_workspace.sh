#!/usr/bin/env bash
# =============================================================================
# migrate_workspace.sh
# =============================================================================
# Self-bootstrapping Databricks workspace export utility.
#
# This script requires NO manual setup. On first run it will:
#   1. Check system dependencies (git, python3, pip3)
#   2. Clone databrickslabs/migrate into ~/.databricks-migrate (once)
#   3. Install Python dependencies
#   4. Copy this project's utility modules into the migrate repo
#   5. Run the full workspace export with live status tracking
#   6. Generate inventory, export HTML, staging diff reports
#
# Authentication — use OAuth M2M (recommended):
#   ./migrate_workspace.sh \
#       --workspace-url https://my-ws.azuredatabricks.net \
#       --client-id     <SERVICE_PRINCIPAL_CLIENT_ID>    \
#       --client-secret <SERVICE_PRINCIPAL_CLIENT_SECRET>
#
# Authentication — PAT token (legacy / fallback):
#   ./migrate_workspace.sh \
#       --workspace-url https://my-ws.azuredatabricks.net \
#       --token dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# =============================================================================

set -euo pipefail

# ── Colour helpers ────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'
info()    { echo -e "${CYAN}[INFO]${RESET}  $*"; }
success() { echo -e "${GREEN}[OK]${RESET}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${RESET}  $*"; }
error()   { echo -e "${RED}[ERROR]${RESET} $*" >&2; }
step()    { echo -e "\n${BOLD}▶  $*${RESET}"; }
header()  { echo -e "\n${BOLD}$*${RESET}"; }

# ── Script locations ──────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Where the databrickslabs/migrate repo will be cloned / already lives.
# Override with env var: DATABRICKS_MIGRATE_DIR=/your/path ./migrate_workspace.sh ...
MIGRATE_REPO_DIR="${DATABRICKS_MIGRATE_DIR:-${HOME}/.databricks-migrate}"
MIGRATE_REPO_URL="https://github.com/databrickslabs/migrate.git"

# ── Default values ────────────────────────────────────────────────────────────
WORKSPACE_URL=""
PAT_TOKEN=""
CLIENT_ID=""
CLIENT_SECRET=""
SESSION_ID=""
EXPORT_DIR="logs"
CLOUD_FLAG=""
NUM_PARALLEL=4
NOTEBOOK_FORMAT="DBC"
SKIP_COMPONENTS=()
SKIP_FAILED=false
NO_SSL=false
INCLUDE_MLFLOW=false
RETRY_TOTAL=10
RETRY_BACKOFF=1.0
DEBUG=false
REPORT_ONLY=false
DRY_RUN=false
FORCE_REINSTALL=false   # --reinstall: re-run setup even if repo already exists

# ── Usage ─────────────────────────────────────────────────────────────────────
usage() {
cat <<EOF
${BOLD}migrate_workspace.sh${RESET} – Self-bootstrapping Databricks workspace export

${BOLD}USAGE${RESET}
  ./migrate_workspace.sh --workspace-url <URL> --client-id <ID> --client-secret <SECRET> [OPTIONS]
  ./migrate_workspace.sh --workspace-url <URL> --token <PAT> [OPTIONS]   # PAT fallback

${BOLD}REQUIRED${RESET}
  -u, --workspace-url URL    Databricks workspace URL (https://...)

${BOLD}AUTHENTICATION — OAuth M2M (recommended)${RESET}
      --client-id ID         Service Principal OAuth client ID
      --client-secret SECRET Service Principal OAuth client secret
      (The script exchanges these for a short-lived access token automatically.)

${BOLD}AUTHENTICATION — PAT token (legacy / fallback)${RESET}
  -t, --token PAT            Personal Access Token (dapi...)

${BOLD}EXPORT OPTIONS${RESET}
  -s, --session ID           Session identifier (auto-generated if omitted)
  -d, --export-dir DIR       Base export directory (default: ./logs/)
      --azure                Azure workspace
      --gcp                  GCP workspace
  -p, --num-parallel N       Download thread count (default: 4)
      --notebook-format FMT  DBC | SOURCE | HTML (default: DBC)
      --skip COMPONENT...    Space-separated components to skip:
                               instance_profiles  users  groups
                               workspace_item_log workspace_acls  notebooks
                               secrets  clusters  instance_pools  jobs
                               metastore  metastore_table_acls  dbfs_libraries
                               sql_warehouses  dlt_pipelines  repos
                               lakeview_dashboards  genie_spaces
                               serving_endpoints  unity_catalog
      --skip-failed          Skip metastore retries on failure
      --include-mlflow       Export MLflow experiments and runs
                             (skipped by default – can be very large)
      --no-ssl-verification  Disable SSL certificate verification
      --retry-total N        Total HTTP retries (default: 10)
      --retry-backoff F      Retry backoff factor (default: 1.0)
      --debug                Enable debug-level logging

${BOLD}UTILITY OPTIONS${RESET}
      --report-only          Re-generate report from existing export_status.json
      --dry-run              Print resolved config and exit without exporting
      --reinstall            Force re-clone and re-install even if already set up
      --migrate-dir DIR      Override migrate repo location (default: ~/.databricks-migrate)
  -h, --help                 Show this help

${BOLD}OUTPUTS (inside <export-dir>/<session>/)${RESET}
  export_status.json         Machine-readable live status (updated after each component)
  export_report.txt          Human-readable detailed report
  library_manifest.json      Library inventory (type, location, job/cluster usage)
  dbfs_files/                Downloaded DBFS jar/whl/egg binaries
  sql_warehouses.json        SQL warehouse configurations
  dlt_pipelines.json         DLT pipeline definitions
  repos.json                 Git repo definitions
  lakeview_dashboards/       AI/BI dashboard content (one JSON per dashboard)
  genie_spaces.json          Genie AI Space definitions
  serving_endpoints.json     Model Serving endpoint configs
  uc_export/                 Unity Catalog DDL + ACLs + import_manifest.json
  clusters.log               Cluster configuration objects
  jobs.log                   Job configuration objects
  users.log / groups/        User and group SCIM objects
  notebooks / artifacts/     Downloaded notebook content
  metastore/                 Hive metastore DDL
  ... (one log file per component)

EOF
}

# ── Argument parsing ──────────────────────────────────────────────────────────
# Allow first two positional args to be URL and token
if [[ $# -ge 2 && "$1" != -* && "$2" != -* ]]; then
    WORKSPACE_URL="$1"
    PAT_TOKEN="$2"
    shift 2
fi

while [[ $# -gt 0 ]]; do
    case "$1" in
        -u|--workspace-url)    WORKSPACE_URL="$2";             shift 2 ;;
        -t|--token)            PAT_TOKEN="$2";                  shift 2 ;;
        --client-id)           CLIENT_ID="$2";                  shift 2 ;;
        --client-secret)       CLIENT_SECRET="$2";              shift 2 ;;
        -s|--session)          SESSION_ID="$2";                 shift 2 ;;
        -d|--export-dir)       EXPORT_DIR="$2";                 shift 2 ;;
        --azure)               CLOUD_FLAG="--azure";            shift   ;;
        --gcp)                 CLOUD_FLAG="--gcp";              shift   ;;
        -p|--num-parallel)     NUM_PARALLEL="$2";               shift 2 ;;
        --notebook-format)     NOTEBOOK_FORMAT="$2";            shift 2 ;;
        --skip)
            shift
            while [[ $# -gt 0 && "$1" != -* ]]; do
                SKIP_COMPONENTS+=("$1"); shift
            done ;;
        --skip-failed)         SKIP_FAILED=true;                shift   ;;
        --include-mlflow)      INCLUDE_MLFLOW=true;             shift   ;;
        --no-ssl-verification) NO_SSL=true;                     shift   ;;
        --retry-total)         RETRY_TOTAL="$2";                shift 2 ;;
        --retry-backoff)       RETRY_BACKOFF="$2";              shift 2 ;;
        --debug)               DEBUG=true;                      shift   ;;
        --report-only)         REPORT_ONLY=true;                shift   ;;
        --dry-run)             DRY_RUN=true;                    shift   ;;
        --reinstall)           FORCE_REINSTALL=true;            shift   ;;
        --migrate-dir)         MIGRATE_REPO_DIR="$2";           shift 2 ;;
        -h|--help)             usage; exit 0 ;;
        *) error "Unknown argument: $1"; echo ""; usage; exit 1 ;;
    esac
done

# ── OAuth token exchange ───────────────────────────────────────────────────────
fetch_oauth_token() {
    info "Fetching OAuth token for client_id: ${CLIENT_ID} …"
    local token_url="${WORKSPACE_URL}/oidc/v1/token"
    local ssl_flag=""
    $NO_SSL && ssl_flag="-k"
    local response
    response=$(curl -s $ssl_flag -X POST "$token_url" \
        -H "Content-Type: application/x-www-form-urlencoded" \
        -d "grant_type=client_credentials&client_id=${CLIENT_ID}&client_secret=${CLIENT_SECRET}&scope=all-apis")
    local tok
    tok=$(echo "$response" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('access_token',''))" 2>/dev/null || true)
    if [[ -z "$tok" ]]; then
        error "OAuth token exchange failed. Response: ${response}"
        exit 1
    fi
    PAT_TOKEN="$tok"
    success "OAuth token obtained successfully (expires in 3600s)"
}

# ── Validation ────────────────────────────────────────────────────────────────
validate_args() {
    local errs=0
    [[ -z "$WORKSPACE_URL" ]] && { error "--workspace-url is required";          (( errs++ )) || true; }
    [[ "$WORKSPACE_URL" != https://* ]] && { error "--workspace-url must start with https://"; (( errs++ )) || true; }

    # Auth: require either PAT token OR both client-id + client-secret
    local has_pat=false; local has_oauth=false
    [[ -n "$PAT_TOKEN"     ]] && has_pat=true
    [[ -n "$CLIENT_ID" && -n "$CLIENT_SECRET" ]] && has_oauth=true

    if ! $has_pat && ! $has_oauth; then
        error "Authentication required: provide --client-id + --client-secret (OAuth M2M, recommended) or --token (PAT)"
        (( errs++ )) || true
    fi
    if [[ -n "$CLIENT_ID" && -z "$CLIENT_SECRET" ]]; then
        error "--client-secret is required when --client-id is provided"
        (( errs++ )) || true
    fi
    if [[ -z "$CLIENT_ID" && -n "$CLIENT_SECRET" ]]; then
        error "--client-id is required when --client-secret is provided"
        (( errs++ )) || true
    fi
    if [[ $errs -gt 0 ]]; then echo ""; usage; exit 1; fi
}

$REPORT_ONLY || validate_args

# Exchange client credentials for a PAT-equivalent OAuth token
if [[ -z "$PAT_TOKEN" && -n "$CLIENT_ID" ]]; then
    fetch_oauth_token
fi

# ── System dependency check ───────────────────────────────────────────────────
check_system_deps() {
    step "Checking system dependencies"
    local missing=0
    for cmd in git python3; do
        if command -v "$cmd" &>/dev/null; then
            success "$cmd  →  $(command -v "$cmd")"
        else
            error "$cmd not found. Please install it and retry."
            (( missing++ )) || true
        fi
    done

    # pip3 / pip – try both
    if command -v pip3 &>/dev/null; then
        PIP_CMD="pip3"
    elif python3 -m pip --version &>/dev/null 2>&1; then
        PIP_CMD="python3 -m pip"
    else
        warn "pip not found – will try 'python3 setup.py install' instead"
        PIP_CMD=""
    fi

    if [[ $missing -gt 0 ]]; then exit 1; fi
    info "Python version: $(python3 --version 2>&1)"
    info "Git version   : $(git --version)"
}

check_system_deps

# ── Clone / update the migrate repo ──────────────────────────────────────────
setup_migrate_repo() {
    step "Setting up databrickslabs/migrate"

    if [[ -d "$MIGRATE_REPO_DIR/.git" ]] && ! $FORCE_REINSTALL; then
        info "Migrate repo already present at: $MIGRATE_REPO_DIR"
        info "Pulling latest changes …"
        git -C "$MIGRATE_REPO_DIR" pull --ff-only --quiet \
            && success "Repo updated" \
            || warn    "Could not pull – continuing with existing version"
    else
        if [[ -d "$MIGRATE_REPO_DIR" ]] && $FORCE_REINSTALL; then
            info "Removing existing repo for reinstall …"
            rm -rf "$MIGRATE_REPO_DIR"
        fi
        info "Cloning $MIGRATE_REPO_URL → $MIGRATE_REPO_DIR"
        git clone --depth 1 "$MIGRATE_REPO_URL" "$MIGRATE_REPO_DIR"
        success "Repo cloned"
    fi
}

setup_migrate_repo

# ── Install Python dependencies ───────────────────────────────────────────────
install_python_deps() {
    step "Installing Python dependencies"

    # Prefer requirements file if it exists
    REQ_FILE="$MIGRATE_REPO_DIR/requirements.txt"
    SETUP_FILE="$MIGRATE_REPO_DIR/setup.py"

    if [[ -n "$PIP_CMD" && -f "$REQ_FILE" ]]; then
        info "Running: $PIP_CMD install -r $REQ_FILE --quiet"
        $PIP_CMD install -r "$REQ_FILE" --quiet \
            && success "Requirements installed" \
            || warn    "pip install had warnings – continuing"
    fi

    if [[ -f "$SETUP_FILE" ]]; then
        info "Running: python3 setup.py install (in $MIGRATE_REPO_DIR)"
        (cd "$MIGRATE_REPO_DIR" && python3 setup.py install --quiet 2>/dev/null) \
            && success "setup.py install complete" \
            || warn    "setup.py install had warnings – continuing"
    fi

    # Verify the core import works
    if PYTHONPATH="$MIGRATE_REPO_DIR/stubs:$MIGRATE_REPO_DIR" python3 -c "import dbclient" &>/dev/null 2>&1; then
        success "dbclient importable  ✓"
    else
        warn "dbclient not yet importable via PYTHONPATH – will use sys.path injection at runtime"
    fi
}

install_python_deps

# ── Copy utility modules into the migrate repo ────────────────────────────────
sync_utility_files() {
    step "Syncing utility modules into migrate repo"

    # Modules to copy from this project's directory into the migrate repo root
    local items=(
        "workspace_export"
        "dbfs_libs"
        "export_dbfs_libs.py"
        "import_dbfs_libs.py"
        "stubs"
    )

    for item in "${items[@]}"; do
        src="$SCRIPT_DIR/$item"
        dst="$MIGRATE_REPO_DIR/$item"
        if [[ -e "$src" ]]; then
            if [[ -d "$src" ]]; then
                cp -r "$src" "$dst"
            else
                cp "$src" "$dst"
            fi
            success "Copied  $item"
        else
            warn "Source not found, skipping: $src"
        fi
    done
}

sync_utility_files

# ── Generate session ID ───────────────────────────────────────────────────────
[[ -z "$SESSION_ID" ]] && SESSION_ID="M$(date +%Y%m%d%H%M)"

# Resolve export dir relative to where the USER invoked the script, not the repo
if [[ "$EXPORT_DIR" != /* ]]; then
    EXPORT_DIR="$(pwd)/$EXPORT_DIR"
fi

SESSION_EXPORT_DIR="${EXPORT_DIR}/${SESSION_ID}"
STATUS_FILE="${SESSION_EXPORT_DIR}/export_status.json"
REPORT_FILE="${SESSION_EXPORT_DIR}/export_report.txt"

mkdir -p "$SESSION_EXPORT_DIR"

# ── Banner ────────────────────────────────────────────────────────────────────
header "═══════════════════════════════════════════════════════════════════════"
header " Databricks Workspace Export"
header "═══════════════════════════════════════════════════════════════════════"
info  "Workspace URL : $WORKSPACE_URL"
info  "Session ID    : $SESSION_ID"
info  "Export dir    : $SESSION_EXPORT_DIR"
info  "Migrate repo  : $MIGRATE_REPO_DIR"
info  "Cloud         : ${CLOUD_FLAG:---azure (default)}"
info  "Parallelism   : $NUM_PARALLEL"
info  "Format        : $NOTEBOOK_FORMAT"
[[ ${#SKIP_COMPONENTS[@]} -gt 0 ]] && info "Skipping      : ${SKIP_COMPONENTS[*]}" || true
if $DEBUG;           then info "Debug         : enabled"; fi
if $NO_SSL;          then warn "SSL verify    : DISABLED"; fi
if $FORCE_REINSTALL; then info "Reinstall     : forced"; fi
if $DRY_RUN;         then warn "Mode          : DRY RUN – no data will be exported"; fi
echo ""

# ── Dry-run early exit ────────────────────────────────────────────────────────
if $DRY_RUN; then
    success "Dry run complete. Configuration looks good."
    info    "Run without --dry-run to start the export."
    exit 0
fi

# ── Report-only mode ──────────────────────────────────────────────────────────
if $REPORT_ONLY; then
    [[ ! -f "$STATUS_FILE" ]] && {
        error "Status file not found: $STATUS_FILE"
        error "Run the export first, or check --session and --export-dir."
        exit 1
    }
    step "Regenerating report from saved status"
    PYTHONPATH="$MIGRATE_REPO_DIR/stubs:$MIGRATE_REPO_DIR" python3 - <<PYEOF
import sys, os
sys.path.insert(0, '$MIGRATE_REPO_DIR')
from workspace_export.report_generator import report_from_file
report = report_from_file('$STATUS_FILE')
print(report)
with open('$REPORT_FILE', 'w', encoding='utf-8') as fp:
    fp.write(report)
print(f"Report saved to: $REPORT_FILE")
PYEOF
    exit 0
fi

# ── Build Python args ─────────────────────────────────────────────────────────
PYTHON_ARGS=(
    --workspace-url  "$WORKSPACE_URL"
    --token          "$PAT_TOKEN"
    --session        "$SESSION_ID"
    --export-dir     "$EXPORT_DIR"
    --num-parallel   "$NUM_PARALLEL"
    --notebook-format "$NOTEBOOK_FORMAT"
    --retry-total    "$RETRY_TOTAL"
    --retry-backoff  "$RETRY_BACKOFF"
)
[[ -n "$CLOUD_FLAG"             ]] && PYTHON_ARGS+=("$CLOUD_FLAG")           || true
if $SKIP_FAILED;    then PYTHON_ARGS+=(--skip-failed); fi
if $INCLUDE_MLFLOW; then PYTHON_ARGS+=(--include-mlflow); fi
if $NO_SSL;         then PYTHON_ARGS+=(--no-ssl-verification); fi
if $DEBUG;       then PYTHON_ARGS+=(--debug); fi
[[ ${#SKIP_COMPONENTS[@]} -gt 0 ]] && PYTHON_ARGS+=(--skip "${SKIP_COMPONENTS[@]}") || true

# ── Step 0: Pre-export workspace inventory ────────────────────────────────────
header "Step 0 of 3 – Pre-Export Workspace Inventory"
info  "Snapshotting all workspace components before export …"
INVENTORY_HTML="${SESSION_EXPORT_DIR}/inventory_pre_export.html"
INVENTORY_XLSX="${SESSION_EXPORT_DIR}/inventory_pre_export.xlsx"
INVENTORY_ARGS=(
    --workspace-url "$WORKSPACE_URL"
    --token         "$PAT_TOKEN"
    --output        "$INVENTORY_HTML"
    --excel-output  "$INVENTORY_XLSX"
)
# Note: PAT_TOKEN is already populated (either directly or via OAuth exchange)
$NO_SSL && INVENTORY_ARGS+=(--no-ssl-verification)
if python3 "${SCRIPT_DIR}/workspace_inventory.py" "${INVENTORY_ARGS[@]}"; then
    success "Inventory complete → ${INVENTORY_HTML}"
else
    warn "Inventory step failed (non-fatal) – export will continue."
fi
echo ""

# ── Run export ────────────────────────────────────────────────────────────────
header "Step 1 of 3 – Export"
STARTED_AT="$(date '+%Y-%m-%d %H:%M:%S')"
info "Started at: $STARTED_AT"
echo ""

EXPORT_EXIT=0
(
    cd "$MIGRATE_REPO_DIR"
    PYTHONPATH="$MIGRATE_REPO_DIR/stubs:$MIGRATE_REPO_DIR" \
        python3 -m workspace_export.full_export "${PYTHON_ARGS[@]}"
) || EXPORT_EXIT=$?

ENDED_AT="$(date '+%Y-%m-%d %H:%M:%S')"
echo ""

# ── Final banner ──────────────────────────────────────────────────────────────
header "═══════════════════════════════════════════════════════════════════════"
if [[ $EXPORT_EXIT -eq 0 ]]; then
    success "Export completed successfully."
else
    error   "Export completed with one or more failures (exit code: $EXPORT_EXIT)."
    error   "See $REPORT_FILE for details."
fi
info "Started   : $STARTED_AT"
info "Ended     : $ENDED_AT"
info "Status    : $STATUS_FILE"
info "Report    : $REPORT_FILE"
header "═══════════════════════════════════════════════════════════════════════"

# ── Inline quick-summary table ────────────────────────────────────────────────
if [[ -f "$STATUS_FILE" ]]; then
    echo ""
    header "Component Summary"
    PYTHONPATH="$MIGRATE_REPO_DIR/stubs:$MIGRATE_REPO_DIR" python3 - <<PYEOF
import sys, json, os
sys.path.insert(0, '$MIGRATE_REPO_DIR')
try:
    with open('$STATUS_FILE') as fp:
        data = json.load(fp)
    comps = list(data.get('components', {}).values())
    icons = {'success':'✓','failed':'✗','skipped':'⊘','pending':'○','in_progress':'⟳'}
    W = 68
    print("  " + "─" * W)
    print(f"  {'Component':<26} {'Status':<14} {'Items':>8}  {'Duration':>8}  Notes")
    print("  " + "─" * W)
    for c in comps:
        icon    = icons.get(c.get('status',''), '?')
        status  = f"{icon} {c.get('status','').upper()}"
        items   = f"{c.get('items_exported') or 0:,}" if c.get('items_exported') else '–'
        dur     = c.get('duration_seconds')
        if dur:
            m, s = divmod(int(dur), 60)
            dur_str = f"{m}m {s}s" if m else f"{s}s"
        else:
            dur_str = '–'
        note = ''
        if c.get('error_message'):
            note = c['error_message'][:28] + ('…' if len(c.get('error_message','')) > 28 else '')
        elif c.get('bytes_downloaded'):
            nb = c['bytes_downloaded']
            for unit in ('B','KB','MB','GB'):
                if nb < 1024: break
                nb /= 1024
            note = f"{nb:.1f} {unit} downloaded"
        elif c.get('notes'):
            note = c['notes'][:32]
        print(f"  {c.get('display_name',''):<26} {status:<14} {items:>8}  {dur_str:>8}  {note}")
    print("  " + "─" * W)
    ok  = sum(1 for c in comps if c.get('status') == 'success')
    err = sum(1 for c in comps if c.get('status') == 'failed')
    skp = sum(1 for c in comps if c.get('status') == 'skipped')
    tot = sum(c.get('items_exported') or 0 for c in comps)
    print(f"  ✓ {ok} succeeded  ✗ {err} failed  ⊘ {skp} skipped   Total items: {tot:,}")
    print("  " + "─" * W)
except Exception as e:
    print(f"  (could not parse status file: {e})")
PYEOF
fi

echo ""
info "To regenerate this report later:"
info "  ./migrate_workspace.sh --workspace-url $WORKSPACE_URL --client-id <ID> --client-secret <SECRET> --session $SESSION_ID --report-only"
echo ""

# ── Step 2 of 3: Auto-staging ─────────────────────────────────────────────────
# Copy the raw export → <session>_staging/, apply all transforms from
# gcp_import_config.json:
#   • GCP cluster rewrite (node types, gcp_attributes, etc.)
#   • User domain remapping  (user_domain_mapping)
#   • User ID mapping        (user_id_mapping)
#   • Exclude filters        (workspace_excludes patterns)
# The original export directory is left untouched so you can diff raw vs staged.
# ──────────────────────────────────────────────────────────────────────────────
STAGING_DIR="${SESSION_EXPORT_DIR}_staging"
header "Step 2 of 3 – Build Staging (review before import)"
info  "Raw export  : ${SESSION_EXPORT_DIR}"
info  "Staging dir : ${STAGING_DIR}"
echo  ""

STAGING_EXIT=0
python3 "${SCRIPT_DIR}/import_jobs_gcp.py" \
    --session-dir   "${SESSION_EXPORT_DIR}" \
    --staging-dir   "${STAGING_DIR}" \
    --config        "${SCRIPT_DIR}/gcp_import_config.json" \
    --build-staging \
    || STAGING_EXIT=$?

if [[ $STAGING_EXIT -eq 0 ]]; then
    success "Staging complete → ${STAGING_DIR}"
    info    "Review the staged files, then run the import pointing to the staging dir."
else
    warn    "Staging step failed (exit ${STAGING_EXIT}) – you can rebuild manually:"
    warn    "  python3 import_jobs_gcp.py --session-dir ${SESSION_EXPORT_DIR} --build-staging"
fi
echo ""

# ── Step 3 of 3: Staging diff report (pre/post change review) ─────────────────
# Generates a full HTML + Excel diff of every transformation applied during
# staging: user remapping, node-type mapping, GCP attribute injection,
# spark-config rewrites, job transforms, ACL changes, etc.
# ──────────────────────────────────────────────────────────────────────────────
STAGING_DIFF_HTML="${SESSION_EXPORT_DIR}/staging_diff_${SESSION_ID}.html"
STAGING_DIFF_XLSX="${SESSION_EXPORT_DIR}/staging_diff_${SESSION_ID}.xlsx"
if [[ -d "${SESSION_EXPORT_DIR}" && -d "${STAGING_DIR}" ]]; then
    header "Step 3 of 3 – Staging Diff Report (pre/post changes)"
    info  "Comparing raw export vs staged files …"
    info  "  Raw   : ${SESSION_EXPORT_DIR}"
    info  "  Stage : ${STAGING_DIR}"
    DIFF_EXIT=0
    python3 "${SCRIPT_DIR}/staging_diff_report.py" \
        --raw-dir     "${SESSION_EXPORT_DIR}" \
        --stage-dir   "${STAGING_DIR}" \
        --html-output "${STAGING_DIFF_HTML}" \
        --excel-output "${STAGING_DIFF_XLSX}" \
        || DIFF_EXIT=$?
    if [[ $DIFF_EXIT -eq 0 ]]; then
        success "Staging diff report generated."
    else
        warn "Staging diff report failed (non-fatal)."
        STAGING_DIFF_HTML=""
        STAGING_DIFF_XLSX=""
    fi
    echo ""
fi
info "Generating HTML export report …"
HTML_EXPORT_REPORT="${SESSION_EXPORT_DIR}/export_report.html"
PYTHONPATH="${MIGRATE_REPO_DIR}:${SCRIPT_DIR}/stubs" \
    python3 -c "
import sys
sys.path.insert(0, '${MIGRATE_REPO_DIR}')
sys.path.insert(0, '${SCRIPT_DIR}')
from workspace_import.html_reporter import generate_export_html
path = generate_export_html('${SESSION_EXPORT_DIR}')
print(path)
" && info "HTML report: ${HTML_EXPORT_REPORT}" \
  || warn "HTML report generation failed (non-fatal)"

echo ""
echo "  ── Output files ─────────────────────────────────────────"
if [[ -f "${INVENTORY_HTML}" ]]; then
    echo "  🗂️  Inventory HTML   : ${INVENTORY_HTML}"
fi
if [[ -f "${INVENTORY_XLSX}" ]]; then
    echo "  🗂️  Inventory Excel  : ${INVENTORY_XLSX}"
fi
echo "  📋 Export text     : ${REPORT_FILE}"
if [[ -f "${HTML_EXPORT_REPORT}" ]]; then
    echo "  📊 Export HTML     : ${HTML_EXPORT_REPORT}"
fi
if [[ -d "${STAGING_DIR}" ]]; then
    echo "  📦 Staged files    : ${STAGING_DIR}   ← review here before import"
fi
if [[ -f "${STAGING_DIFF_HTML:-}" ]]; then
    echo "  🔍 Staging diff HTML  : ${STAGING_DIFF_HTML}"
fi
if [[ -f "${STAGING_DIFF_XLSX:-}" ]]; then
    echo "  🔍 Staging diff Excel : ${STAGING_DIFF_XLSX}"
fi
echo ""
echo "  ── Next step ────────────────────────────────────────────"
echo "  Review staging dir and staging diff report, then run:"
echo "  ./import_gcp.sh --workspace-url <TARGET_URL> --client-id <ID> --client-secret <SECRET> --session ${SESSION_ID}"
echo ""
exit $EXPORT_EXIT
