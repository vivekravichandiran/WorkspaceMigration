"""
generate_design_doc.py
Creates architecture_design.docx using only Python stdlib (zipfile + xml).
Run: python3 generate_design_doc.py
"""
import zipfile
import io
import os
from datetime import datetime

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                   "architecture_design.docx")


# ─── helpers ─────────────────────────────────────────────────────────────────
def _esc(s: str) -> str:
    return (s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;")
             .replace('"', "&quot;"))


def para(text: str, style: str = "Normal", bold: bool = False,
         size_pt: int = 0, color: str = "", align: str = "",
         space_before: int = 0, space_after: int = 120) -> str:
    """Return a <w:p> element."""
    run_props = ""
    if bold:
        run_props += "<w:b/>"
    if size_pt:
        run_props += f"<w:sz w:val='{size_pt * 2}'/><w:szCs w:val='{size_pt * 2}'/>"
    if color:
        run_props += f"<w:color w:val='{color}'/>"

    para_props = f"<w:pStyle w:val='{_esc(style)}'/>"
    if align:
        para_props += f"<w:jc w:val='{align}'/>"
    para_props += (f"<w:spacing w:before='{space_before}' w:after='{space_after}' "
                   f"w:line='276' w:lineRule='auto'/>")

    rpr = f"<w:rPr>{run_props}</w:rPr>" if run_props else ""
    return (f"<w:p><w:pPr>{para_props}</w:pPr>"
            f"<w:r>{rpr}<w:t xml:space='preserve'>{_esc(text)}</w:t></w:r></w:p>")


def h1(text: str) -> str:
    return para(text, style="Heading1", bold=True, size_pt=20, color="1A2744",
                space_before=300, space_after=120)


def h2(text: str) -> str:
    return para(text, style="Heading2", bold=True, size_pt=14, color="1565C0",
                space_before=200, space_after=80)


def h3(text: str) -> str:
    return para(text, style="Heading3", bold=True, size_pt=12, color="6A1B9A",
                space_before=160, space_after=60)


def body(text: str) -> str:
    return para(text, space_after=80)


def bullet(text: str, level: int = 1) -> str:
    indent = level * 360
    return (f"<w:p><w:pPr>"
            f"<w:ind w:left='{indent}' w:hanging='360'/>"
            f"<w:spacing w:after='60'/>"
            f"</w:pPr>"
            f"<w:r><w:rPr><w:sz w:val='22'/></w:rPr>"
            f"<w:t xml:space='preserve'>• {_esc(text)}</w:t></w:r></w:p>")


def table_row(cells: list, header: bool = False) -> str:
    tc_xml = ""
    for cell in cells:
        shd = ("<w:shd w:val='clear' w:color='auto' w:fill='1A2744'/>"
               if header else
               "<w:shd w:val='clear' w:color='auto' w:fill='F5F7FA'/>")
        txt_color = "FFFFFF" if header else "1A2332"
        txt_bold  = "<w:b/>" if header else ""
        tc_xml += (f"<w:tc><w:tcPr>{shd}"
                   f"<w:tcMar><w:top w:w='80' w:type='dxa'/>"
                   f"<w:left w:w='120' w:type='dxa'/>"
                   f"<w:bottom w:w='80' w:type='dxa'/>"
                   f"<w:right w:w='120' w:type='dxa'/></w:tcMar>"
                   f"</w:tcPr>"
                   f"<w:p><w:pPr><w:spacing w:after='60'/></w:pPr>"
                   f"<w:r><w:rPr>{txt_bold}"
                   f"<w:sz w:val='20'/><w:szCs w:val='20'/>"
                   f"<w:color w:val='{txt_color}'/></w:rPr>"
                   f"<w:t xml:space='preserve'>{_esc(str(cell))}</w:t>"
                   f"</w:r></w:p></w:tc>")
    return f"<w:tr>{tc_xml}</w:tr>"


def make_table(rows: list, col_widths: list = None) -> str:
    if col_widths is None:
        col_widths = [2000] * len(rows[0]) if rows else []
    grid = "".join(f"<w:gridCol w:w='{w}'/>" for w in col_widths)
    total = sum(col_widths)
    row_xml = table_row(rows[0], header=True) if rows else ""
    for r in rows[1:]:
        row_xml += table_row(r)
    return (f"<w:tbl>"
            f"<w:tblPr>"
            f"<w:tblW w:w='{total}' w:type='dxa'/>"
            f"<w:tblBorders>"
            f"<w:top w:val='single' w:sz='4' w:color='CCCCCC'/>"
            f"<w:left w:val='single' w:sz='4' w:color='CCCCCC'/>"
            f"<w:bottom w:val='single' w:sz='4' w:color='CCCCCC'/>"
            f"<w:right w:val='single' w:sz='4' w:color='CCCCCC'/>"
            f"<w:insideH w:val='single' w:sz='4' w:color='CCCCCC'/>"
            f"<w:insideV w:val='single' w:sz='4' w:color='CCCCCC'/>"
            f"</w:tblBorders>"
            f"<w:tblLook w:val='04A0'/>"
            f"</w:tblPr>"
            f"<w:tblGrid>{grid}</w:tblGrid>"
            f"{row_xml}"
            f"</w:tbl>")


def divider() -> str:
    return ("<w:p><w:pPr>"
            "<w:pBdr><w:bottom w:val='single' w:sz='6' w:space='1' w:color='CCCCCC'/></w:pBdr>"
            "<w:spacing w:before='120' w:after='120'/>"
            "</w:pPr></w:p>")


# ─── build document body ─────────────────────────────────────────────────────
def build_document() -> str:
    now = datetime.now().strftime("%B %d, %Y")
    parts = []

    # Cover / title
    parts.append(para("DATABRICKS WORKSPACE MIGRATION TOOL",
                       bold=True, size_pt=22, color="1A2744", align="center",
                       space_before=200, space_after=60))
    parts.append(para("Architecture Design Document",
                       size_pt=14, color="1565C0", align="center",
                       space_before=0, space_after=60))
    parts.append(para(f"Azure Databricks → Google Cloud Platform  ·  {now}",
                       size_pt=11, color="546E7A", align="center",
                       space_before=0, space_after=200))
    parts.append(divider())

    # ── 1. Overview ──────────────────────────────────────────────
    parts.append(h1("1  Executive Overview"))
    parts.append(body(
        "The Databricks Workspace Migration Tool is a self-bootstrapping, "
        "end-to-end automation framework that migrates all Databricks workspace "
        "components from a Microsoft Azure deployment to a Google Cloud Platform "
        "deployment. It combines the open-source databrickslabs/migrate library "
        "with a custom Python and shell-script engine to cover 16 component types "
        "through three sequential phases: Export, Transform, and Import."
    ))
    parts.append(body(
        "The tool is designed for platform architects and data engineers who need "
        "a repeatable, auditable migration with minimal manual steps. Every run "
        "produces structured HTML reports and log files for full traceability."
    ))

    # Key stats table
    parts.append(h2("1.1  At a Glance"))
    parts.append(make_table([
        ["Attribute", "Value"],
        ["Source Platform", "Databricks on Microsoft Azure"],
        ["Target Platform", "Databricks on Google Cloud Platform"],
        ["Component Types Migrated", "16"],
        ["Phases", "Pre-Export Inventory · Export · Transform · Import · Report"],
        ["Automation Level", "~95% automated, ~5% manual (Genie Spaces, Secret Values)"],
        ["Primary Languages", "Python 3.6+, Bash / Zsh Shell"],
        ["Key Dependencies", "databrickslabs/migrate, requests, urllib3"],
        ["Authentication", "Personal Access Tokens (PAT) for source and target"],
        ["Idempotency", "Checkpointed — safe to re-run; skips already-imported items"],
        ["Report Formats", "Self-contained HTML (inventory, export, import, comparison)"],
    ], col_widths=[2800, 5200]))

    parts.append(divider())

    # ── 2. Architecture ───────────────────────────────────────────
    parts.append(h1("2  Architecture"))
    parts.append(h2("2.1  High-Level Flow"))
    parts.append(body(
        "The migration is orchestrated by two shell scripts. "
        "'export_azure.sh' drives the export side and 'import_gcp.sh' "
        "drives the transform + import side. Both scripts generate a session ID "
        "(format: YYYYMMDDHHMI) that is used as the folder name for all logs and "
        "exported artefacts."
    ))
    for step in [
        "Step 0 – Pre-Export Inventory: workspace_inventory.py calls REST APIs on "
        "the source workspace and produces inventory_pre_export.html showing all "
        "16 component counts before migration starts.",
        "Step 1 – Export: workspace_export/full_export.py exports all components "
        "to a local staging directory (logs_staging/<session>/) as JSON files, "
        "log files and binary notebook archives.",
        "Step 2 – SP Reconciliation: workspace_import/sp_migrator.py scans every "
        "staging file for Service Principal app IDs, creates the corresponding SPs "
        "on the GCP target via SCIM, and patches all references in-place.",
        "Step 3 – GCP Transform: import_jobs_gcp.py applies cloud-specific "
        "transformations: node type mapping via CSV, gcp_attributes injection, "
        "Azure field stripping, availability policy mapping, and warehouse ID resolution.",
        "Step 4 – Core Import (migrate tool): migration_pipeline.py executes the "
        "databrickslabs/migrate pipeline in dependency order: pools → policies → "
        "clusters → jobs → users → notebooks → ACLs.",
        "Step 5 – Extra Import: workspace_import/extra_importers.py imports the "
        "7 REST-only components: SQL Warehouses, DLT Pipelines, Git Repos, "
        "AI/BI Dashboards, Genie Spaces, Model Endpoints, Workspace Files.",
        "Step 6 – Reporting: export_report.html, import_report.html and "
        "comparison_report.html are generated as self-contained HTML files "
        "stored inside the session log directory.",
    ]:
        parts.append(bullet(step))

    parts.append(h2("2.2  Directory Structure"))
    parts.append(body("Each migration run creates a session directory:"))
    for line in [
        "logs_staging/<session>/            ← Primary export + staging root",
        "  export_status.json               ← Component-level export metrics",
        "  gcp_transform_manifest.json      ← Transform decisions log",
        "  inventory_pre_export.html        ← Pre-export inventory report",
        "  export_report.html               ← Post-export HTML report",
        "  import_report.html               ← Post-import HTML report",
        "  comparison_report.html           ← Side-by-side comparison",
        "  app_logs/wm_logs.log             ← Unified application log",
        "  users.log / groups.log           ← SCIM identity exports",
        "  jobs.log / clusters.log          ← Compute exports",
        "  sql_warehouses.json              ← Warehouse configs (with jdbc_url)",
        "  genie_spaces.json                ← Genie space configs (warehouse-patched)",
        "  lakeview_dashboards/             ← Dashboard JSON files",
        "  workspace_files/                 ← Non-notebook workspace files",
        "  checkpoint/                      ← Component import checkpoints",
    ]:
        parts.append(bullet(line))

    parts.append(divider())

    # ── 3. Component Matrix ───────────────────────────────────────
    parts.append(h1("3  Component Migration Matrix"))
    parts.append(make_table([
        ["Category", "Component", "Export Method", "Import Method", "Status"],
        ["Identity", "Users", "SCIM /Users", "SCIM /Users", "Automated"],
        ["Identity", "Groups", "SCIM /Groups", "SCIM /Groups", "Automated"],
        ["Identity", "Service Principals", "SCIM /ServicePrincipals", "SCIM + ID remap", "Automated"],
        ["Workspace", "Notebooks", "Workspace export API (DBC)", "databrickslabs/migrate", "Automated"],
        ["Workspace", "Workspace Files (.py, .sql, ...)", "Workspace export API (raw)", "Workspace import API", "Automated"],
        ["Workspace", "Git Repos", "Repos API", "Repos API", "Automated"],
        ["Compute", "All-Purpose Clusters", "Clusters API", "migrate + GCP transform", "Automated"],
        ["Compute", "Instance Pools", "Instance Pools API", "Pools API + ID remap", "Automated"],
        ["Compute", "Cluster Policies", "Policies API", "Policies API", "Automated"],
        ["Jobs", "Jobs & Workflows", "Jobs API", "Jobs API + pool remap", "Automated"],
        ["Jobs", "DLT Pipelines", "Pipelines API", "Pipelines API", "Automated"],
        ["SQL", "SQL Warehouses", "SQL Warehouses API", "Warehouses API", "Automated"],
        ["SQL", "AI/BI Dashboards", "Lakeview API", "Lakeview API", "Automated"],
        ["SQL", "Genie AI Spaces", "Genie API (no serialized_space)", "Warehouse ID patched; UI recreation", "Manual *"],
        ["Security", "Secret Scopes", "Secrets API (names only)", "Secrets API", "Automated"],
        ["Security", "Secret Values", "Not accessible via API", "Manual re-entry", "Manual *"],
        ["Security", "ACLs", "Permissions API", "Permissions API", "Automated"],
        ["Data", "DBFS Libraries", "DBFS REST API", "DBFS upload", "Automated"],
        ["Data", "Unity Catalog", "SQL DDL export", "SQL file apply (manual)", "Partial *"],
        ["AI/ML", "Model Serving Endpoints", "Serving API", "Serving API", "Automated"],
        ["AI/ML", "MLflow / Model Registry", "Not included", "mlflow-export-import", "External *"],
    ], col_widths=[1000, 1800, 1800, 1800, 1200]))
    parts.append(para("* See Section 6 for manual step guidance.", size_pt=9,
                       color="E65100", space_after=80))

    parts.append(divider())

    # ── 4. Key Design Decisions ───────────────────────────────────
    parts.append(h1("4  Key Design Decisions"))

    for title, detail in [
        ("4.1  Service Principal Reconciliation",
         "Service Principal App IDs are cloud-specific UUIDs that differ between Azure AD and "
         "GCP IAM. The sp_migrator.py module implements a three-step reconciliation: "
         "(1) scan all staging JSON files to collect every SP UUID and display_name; "
         "(2) call the GCP workspace SCIM API to create the SPs; "
         "(3) build an old_id → new_id map and patch every file in the staging directory. "
         "A guard prevents accidental deletion of export data when source_dir == staging_dir."),

        ("4.2  Cloud Attribute Mapping",
         "Azure clusters carry azure_attributes (availability: ON_DEMAND_AZURE, SPOT_AZURE, "
         "SPOT_WITH_FALLBACK_AZURE). GCP requires gcp_attributes (availability: ON_DEMAND_GCP, "
         "PREEMPTIBLE_GCP, SPOT_WITH_FALLBACK_GCP). The transform automatically maps these "
         "values. A gcp_import_config.json file provides the default fallback when no "
         "azure_attributes are present. AWS-specific fields are stripped entirely."),

        ("4.3  Node Type Mapping",
         "Azure VM SKUs (e.g. Standard_DS3_v2) have no GCP equivalent by the same name. "
         "A node_type_mapping.csv lookup table translates Azure types to GCP machine types "
         "(e.g. n2-highmem-4) and optionally carries a gcp_zone_hint column. The transform "
         "applies this map to every cluster spec, job cluster, and instance pool. "
         "CLASSIC warehouse type is automatically upgraded to PRO for GCP compatibility."),

        ("4.4  Warehouse ID Mapping for Genie",
         "Genie Spaces store a warehouse_id that is unique per workspace. The export tool "
         "strips the 'id' field from sql_warehouses.json, but the jdbc_url field retains "
         "the original 16-character hex warehouse ID. During import, _build_warehouse_id_map() "
         "parses jdbc_url to recover the original ID, matches warehouses by name on the GCP "
         "target, creates any missing ones, and patches genie_spaces.json with the new IDs "
         "before attempting Genie space creation."),

        ("4.5  Checkpointing and Resumability",
         "The databrickslabs/migrate CheckpointService writes a record for each successfully "
         "imported object. If the import script fails mid-run, a re-run will skip already-"
         "imported items and continue from the failure point. To force a full reimport of a "
         "component, the corresponding .log file in the checkpoint/ directory can be deleted."),

        ("4.6  Idempotent Extra Importers",
         "ExtraImporter.import_*() methods call list_existing() on the target before creating "
         "anything. If an object with the same name already exists, it is skipped with a "
         "SKIP log entry. This makes each importer safe to re-run without creating duplicates."),

        ("4.7  Self-Contained HTML Reports",
         "All HTML reports (inventory, export, import, comparison) embed all CSS and "
         "JavaScript inline with no external CDN dependencies. The Databricks logo is "
         "embedded as an inline SVG. Reports can be shared as single files and opened in "
         "any browser without an internet connection."),
    ]:
        parts.append(h2(title))
        parts.append(body(detail))

    parts.append(divider())

    # ── 5. Scripts Reference ──────────────────────────────────────
    parts.append(h1("5  Script & Module Reference"))
    parts.append(make_table([
        ["File", "Role", "Trigger"],
        ["export_azure.sh", "Export orchestrator — generates session, runs Step 0 inventory, invokes full_export.py", "Manual / CI"],
        ["import_gcp.sh", "Import orchestrator — SP reconcile → GCP transform → migrate → extra_importers → reports", "Manual / CI"],
        ["workspace_export/full_export.py", "Export engine; calls REST APIs for all 16 component types; writes export_status.json", "export_azure.sh"],
        ["workspace_export/exporters/base.py", "Base HTTP client for export API calls with retry logic", "full_export.py"],
        ["workspace_export/exporters/workspace_files.py", "Downloads all non-notebook workspace files preserving paths", "full_export.py"],
        ["workspace_import/sp_migrator.py", "SP scan + create + patch; in-place patch guard; generates sp_mapping.json", "import_gcp.sh Step 2"],
        ["import_jobs_gcp.py", "GCP-specific transform: node types, gcp_attributes, availability map, field strip", "import_gcp.sh Step 3"],
        ["workspace_import/migration_pipeline.py", "DAG for databrickslabs/migrate; enforces pool → cluster → job order", "import_gcp.sh Step 4"],
        ["workspace_import/extra_importers.py", "Warehouses, pipelines, repos, dashboards, Genie (warehouse remap), endpoints, files", "import_gcp.sh Step 5"],
        ["workspace_import/workspace_files_importer.py", "Upload workspace files from manifest; base64 encode; mkdirs", "extra_importers.py"],
        ["workspace_import/html_reporter.py", "Generates export_report.html and import_report.html with tabs, KPIs, checklist", "import_gcp.sh"],
        ["workspace_import/compare_report.py", "Side-by-side comparison report; highlights errors; generates comparison_report.html", "import_gcp.sh"],
        ["workspace_inventory.py", "Standalone inventory via REST APIs; 16 component types; paginated HTML report", "export_azure.sh Step 0"],
        ["node_type_mapping.csv", "Azure VM → GCP machine type lookup table with optional zone hints", "import_jobs_gcp.py"],
        ["gcp_import_config.json", "GCP-specific defaults: availability, fields to remove, schedule behaviour", "import_jobs_gcp.py"],
    ], col_widths=[2200, 4000, 1800]))

    parts.append(divider())

    # ── 6. Limitations ────────────────────────────────────────────
    parts.append(h1("6  Known Limitations & Manual Steps"))
    parts.append(make_table([
        ["Component", "Limitation", "Manual Action Required"],
        ["Genie AI Spaces",
         "POST /api/2.0/genie/spaces requires 'serialized_space' (internal protobuf not exposed by export APIs)",
         "Databricks UI → New → AI/BI Genie. Warehouse IDs are auto-resolved and shown in the import warnings."],
        ["Secret Values",
         "Secret values cannot be read via API (security by design)",
         "Re-insert secrets manually or via Vault/TF script after migration."],
        ["Unity Catalog Data",
         "Table DDL is exported as SQL; underlying data in object storage is not moved",
         "Run a separate data copy job (e.g. CLONE or cloud storage transfer) for table data."],
        ["MLflow / Model Registry",
         "MLflow experiments and runs are outside this tool's scope",
         "Use mlflow-export-import tool for fine-grained MLflow migration."],
        ["Git Repo Contents",
         "Only metadata (URL, branch) is migrated; actual code lives in the remote Git host",
         "Ensure the same remote Git repositories are accessible from GCP workspace."],
        ["Network / Firewall",
         "Tool runs on operator machine; both workspace URLs must be reachable",
         "Configure VPN or Direct Connect for private-endpoint workspaces before running."],
    ], col_widths=[1400, 2800, 3800]))

    parts.append(divider())

    # ── 7. Operational Guide ──────────────────────────────────────
    parts.append(h1("7  Quick-Start Operational Guide"))
    parts.append(h2("7.1  Prerequisites"))
    for item in [
        "Python 3.6+ and the 'requests' library installed on the operator machine",
        "Databricks CLI configured with profiles for source (Azure) and target (GCP)",
        "Personal Access Tokens for both workspaces with admin-level permissions",
        "Network access to both workspace URLs (HTTPS 443)",
        "node_type_mapping.csv populated with all Azure VM types in use",
        "gcp_import_config.json reviewed and tailored for the target workspace",
    ]:
        parts.append(bullet(item))

    parts.append(h2("7.2  Export Command"))
    parts.append(body("Run the following to start an export from the Azure workspace:"))
    parts.append(bullet(
        "cd /path/to/WorkspaceMigration && ./export_azure.sh "
        "--workspace-url https://<azure>.azuredatabricks.net "
        "--token <azure_pat>"
    ))

    parts.append(h2("7.3  Import Command"))
    parts.append(body("Run the following to import into the GCP workspace:"))
    parts.append(bullet(
        "./import_gcp.sh "
        "--workspace-url https://<gcp>.gcp.databricks.com "
        "--token <gcp_pat> "
        "--staging-dir logs_staging/<session_id>"
    ))

    parts.append(h2("7.4  Inventory Only"))
    parts.append(body("To generate a standalone workspace inventory report at any time:"))
    parts.append(bullet(
        "python3 workspace_inventory.py "
        "--workspace-url https://<workspace>.azuredatabricks.net "
        "--token <pat> "
        "--output my_inventory.html"
    ))

    parts.append(divider())

    # footer
    parts.append(para(
        f"Databricks Workspace Migration Tool · Architecture Design Document · {now} · "
        "For internal architecture review",
        size_pt=9, color="90A4AE", align="center", space_before=200, space_after=0
    ))

    return "\n".join(parts)


# ─── ZIP assembly ─────────────────────────────────────────────────────────────
CONTENT_TYPES = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml"  ContentType="application/xml"/>
  <Override PartName="/word/document.xml"
    ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>
  <Override PartName="/word/styles.xml"
    ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.styles+xml"/>
  <Override PartName="/word/settings.xml"
    ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.settings+xml"/>
</Types>"""

RELS = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument"
    Target="word/document.xml"/>
</Relationships>"""

WORD_RELS = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles"
    Target="styles.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/settings"
    Target="settings.xml"/>
</Relationships>"""

STYLES = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:styles xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"
          xmlns:w14="http://schemas.microsoft.com/office/word/2010/wordml"
          w:docDefaults="">
  <w:docDefaults>
    <w:rPrDefault><w:rPr>
      <w:rFonts w:ascii="Calibri" w:hAnsi="Calibri" w:cs="Calibri"/>
      <w:sz w:val="22"/><w:szCs w:val="22"/>
      <w:lang w:val="en-US"/>
    </w:rPr></w:rPrDefault>
  </w:docDefaults>
  <w:style w:type="paragraph" w:styleId="Normal" w:default="1">
    <w:name w:val="Normal"/>
    <w:pPr><w:spacing w:after="120"/></w:pPr>
  </w:style>
  <w:style w:type="paragraph" w:styleId="Heading1">
    <w:name w:val="heading 1"/>
    <w:pPr><w:spacing w:before="300" w:after="120"/>
      <w:pBdr><w:bottom w:val="single" w:sz="8" w:space="1" w:color="1565C0"/></w:pBdr>
    </w:pPr>
    <w:rPr><w:rFonts w:ascii="Calibri" w:hAnsi="Calibri"/>
      <w:b/><w:sz w:val="40"/><w:color w:val="1A2744"/>
    </w:rPr>
  </w:style>
  <w:style w:type="paragraph" w:styleId="Heading2">
    <w:name w:val="heading 2"/>
    <w:pPr><w:spacing w:before="200" w:after="80"/></w:pPr>
    <w:rPr><w:rFonts w:ascii="Calibri" w:hAnsi="Calibri"/>
      <w:b/><w:sz w:val="28"/><w:color w:val="1565C0"/>
    </w:rPr>
  </w:style>
  <w:style w:type="paragraph" w:styleId="Heading3">
    <w:name w:val="heading 3"/>
    <w:pPr><w:spacing w:before="160" w:after="60"/></w:pPr>
    <w:rPr><w:rFonts w:ascii="Calibri" w:hAnsi="Calibri"/>
      <w:b/><w:sz w:val="24"/><w:color w:val="6A1B9A"/>
    </w:rPr>
  </w:style>
</w:styles>"""

SETTINGS = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:settings xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:defaultTabStop w:val="720"/>
  <w:compat><w:compatSetting w:name="compatibilityMode" w:uri="http://schemas.microsoft.com/office/word" w:val="15"/></w:compat>
</w:settings>"""


def build_docx(body_xml: str, out_path: str) -> None:
    NS = 'xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"'
    document_xml = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:document {NS}
  xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <w:body>
    <w:sectPr>
      <w:pgSz w:w="12240" w:h="15840"/>
      <w:pgMar w:top="1080" w:right="1080" w:bottom="1080" w:left="1080"
               w:header="720" w:footer="720" w:gutter="0"/>
    </w:sectPr>
    {body_xml}
  </w:body>
</w:document>"""

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", CONTENT_TYPES)
        zf.writestr("_rels/.rels", RELS)
        zf.writestr("word/_rels/document.xml.rels", WORD_RELS)
        zf.writestr("word/styles.xml", STYLES)
        zf.writestr("word/settings.xml", SETTINGS)
        zf.writestr("word/document.xml", document_xml)
    with open(out_path, "wb") as f:
        f.write(buf.getvalue())


if __name__ == "__main__":
    body_xml = build_document()
    build_docx(body_xml, OUT)
    print(f"✓ Word document generated: {OUT}")
