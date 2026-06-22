# Metadata Governance Platform

> Intelligent Data Validation, Compliance Monitoring & AI-Powered Governance on Databricks  
> **Project Code:** Metadatata Governance Platform · **Organization:** Genpact Kosovo · **Demo:** June, 2026

---

## Overview

The Metadata Governance Platform is a production-ready data governance solution built entirely on Databricks. It ingests a column-grain metadata CSV from Azure Blob Storage (ADLS Gen2), processes it through a fully automated Bronze → Silver → Gold medallion pipeline, scores every column against a versioned 11-check certification rubric, and surfaces the results through three consumption interfaces: a Genie AI chatbot, an AI/BI Dashboard, and a self-service Databricks App.

The platform is designed around one architectural principle: **all governance logic is pre-computed in the Gold layer**. Genie, the dashboard, and the app are read-only consumers — they never run ad-hoc checks against raw data.

This makes the system fast, consistent, testable, and free of hallucination risk for compliance-critical answers.

---

## Architecture

```
Azure Blob Storage (ADLS Gen2)
         CSV metadata file (column-grain, one row per column)
                    ↓
         Bronze Layer (DLT)
         Raw ingestion · all 26 columns as STRING
         Audit columns: _ingest_file, _ingest_ts, _batch_id
         No filtering, no casting, no deduplication
                    ↓
         Silver Layer (DLT)
         Type casting · normalisation · deduplication
         11 boolean DQ checks · quarantine for hard failures
                    ↓
         Gold Layer (DLT)
         Governance scoring · KPI aggregation · gap detection
         Structural consistency · access control subsystem
                    ↓
    ┌────────────────────────────────────┐
    │         Consumption Layer          │
    │  Genie AI  │  AI/BI Dashboard  │  App  │
    └────────────────────────────────────┘
```

### Architectural Principles

- **Pre-compute everything in Gold.** Genie and the dashboard read results — they never compute live. This guarantees consistency, speed, and testability.
- **Single Lakeflow Declarative Pipeline.** All medallion logic (Bronze, Silver, Gold) lives in one DAG — required by Databricks Free Edition's one-active-pipeline-per-type constraint, and also the correct pattern for tightly coupled medallion layers.
- **Source decoupled from compute.** The CSV lives in Azure ADLS Gen2, read over `abfss://` with the account key held in a Databricks secret scope. Never in the repository.
- **Environments via Git branches.** One workspace, one Unity Catalog. Isolation is handled by the branch model (`feature/* → dev → main`), not by duplicating infrastructure.
- **Versioned governance logic.** The certification rubric lives in `config/rubric.yaml`, versioned in GitHub, deployed via CI/CD. A change to a check threshold is a pull request — not a notebook edit.

---

## Pipeline Detail

### Bronze — `dbx_metadata_governance_dev.bronze.metadata_raw`

Raw, immutable landing zone. Reads the metadata CSV directly from Azure ADLS Gen2 via `abfss://`, ingests all 26 columns as `STRING` with no casting, no filtering, and no deduplication. Adds three audit columns (`_ingest_file`, `_ingest_ts`, `_batch_id`). Nothing is rejected at this layer — Bronze is the replayable source of truth.

### Silver — `dbx_metadata_governance_dev.silver.metadata_validated`

The validation and quality engine. One clean row per `column_id`. Processing steps:

1. **Type casting** — flags to `BOOLEAN`, counts to `BIGINT`, text to `STRING`
2. **Normalisation** — trim whitespace, empty strings to `NULL` (critical: blanks must become `NULL` for governance checks to fire correctly), standardise categoricals
3. **Deduplication** — latest row per `column_id` by `_ingest_ts`
4. **Hard expectations** — `column_id`, `column_name`, and `table_id` must be non-null; failures routed to quarantine table
5. **DQ checks** — all 11 per-row check columns computed as `BOOLEAN`

### Gold — `dbx_metadata_governance_dev.gold.*`

Scoring, aggregation, and business-ready output. All governance intelligence lives here.

| Table                        | Description                                                                   |
| ---------------------------- | ----------------------------------------------------------------------------- |
| `scores_pre`                 | Per-column governance scores: documentation, quality, classification          |
| `table_governance`           | Rollup per table: avg score, column count, PII count                          |
| `table_structure`            | Database / schema / table / column structure                                  |
| `profile`                    | Column-level metadata profile: PII, certification, steward                    |
| `dlt_summary`                | Pipeline DQ pass/fail statistics                                              |
| `kpi_summary`                | Executive KPIs: total tables, columns, avg scores, certification distribution |
| `cert_level_summary`         | Certification distribution per table                                          |
| `pii_classification_summary` | PII and sensitivity coverage rates                                            |
| `domain_summary`             | Governance scores grouped by business domain                                  |
| `structural_consistency`     | Per-table column count vs standard (±30% tolerance)                           |
| `structural_summary`         | Catalog-level structural consistency KPIs                                     |
| `governance_gaps`            | Columns failing governance checks with gap reasons                            |
| `region_assignment`          | Explicit data residency policy per database (EU / US / UNRESTRICTED)          |
| `user_profile`               | User access group assignments by region                                       |
| `asset_access_policy`        | Per-table access policy inherited from region assignment                      |
| `asset_access_check`         | Resolved Eligible / Not Eligible decision per user × table                    |

---

## Governance Scoring

Every column is scored against three metrics:

```
documentation_score  = completeness_pct                    (50% weight)
quality_score        = 100 - dq_invalid_pct                (30% weight)
classification_score = 100 if security_classification else 0  (20% weight)

overall_governance_score = (documentation × 0.50)
                         + (quality        × 0.30)
                         + (classification × 0.20)
```

---

## Certification Rubric

Every column is scored against 11 boolean checks grouped into three ascending certification levels:

| Level          | Criteria                                                                                             | Business Meaning                                              |
| -------------- | ---------------------------------------------------------------------------------------------------- | ------------------------------------------------------------- |
| **Registered** | `column_name`, `table_name`, `system_name`, `database_name` present + `security_classification`      | Asset is known and classified. Baseline compliance.           |
| **Documented** | Registered + `column_desc` + `table_desc` + `term_name` + `data_steward`                             | Asset has human-readable context and an accountable owner.    |
| **Certified**  | Documented + `term_description` + `tag` + `subdomain` + `pii_consistency` + `dq_pass` (<10% invalid) | Fully governed. PII-safe. Quality-verified. Production-ready. |

Metadata completeness is expressed as `(checks passed / 11)`, mapped to maturity tiers:

- **High** ≥ 90%
- **Medium** 50–89%
- **Low** < 50%

### Governance Gap Checks (9 checks in `governance_gaps`)

| Check                                                | Severity |
| ---------------------------------------------------- | -------- |
| Missing data steward                                 | Critical |
| PII or CDE flag is null                              | Critical |
| Missing or invalid security classification           | Critical |
| Data quality failed: invalid record ratio exceeds 5% | High     |
| Missing or invalid security classification           | High     |
| Missing domain tag                                   | Medium   |
| Missing column description                           | Medium   |
| Missing business term                                | Medium   |
| Invalid schema assignment                            | Medium   |

---

## App Views

| View                | Data Source                                    | Description                                                                               |
| ------------------- | ---------------------------------------------- | ----------------------------------------------------------------------------------------- |
| **Overview**        | `gold.kpi_summary` · `gold.domain_summary`     | Executive KPI cards, domain governance bars, certification distribution                   |
| **Pipeline**        | `gold.dlt_summary` · `gold.structural_summary` | Live Bronze → Silver → Gold pipeline status with DQ warnings                              |
| **Asset Explorer**  | `gold.table_governance`                        | All tables ordered by governance score with steward and PII info                          |
| **Governance Gaps** | `gold.governance_gaps`                         | Open issues by severity: Critical / High / Medium                                         |
| **Genie Chat**      | Genie Conversation API · `dg_poc_genie` space  | Natural-language governance Q&A grounded on Gold tables                                   |
| **Alert Rules**     | `gold.kpi_summary` · `gold.structural_summary` | Live alert status: DQ below 95%, completeness below 90%, structural consistency below 85% |
| **Monitoring**      | In-session `monitoring-store`                  | Genie response quality stats, 👍/👎 feedback log, prompt version tracking                 |

---

## Genie AI Chatbot

The Genie space (`dg_poc_genie`) answers governance questions in plain English — no SQL required, no custom model training, no GPU.

**Example questions answered out of the box:**

- Do all tables follow the defined structural standard?
- What is the certification level of `[table]`?
- What does `[table]` need to reach Certified level?
- Which tables have unclassified PII columns?
- Which tables are missing a data steward?
- What are the top governance gaps across the `[domain]` domain?
- Does `[user]` have access to `[table]`?

The chatbot is grounded via pre-computed Gold tables — it reads results, never computes live. This eliminates hallucination risk for compliance-critical queries.

---

## Structural Consistency (Objective 1)

The platform automatically detects tables that deviate from the catalog's structural standard:

```python
STRUCTURAL_TOLERANCE = 0.30  # ±30% band around modal column count

# Standard = modal column count across all tables (most common value)
# column_count < 70% of standard → "Outlier: too few columns"
# column_count > 130% of standard → "Outlier: too many columns"
# Within band → "Compliant"
```

Results are available in `gold.structural_consistency` (per table) and `gold.structural_summary` (catalog-level KPIs).

---

## Access Control Subsystem

Region-based access control determines eligibility per user × table:

```
region_assignment  (EU / US / UNRESTRICTED per database)
        +
user_profile       (user region + access group)
        ↓
asset_access_policy  (policy per table)
        ↓
asset_access_check   → "Eligible" or "Not Eligible" + reason
```

Access decisions are pre-computed in Gold and queryable via Genie.

---

## Tech Stack

| Layer                  | Technology                                                |
| ---------------------- | --------------------------------------------------------- |
| Source storage         | Azure Blob Storage (ADLS Gen2) · `abfss://`               |
| Compute platform       | Databricks (Free Edition)                                 |
| Pipeline orchestration | Delta Live Tables (DLT) · Declarative Lakeflow            |
| Catalog & governance   | Unity Catalog · `dbx_metadata_governance_dev`             |
| Job scheduling         | Databricks Jobs · `Metadata_Medallion`                    |
| AI chatbot             | Databricks Genie Conversation API                         |
| App framework          | Dash (Python) · `databricks-sql-connector`                |
| Authentication         | OAuth2 · Service Principal (`app-3q3vbl metadata-gov-ui`) |
| App hosting            | Databricks Apps (Serverless)                              |
| CI/CD                  | GitHub Actions + Databricks Asset Bundles (DABs)          |
| Secret management      | Databricks Secret Scope · GitHub Secrets                  |
| Version control        | GitHub · `gegedobruna/data-capstone`                      |

---

## Repository Structure

```
data-capstone/
├── app/
│   ├── app.py              # Dash app — all callbacks and views
│   ├── layout.py           # App layout, stores, topbar, nav
│   ├── data.py             # Gold table queries via SP OAuth
│   ├── genie_client.py     # Genie Conversation API client
│   └── alerts.py           # Alert engine
├── ingestion/              # Bronze DLT pipeline
├── transformation/         # Silver + Gold DLT pipelines
├── validation/             # Gold layer validation
├── chatbot/                # Genie space configuration and trusted SQL
├── dashboard/              # Lakeview AI/BI dashboard files
├── src/                    # Bundle-managed resources (.lvdash.json, .geniespace.json)
├── config/                 # rubric.yaml — versioned governance rubric
├── tests/                  # Pipeline and validation tests
├── docs/                   # Architecture and design documentation
├── resources/              # Supporting resources
├── .github/                # GitHub Actions CI/CD workflows
├── databricks.yml          # Databricks Asset Bundle configuration
├── app.yaml                # Databricks App configuration
├── silver_layer.py         # Silver DLT pipeline entry point
├── requirements.txt        # Python dependencies
├── .env.template           # Environment variable template (no secrets)
└── .gitignore              # .env and secrets excluded
```

---

## Environment Variables

| Variable                   | Description                                     | Where Set                                       |
| -------------------------- | ----------------------------------------------- | ----------------------------------------------- |
| `DATABRICKS_HOST`          | Workspace URL                                   | Databricks App Environment + `.env`             |
| `DATABRICKS_CLIENT_ID`     | Service Principal Application ID                | Auto-injected by Databricks Apps                |
| `DATABRICKS_CLIENT_SECRET` | Service Principal OAuth secret                  | Auto-injected by Databricks Apps                |
| `DATABRICKS_TOKEN`         | Personal access token (local dev fallback only) | `.env` only                                     |
| `SQL_WAREHOUSE_ID`         | Serverless Starter Warehouse ID                 | Databricks App Environment + `.env`             |
| `GENIE_SPACE_ID`           | Genie Space ID for Conversation API             | Databricks App Environment + `.env`             |
| `GOLD_CATALOG`             | Unity Catalog name                              | `.env` (default: `dbx_metadata_governance_dev`) |
| `GOLD_SCHEMA`              | Gold schema name                                | `.env` (default: `gold`)                        |

> ⚠️ Never commit `.env` to the repository. Only `.env.template` with placeholder values is safe for GitHub.

---

## Deployment

### Prerequisites

- Databricks CLI installed
- Access to workspace `https://dbc-3a1cd165-94e4.cloud.databricks.com`
- GitHub access to `gegedobruna/data-capstone`

### Steps

```bash
# 1. Install Databricks CLI
winget install Databricks.DatabricksCLI

# 2. Authenticate
databricks auth login --host https://dbc-3a1cd165-94e4.cloud.databricks.com

# 3. Verify login
databricks current-user me

# 4. Clone the repository
git clone https://github.com/gegedobruna/data-capstone.git
cd data-capstone

# 5. Validate the bundle
databricks bundle validate -t dev

# 6. Deploy all resources
databricks bundle deploy -t dev

# 7. Run the medallion pipeline (Bronze → Silver → Gold)
databricks bundle run Metadata_Medallion -t dev

# 8. Start the Databricks App
databricks apps start metadata-gov-ui
```

### What `databricks bundle deploy` Creates

```
✅ DLT Pipeline    → Pipeline_01_bronze_ingestion
✅ DLT Pipeline    → Pipeline_02_silver_validate
✅ DLT Pipeline    → Pipeline_03_gold_governance
✅ Job             → Metadata_Medallion (Bronze → Silver → Gold)
✅ Dashboard       → Metadata Governance Dashboard
✅ Genie Space     → Metadata Gov AI
✅ App             → metadata-gov-ui
```

---

## CI/CD Pipeline

Every pull request to `dev` triggers the GitHub Actions workflow:

```
feature/* branch
      ↓  Pull Request
   dev branch
      ↓  GitHub Actions
      ├── databricks bundle validate -t dev
      └── databricks bundle deploy -t dev
      ↓  Merge to main
   main branch (production)
```

Secrets are held in GitHub Secrets and Databricks Secret Scope — never committed to the repository.

---

## Authentication

The Databricks App uses **OAuth2 Service Principal** authentication. The Service Principal (`app-3q3vbl metadata-gov-ui`, Application ID `2aa15d7c-f227-4b1a-b89a-b5dc311d01cc`) is attached as the app's running identity.

`DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET` are injected automatically by the Databricks Apps runtime — no manual token management needed in production.

For local development, `DATABRICKS_TOKEN` is used as a fallback.

---

## Project Timeline

| Phase                 | Period    | Milestone                                                    |
| --------------------- | --------- | ------------------------------------------------------------ |
| Phase 1: Foundation   | Jun 8–14  | Bronze pipeline live · workspace configured · secrets set up |
| Phase 2: Core Logic   | Jun 15–21 | Silver validation · Gold scoring · CI/CD operational         |
| Phase 3: Integration  | Jun 22–26 | Genie + Dashboard live · App deployed                        |
| Phase 4: Presentation | Jun 27–28 | End-to-end dry run · client demo                             |

**Demo date: June 28, 2026**

---

## Team

| #   | Name              | Role                             | Ownership                                                                    |
| --- | ----------------- | -------------------------------- | ---------------------------------------------------------------------------- |
| 01  | [Gegë Dobruna]()  | Technical Lead & Governance Lead | Architecture · `config/` · `docs/` · code reviews                            |
| 02  | [Flandër Canaj]() | Platform & DevOps Engineer       | Databricks workspace · DABs · secret scopes · `.github/` · `infrastructure/` |
| 03  | [Bledi Rexha]()   | Data Ingestion Engineer          | Azure ADLS read · Bronze pipeline · `ingestion/`                             |
| 04  | [Erzen Çitaku]()  | Metadata & Catalog Engineer      | Gold tables · structural profiling · `transformation/` (Gold)                |
| 05  | [Erza Ademi]()    | Data Quality & Rules Engineer    | Silver validation · 11 DQ checks · quarantine · `validation/`                |
| 06  | [Ajete Isaku]()   | AI & Knowledge Engineer          | Genie space · trusted SQL · NL grounding · `chatbot/`                        |
| 07  | [Erza Ademi]()    | BI & Analytics Engineer          | AI/BI Dashboard · KPI design · `dashboard/`                                  |
| 08  | [Gresa Hasani]()  | MLOps / AI Ops Engineer          | Databricks App · Genie API · monitoring · feedback loop · `app/` · `tests/`  |

> Add GitHub profile links for each team member above.

---

## Workspace

| Resource          | Value                                                                 |
| ----------------- | --------------------------------------------------------------------- |
| Workspace         | `https://dbc-3a1cd165-94e4.cloud.databricks.com`                      |
| App URL           | `https://metadata-gov-ui-7474646354040651.aws.databricksapps.com`     |
| SQL Warehouse     | `6d3a6392d8691ed6` (Serverless Starter)                               |
| Genie Space       | `01f1699b3c5718c395e11d9877d814d0`                                    |
| Unity Catalog     | `dbx_metadata_governance_dev`                                         |
| Service Principal | `app-3q3vbl metadata-gov-ui` · `2aa15d7c-f227-4b1a-b89a-b5dc311d01cc` |

---

_Project Metadata Governance Platform · Genpact Kosovo · Data & AI Delivery Team · June 2026_
