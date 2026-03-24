# 🚇 IDFM Analytics DataOps

[![Python](https://img.shields.io/badge/Python-3.12-blue)](https://python.org)
[![dbt](https://img.shields.io/badge/dbt-1.8.7-orange)](https://getdbt.com)
[![Airflow](https://img.shields.io/badge/Airflow-2.10.0-green)](https://airflow.apache.org)
[![BigQuery](https://img.shields.io/badge/BigQuery-GCP-blue)](https://cloud.google.com/bigquery)
[![Terraform](https://img.shields.io/badge/Terraform-1.5+-purple)](https://terraform.io)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue)](https://docker.com)
[![CI](https://github.com/elliepsc/idfm-analytics-dataops/actions/workflows/lint-and-test.yml/badge.svg)](https://github.com/elliepsc/idfm-analytics-dataops/actions)
[![dbt docs](https://img.shields.io/badge/dbt_docs-GitHub_Pages-orange)](https://elliepsc.github.io/idfm-analytics-dataops)

> **Production-grade batch analytics pipeline for Paris public transport (IDFM network)**
> API Ingestion → BigQuery (Medallion) → dbt → Airflow (4 DAGs) → Data Quality → Dashboard

---

## Table of Contents

- [Problem Statement](#-problem-statement)
- [Overview](#-overview)
- [Tech Stack](#-tech-stack)
- [Architecture](#-architecture)
- [Data Pipeline](#-data-pipeline)
- [Historical Backfill](#-historical-backfill)
- [Data Warehouse & dbt Models](#-data-warehouse--dbt-models)
- [Data Quality & Testing](#-data-quality--testing)
- [CI/CD](#-cicd)
- [Dashboard](#-dashboard)
- [Steps to Reproduce](#-steps-to-reproduce)
- [Project Structure](#-project-structure)
- [Architecture Decisions](#-architecture-decisions)

---

## 🎯 Problem Statement

Île-de-France Mobilités (IDFM) operates the densest public transport network in Europe: **~10 million trips per day**, 300+ lines, 5,000+ stops. IDFM publishes open data via REST APIs — ticket validations, train punctuality, stop and line reference data.

These datasets are scattered across heterogeneous APIs, with no consolidation or historization. There is no unified view to answer simple but essential questions:

- Which train lines are chronically late?
- Has punctuality improved or degraded over 6 months?
- Which stations concentrate the most passenger validations?
- Are the data pipelines fresh and meeting SLA targets?

This project builds a **production-grade analytics pipeline** that automatically ingests, transforms, and serves these datasets to answer exactly those questions.

---

## 📌 Overview

This is a complete end-to-end data engineering project that:

1. **Extracts** data daily from IDFM open APIs (validations, punctuality, stops, lines)
2. **Loads** raw data into BigQuery (Bronze layer)
3. **Transforms** with dbt following the Medallion architecture (Bronze → Silver → Gold)
4. **Orchestrates** the full pipeline with Apache Airflow (4 DAGs, daily schedule)
5. **Validates** data quality with dbt tests (71) and Great Expectations (9 expectations, CI)
6. **Provisions** all infrastructure with Terraform (IaC)
7. **Exposes** KPIs in a Looker Studio dashboard

Additionally, a manifest-driven **historical backfill** loaded 2023–2025 data (~2.3M rows) into BigQuery, giving the dashboard meaningful multi-year trends from day one.

---

## 🛠️ Tech Stack

| Component | Technology | Role |
|-----------|-----------|------|
| Cloud Platform | Google Cloud Platform | Infrastructure, storage, compute |
| Data Warehouse | BigQuery | Analytical SQL, partitioned tables |
| IaC | Terraform | Reproducible GCP resource provisioning |
| Orchestration | Apache Airflow 2.10 | DAG scheduling, retry, alerting |
| Transformation | dbt-bigquery 1.8.7 | SQL models, tests, documentation |
| Containerization | Docker Compose | Local Airflow environment |
| Data Quality | Great Expectations | CI-integrated validation suite |
| CI/CD | GitHub Actions | Lint, test, dbt docs on every push |
| Visualization | Looker Studio | Interactive dashboard |
| Language | Python 3.12 | Ingestion, backfill, tests |

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                         IDFM APIs                                │
│   API Validations · API Ponctualité · API Référentiels           │
│   + Historical backfill (2023–2025, ~2.3M rows)                  │
└─────────────────────┬────────────────────────────────────────────┘
                      │ Python (pagination, retry, rate limiting)
                      ▼
┌──────────────────────────────────────────────────────────────────┐
│               BRONZE — Raw Layer                                 │
│                  BigQuery: transport_raw                         │
│   raw_validations · raw_punctuality · raw_ref_stops/lines        │
└─────────────────────┬────────────────────────────────────────────┘
                      │ dbt staging models
                      ▼
┌──────────────────────────────────────────────────────────────────┐
│              SILVER — Staging Layer                              │
│                BigQuery: transport_staging                       │
│      stg_validations · stg_punctuality · stg_ref_stops/lines     │
└─────────────────────┬────────────────────────────────────────────┘
                      │ dbt core + marts models
                      ▼
┌──────────────────────────────────────────────────────────────────┐
│               GOLD — Analytics Layer                             │
│     BigQuery: transport_staging_core · transport_staging_analytics│
│   dim_stop · dim_line · dim_date · dim_ticket_type               │
│   fct_validations_daily (partitioned by date · clustered)        │
│   fct_punctuality_monthly · mart_network_scorecard_monthly       │
│   fct_data_health_daily · metrics_* · all_metrics                │
└─────────────────────┬────────────────────────────────────────────┘
                      │
         ┌────────────┴────────────┐
         ▼                         ▼
  Looker Studio              dbt Docs
   Dashboard              (GitHub Pages)
```

### Why Medallion Architecture?

| Layer | Role | Key Benefit |
|-------|------|-------------|
| **Bronze (Raw)** | Immutable source data, never modified | Full audit trail — always reprocessable |
| **Silver (Staging)** | Cleaning, typing, column renaming | Source changes don't cascade to analytics |
| **Gold (Marts)** | Business aggregations, KPIs, metrics | BI-ready, pre-computed, fully tested |

---

## 🔄 Data Pipeline

### Batch Strategy

This project uses a **daily batch architecture** — the right choice for IDFM data because:
- Validation data is published by IDFM with a 24h delay (not real-time)
- Punctuality data is aggregated monthly at source
- Daily grain is sufficient for trend analysis and operational reporting

The pipeline runs at **2 AM daily** to ensure previous day's data is available. Each run is idempotent: `WRITE_TRUNCATE` on raw tables + incremental `MERGE` on fact tables means re-running never creates duplicates.

### Ingestion Layer

| Script | Source | Volume | Strategy |
|--------|--------|--------|---------|
| `extract_validations.py` | IDFM Validations API | ~15k records/day | Paginated GET, date filter |
| `extract_punctuality.py` | Transilien Punctuality API | 156 records/month | Full monthly extract |
| `extract_ref_stops.py` | IDFM Stops API | ~10k stops | Full refresh weekly |
| `extract_ref_lines.py` | IDFM Lines API | ~2100 lines | Full refresh weekly |
| `load_bigquery_raw.py` | Local JSON | All above | WRITE_TRUNCATE → RAW |

### DAG 1: `transport_daily_pipeline`
**Schedule**: daily at 2 AM · **Average duration**: ~3 min

```
extract_validations  ─┐
extract_punctuality   ─┼──► load_bigquery_raw ──► dbt_build ──► check_sla ──► notify_success
extract_referentials  ─┘
      (parallel)                                   (sequential)
```

| Task | Operator | Description |
|------|----------|-------------|
| `extract_validations` | PythonOperator | Fetch ticket validations from IDFM API → JSON |
| `extract_punctuality` | PythonOperator | Fetch Transilien punctuality data → JSON |
| `extract_referentials` | PythonOperator | Fetch stops and lines reference data → JSON |
| `load_bigquery_raw` | PythonOperator | Load JSON → BigQuery RAW (WRITE_TRUNCATE) |
| `dbt_build` | BashOperator | `dbt deps && dbt build` — run all models + tests |
| `check_sla` | PythonOperator | Freshness + quality via `fct_data_health_daily` |
| `notify_success` | PythonOperator | Slack alert on success (silent if unconfigured) |

### DAG 2: `dbt_daily`
**Schedule**: daily at 3 AM. dbt transformations only, no ingestion.

Tasks: `dbt_deps` → `dbt_run_staging` → `dbt_run_core` → `dbt_run_marts` → `dbt_test` → `dbt_docs_generate` → `notify_success`

### DAG 3: `transport_backfill`
**Schedule**: manual trigger. Historical data reload over a configurable date range.

### DAG 4: `transport_monitoring`
**Schedule**: daily at 7 AM. BigQuery freshness checks, volume threshold alerts (>20% drop), SLA checks. Results written to `fct_data_health_daily`.

### Error Handling
- **Retries**: 3 attempts with 5-min delay
- **Timeout**: 2h max per run
- **Alerting**: Slack webhook on failure (graceful skip if unconfigured)
- **Monitoring**: `fct_data_health_daily` tracks freshness and row counts per table

---

## 🗃️ Historical Backfill

A **manifest-driven backfill module** loaded 2023–2025 historical data into BigQuery on project initialization, giving the dashboard multi-year trends from day one.

### Data Loaded

| Period | Rows | Source Strategy |
|--------|------|----------------|
| 2023 S1 (Jan–Jun) | ~400k | Dynamic ZIP URL — resolved from IDFM catalog API at runtime |
| 2023 S2 (Jul–Dec) | ~400k | Dynamic ZIP URL — resolved from IDFM catalog API at runtime |
| 2024 S1 (Jan–Jun) | ~400k | Dynamic ZIP URL — resolved from IDFM catalog API at runtime |
| 2024 T3 (Jul–Sep) | ~400k | Dynamic ZIP URL — resolved from IDFM catalog API at runtime |
| 2024 T4 (Oct–Dec) | 469k | Public GCS snapshot (`gs://idfm-backfill-sources`) |
| 2025 recent | 477k | Direct CSV from IDFM rolling dataset |
| **Total** | **~2.3M** | |

### Key Design Decisions

**Dynamic URL resolution**: IDFM ZIP file hashes change when they update their data. Rather than hardcoding URLs, the backfill queries the IDFM catalog API at runtime to always resolve the current download URL for each year.

**GCS public archive for T4 2024**: The original IDFM URL for T4 2024 is a rolling dataset that now serves 2025 data. The snapshot was archived in a public GCS bucket so anyone cloning the repo can reproduce the exact same BigQuery state without any manual steps.

**MERGE strategy (idempotent)**: Staging + MERGE on the natural key `(date, stop_id, ticket_type)`. Re-running on already-loaded data inserts 0 rows — safe to replay at any time.

```bash
# Dry-run first — parse all files, no BigQuery writes
python ingestion/backfill/run_backfill.py --dry-run

# Full backfill (~2.3M rows, ~10 min)
python ingestion/backfill/run_backfill.py --base-dir "/path/to/downloads"

# Single period
python ingestion/backfill/run_backfill.py --period 2024-T4 --force
```

---

## 🔧 Data Warehouse & dbt Models

### BigQuery Datasets

| Dataset | Layer | Content |
|---------|-------|---------|
| `transport_raw` | Bronze | Raw data from APIs and backfill |
| `transport_staging` | Silver | Cleaned, typed, renamed |
| `transport_staging_core` | Gold | Dimensions + Facts |
| `transport_staging_analytics` | Gold | Marts + Metrics |
| `transport_snapshots` | — | SCD Type 2 historization |

### 17 dbt Models

```
warehouse/dbt/models/
│
├── staging/                              # SILVER — 1:1 with source tables
│   ├── stg_validations_rail_daily.sql    # Clean + normalize raw validations
│   ├── stg_punctuality_monthly.sql       # Clean + normalize punctuality
│   ├── stg_ref_stops.sql                 # Normalize stop reference
│   ├── stg_ref_lines.sql                 # Normalize line reference
│   └── stg_ref_stop_lines.sql            # Stop ↔ Line mapping
│
├── core/                                 # GOLD — Dimensional model (Kimball)
│   ├── dim_stop.sql                      # Stop dimension (8,500 stops)
│   ├── dim_line.sql                      # Line dimension (2,100 lines)
│   ├── dim_date.sql                      # Date dimension
│   ├── dim_ticket_type.sql               # Ticket category dimension
│   ├── fct_validations_daily.sql         # ★ Main fact table — incremental
│   │                                     #   partitioned by validation_date (DAY)
│   │                                     #   clustered by stop_id, ticket_type
│   └── fct_punctuality_monthly.sql       # Punctuality fact — incremental
│
└── marts/                                # GOLD — Pre-computed analytics
    ├── fct_data_health_daily.sql         # Data quality monitoring
    ├── mart_network_scorecard_monthly.sql # Executive KPI scorecard
    ├── metrics_fct_validations_daily.sql
    ├── metrics_fct_punctuality_monthly.sql
    ├── metrics_mart_network_scorecard_monthly.sql
    └── all_metrics.sql                   # Consolidated KPI view (dashboard source)

warehouse/dbt/snapshots/
├── snap_ref_stops.sql                    # SCD Type 2 — stop historization
└── snap_ref_lines.sql                    # SCD Type 2 — line historization
```

### Materialization Strategy

| Model | Type | Reason |
|-------|------|--------|
| `stg_*` | View | Lightweight, recomputed on demand |
| `dim_*` | Table | Stable reference, frequent joins |
| `fct_validations_daily` | Incremental | ~15k rows/day, append-only pattern |
| `fct_punctuality_monthly` | Incremental | Monthly data, no recomputation needed |
| `mart_*`, `metrics_*` | Table | Expensive aggregations, pre-computed |
| `snap_*` | Snapshot | SCD Type 2 — tracks reference changes over time |

### Partitioning & Clustering

`fct_validations_daily` is **partitioned by `validation_date` (DAY)** and **clustered by `stop_id, ticket_type`**.

This directly optimizes the most common query patterns:
- Date-range filters (`WHERE validation_date BETWEEN ...`) skip irrelevant partitions entirely
- Aggregations by station or ticket category (`GROUP BY stop_id`) benefit from clustering
- At 3.6M+ rows, this reduces bytes scanned and dashboard query costs significantly

### dbt Test Results

```
✅ 71 PASS   ⚠️ 5 WARN   ❌ 0 ERROR
```

| Test Type | Severity | Examples |
|-----------|----------|---------|
| `not_null` | ERROR | IDs, dates, counts |
| `unique` | WARN | Primary keys (known source duplicates) |
| `accepted_values` | ERROR | Ticket categories, risk levels |
| `relationships` | WARN | Orphan FKs — line codes misaligned at source |
| `assert_positive_values` | ERROR | `validation_count > 0` (custom macro) |
| `assert_valid_date_range` | ERROR | `date >= 2020-01-01` (custom macro) |

> The 5 warnings are **source data quality issues** — line codes in validations don't always match the reference dataset. `severity: warn` monitors without blocking the pipeline.

### dbt — Key Implementation Details

**Model Lineage**
```
raw_validations (Bronze)
    └── stg_validations_rail_daily     # clean, type, rename
            └── fct_validations_daily  # incremental fact (partitioned)
                    ├── mart_network_scorecard_monthly
                    ├── metrics_fct_validations_daily
                    └── all_metrics

raw_ref_stops + raw_ref_lines (Bronze)
    ├── stg_ref_stops └── dim_stop
    └── stg_ref_lines └── dim_line

raw_punctuality (Bronze)
    └── stg_punctuality_monthly
            └── fct_punctuality_monthly  # incremental fact
                    └── metrics_fct_punctuality_monthly
```

**Custom Generic Tests (macros/)**

`test_assert_positive_values` — ensures `validation_count > 0`. Catches upstream ingestion bugs where counts come in as 0 or negative, which would silently corrupt aggregations.

`test_assert_valid_date_range` — ensures `date >= 2020-01-01`. Catches encoding bugs in the multi-format CSV parser (e.g. 2-digit year misparse producing dates in 1924).

**Incremental Strategy on `fct_validations_daily`**
```sql
-- Only process new dates not already in the table
{% if is_incremental() %}
  WHERE validation_date > (SELECT MAX(validation_date) FROM {{ this }})
{% endif %}
```
This means daily runs process ~15k rows instead of re-scanning 4M+ rows — direct cost and performance optimization.

---

## 🧪 Data Quality & Testing

### 1. dbt Tests (71 tests)
Run at every `dbt build`. Blocking (ERROR) on critical issues, non-blocking (WARN) on known source anomalies. Custom generic tests in `macros/`: `test_assert_positive_values.sql`, `test_assert_valid_date_range.sql`.

### 2. Great Expectations (CI-integrated)
9 expectations on `raw_validations`, executed on every push via `data-quality.yml`:

| Expectation | Constraint |
|-------------|-----------|
| `validation_count` | between 0 and 1,000,000 |
| `date` | not null |
| `stop_id` | not null |
| `line_code_trns` | not null |
| `ticket_type` | in expected value set |
| Column types | conformant to schema |

### 3. Python Unit Tests (pytest)
`tests/unit/` — covers extraction scripts and transformation logic. Run via `make test` or `lint-and-test.yml`.

### 4. Monitoring DAG
Daily freshness checks, row count thresholds, SLA compliance. Results in `fct_data_health_daily`.

---

## 🚀 CI/CD

```
Push / PR → GitHub Actions
              ├── lint-and-test.yml    Black · isort · pytest
              ├── data-quality.yml     Great Expectations
              └── dbt-docs.yml         dbt docs → GitHub Pages (main only)
```

| Workflow | Trigger | Jobs |
|----------|---------|------|
| `lint-and-test.yml` | push, PR | Black, isort, pytest |
| `data-quality.yml` | push, PR | Great Expectations on raw_validations |
| `dbt-docs.yml` | push to main | dbt docs generate → GitHub Pages |

dbt documentation with full lineage DAG: https://elliepsc.github.io/idfm-analytics-dataops

---

## 📊 Dashboard

> 🔗 **[Looker Studio Dashboard](https://lookerstudio.google.com/reporting/153588f1-5147-4a92-8a81-f74c7dec8bf4)**

4-page interactive dashboard built on BigQuery Gold layer tables.

| Page | Title | Key questions answered |
|------|-------|----------------------|
| **1** | Ridership Overview | Which stations concentrate the most validations? How has ridership evolved 2023 vs 2024? |
| **2** | Ticket Type Analysis | Which ticket types drive ridership? How does the mix change year over year? |
| **3** | Punctuality Analysis | Which Transilien lines are chronically late? Has punctuality improved over 2024? |
| **4** | Data Health — SLA Pipeline | Are all pipeline tables fresh and meeting SLA targets? |

Sources: `fct_validations_daily` (4.1M rows, 2023–2025) · `fct_punctuality_monthly` (12 Transilien lines, 2024) · `fct_data_health_daily`

## Dashboard

Interactive dashboard: [Open in Looker Studio](https://lookerstudio.google.com/reporting/153588f1-5147-4a92-8a81-f74c7dec8bf4)

## Preview

[![Looker Page 1](docs/looker/looker_page1.png)](https://lookerstudio.google.com/reporting/153588f1-5147-4a92-8a81-f74c7dec8bf4)
[![Looker Page 2](docs/looker/looker_page2.png)](https://lookerstudio.google.com/reporting/153588f1-5147-4a92-8a81-f74c7dec8bf4)
[![Looker Page 3](docs/looker/looker_page3.png)](https://lookerstudio.google.com/reporting/153588f1-5147-4a92-8a81-f74c7dec8bf4)

---

## 🔁 Steps to Reproduce

### Prerequisites
- Docker Desktop
- Google Cloud account with BigQuery enabled
- gcloud CLI installed and authenticated
- IDFM API token ([register here](https://data.iledefrance-mobilites.fr))

### 1. Clone

```bash
git clone https://github.com/elliepsc/idfm-analytics-dataops.git
cd idfm-analytics-dataops
```

### 2. Configure environment

```bash
cp .env.example .env
# Fill in GCP_PROJECT_ID and IDFM_API_KEY
```

### 3. Authenticate GCP

```bash
gcloud auth application-default login
chmod o+r ~/.config/gcloud/application_default_credentials.json
```

### 4. Provision infrastructure

```bash
cd terraform
terraform init && terraform apply
cd ..
```

### 5. Start Airflow

```bash
cd orchestration/airflow
docker compose up -d
# UI: http://localhost:8081  (admin / admin)
```

### 6. Run historical backfill

```bash
source venv/bin/activate

# Validate first (no BQ writes)
python ingestion/backfill/run_backfill.py --dry-run

# Load all periods (~2.3M rows)
python ingestion/backfill/run_backfill.py --base-dir "/path/to/downloads"
```

> T4 2024 is archived in a public GCS bucket and downloaded automatically.

### 7. Run dbt

```bash
cd warehouse/dbt
dbt deps
dbt build --target prod
```

### 8. Trigger the pipeline

Enable and trigger `transport_daily_pipeline` in the Airflow UI (http://localhost:8081).

---

## 📁 Project Structure

```
idfm-analytics-dataops/
│
├── ingestion/                         # Extract & Load
│   ├── odsv2_client.py                # Opendatasoft API client (pagination, retry)
│   ├── extract_validations.py         # Daily ticket validation ingestion
│   ├── extract_punctuality.py         # Monthly punctuality ingestion
│   ├── extract_ref_stops.py           # Stop reference ingestion
│   ├── extract_ref_lines.py           # Line reference ingestion
│   ├── load_bigquery_raw.py           # JSON → BigQuery RAW loader
│   └── backfill/                      # Historical backfill (2023–2025)
│       ├── run_backfill.py            # Manifest-driven orchestrator
│       ├── load_backfill_bq.py        # Staging + MERGE strategy (idempotent)
│       ├── parse_csv_historical.py    # Multi-format CSV parser (5 encoding variants)
│       └── backfill_sources.yml       # Manifest: sources, URLs, loaded status
│
├── warehouse/
│   └── dbt/                           # Transform
│       ├── models/
│       │   ├── staging/               # Silver — 5 models
│       │   ├── core/                  # Gold — 6 models (dims + facts)
│       │   └── marts/                 # Gold — 6 models (aggregations + metrics)
│       ├── snapshots/                 # SCD Type 2 (stops, lines)
│       ├── macros/                    # Custom generic tests
│       ├── seeds/                     # ticket_type_mapping.csv
│       └── dbt_project.yml
│
├── orchestration/
│   └── airflow/                       # Orchestrate
│       ├── dags/
│       │   ├── transport_daily_pipeline.py   # Main DAG (daily, 7 tasks)
│       │   ├── dbt_daily.py                  # dbt-only DAG
│       │   ├── transport_backfill.py         # Manual backfill DAG
│       │   ├── monitoring_dag.py             # Monitoring DAG
│       │   └── utils/monitoring.py           # SLA callbacks, BQ metrics
│       └── docker-compose.yml
│
├── terraform/                         # Infrastructure as Code
│   ├── main.tf                        # GCP provider config
│   ├── bigquery.tf                    # All BigQuery datasets
│   └── variables.tf                   # Project ID, location, labels
│
├── tests/
│   ├── unit/                          # pytest
│   └── data_quality/                  # Great Expectations suite
│
├── .github/workflows/
│   ├── lint-and-test.yml              # Black, isort, pytest
│   ├── data-quality.yml               # Great Expectations
│   └── dbt-docs.yml                   # dbt docs → GitHub Pages
│
├── Makefile                           # Developer commands (make help)
├── .env.example
└── README.md
```

---

## 🏛️ Architecture Decisions

**Why dbt over raw SQL?**
Versioning, tests, documentation, and dependency management built-in. A blocking `dbt test` before every deploy prevents shipping corrupt data to production.

**Why Airflow over a cron job?**
Dependency graph visibility, automatic retry with backoff, date-range backfill, and Slack alerting. A pipeline that fails silently is worse than no pipeline.

**Why BigQuery over Postgres?**
Native analytical SQL, serverless scaling, and a coherent GCP ecosystem. Free tier covers this project entirely.

**Why partition + cluster `fct_validations_daily`?**
At 3.6M+ rows, partitioning by `validation_date` (DAY) eliminates irrelevant partitions on date-range queries. Clustering by `stop_id, ticket_type` reduces bytes scanned for the most common aggregation patterns — directly improving dashboard performance and cost.

**Why ADC over a service account key?**
More secure (no JSON file to store), GCP-recommended, and key creation was blocked by the org policy anyway.

**Why MERGE for the backfill?**
Idempotency: re-running inserts 0 rows on already-loaded data. Safe to replay for full reproducibility.

**Why a public GCS bucket for T4 2024?**
The original IDFM URL is a rolling dataset now serving 2025 data. The GCS archive ensures full reproducibility for anyone cloning the repo.

**Why `severity: warn` on relationship tests?**
Line codes in validations don't match the reference dataset at source. Blocking on a source issue would be counterproductive — `warn` monitors without blocking.

---

## 📬 Contact

- GitHub: [@elliepsc](https://github.com/elliepsc)
- Project: [idfm-analytics-dataops](https://github.com/elliepsc/idfm-analytics-dataops)

---

*Developed as part of the [DataTalks.Club Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) — March 2026*
