# Peer Review Guide — IDFM Analytics DataOps

> This document is a reviewer companion to help you quickly locate the evidence
> for each grading criterion without having to dig through the full codebase.

---

## Grading Checklist

### ✅ Problem Description (4/4)

**Where to look**: `README.md` — sections *"What This Project Does"* and *"Architecture"*

The project ingests, transforms, and serves Paris public transport data (IDFM network):
- Real open-data APIs (ticket validations, train punctuality, stop/line references)
- Business questions clearly stated (ridership trends, punctuality by line, SLA monitoring)
- Full Medallion architecture (Bronze → Silver → Gold) with rationale

---

### ✅ Cloud — GCP (4/4)

**Where to look**: `terraform/` · `orchestration/airflow/docker-compose.yml`

| Resource | Details |
|----------|---------|
| **BigQuery** | 5 datasets (`transport_raw`, `transport_staging`, `transport_staging_core`, `transport_staging_analytics`, `transport_snapshots`) |
| **GCS** | Public archive bucket `gs://idfm-backfill-sources` for reproducible backfill |
| **Terraform IaC** | All datasets provisioned via `terraform apply` — not click-ops |
| **Auth** | Application Default Credentials (ADC) — no JSON key stored in repo |

```bash
# Provision all GCP resources
cd terraform
terraform init
terraform apply
```

---

### ✅ Data Ingestion — Batch (4/4)

**Where to look**: `ingestion/` · `ingestion/backfill/`

Two ingestion paths:

**Daily pipeline** (Airflow-orchestrated):
- `extract_validations.py`, `extract_punctuality.py`, `extract_ref_stops.py`, `extract_ref_lines.py`
- Opendatasoft API client with pagination and retry: `odsv2_client.py`
- `load_bigquery_raw.py` → BigQuery RAW (WRITE_TRUNCATE)

**Historical backfill** (manifest-driven, 2023–2025):

| Period | Rows | Strategy |
|--------|------|---------|
| 2023 S1 + S2 | ~800k | Dynamic ZIP URL from IDFM catalog API |
| 2024 S1 + T3 | ~800k | Dynamic ZIP URL from IDFM catalog API |
| 2024 T4 | 469k | Public GCS archive (reproducible snapshot) |
| 2025 recent | 477k | Direct CSV from IDFM rolling dataset |

```bash
# Run full backfill (idempotent — safe to re-run)
python ingestion/backfill/run_backfill.py --base-dir "/path/to/downloads"

# Dry-run validation (no BigQuery writes)
python ingestion/backfill/run_backfill.py --dry-run
```

The backfill uses a **staging + MERGE strategy**: inserts only rows not already present on the natural key `(date, stop_id, ticket_type)`. Re-running inserts 0 rows if data already loaded.

---

### ✅ Data Warehouse — BigQuery (4/4)

**Where to look**: `warehouse/dbt/models/`

**17 dbt models** across 3 layers:

```
staging/   stg_validations_rail_daily · stg_punctuality_monthly
           stg_ref_stops · stg_ref_lines · stg_ref_stop_lines

core/      dim_stop · dim_line · dim_date · dim_ticket_type
           fct_validations_daily (incremental)
           fct_punctuality_monthly (incremental)

marts/     fct_data_health_daily · mart_network_scorecard_monthly
           metrics_fct_validations_daily · metrics_fct_punctuality_monthly
           metrics_mart_network_scorecard_monthly · all_metrics
```

**Partitioning / Clustering**: `fct_validations_daily` is incremental, appended daily. BigQuery handles storage optimisation via clustering on `date`.

**Star schema**: fact tables join to `dim_stop`, `dim_line`, `dim_date`, `dim_ticket_type` — standard Kimball dimensional model.

---

### ✅ Transformations — dbt (4/4)

**Where to look**: `warehouse/dbt/`

- **71 tests** — `not_null`, `unique`, `accepted_values`, `relationships`, custom macros
- **Test results**: ✅ 71 PASS · ⚠️ 5 WARN (known source data issues) · ❌ 0 ERROR
- **Custom generic tests** in `macros/`: `test_assert_positive_values.sql`, `test_assert_valid_date_range.sql`
- **SCD Type 2 snapshots**: `snap_ref_stops.sql`, `snap_ref_lines.sql`
- **Seeds**: `ticket_type_mapping.csv`
- **dbt docs** published on GitHub Pages (auto-generated on push to `main`)

```bash
cd warehouse/dbt
dbt deps
dbt build --target prod   # run all models + all tests
dbt docs generate && dbt docs serve
```

---

### ✅ Dashboard — Looker Studio (4/4)

**Dashboard link**: https://lookerstudio.google.com/reporting/153588f1-5147-4a92-8a81-f74c7dec8bf4

4 pages built on `fct_validations_daily`, `fct_punctuality_monthly`, `fct_data_health_daily`:

| Page | Title | Charts |
|------|-------|--------|
| 1 | Ridership Overview | Top 10 stations by validations · Monthly validations 2023 vs 2024 |
| 2 | Ticket Type Analysis | Volume by ticket type · Share % by ticket type (2023 vs 2024) |
| 3 | Punctuality Analysis | Punctuality rate by line and month · Avg punctuality by line |
| 4 | Data Health — SLA Pipeline | Freshness gauge · SLA status table (3 tables monitored, all ✅) |

## Dashboard

Interactive dashboard: [Open in Looker Studio](https://lookerstudio.google.com/reporting/153588f1-5147-4a92-8a81-f74c7dec8bf4)

## Preview

[![Looker Page 1](docs/looker/looker_page1.png)](https://lookerstudio.google.com/reporting/153588f1-5147-4a92-8a81-f74c7dec8bf4)
[![Looker Page 2](docs/looker/looker_page2.png)](https://lookerstudio.google.com/reporting/153588f1-5147-4a92-8a81-f74c7dec8bf4)
[![Looker Page 3](docs/looker/looker_page3.png)](https://lookerstudio.google.com/reporting/153588f1-5147-4a92-8a81-f74c7dec8bf4)

---

### ✅ Reproducibility (4/4)

**Where to look**: `README.md` · `terraform/` · `ingestion/backfill/backfill_sources.yml`

A reviewer can reproduce the full pipeline from scratch:

```bash
# 1. Clone
git clone https://github.com/your-username/idfm-analytics-dataops.git
cd idfm-analytics-dataops

# 2. Authenticate GCP
gcloud auth application-default login
chmod o+r ~/.config/gcloud/application_default_credentials.json

# 3. Provision infrastructure
cd terraform && terraform init && terraform apply

# 4. Start Airflow
cd orchestration/airflow && docker compose up -d

# 5. Run historical backfill (downloads from public GCS + IDFM APIs)
python ingestion/backfill/run_backfill.py --base-dir "/path/to/downloads"

# 6. Run dbt
cd warehouse/dbt && dbt deps && dbt build --target prod
```

**Reproducibility notes**:
- T4 2024 data is archived in a **public GCS bucket** (`gs://idfm-backfill-sources`) — no manual download needed
- All other historical ZIPs are resolved dynamically from the IDFM catalog API at runtime
- All GCP infrastructure is in `terraform/` — no manual dataset creation
- `.env.example` documents all required environment variables
- `requirements.txt` pins all Python dependencies

---

## CI/CD Status

| Workflow | Status |
|----------|--------|
| `lint-and-test.yml` | [![CI](https://github.com/your-username/idfm-analytics-dataops/workflows/CI/badge.svg)](https://github.com/your-username/idfm-analytics-dataops/actions) |
| `data-quality.yml` | Great Expectations — 9 expectations on `raw_validations` |
| `dbt-docs.yml` | dbt docs → GitHub Pages on push to `main` |

---

## Questions?

Any issues reproducing the pipeline → open a GitHub Issue or contact via the Zoomcamp cohort channel.
