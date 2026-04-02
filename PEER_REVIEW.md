# Peer Review Guide — IDFM Analytics DataOps

> This document is a reviewer companion to help you quickly locate the evidence
> for each grading criterion without having to dig through the full codebase.

---

## Grading Checklist

### ✅ Problem Description (4/4)

**Where to look**: `README.md` — sections *"Problem Statement"* and *"Architecture"*

The project ingests, transforms, and serves Paris public transport data (IDFM network):
- Real open-data APIs (ticket validations, train punctuality, stop/line/station references)
- Business questions clearly stated (ridership trends, punctuality by line, station map, SLA monitoring)
- Full Medallion architecture (Bronze → Silver → Gold) with rationale

---

### ✅ Cloud — GCP (4/4)

**Where to look**: `terraform/` · `orchestration/airflow/docker-compose.yml`

| Resource | Details |
|----------|---------|
| **BigQuery** | 6 datasets (`transport_raw`, `transport_staging_staging`, `transport_staging_core`, `transport_staging_analytics`, `transport_snapshots`, `transport_staging_elementary`) |
| **GCS** | Public archive bucket `gs://idfm-backfill-sources` for reproducible backfill |
| **Terraform IaC** | All datasets provisioned via `terraform apply` — not click-ops |
| **Auth** | Workload Identity Federation (WIF) in CI + ADC locally — no JSON key stored |

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
- `extract_validations.py`, `extract_punctuality.py`, `extract_ref_stops.py`, `extract_ref_lines.py`, `extract_ref_stations.py`
- Opendatasoft API client with pagination and retry: `odsv2_client.py`
- `load_bigquery_raw.py` → BigQuery RAW (WRITE_TRUNCATE)

**Historical backfill** (manifest-driven, 2023–2025):

| Period | Rows | Strategy |
|--------|------|---------|
| 2023 S1 (Jan–Jun) | 971,151 | Dynamic ZIP URL from IDFM catalog API |
| 2023 S2 (Jul–Dec) | 843,825 | Dynamic ZIP URL from IDFM catalog API |
| 2024 S1 (Jan–Jun) | 853,709 | Dynamic ZIP URL from IDFM catalog API |
| 2024 T3 (Jul–Sep) | 463,280 | Dynamic ZIP URL from IDFM catalog API |
| 2024 T4 (Oct–Dec) | 469,093 | Public GCS archive (reproducible snapshot) |
| 2025 recent | 476,685 | Direct CSV from IDFM rolling dataset |
| **Total** | **4,077,743** | |

```bash
# Dry-run validation (no BigQuery writes)
make historical-backfill-dry

# Run full backfill (idempotent — safe to re-run)
make historical-backfill

# Force reload all periods (e.g. after schema change)
make historical-backfill-force
```

The backfill uses a **staging + MERGE strategy**: inserts only rows not already present on the natural key `(date, stop_id, ticket_type)`. Re-running inserts 0 rows if data already loaded.

---

### ✅ Data Warehouse — BigQuery (4/4)

**Where to look**: `warehouse/dbt/models/`

**49 dbt models** across 3 layers:

```
staging/   stg_validations_rail_daily  # incl. station_id_zdc (cast from ida field)
           stg_punctuality_monthly
           stg_ref_stops · stg_ref_lines · stg_ref_stop_lines
           stg_ref_stations             # UNION ALL: 1240 primary + 22 complement seed

core/      dim_stop · dim_line · dim_date · dim_ticket_type
           fct_validations_daily        # ★ incremental · 4.1M rows · station_id_zdc
           fct_punctuality_monthly      # incremental

marts/     fct_data_health_daily        # SLA monitoring (5 SLA columns)
           mart_network_scorecard_monthly
           mart_validations_station_daily  # ★ lat/lon for station map
           metrics_fct_validations_daily · metrics_fct_punctuality_monthly
           all_metrics

seeds/     ticket_type_mapping.csv
           stop_coordinates_complement.csv  # 22 stations missing from IDFM source
           stop_coordinates_mapping.csv
```

**Partitioning / Clustering**: `fct_validations_daily` is partitioned by `validation_date` (DAY) and clustered by `stop_id, ticket_type` — directly optimises dashboard query patterns and reduces bytes scanned.

**Star schema**: fact tables join to `dim_stop`, `dim_line`, `dim_date`, `dim_ticket_type` — standard Kimball dimensional model.

---

### ✅ Transformations — dbt (4/4)

**Where to look**: `warehouse/dbt/`

- **107 tests** — `not_null`, `unique`, `accepted_values`, `relationships`, custom macros
- **Test results**: ✅ 102 PASS · ⚠️ 3 WARN (known source data issues) · ❌ 0 ERROR
- **Custom generic tests** in `macros/`: `test_assert_positive_values.sql`, `test_assert_valid_date_range.sql`
- **SCD Type 2 snapshots**: `snap_ref_stops.sql`, `snap_ref_lines.sql`
- **Seeds**: `ticket_type_mapping.csv` + station coordinate seeds
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

5 pages built on `fct_validations_daily`, `fct_punctuality_monthly`, `fct_data_health_daily`, `mart_validations_station_daily`:

| Page | Title | Charts |
|------|-------|--------|
| 1 | Ridership Overview | Top 10 stations by validations · Monthly validations 2023–2025 |
| 2 | Ticket Type Analysis | Volume by ticket type · Share % by ticket type (2023 vs 2024) |
| 3 | Punctuality Analysis | Punctuality rate by line and month · Avg punctuality by line |
| 4 | Data Health — SLA Pipeline | Freshness gauge · SLA status table |
| 5 | Station Map | Geographic bubble map — validation density by station (lat/lon) |

Sources: `fct_validations_daily` (4.1M rows, 2023–2025) · `mart_validations_station_daily` (93% station coverage — 740/796 stations with coordinates)

## Dashboard Preview

[![Looker Page 1](docs/looker/looker_page1.png)](https://lookerstudio.google.com/reporting/153588f1-5147-4a92-8a81-f74c7dec8bf4)
[![Looker Page 2](docs/looker/looker_page2.png)](https://lookerstudio.google.com/reporting/153588f1-5147-4a92-8a81-f74c7dec8bf4)
[![Looker Page 3](docs/looker/looker_page3.png)](https://lookerstudio.google.com/reporting/153588f1-5147-4a92-8a81-f74c7dec8bf4)

---

### ✅ Reproducibility (4/4)

Reproducibility is not just the README — it is the combined result of multiple files working together:

| File / Folder | Reproducibility contribution |
|---------------|------------------------------|
| `README.md` | Step-by-step setup guide from clone to dashboard |
| `SETUP.md` | Detailed environment setup — dbt profiles, ADC, Airflow, aliases |
| `.env.example` | Documents every required environment variable with descriptions |
| `orchestration/airflow/.env.example` | Airflow-specific env variables (connections, Slack webhook) |
| `terraform/` | All GCP datasets provisioned via `terraform apply` — zero click-ops |
| `terraform/envs/dev.tfvars.example` | Fictitious dev values — reviewer can adapt for their own project |
| `terraform/envs/prod.tfvars.example` | Fictitious prod values |
| `ingestion/backfill/backfill_sources.yml` | Manifest with every historical source URL, period, and loaded status |
| `ingestion/backfill/run_backfill.py` | Self-contained orchestrator — downloads ZIPs, parses, MERGEs into BQ |
| `Makefile` | Single entry point for every operation (`make help` lists all targets) |
| `warehouse/dbt/profiles.yml` | Dev/prod/elementary targets — no hardcoded values, all from env vars |
| `warehouse/dbt/seeds/` | Station coordinate complement — small reference data in version control |
| `requirements.txt` | Pinned Python dependencies |
| `orchestration/airflow/docker-compose.yml` | Airflow stack fully containerised — one command to start |

A reviewer can reproduce the full pipeline from scratch:

```bash
# 1. Clone
git clone https://github.com/elliepsc/idfm-analytics-dataops.git
cd idfm-analytics-dataops

# 2. Configure environment
cp .env.example .env
# Fill in GCP_PROJECT_ID and IDFM_API_KEY

# 3. Authenticate GCP
gcloud auth application-default login
chmod o+r ~/.config/gcloud/application_default_credentials.json

# 4. Provision infrastructure
cd terraform && terraform init && terraform apply && cd ..

# 5. Start Airflow
cd orchestration/airflow && docker compose up -d && cd ../..

# 6. Load environment variables
alias load-idfm-env='set -a && source .env && set +a'
load-idfm-env

# 7. Run historical backfill (4.1M rows — downloads from public GCS + IDFM APIs)
make historical-backfill

# 8. Run dbt
cd warehouse/dbt && dbt deps && dbt build --target prod
```

**Reproducibility notes**:
- T4 2024 data is archived in a **public GCS bucket** (`gs://idfm-backfill-sources`) — no manual download needed
- All other historical ZIPs are resolved dynamically from the IDFM catalog API at runtime
- All GCP infrastructure is in `terraform/` — no manual dataset creation
- `make historical-backfill-force` safely reloads all periods after any schema change (MERGE idempotent)
- `make help` lists every available command — no tribal knowledge required

---

### ✅ Additional — Observability & Data Quality

**Elementary 0.23.0** — `warehouse/dbt/packages.yml`

29 monitoring tables in `transport_staging_elementary` tracking model run results, test results, schema snapshots, and anomaly scores.

```bash
make elementary-report   # generates HTML observability report
```

**Statistical Anomaly Detection** — `orchestration/airflow/dags/utils/monitoring.py`

Z-score check on `validation_count` running daily in `transport_monitoring` DAG:
- Rolling 7-day mean and standard deviation
- Alert threshold: z-score > 2.5
- Results logged to `transport_raw.dag_metrics` (`z_score FLOAT64`, `is_anomaly BOOL`)
- Enriched Slack alert with current value, 7-day mean, z-score, and threshold

**Great Expectations** — `tests/data_quality/`

9 expectations on `raw_validations`, executed on every push via `data-quality.yml` CI workflow.

---

## CI/CD Status

| Workflow | Trigger | What runs |
|----------|---------|-----------|
| `ci.yml` | push, PR | Black · isort · flake8 · pytest · terraform fmt/validate |
| `data-quality.yml` | push, PR, schedule 6h UTC | dbt build prod + Great Expectations + SLA check (WIF auth) |
| `dbt-docs.yml` | push to main | dbt docs → GitHub Pages |
| `release.yml` | push tag `v*.*.*` | terraform validate + GitHub Release |

[![CI](https://github.com/elliepsc/idfm-analytics-dataops/actions/workflows/lint-and-test.yml/badge.svg)](https://github.com/elliepsc/idfm-analytics-dataops/actions)

---

## Quick Reference — Key Files

| What you want to verify | Where to look |
|------------------------|---------------|
| Pipeline architecture | `README.md` — Architecture section |
| dbt models + tests | `warehouse/dbt/models/` + `warehouse/dbt/models/schema.yml` |
| Backfill strategy | `ingestion/backfill/run_backfill.py` + `backfill_sources.yml` |
| Terraform resources | `terraform/bigquery.tf` |
| Airflow DAGs | `orchestration/airflow/dags/` |
| Station map pipeline | `warehouse/dbt/models/marts/business/mart_validations_station_daily.sql` |
| Anomaly detection | `orchestration/airflow/dags/utils/monitoring.py` |
| CI/CD workflows | `.github/workflows/` |
| Reproducibility steps | `README.md` — Steps to Reproduce |

---

## Questions?

Any issues reproducing the pipeline → open a GitHub Issue or contact via the Zoomcamp cohort channel.
