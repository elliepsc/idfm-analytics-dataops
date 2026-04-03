.PHONY: help setup install test lint clean ingest load-raw dbt-build check-sla

# Variables
PYTHON := python3
PIP := pip3
DBT := dbt
START_DATE ?= 2024-01-01
END_DATE ?= 2024-01-31


.PHONY: airflow-start airflow-stop airflow-logs airflow-ui

airflow-start:  ## Start Airflow locally
	cd orchestration/airflow && docker-compose up -d
	@echo "✅ Airflow started at http://localhost:8080"
	@echo "   Username: airflow"
	@echo "   Password: airflow"

airflow-stop:  ## Stop Airflow
	cd orchestration/airflow && docker-compose down

airflow-logs:  ## Show Airflow logs
	cd orchestration/airflow && docker-compose logs -f

airflow-ui:  ## Open Airflow UI
	@echo "Opening http://localhost:8080"
	@open http://localhost:8080 || xdg-open http://localhost:8080

airflow-trigger-daily:  ## Manually trigger the daily DAG
	docker exec -it airflow-scheduler airflow dags trigger transport_daily_pipeline

airflow-backfill:  ## Backfill (START_DATE and END_DATE required)
	docker exec -it airflow-scheduler airflow dags trigger transport_backfill \
		--conf '{"start_date":"$(START_DATE)", "end_date":"$(END_DATE)"}'

help:  ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup: install setup-gcp  ## Full installation

install:  ## Install Python dependencies
	$(PIP) install -r requirements.txt
	cd warehouse/dbt && $(DBT) deps

setup-gcp:  ## Configure BigQuery (datasets + tables)
	$(PYTHON) scripts/setup_bigquery.py

test:  ## Run unit tests
	pytest tests/unit/ -v --cov=ingestion --cov=scripts --cov-report=term-missing

lint:  ## Check code style (Python + SQL)
	black --check ingestion/ scripts/
	isort --check-only ingestion/ scripts/
	flake8 ingestion/ scripts/ --max-line-length=120

lint-sql:  ## Lint dbt SQL (sqlfluff - best effort)
	sqlfluff lint warehouse/dbt/models --dialect bigquery

format:  ## Format code
	black ingestion/ scripts/
	isort ingestion/ scripts/
	sqlfluff fix warehouse/dbt/models --dialect bigquery

clean:  ## Remove temporary files
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache .coverage htmlcov/
	cd warehouse/dbt && rm -rf target/ dbt_packages/ logs/

install-terraform:  ## Install Terraform
	sudo snap install terraform --classic  # Requires sudo and snapd

# ─────────────────────────────────────────────────────────────
# INGESTION
# ─────────────────────────────────────────────────────────────

ingest: ingest-validations ingest-punctuality ingest-refs  ## Full ingestion

ingest-validations:  ## Ingest validations (START_DATE to END_DATE)
	$(PYTHON) ingestion/extract_validations.py --start $(START_DATE) --end $(END_DATE)

ingest-punctuality:  ## Ingest punctuality data
	$(PYTHON) ingestion/extract_punctuality.py --start $(START_DATE) --end $(END_DATE)

ingest-refs:  ## Ingest referentials (stops, lines, mappings)
	$(PYTHON) ingestion/extract_ref_stops.py
	$(PYTHON) ingestion/extract_ref_lines.py
	$(PYTHON) ingestion/extract_ref_stop_lines.py

load-raw:  ## Load data into BigQuery RAW
	$(PYTHON) ingestion/load_bigquery_raw.py

# ─────────────────────────────────────────────────────────────
# DBT
# ─────────────────────────────────────────────────────────────

dbt-build:  ## dbt run + test
	cd warehouse/dbt && $(DBT) build --target dev

dbt-run:  ## dbt run only
	cd warehouse/dbt && $(DBT) run --target dev

dbt-test:  ## dbt test only
	cd warehouse/dbt && $(DBT) test --target dev

dbt-docs:  ## Generate and serve dbt documentation
	cd warehouse/dbt && $(DBT) docs generate --target dev
	cd warehouse/dbt && $(DBT) docs serve

dbt-compile:  ## Compile models (no execution)
	cd warehouse/dbt && $(DBT) compile --target dev

dbt-parse:  ## Parse models (CI)
	cd warehouse/dbt && $(DBT) parse --profiles-dir . --profile transport_ci --target ci

dbt-refresh-prod:  ## Full-refresh a model in prod (use MODEL=fct_punctuality_monthly for retroactive SNCF corrections)
	# ⚠️  Use case for fct_punctuality_monthly:
	#     SNCF occasionally publishes retroactive corrections for past months.
	#     insert_overwrite only recomputes the current partition on normal runs.
	#     Past-month corrections are silently ignored by the nightly DAG.
	#
	#     PREREQUISITE — run ingestion FIRST to load the correction into raw_punctuality:
	#       make ingest-punctuality START_DATE=YYYY-MM-01 END_DATE=YYYY-MM-31
	#     Without this step, the refresh reads stale raw data and the correction
	#     is NOT captured — the result is identical to before the refresh.
	#
	#     THEN recompute fct_punctuality_monthly from the updated raw:
	#       make dbt-refresh-prod MODEL=fct_punctuality_monthly
	#
	#     Full procedure:
	#       1. Confirm correction published by SNCF (check raw_punctuality source)
	#       2. make ingest-punctuality START_DATE=<month_start> END_DATE=<month_end>
	#       3. make dbt-refresh-prod MODEL=fct_punctuality_monthly
	#       4. Verify in Looker Studio that the corrected month reflects the update
	#
	#     NOTE: this is a partial mitigation — requires manual human action.
	#     A proper fix would switch the incremental strategy to merge
	#     (unique_key=punctuality_key) so corrections are picked up automatically.
	#     Tracked as post-V3 backlog item.
	docker exec -it airflow-airflow-scheduler-1 bash -c \
	  "cd /opt/airflow/warehouse/dbt && \
	   /home/airflow/.local/bin/dbt run \
	   --select $(MODEL) \
	   --full-refresh \
	   --target prod"

# ─────────────────────────────────────────────────────────────
# MONITORING
# ─────────────────────────────────────────────────────────────

check-sla:  ## Check SLA compliance (data health)
	$(PYTHON) scripts/check_sla.py

ge-validate:  ## Run Great Expectations data quality checks locally (generates fixture data first)
	$(PYTHON) scripts/create_test_data.py
	$(PYTHON) scripts/validate_data_quality.py

# ─────────────────────────────────────────────────────────────
# FULL WORKFLOWS
# ─────────────────────────────────────────────────────────────

elementary-report:  ## Generate Elementary data observability report
	set -a && source .env && set +a && \
	cd warehouse/dbt && \
	edr report \
	  --project-dir . \
	  --profiles-dir . \
	  --profile-target default \
	  --project-profile-target prod \
	  --days-back 30 \
	  --file-path ../../docs/elementary_report.html \
	  --open-browser false
	@echo "✅ Report generated: docs/elementary_report.html"

pipeline-daily: ingest load-raw dbt-build  ## Full daily pipeline

historical-backfill:  ## Load historical validation data (2023-2024) from IDFM ZIP files
	@export $$(grep -v '^#' .env | xargs) && python3 ingestion/backfill/run_backfill.py

historical-backfill-dry:  ## Dry run — download + parse only, no BigQuery writes
	@export $$(grep -v '^#' .env | xargs) && python3 ingestion/backfill/run_backfill.py --dry-run

historical-backfill-force:  ## Force reload all periods (even already loaded)
	@export $$(grep -v '^#' .env | xargs) && python3 ingestion/backfill/run_backfill.py --force

pipeline-backfill:  ## Backfill (requires START_DATE and END_DATE)
	@echo "Backfill from $(START_DATE) to $(END_DATE)"
	$(MAKE) ingest START_DATE=$(START_DATE) END_DATE=$(END_DATE)
	$(MAKE) load-raw
	$(MAKE) dbt-build

all: setup pipeline-daily  ## Full installation + pipeline
