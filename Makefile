.PHONY: help setup install test lint clean ingest load-raw dbt-build check-sla
.PHONY: airflow-start airflow-stop airflow-logs airflow-ui airflow-trigger-daily airflow-backfill reviewer
.PHONY: ci-install python-quality-local data-quality-local data-quality-prod-local dbt-parse-local terraform-validate-local ci-local

# Variables
PYTHON := python3
PIP := pip3
DBT := dbt
START_DATE ?= 2024-01-01
END_DATE ?= 2024-01-31
ROOT_ENV_FILE := .env
AIRFLOW_COMPOSE_DIR := orchestration/airflow
AIRFLOW_COMPOSE_ENV_FILE := ../../.env
AIRFLOW_COMPOSE := cd $(AIRFLOW_COMPOSE_DIR) && docker compose --env-file $(AIRFLOW_COMPOSE_ENV_FILE)
AIRFLOW_HOST_PORT ?= $(strip $(shell sh -c "grep -E '^AIRFLOW_HOST_PORT=' $(ROOT_ENV_FILE) 2>/dev/null | tail -n1 | cut -d= -f2 | tr -d '\r'"))
AIRFLOW_HOST_PORT := $(if $(AIRFLOW_HOST_PORT),$(AIRFLOW_HOST_PORT),8081)
AIRFLOW_USERNAME ?= $(strip $(shell sh -c "grep -E '^_AIRFLOW_WWW_USER_USERNAME=' $(ROOT_ENV_FILE) 2>/dev/null | tail -n1 | cut -d= -f2 | tr -d '\r'"))
AIRFLOW_USERNAME := $(if $(AIRFLOW_USERNAME),$(AIRFLOW_USERNAME),airflow)
AIRFLOW_PASSWORD ?= $(strip $(shell sh -c "grep -E '^_AIRFLOW_WWW_USER_PASSWORD=' $(ROOT_ENV_FILE) 2>/dev/null | tail -n1 | cut -d= -f2 | tr -d '\r'"))
AIRFLOW_PASSWORD := $(if $(AIRFLOW_PASSWORD),$(AIRFLOW_PASSWORD),airflow)
AIRFLOW_DBT_BIN ?= $(strip $(shell sh -c "grep -E '^DBT_BIN=' $(ROOT_ENV_FILE) 2>/dev/null | tail -n1 | cut -d= -f2 | tr -d '\r'"))
AIRFLOW_DBT_BIN := $(if $(AIRFLOW_DBT_BIN),$(AIRFLOW_DBT_BIN),/home/airflow/.local/bin/dbt)
AIRFLOW_UI_URL := http://localhost:$(AIRFLOW_HOST_PORT)
CI_DBT_PROFILES_DIR ?= /tmp/idfm_ci_profiles

airflow-start:  ## Start Airflow locally
	$(AIRFLOW_COMPOSE) up -d
	@echo "Airflow started at $(AIRFLOW_UI_URL)"
	@echo "Username: $(AIRFLOW_USERNAME)"
	@echo "Password: $(AIRFLOW_PASSWORD)"
	@echo "Port is controlled by AIRFLOW_HOST_PORT in $(ROOT_ENV_FILE)"

airflow-stop:  ## Stop Airflow
	$(AIRFLOW_COMPOSE) down

airflow-logs:  ## Show Airflow logs
	$(AIRFLOW_COMPOSE) logs -f

airflow-ui:  ## Open Airflow UI
	@echo "Opening $(AIRFLOW_UI_URL)"
	@open $(AIRFLOW_UI_URL) || xdg-open $(AIRFLOW_UI_URL)

airflow-trigger-daily:  ## Manually trigger the daily DAG
	$(AIRFLOW_COMPOSE) exec -T airflow-scheduler airflow dags trigger transport_daily_pipeline

airflow-backfill:  ## Backfill (START_DATE and END_DATE required)
	$(AIRFLOW_COMPOSE) exec -T airflow-scheduler airflow dags trigger transport_backfill \
		--conf '{"start_date":"$(START_DATE)", "end_date":"$(END_DATE)"}'

help:  ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# Reviewer entry points
run: install dbt-build check-sla  ## Requires GCP + ADC. Install deps, run dbt build, then SLA checks (no ingestion)
	@echo "Pipeline complete. Check Looker Studio for dashboard."
	@echo "For ingestion: make ingest START_DATE=2024-01-01 END_DATE=2024-01-31"
	@echo "For docs:      make dbt-docs"

demo: install dbt-build elementary-report  ## Requires GCP + ADC. Build dbt artefacts and generate the Elementary report
	@echo "Demo complete. Open docs/elementary_report.html for data health report."

reviewer: demo  ## Short reviewer path: same as demo, requires a real GCP project plus ADC

setup: install setup-gcp  ## Full installation for a configured GCP project

install:  ## Install Python dependencies
	$(PIP) install -r requirements.txt
	cd warehouse/dbt && $(DBT) deps

ci-install:  ## Install local tooling used by CI/data-quality workflows
	$(PIP) install -r requirements.txt
	$(PIP) install black flake8 isort pytest pytest-cov dbt-core==1.8.7 dbt-bigquery==1.8.2

setup-gcp:  ## Configure GCP: BigQuery datasets + GCS raw landing zone bucket
	$(PYTHON) scripts/setup_bigquery.py

test:  ## Run unit tests
	pytest tests/unit/ -v --cov=ingestion --cov=scripts --cov-report=term-missing

lint:  ## Check code style (Python + SQL)
	black --check ingestion/ scripts/
	isort --check-only ingestion/ scripts/
	flake8 ingestion/ scripts/ --max-line-length=120

python-quality-local:  ## Reproduce the GitHub CI python-quality job locally
	black --check ingestion/ scripts/ orchestration/airflow/dags/ tests/
	isort --check-only ingestion/ scripts/ orchestration/airflow/dags/ tests/
	flake8 ingestion/ scripts/ orchestration/airflow/dags/ tests/ --max-line-length=120 --extend-ignore=E203
	pytest tests/unit/ -v --cov=ingestion --cov=scripts --cov-report=xml

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

# Ingestion
ingest: ingest-validations ingest-punctuality ingest-refs  ## Full ingestion

ingest-validations:  ## Ingest validations (START_DATE to END_DATE)
	$(PYTHON) ingestion/extract_validations.py --start $(START_DATE) --end $(END_DATE)

ingest-punctuality:  ## Ingest punctuality data
	$(PYTHON) ingestion/extract_ponctuality.py --start $(START_DATE) --end $(END_DATE)

ingest-refs:  ## Ingest referentials (stops, lines, mappings)
	$(PYTHON) ingestion/extract_ref_stops.py
	$(PYTHON) ingestion/extract_ref_lines.py
	$(PYTHON) ingestion/extract_ref_stop_lines.py

load-raw:  ## Load data into BigQuery RAW
	$(PYTHON) ingestion/load_bigquery_raw.py

# dbt
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

dbt-parse-local:  ## Reproduce the GitHub CI dbt-parse job locally
	mkdir -p $(CI_DBT_PROFILES_DIR)
	cp warehouse/dbt/profile_ci.yml $(CI_DBT_PROFILES_DIR)/profiles.yml
	cd warehouse/dbt && $(DBT) deps --profiles-dir $(CI_DBT_PROFILES_DIR) --profile transport_ci --target ci
	cd warehouse/dbt && GCP_PROJECT_ID=dummy-project $(DBT) parse --profiles-dir $(CI_DBT_PROFILES_DIR) --profile transport_ci --target ci

dbt-refresh-prod:  ## Full-refresh a model in prod (use MODEL=fct_punctuality_monthly for retroactive SNCF corrections)
	# Use case for fct_punctuality_monthly:
	# SNCF occasionally publishes retroactive corrections for past months.
	# insert_overwrite only recomputes the current partition on normal runs.
	# Past-month corrections are silently ignored by the nightly DAG.
	#
	# PREREQUISITE - run ingestion FIRST to load the correction into raw_punctuality:
	#   make ingest-punctuality START_DATE=YYYY-MM-01 END_DATE=YYYY-MM-31
	# Without this step, the refresh reads stale raw data and the correction
	# is not captured - the result is identical to before the refresh.
	#
	# THEN recompute fct_punctuality_monthly from the updated raw:
	#   make dbt-refresh-prod MODEL=fct_punctuality_monthly
	#
	# Full procedure:
	#   1. Confirm correction published by SNCF (check raw_punctuality source)
	#   2. make ingest-punctuality START_DATE=<month_start> END_DATE=<month_end>
	#   3. make dbt-refresh-prod MODEL=fct_punctuality_monthly
	#   4. Verify in Looker Studio that the corrected month reflects the update
	#
	# NOTE: this is a partial mitigation - requires manual human action.
	# A proper fix would switch the incremental strategy to merge
	# (unique_key=punctuality_key) so corrections are picked up automatically.
	# Tracked as post-V3 backlog item.
	$(AIRFLOW_COMPOSE) exec -T airflow-scheduler bash -lc \
	  "cd /opt/airflow/warehouse/dbt && \
	   $(AIRFLOW_DBT_BIN) run \
	   --select $(MODEL) \
	   --full-refresh \
	   --target prod"

# Monitoring
check-sla:  ## Check SLA compliance (data health)
	$(PYTHON) scripts/check_sla.py

ge-validate:  ## Run Great Expectations data quality checks locally (generates fixture data first)
	$(PYTHON) scripts/create_test_data.py
	$(PYTHON) scripts/validate_data_quality.py

data-quality-local:  ## Reproduce the local part of the GitHub data-quality workflow
	pytest tests/unit/ -v --cov=ingestion --cov-report=xml
	$(PYTHON) -m py_compile scripts/check_sla.py
	$(PYTHON) scripts/create_test_data.py
	$(PYTHON) scripts/validate_data_quality.py

data-quality-prod-local:  ## Run the prod BigQuery SLA check locally (requires ADC + .env)
	@set -a; . ./.env; set +a; $(PYTHON) scripts/check_sla.py

terraform-validate-local:  ## Reproduce the GitHub Terraform validation job locally
	cd terraform && terraform fmt -check -recursive
	cd terraform && terraform init -backend=false
	cd terraform && terraform validate

ci-local: python-quality-local terraform-validate-local dbt-parse-local  ## Run the main CI jobs locally

# Full workflows
elementary-report:  ## Generate Elementary data observability report
	set -a; . ./.env; set +a; \
	cd warehouse/dbt && \
	edr report \
	  --project-dir . \
	  --profiles-dir . \
	  --profile-target default \
	  --project-profile-target prod \
	  --days-back 30 \
	  --file-path ../../docs/elementary_report.html \
	  --open-browser false
	@echo "Report generated: docs/elementary_report.html"

pipeline-daily: ingest load-raw dbt-build  ## Full daily pipeline

historical-backfill:  ## Load historical validation data (2023-2024) from IDFM ZIP files
	@set -a; . ./.env; set +a; python3 ingestion/backfill/run_backfill.py

historical-backfill-dry:  ## Dry run - download + parse only, no BigQuery writes
	@set -a; . ./.env; set +a; python3 ingestion/backfill/run_backfill.py --dry-run

historical-backfill-force:  ## Force reload all periods (even already loaded)
	@set -a; . ./.env; set +a; python3 ingestion/backfill/run_backfill.py --force

pipeline-backfill:  ## Backfill (requires START_DATE and END_DATE)
	@echo "Backfill from $(START_DATE) to $(END_DATE)"
	$(MAKE) ingest START_DATE=$(START_DATE) END_DATE=$(END_DATE)
	$(MAKE) load-raw
	$(MAKE) dbt-build

all: setup pipeline-daily  ## Full installation + pipeline
