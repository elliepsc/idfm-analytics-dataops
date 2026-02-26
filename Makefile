.PHONY: help setup install test lint clean ingest load-raw dbt-build check-sla

# Variables
PYTHON := python3
PIP := pip3
DBT := dbt
START_DATE ?= 2024-01-01
END_DATE ?= 2024-01-31


.PHONY: airflow-start airflow-stop airflow-logs airflow-ui

airflow-start:  ## Démarre Airflow local
	cd orchestration/airflow && docker-compose up -d
	@echo "✅ Airflow started at http://localhost:8080"
	@echo "   Username: airflow"
	@echo "   Password: airflow"

airflow-stop:  ## Arrête Airflow
	cd orchestration/airflow && docker-compose down

airflow-logs:  ## Affiche logs Airflow
	cd orchestration/airflow && docker-compose logs -f

airflow-ui:  ## Ouvre l'UI Airflow
	@echo "Opening http://localhost:8080"
	@open http://localhost:8080 || xdg-open http://localhost:8080

airflow-trigger-daily:  ## Trigger manuel du DAG daily
	docker exec -it airflow-scheduler airflow dags trigger transport_daily_pipeline

airflow-backfill:  ## Backfill (START_DATE et END_DATE requis)
	docker exec -it airflow-scheduler airflow dags trigger transport_backfill \
		--conf '{"start_date":"$(START_DATE)", "end_date":"$(END_DATE)"}'

help:  ## Affiche l'aide
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup: install setup-gcp  ## Installation complète

install:  ## Installe les dépendances Python
	$(PIP) install -r requirements.txt
	cd warehouse/dbt && $(DBT) deps

setup-gcp:  ## Configure BigQuery (datasets + tables)
	$(PYTHON) scripts/setup_bigquery.py

test:  ## Lance les tests unitaires
	pytest tests/unit/ -v --cov=ingestion

lint:  ## Vérifie le code (Python + SQL)
	black --check ingestion/ scripts/
	isort --check-only ingestion/ scripts/
	flake8 ingestion/ scripts/ --max-line-length=120
	sqlfluff lint warehouse/dbt/models --dialect bigquery

format:  ## Formate le code
	black ingestion/ scripts/
	isort ingestion/ scripts/
	sqlfluff fix warehouse/dbt/models --dialect bigquery

clean:  ## Nettoie les fichiers temporaires
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache .coverage htmlcov/
	cd warehouse/dbt && rm -rf target/ dbt_packages/ logs/

# ─────────────────────────────────────────────────────────────
# INGESTION
# ─────────────────────────────────────────────────────────────

ingest: ingest-validations ingest-punctuality ingest-refs  ## Ingestion complète

ingest-validations:  ## Ingère validations (START_DATE à END_DATE)
	$(PYTHON) ingestion/extract_validations.py --start $(START_DATE) --end $(END_DATE)

ingest-punctuality:  ## Ingère ponctualité
	$(PYTHON) ingestion/extract_punctuality.py --start $(START_DATE) --end $(END_DATE)

ingest-refs:  ## Ingère référentiels (arrêts, lignes, mappings)
	$(PYTHON) ingestion/extract_ref_stops.py
	$(PYTHON) ingestion/extract_ref_lines.py
	$(PYTHON) ingestion/extract_ref_stop_lines.py

load-raw:  ## Charge les données dans BigQuery RAW
	$(PYTHON) ingestion/load_bigquery_raw.py

# ─────────────────────────────────────────────────────────────
# DBT
# ─────────────────────────────────────────────────────────────

dbt-build:  ## dbt run + test
	cd warehouse/dbt && $(DBT) build --target dev

dbt-run:  ## dbt run uniquement
	cd warehouse/dbt && $(DBT) run --target dev

dbt-test:  ## dbt test uniquement
	cd warehouse/dbt && $(DBT) test --target dev

dbt-docs:  ## Génère et ouvre la documentation dbt
	cd warehouse/dbt && $(DBT) docs generate --target dev
	cd warehouse/dbt && $(DBT) docs serve

dbt-compile:  ## Compile les modèles (sans exécution)
	cd warehouse/dbt && $(DBT) compile --target dev

dbt-parse:  ## Parse les modèles (CI)
	cd warehouse/dbt && $(DBT) parse --profiles-dir . --profile transport_ci --target ci

# ─────────────────────────────────────────────────────────────
# MONITORING
# ─────────────────────────────────────────────────────────────

check-sla:  ## Vérifie les SLA (data health)
	$(PYTHON) scripts/check_sla.py

# ─────────────────────────────────────────────────────────────
# WORKFLOWS COMPLETS
# ─────────────────────────────────────────────────────────────

pipeline-daily: ingest load-raw dbt-build  ## Pipeline daily complet

pipeline-backfill:  ## Backfill (nécessite START_DATE et END_DATE)
	@echo "Backfill de $(START_DATE) à $(END_DATE)"
	$(MAKE) ingest START_DATE=$(START_DATE) END_DATE=$(END_DATE)
	$(MAKE) load-raw
	$(MAKE) dbt-build

all: setup pipeline-daily  ## Installation + pipeline complet
