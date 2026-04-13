# Quickstart - IDFM Analytics DataOps

Shortest supported path to reproduce the project locally.

## Supported Environments

- Linux or macOS shell
- WSL2 on Windows
- Windows native PowerShell is not the documented path for the `make` and Bash-based commands in this repo

## Reviewer Path

Use this path if you want to validate the project quickly with a real BigQuery project.

```bash
git clone https://github.com/elliepsc/idfm-analytics-dataops.git
cd idfm-analytics-dataops

cp .env.example .env
# Fill in GCP_PROJECT_ID and IDFM_API_KEY

gcloud auth application-default login

cd terraform
terraform init
terraform apply
cd ..

make reviewer
make airflow-start
```

What this path gives you:

- BigQuery datasets provisioned with Terraform
- Python dependencies plus dbt packages installed
- `dbt build --target dev` executed
- `docs/elementary_report.html` generated
- local Airflow at the URL derived from `AIRFLOW_HOST_PORT` in `.env` (default: `http://localhost:8081`)

Default Airflow login comes from the root `.env` file:

- `_AIRFLOW_WWW_USER_USERNAME=airflow`
- `_AIRFLOW_WWW_USER_PASSWORD=airflow`

No Airflow UI variables are required for the happy path. The containers read the root `.env` directly.

## Fresh Project Path

Use the full setup flow in [SETUP.md](SETUP.md) if you also need:

- GCP API enablement
- IAM/service account setup for CI/CD
- Elementary initialization in production datasets
- historical backfill

## When To Use `terraform import`

Use `terraform import` only if the BigQuery datasets already exist in your project.

Fresh project:

```bash
cd terraform
terraform init
terraform apply
```

Existing datasets:

```bash
cd terraform
terraform init
terraform import google_bigquery_dataset.transport_raw YOUR_PROJECT_ID/transport_raw
terraform import google_bigquery_dataset.transport_snapshots YOUR_PROJECT_ID/transport_snapshots
terraform import google_bigquery_dataset.transport_staging YOUR_PROJECT_ID/transport_staging
terraform import google_bigquery_dataset.transport_core YOUR_PROJECT_ID/transport_core
terraform import google_bigquery_dataset.transport_analytics YOUR_PROJECT_ID/transport_analytics
terraform apply
```

## Common Failures

- ADC works locally but fails inside Airflow: run `gcloud auth application-default login` again; on Linux/WSL you may also need `chmod o+r ~/.config/gcloud/application_default_credentials.json`
- Airflow URL mismatch: set `AIRFLOW_HOST_PORT` once in the root `.env`; Docker Compose, Airflow `base_url`, and the Makefile follow it
- Airflow login mismatch: set `_AIRFLOW_WWW_USER_USERNAME` and `_AIRFLOW_WWW_USER_PASSWORD` in the root `.env`
- GTFS extraction fails immediately: add `IDFM_API_KEY` to the root `.env`
- `make reviewer` or `make demo` still require a real GCP project plus ADC; this repo does not yet provide an offline sample-data mode
