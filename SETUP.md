# 🛠️ Setup Guide — IDFM Analytics DataOps

Complete step-by-step guide to set up the project from scratch.

> **Auth rule — non-negotiable**: This project uses Application Default Credentials (ADC) exclusively.
> Never create JSON keys. Never set `GOOGLE_APPLICATION_CREDENTIALS`. See [Section 3](#3-google-cloud-platform-setup).

---

## 📋 Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Local Development Setup](#2-local-development-setup)
3. [Google Cloud Platform Setup](#3-google-cloud-platform-setup)
4. [dbt Setup](#4-dbt-setup)
5. [Airflow Setup](#5-airflow-setup)
6. [Elementary Setup](#6-elementary-setup)
7. [Terraform Setup](#7-terraform-setup)
8. [CI/CD — Workload Identity Federation](#8-cicd--workload-identity-federation)
9. [Testing the Setup](#9-testing-the-setup)
10. [Troubleshooting](#10-troubleshooting)

---

## 1. Prerequisites

### Required Software

| Tool | Version | Install |
|---|---|---|
| Python | 3.12+ | [python.org](https://www.python.org/downloads/) |
| Docker Desktop | Latest | [docker.com](https://www.docker.com/products/docker-desktop/) |
| Git | Latest | [git-scm.com](https://git-scm.com/downloads) |
| Google Cloud CLI | Latest | [cloud.google.com/sdk](https://cloud.google.com/sdk/docs/install) |
| Terraform | >= 1.5 | `sudo snap install terraform --classic` |
| Make | Any | Pre-installed on Linux/macOS |

### Required Accounts

- **Google Cloud Platform** account with a project created
- **GitHub** account

### Estimated Setup Time

⏱️ ~1 hour (first time)

---

## 2. Local Development Setup

### Step 1 — Clone & virtual environment

```bash
git clone https://github.com/elliepsc/idfm-analytics-dataops.git
cd idfm-analytics-dataops

python3 -m venv venv
source venv/bin/activate
```

### Step 2 — Install dependencies

```bash
pip install -r requirements.txt
```

### Step 3 — Configure environment variables

```bash
cp .env.example .env
nano .env  # Fill in your values
```

Required values in `.env`:

```bash
GCP_PROJECT_ID=your-gcp-project-id
GCP_REGION=europe-west1
BQ_DATASET_RAW=transport_raw
BQ_DATASET_STAGING=transport_staging
BQ_DATASET_ANALYTICS=transport_staging_analytics
```

### Step 4 — Add the load-idfm-env alias (recommended)

Add to `~/.bashrc` so you can load env vars in any terminal:

```bash
echo 'alias load-idfm-env="set -a && source /path/to/idfm-analytics-dataops/.env && set +a"' >> ~/.bashrc
source ~/.bashrc
```

Usage: `load-idfm-env` before running any script that needs env vars.

---

## 3. Google Cloud Platform Setup

### Step 1 — Authenticate with ADC

```bash
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID
```

This creates `~/.config/gcloud/application_default_credentials.json`.
**This file must never be committed to git.**

> ⚠️ **Never use JSON keys.** The org policy on this project blocks their creation.
> ADC is the only supported authentication method.
>
> NEVER do this:
> ```bash
> # ❌ FORBIDDEN
> gcloud iam service-accounts keys create credentials/service-account.json
> export GOOGLE_APPLICATION_CREDENTIALS=...
> ```

### Step 2 — Enable required APIs

```bash
gcloud services enable bigquery.googleapis.com \
  storage.googleapis.com \
  iamcredentials.googleapis.com \
  --project=YOUR_PROJECT_ID
```

> Note: `iamcredentials.googleapis.com` is required for Workload Identity Federation (CI/CD).

### Step 3 — Create Service Account

```bash
gcloud iam service-accounts create idfm-dataops-sa \
  --display-name="IDFM DataOps SA" \
  --project=YOUR_PROJECT_ID

# Grant required roles
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:idfm-dataops-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:idfm-dataops-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:idfm-dataops-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"
```

### Step 4 — Provision BigQuery datasets with Terraform

See [Section 7 — Terraform Setup](#7-terraform-setup).

---

## 4. dbt Setup

### Step 1 — Install dbt
```bash
pip install dbt-core dbt-bigquery
```

### Step 2 — Install dbt packages
```bash
cd warehouse/dbt
dbt deps
```

### Step 3 — Profiles

All profiles are already in `warehouse/dbt/profiles.yml` — nothing to configure locally.
The file uses `env_var('GCP_PROJECT_ID')` — always run `load-idfm-env` before dbt commands.

Three profiles in `warehouse/dbt/profiles.yml`:

| Profile | Target | Dataset | Usage |
|---|---|---|---|
| `transport` | `dev` | `transport_staging_dev_*` | Local development |
| `transport` | `prod` | `transport_staging_*` | Production |
| `elementary` | `default` | `transport_staging_elementary` | `edr report` CLI |

Run dbt locally (dev target):
```bash
cd warehouse/dbt
load-idfm-env
dbt build --target dev
```

Run against prod:
```bash
dbt build --target prod
```

> No `~/.dbt/profiles.yml` needed for this project — everything is in the repo.
> The only exception is if you have other dbt projects on the same machine
> (e.g. the Zoomcamp taxi project) which use their own `~/.dbt/profiles.yml`.

## 5. Airflow Setup

### Step 1 — Start Airflow

```bash
make airflow-start
# Or: cd orchestration/airflow && docker-compose up -d
```

Access UI: `http://localhost:8080` — login: `airflow` / `airflow`

### Step 2 — Verify DAGs

Four DAGs should appear in the UI:
- `transport_daily_pipeline` — daily ingestion + load
- `dbt_daily` — dbt build + docs
- `transport_backfill` — historical backfill
- `monitoring_dag` — SLA checks + Slack alerts

### Step 3 — Environment in Airflow

Airflow reads from `orchestration/airflow/.env`. Copy the template:

```bash
cp orchestration/airflow/.env.example orchestration/airflow/.env
# Fill in GCP_PROJECT_ID and BQ_DATASET_ANALYTICS
```

---

## 6. Elementary Setup

Elementary provides data observability — test history, lineage, anomaly detection.

### Step 1 — Install Elementary CLI

```bash
pip install 'elementary-data[bigquery]'
```

### Step 2 — Initialize Elementary tables in BigQuery

```bash
cd warehouse/dbt
load-idfm-env
dbt run --select elementary --target prod
```

This creates 29 tables in `transport_staging_elementary` dataset.

### Step 3 — Generate observability report

```bash
make elementary-report
# Or directly:
load-idfm-env
cd warehouse/dbt && edr report \
  --project-dir . \
  --profiles-dir . \
  --profile-target default \
  --project-profile-target prod \
  --days-back 30 \
  --file-path ../../docs/elementary_report.html \
  --open-browser false
```

Open `docs/elementary_report.html` in your browser.

> The `elementary` profile in `warehouse/dbt/profiles.yml` uses `env_var('GCP_PROJECT_ID')` —
> always run `load-idfm-env` before `edr report`.

---

## 7. Terraform Setup

Terraform provisions the BigQuery datasets. Tables are managed by dbt — not Terraform.

### Step 1 — Initialize

```bash
cd terraform
terraform init
```

### Step 2 — Configure variables

```bash
cp terraform.tfvars.example terraform.tfvars
nano terraform.tfvars  # Fill in your project_id
```

### Step 3 — Preview changes

```bash
terraform plan
```

### Step 4 — Apply

```bash
# Authenticate first
gcloud auth application-default login

terraform apply
```

### Environments

```
terraform/
  envs/
    dev.tfvars.example   → copy to dev.tfvars for dev environment
    prod.tfvars.example  → copy to prod.tfvars for prod environment
```

> `terraform.tfvars`, `*.tfstate`, and `.terraform/` are in `.gitignore`.
> See [README_TERRAFORM.md](terraform/README_TERRAFORM.md) for full documentation.

---

## 8. CI/CD — Workload Identity Federation

GitHub Actions connects to BigQuery via WIF — no JSON keys required.

### Required GitHub Secrets

| Secret | Description |
|---|---|
| `GCP_PROJECT_ID` | Your GCP project ID |
| `WIF_PROVIDER` | `projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/github-actions-pool/providers/github-provider` |
| `WIF_SERVICE_ACCOUNT` | `idfm-dataops-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com` |

### WIF Setup (one-time)

```bash
# Create WIF Pool
gcloud iam workload-identity-pools create "github-actions-pool" \
  --project="YOUR_PROJECT_ID" \
  --location="global" \
  --display-name="GitHub Actions Pool"

# Create GitHub Provider
gcloud iam workload-identity-pools providers create-oidc "github-provider" \
  --project="YOUR_PROJECT_ID" \
  --location="global" \
  --workload-identity-pool="github-actions-pool" \
  --attribute-mapping="google.subject=assertion.sub,attribute.actor=assertion.actor,attribute.repository=assertion.repository" \
  --attribute-condition="assertion.repository=='YOUR_GITHUB_USERNAME/idfm-analytics-dataops'" \
  --issuer-uri="https://token.actions.githubusercontent.com"

# Get project number
PROJECT_NUMBER=$(gcloud projects describe YOUR_PROJECT_ID --format='value(projectNumber)')

# Bind Service Account
gcloud iam service-accounts add-iam-policy-binding \
  "idfm-dataops-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --project="YOUR_PROJECT_ID" \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/github-actions-pool/attribute.repository/YOUR_GITHUB_USERNAME/idfm-analytics-dataops"
```

> See [.github/README_WIF.md](.github/README_WIF.md) for full documentation.

---

## 9. Testing the Setup

### Quick smoke test

```bash
# 1. Python unit tests
make test

# 2. dbt parse (no BQ connection needed)
make dbt-parse

# 3. dbt build dev
load-idfm-env && make dbt-build

# 4. Terraform validate
cd terraform && terraform init -backend=false && terraform validate
```

### Full pipeline test

```bash
load-idfm-env
make ingest START_DATE=2024-01-01 END_DATE=2024-01-07
make load-raw
make dbt-build
make check-sla
```

---

## 10. Troubleshooting

### GCP authentication fails

```bash
# Always use ADC — never JSON keys
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID

# Verify
gcloud auth application-default print-access-token
```

### dbt can't find profiles

```bash
# Run from warehouse/dbt/ directory
cd warehouse/dbt
dbt build --target dev  # uses warehouse/dbt/profiles.yml automatically
```

### env vars not loaded

```bash
# Load before any script
load-idfm-env
# Or manually:
set -a && source .env && set +a
```

### Airflow won't start

```bash
cd orchestration/airflow
docker-compose down -v
docker-compose build --no-cache
docker-compose up -d
```

### Elementary profile not found

```bash
# Make sure GCP_PROJECT_ID is set
load-idfm-env
echo $GCP_PROJECT_ID  # should show your project ID

# Run edr from warehouse/dbt/
cd warehouse/dbt
edr report --project-dir . --profiles-dir . --profile-target default ...
```

---

## ✅ Setup Checklist

- [ ] Python venv created and activated
- [ ] Dependencies installed (`pip install -r requirements.txt`)
- [ ] `.env` configured from `.env.example`
- [ ] `load-idfm-env` alias added to `~/.bashrc`
- [ ] GCP authenticated via ADC (`gcloud auth application-default login`)
- [ ] BigQuery datasets provisioned via Terraform
- [ ] `dbt deps` run in `warehouse/dbt/`
- [ ] `dbt build --target dev` passes
- [ ] Airflow running at `http://localhost:8080`
- [ ] Elementary tables initialized (`dbt run --select elementary --target prod`)
- [ ] GitHub secrets configured (WIF) — for CI/CD only

---

**Next**: See [QUICKSTART.md](QUICKSTART.md) for day-to-day usage guide.
