# 🛠️ Setup Guide - IDFM Analytics DataOps

Complete step-by-step guide to set up the project from scratch.

---

## 📋 Table of Contents

1. [Prerequisites](#prerequisites)
2. [Local Development Setup](#local-development-setup)
3. [Google Cloud Platform Setup](#google-cloud-platform-setup)
4. [Airflow Setup](#airflow-setup)
5. [Testing the Setup](#testing-the-setup)
6. [Troubleshooting](#troubleshooting)

---

## 1. Prerequisites

### Required Software

- **Python 3.11+**: [Download](https://www.python.org/downloads/)
- **Docker Desktop**: [Download](https://www.docker.com/products/docker-desktop/)
- **Git**: [Download](https://git-scm.com/downloads)
- **Make** (optional but recommended)

### Required Accounts

- **Google Cloud Platform** account with billing enabled
- **GitHub** account (for version control)

### Estimated Setup Time
⏱️ Total: ~45 minutes

---

## 2. Local Development Setup

### Step 1: Clone Repository & Create Virtual Environment

```bash
git clone https://github.com/your-username/idfm-analytics-dataops.git
cd idfm-analytics-dataops

# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate  # macOS/Linux
# venv\Scripts\activate   # Windows
```

### Step 2: Install Dependencies

```bash
make install
# Or: pip install -r requirements.txt && cd warehouse/dbt && dbt deps
```

### Step 3: Configure Environment

```bash
cp .env.example .env
nano .env  # Edit with your values
```

---

## 3. Google Cloud Platform Setup

### Step 1: Create GCP Project
- Project name: `idfm-analytics`
- Note your Project ID

### Step 2: Enable APIs
```bash
gcloud services enable bigquery.googleapis.com storage.googleapis.com
```

### Step 3: Create Service Account
```bash
gcloud iam service-accounts create idfm-dataops-sa

# Grant roles
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
    --member="serviceAccount:idfm-dataops-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataEditor"
```

### Step 4: Download Credentials
```bash
gcloud iam service-accounts keys create credentials/service-account.json \
    --iam-account=idfm-dataops-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com
```

### Step 5: Create BigQuery Datasets
```bash
python scripts/setup_bigquery.py
```

---

## 4. Airflow Setup

```bash
cd orchestration/airflow
docker-compose build
docker-compose up -d

# Access UI: http://localhost:8080
# Login: airflow / airflow
```

---

## 5. Testing the Setup

### Test Local Extraction
```bash
python ingestion/extract_validations.py --start 2024-01-01 --end 2024-01-07
```

### Test BigQuery Load
```bash
python ingestion/load_bigquery_raw.py
```

### Test dbt
```bash
cd warehouse/dbt
dbt run --target dev
dbt test --target dev
```

### Test Airflow DAG
Trigger `transport_daily_pipeline` in UI

---

## 6. Troubleshooting

### Python packages not found
```bash
source venv/bin/activate
pip install -r requirements.txt
```

### GCP authentication fails
```bash
export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/credentials/service-account.json"
```

### Airflow won't start
```bash
cd orchestration/airflow
docker-compose down -v
docker-compose build --no-cache
docker-compose up -d
```

---

## ✅ Setup Checklist

- [ ] Virtual environment created
- [ ] Dependencies installed
- [ ] GCP project & service account created
- [ ] BigQuery datasets created
- [ ] Airflow running at http://localhost:8080
- [ ] Test extraction works
- [ ] Test dbt transformations work

---

**Next**: See [QUICKSTART.md](QUICKSTART.md) for execution guide
