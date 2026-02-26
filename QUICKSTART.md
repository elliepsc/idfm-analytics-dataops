# 🚀 Quickstart Guide - IDFM Analytics DataOps

Step-by-step guide to run your first data pipeline and understand the project.

---

## 📖 What is This Project?

This is a **production-grade data engineering project** that demonstrates:

### Business Problem
Analyze Paris public transport (IDFM) to answer:
- Which train lines are most punctual?
- Which stations have the highest ridership?
- How has ridership changed over time?
- Are there data quality issues in the reporting?

### Technical Solution
A modern ELT (Extract-Load-Transform) pipeline:
1. **Extract** data from French government open data APIs
2. **Load** raw JSON into BigQuery (data lake pattern)
3. **Transform** with dbt into dimensional model (star schema)
4. **Orchestrate** with Airflow for daily automation
5. **Monitor** data quality and pipeline health

### Skills Demonstrated
✅ Data Engineering: API ingestion, batch processing  
✅ Cloud Infrastructure: Google Cloud Platform (BigQuery, GCS)  
✅ Data Modeling: Dimensional modeling (Kimball methodology)  
✅ Modern Stack: dbt, Airflow, Docker, Python  
✅ Best Practices: Testing, CI/CD, documentation, version control

---

## 📁 Project Structure Explained

```
idfm-analytics-dataops/
│
├── 📂 ingestion/                   # Extract: API → JSON files
│   ├── odsv2_client.py            # Reusable API client (pagination, retry)
│   ├── extract_validations.py      # Fetch ticket validation data
│   ├── extract_ponctuality.py      # Fetch train punctuality data
│   ├── extract_ref_*.py            # Fetch reference data (stops, lines)
│   └── load_bigquery_raw.py        # Load: JSON → BigQuery RAW tables
│
├── 📂 warehouse/dbt/               # Transform: RAW → Analytics
│   └── models/
│       ├── staging/                # Clean & standardize (1:1 with source)
│       ├── core/                   # Business logic (dimensions + facts)
│       └── marts/                  # Final analytics tables
│
├── 📂 orchestration/airflow/       # Orchestrate: Automate the pipeline
│   ├── dags/
│   │   ├── transport_daily_pipeline.py   # Run every morning at 2am
│   │   └── transport_backfill.py         # Historical data loading
│   └── docker-compose.yml          # Airflow containers definition
│
├── 📂 config/                      # Configuration files
│   ├── apis.yml                    # API endpoints & field mappings
│   └── tables.yml                  # BigQuery schemas
│
├── 📂 scripts/                     # Utility scripts
│   ├── setup_bigquery.py           # Create BigQuery datasets
│   └── check_sla.py                # Validate data quality SLAs
│
└── 📂 tests/                       # Quality assurance
    └── unit/                       # Unit tests for Python code
```

### Data Flow

```
┌─────────────┐
│  IDFM API   │  French govt open data APIs
│  Transilien │  (Opendatasoft platform)
└──────┬──────┘
       │ Python extract_*.py
       │ (with retry, pagination, rate limiting)
       ▼
┌─────────────┐
│ Bronze/Raw  │  JSON files (temporary storage)
│ data/bronze │  
└──────┬──────┘
       │ load_bigquery_raw.py
       │ (batch insert to BigQuery)
       ▼
┌─────────────┐
│ BigQuery    │  Raw data (exact copy of API response)
│ RAW dataset │  Columns in French (preserves source)
└──────┬──────┘
       │ dbt models (SQL transformations)
       │ - staging: clean & rename to English
       │ - core: business logic, dimensional model
       │ - marts: final analytics tables
       ▼
┌─────────────┐
│ BigQuery    │  Analytics-ready tables
│ ANALYTICS   │  ✓ Dimensional model (star schema)
│ dataset     │  ✓ English column names
└─────────────┘  ✓ Tested & documented
```

---

## 🎯 Running Your First Pipeline

### Prerequisites
✅ You've completed [SETUP.md](SETUP.md)  
✅ Airflow is running at http://localhost:8080  
✅ You have GCP credentials configured

---

## Step 1: Extract Data (Bronze Layer)

**What**: Fetch raw data from APIs and save as JSON files

```bash
# Extract 1 week of ticket validations (ridership data)
python ingestion/extract_validations.py \
    --start 2024-01-01 \
    --end 2024-01-07 \
    --output data/bronze/validations

# Expected output:
# INFO - Fetching all records (max: unlimited)
# INFO - Total records available: 15234
# INFO - Fetched 15234/15234 records
# INFO - ✅ Saved 15234 records to data/bronze/validations/validations_2024-01-01_2024-01-07.json
```

**What happened?**
- Connected to IDFM Opendatasoft API
- Paginated through results (100 records per API call)
- Applied rate limiting (0.5s between requests)
- Saved to JSON: `data/bronze/validations/validations_2024-01-01_2024-01-07.json`

**Check the output:**
```bash
# View first record
head -20 data/bronze/validations/validations_2024-01-01_2024-01-07.json

# Should show structure like:
# {
#   "date": "2024-01-01",
#   "stop_name": "Chatelet",
#   "validation_count": 45231,
#   "ticket_category": "Navigo",
#   "ingestion_ts": "2024-02-03T10:30:00",
#   "source": "idfm_validations"
# }
```

### Extract Punctuality Data

```bash
# Extract punctuality for January 2024
python ingestion/extract_ponctuality.py \
    --start 2024-01-01 \
    --end 2024-01-31 \
    --output data/bronze/punctuality
```

### Extract Reference Data

```bash
# Stops (stations)
python ingestion/extract_ref_stops.py --output data/bronze/referentials

# Lines (e.g., RER A, Metro 1, etc.)
python ingestion/extract_ref_lines.py --output data/bronze/referentials

# Stop-Line mappings (which lines stop at which stations)
python ingestion/extract_ref_stop_lines.py --output data/bronze/referentials
```

---

## Step 2: Load to BigQuery RAW (Silver Layer)

**What**: Upload JSON files to BigQuery as raw tables

```bash
python ingestion/load_bigquery_raw.py

# Expected output:
# INFO - Loading validations_2024-01-01_2024-01-07.json
# INFO - ✅ Loaded 15234 rows to transport_raw.raw_validations
# INFO - Loading punctuality_2024-01-01_2024-01-31.json
# INFO - ✅ Loaded 248 rows to transport_raw.raw_punctuality
# INFO - ✅ All files loaded successfully
```

**What happened?**
- Read all JSON files from `data/bronze/`
- Inferred BigQuery schema from JSON structure
- Batch inserted to `transport_raw` dataset
- Tables created: `raw_validations`, `raw_punctuality`, `raw_ref_stops`, etc.

**Verify in BigQuery Console:**
```sql
-- Check row counts
SELECT 
    'validations' as dataset,
    COUNT(*) as row_count 
FROM `your-project.transport_raw.raw_validations`

UNION ALL

SELECT 
    'punctuality',
    COUNT(*) 
FROM `your-project.transport_raw.raw_punctuality`;
```

---

## Step 3: Transform with dbt (Gold Layer)

**What**: Clean, standardize, and model data into analytics-ready tables

```bash
cd warehouse/dbt

# Compile models (check SQL syntax)
dbt compile --target dev

# Run all transformations
dbt run --target dev

# Expected output:
# Running with dbt=1.7.4
# Found 12 models, 15 tests, 0 snapshots
#
# 15:30:00 | Staging models (5/5)
# 15:30:05 | Core models (5/5)
# 15:30:12 | Mart models (2/2)
#
# Completed successfully
```

**What happened?**

### Staging Layer (Clean & Standardize)
```sql
-- models/staging/stg_validations_rail_daily.sql
-- Renames French columns to English
-- Standardizes data types
-- Adds metadata

SELECT
    jour AS date,                      -- French → English
    libelle_arret AS stop_name,
    id_refa_lda AS stop_id,
    nb_vald AS validation_count,
    categorie_titre AS ticket_category,
    ingestion_ts,
    source
FROM {{ source('raw', 'validations') }}
WHERE jour IS NOT NULL
```

### Core Layer (Business Logic)

**Dimensions** (who, what, where):
- `dim_stop`: All metro/train stations with coordinates
- `dim_line`: All transport lines with colors
- `dim_date`: Date dimension (day, month, year, weekday)
- `dim_ticket_type`: Types of tickets/passes

**Facts** (metrics, measures):
- `fct_validations_daily`: Daily ridership by stop/ticket type
- `fct_punctuality_monthly`: Monthly punctuality by line

### Marts Layer (Analytics)

**Business Mart**:
```sql
-- models/marts/business/mart_network_scorecard_monthly.sql
-- Executive dashboard: Network performance KPIs

SELECT
    p.month,
    l.line_name,
    p.punctuality_rate,
    v.total_validations,
    p.cancelled_trains / p.scheduled_trains AS cancellation_rate
FROM fct_punctuality_monthly p
JOIN dim_line l ON p.line_id = l.line_id
JOIN validations_agg v ON v.month = p.month
```

**Monitoring Mart**:
```sql
-- models/marts/monitoring/fct_data_health_daily.sql
-- Data quality checks

SELECT
    CURRENT_DATE() AS check_date,
    'validations' AS dataset_name,
    COUNT(*) AS row_count,
    CASE 
        WHEN COUNT(*) > 10000 THEN 'pass'
        WHEN COUNT(*) > 5000 THEN 'warn'
        ELSE 'fail'
    END AS check_status
FROM fct_validations_daily
WHERE date = CURRENT_DATE() - 1
```

**Verify transformations:**
```sql
-- Check marts
SELECT * FROM transport_analytics.mart_network_scorecard_monthly
ORDER BY month DESC
LIMIT 10;
```

---

## Step 4: Run Tests

**What**: Validate data quality with dbt tests

```bash
# Still in warehouse/dbt
dbt test --target dev

# Expected output:
# Running with dbt=1.7.4
# Found 15 tests
#
# 15:32:00 | Test not_null_stg_validations_date                   PASS
# 15:32:01 | Test unique_dim_stop_stop_id                         PASS
# 15:32:02 | Test relationships_fct_validations_stop_id           PASS
# ...
# 15:32:15 | Completed successfully
#
# Done. PASS=15 WARN=0 ERROR=0 SKIP=0 TOTAL=15
```

**What tests ran?**
- **Not null**: Critical fields have no nulls
- **Unique**: Primary keys are unique
- **Relationships**: Foreign keys exist in parent tables
- **Accepted values**: Enum fields have valid values

---

## Step 5: Check Data Quality (SLA)

**What**: Verify pipeline meets Service Level Agreements

```bash
cd ../..  # Back to project root
python scripts/check_sla.py

# Expected output:
# Checking SLA for 2024-01-07...
#
# ✅ Dataset: validations
#    Status: pass
#    Rows: 15234 (expected: > 10000)
#    Freshness: 2 hours (expected: < 24 hours)
#
# ✅ Dataset: punctuality
#    Status: pass
#    Rows: 31 (expected: > 20)
#
# ✅ Overall: ALL CHECKS PASSED
```

---

## Step 6: Run Full Pipeline with Airflow

**What**: Automate steps 1-5 with Airflow orchestration

### Via UI (Recommended):

1. Open http://localhost:8080
2. Find DAG: `transport_daily_pipeline`
3. Toggle **ON** (unpause)
4. Click **Trigger DAG** ▶️
5. Click DAG name → **Graph** view
6. Watch tasks turn green ✅

### Via CLI:

```bash
cd orchestration/airflow
docker exec -it airflow-scheduler airflow dags trigger transport_daily_pipeline

# Monitor logs
docker-compose logs -f airflow-scheduler
```

### DAG Structure:

```
┌──────────────────────────────────────────┐
│    transport_daily_pipeline (DAG)       │
└──────────────────────────────────────────┘
           │
           ├─ [EXTRACT] (parallel)
           │   ├─ extract_validations
           │   ├─ extract_punctuality
           │   └─ extract_referentials
           │
           ├─ [LOAD]
           │   └─ load_bigquery_raw
           │
           ├─ [TRANSFORM]
           │   └─ dbt_build (run + test)
           │
           ├─ [VALIDATE]
           │   └─ check_sla
           │
           └─ [ALERT]
               └─ notify_success (Slack)
```

**Execution time**: ~10-15 minutes for 1 week of data

---

## Step 7: Explore the Results

### Option 1: BigQuery Console

```sql
-- Top 10 stations by ridership (Jan 2024)
SELECT 
    s.stop_name,
    SUM(v.validation_count) AS total_validations
FROM transport_analytics.fct_validations_daily v
JOIN transport_core.dim_stop s ON v.stop_id = s.stop_id
WHERE v.date BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY s.stop_name
ORDER BY total_validations DESC
LIMIT 10;

-- Average punctuality by line (Jan 2024)
SELECT 
    l.line_name,
    AVG(p.punctuality_rate) AS avg_punctuality
FROM transport_analytics.fct_punctuality_monthly p
JOIN transport_core.dim_line l ON p.line_id = l.line_id
WHERE p.month = '2024-01'
GROUP BY l.line_name
ORDER BY avg_punctuality DESC;
```

### Option 2: dbt Documentation

```bash
cd warehouse/dbt
dbt docs generate --target dev
dbt docs serve

# Opens http://localhost:8080
# Browse data lineage, column descriptions, tests
```

### Option 3: Airflow Logs

View execution details, errors, and metrics in Airflow UI.

---

## 🎓 Understanding the Pipeline

### Why This Architecture?

**Bronze (Raw) Layer**
- ✅ Preserves original data for auditing
- ✅ Enables reprocessing without API calls
- ✅ Decouples ingestion from transformation

**Silver (Staging) Layer**
- ✅ Cleans data once, use many times
- ✅ Standardizes naming conventions
- ✅ Provides consistent data types

**Gold (Marts) Layer**
- ✅ Optimized for specific analytics use cases
- ✅ Pre-aggregated for performance
- ✅ Business-friendly naming

### Key Design Patterns

1. **Idempotency**: Running pipeline twice produces same result
2. **Incremental Loading**: Only process new data (not implemented in V1)
3. **Dimensional Modeling**: Star schema for fast queries
4. **Data Quality Tests**: Fail fast on bad data
5. **Monitoring**: SLA checks ensure data freshness

---

## 🔄 Daily Operations

### Morning Check (If Automated)

```bash
# Check yesterday's pipeline run
# Airflow UI → DAGs → transport_daily_pipeline → Recent Runs

# Or via CLI
docker exec -it airflow-scheduler \
    airflow dags list-runs -d transport_daily_pipeline --state success

# Expected: Daily run completed successfully
```

### Weekly Maintenance

```bash
# Re-run failed dates
make airflow-backfill START_DATE=2024-01-15 END_DATE=2024-01-21

# Update reference data (changes infrequently)
python ingestion/extract_ref_stops.py
python ingestion/extract_ref_lines.py
```

### Monthly Tasks

- Review BigQuery storage costs
- Check data quality trends in `fct_data_health_daily`
- Rotate GCP service account keys
- Update documentation

---

## 📊 Sample Queries for Analysis

```sql
-- 1. Weekday vs Weekend ridership
SELECT 
    d.is_weekend,
    AVG(v.validation_count) AS avg_validations
FROM fct_validations_daily v
JOIN dim_date d ON v.date = d.date
GROUP BY d.is_weekend;

-- 2. Most improved punctuality (YoY)
WITH this_year AS (
    SELECT line_id, AVG(punctuality_rate) AS rate
    FROM fct_punctuality_monthly
    WHERE month BETWEEN '2024-01' AND '2024-12'
    GROUP BY line_id
),
last_year AS (
    SELECT line_id, AVG(punctuality_rate) AS rate
    FROM fct_punctuality_monthly
    WHERE month BETWEEN '2023-01' AND '2023-12'
    GROUP BY line_id
)
SELECT 
    l.line_name,
    this_year.rate - last_year.rate AS improvement
FROM this_year
JOIN last_year ON this_year.line_id = last_year.line_id
JOIN dim_line l ON this_year.line_id = l.line_id
ORDER BY improvement DESC
LIMIT 5;

-- 3. Data quality over time
SELECT 
    check_date,
    dataset_name,
    check_status,
    row_count
FROM fct_data_health_daily
WHERE check_status != 'pass'
ORDER BY check_date DESC;
```

---

## 🚀 Next Steps

### Enhance the Pipeline

1. **Add Incremental Models**: Only process new/changed data
   ```sql
   {{
     config(
       materialized='incremental',
       unique_key='date'
     )
   }}
   ```

2. **Implement Great Expectations** (V2): Advanced data quality
3. **Add Looker/Tableau**: Business intelligence layer
4. **Set up Alerting**: Slack/email on failures
5. **Add CI/CD**: Automated testing on pull requests

### Portfolio Showcase

This project demonstrates:
- ✅ Modern data stack (dbt, Airflow, BigQuery)
- ✅ Engineering best practices (testing, documentation, version control)
- ✅ Production-ready code (error handling, logging, monitoring)
- ✅ Cloud infrastructure (GCP, Docker)
- ✅ Dimensional modeling (Kimball methodology)

---

## 📚 Additional Resources

- **dbt Docs**: https://docs.getdbt.com
- **Airflow Docs**: https://airflow.apache.org/docs
- **BigQuery Docs**: https://cloud.google.com/bigquery/docs
- **Field Dictionary**: [docs/FIELD_DICTIONARY.md](docs/FIELD_DICTIONARY.md)

---

**Congratulations! You've run a complete data pipeline.** 🎉
