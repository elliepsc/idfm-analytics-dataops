# 🚇 IDFM Analytics DataOps

> Production-grade batch analytics platform for Paris public transport (IDFM network)

[![CI](https://github.com/your-username/idfm-analytics-dataops/workflows/CI/badge.svg)](https://github.com/your-username/idfm-analytics-dataops/actions)
[![dbt](https://img.shields.io/badge/dbt-1.7.4-orange.svg)](https://www.getdbt.com/)
[![Python](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/)
[![Airflow](https://img.shields.io/badge/airflow-2.8.1-blue.svg)](https://airflow.apache.org/)

A complete data engineering project demonstrating modern ELT pipeline with **dbt**, **Airflow**, and **BigQuery** to analyze Paris public transportation data from open government APIs.

---

## 📖 What Does This Project Do?

Analyzes Paris public transport network (IDFM) to answer questions like:
- 📊 Which train lines are most/least punctual?
- 🚉 Which stations have the highest ridership?
- 📈 How has ridership changed over time?
- ⚠️ Are there data quality issues in public reporting?

### Business Value
Provides actionable insights for:
- Transport operations teams
- Urban planners
- Policy makers
- Data journalists
- Public

---

## 🎯 Key Features

- ✅ **Modern Data Stack**: dbt + Airflow + BigQuery
- ✅ **Production-Ready**: Error handling, retry logic, monitoring
- ✅ **Best Practices**: Tests, CI/CD, documentation, version control
- ✅ **Dimensional Modeling**: Star schema (Kimball methodology)
- ✅ **Data Quality**: Automated SLA checks, dbt tests
- ✅ **Containerized**: Docker for local development
- ✅ **Open Data**: Uses public French government APIs

---

## 🏗️ Architecture

```
┌─────────────┐
│ IDFM/SNCF   │  French government open data APIs
│   APIs      │  (Opendatasoft platform)
└──────┬──────┘
       │ Python ingestion (pagination + retry)
       ▼
┌─────────────┐
│  Bronze     │  Raw JSON files (landing zone)
│  Layer      │
└──────┬──────┘
       │ load_bigquery_raw.py
       ▼
┌─────────────┐
│ BigQuery    │  Raw tables (French column names)
│ RAW dataset │  Preserves source structure
└──────┬──────┘
       │ dbt transformations (SQL)
       ├─ Staging: Clean & standardize (English names)
       ├─ Core: Dimensional model (facts + dimensions)
       └─ Marts: Analytics-ready tables
       ▼
┌─────────────┐
│ BigQuery    │  Star schema optimized for BI tools
│ ANALYTICS   │  ✓ Tested ✓ Documented ✓ Monitored
└─────────────┘
```

### Data Flow Explained

1. **Extract** (Python): Fetch from APIs → Save JSON
2. **Load** (Python): JSON → BigQuery RAW tables
3. **Transform** (dbt): RAW → Staging → Core → Marts
4. **Validate** (dbt + custom): Run tests, check SLAs
5. **Orchestrate** (Airflow): Automate daily at 2 AM

---

## 🚀 Quick Start

### Prerequisites
- Python 3.11+, Docker, GCP account
- 45 minutes setup time

### 1. Setup Environment

```bash
git clone https://github.com/your-username/idfm-analytics-dataops.git
cd idfm-analytics-dataops

python -m venv venv
source venv/bin/activate
make install

cp .env.example .env
# Edit .env with your GCP credentials
```

### 2. Create GCP Resources

```bash
# Create service account and download key to credentials/
# Enable BigQuery API
# Create datasets
python scripts/setup_bigquery.py
```

### 3. Run Pipeline Locally

```bash
# Extract 1 week of data
python ingestion/extract_validations.py --start 2024-01-01 --end 2024-01-07
python ingestion/extract_ponctuality.py --start 2024-01-01 --end 2024-01-31

# Load to BigQuery
python ingestion/load_bigquery_raw.py

# Transform with dbt
cd warehouse/dbt
dbt run --target dev
dbt test --target dev
```

### 4. Start Airflow

```bash
cd orchestration/airflow
docker-compose up -d

# Access UI: http://localhost:8080
# Username: airflow, Password: airflow
```

**📚 Full documentation**: See [SETUP.md](SETUP.md) and [QUICKSTART.md](QUICKSTART.md)

---

## 📂 Project Structure

```
idfm-analytics-dataops/
│
├── 📂 ingestion/             # Extract & Load (Python)
│   ├── odsv2_client.py       # API client with retry/pagination
│   ├── extract_*.py          # Data extraction scripts
│   └── load_bigquery_raw.py  # Load JSON → BigQuery
│
├── 📂 warehouse/dbt/         # Transform (SQL)
│   └── models/
│       ├── staging/          # Clean & standardize (1:1)
│       ├── core/             # Dimensions + Facts
│       └── marts/            # Analytics-ready tables
│
├── 📂 orchestration/airflow/ # Orchestrate
│   ├── dags/
│   │   ├── transport_daily_pipeline.py
│   │   └── transport_backfill.py
│   └── docker-compose.yml
│
├── 📂 scripts/               # Utilities
│   ├── setup_bigquery.py     # Setup GCP
│   └── check_sla.py          # Data quality checks
│
├── 📂 config/                # Configuration
│   ├── apis.yml              # API endpoints & mappings
│   └── tables.yml            # BigQuery schemas
│
├── 📂 tests/                 # Quality assurance
│   └── unit/                 # Python unit tests
│
├── 📂 docs/                  # Documentation
│   └── FIELD_DICTIONARY.md   # French ↔ English mappings
│
├── .env.example              # Environment template
├── requirements.txt          # Python dependencies
├── Makefile                  # Developer commands
├── README.md                 # This file
├── SETUP.md                  # Detailed setup guide
└── QUICKSTART.md             # Step-by-step tutorial
```

---

## 💻 Available Commands

```bash
# Installation
make install              # Install Python + dbt dependencies
make setup-gcp            # Create BigQuery datasets

# Development
make ingest               # Extract all data sources
make load-raw             # Load to BigQuery RAW
make dbt-build            # dbt run + test
make pipeline-daily       # Full local pipeline

# Airflow
make airflow-start        # Start Airflow containers
make airflow-stop         # Stop Airflow
make airflow-trigger-daily  # Manual DAG trigger

# Quality
make test                 # Run pytest
make lint                 # Check code quality
make format               # Auto-format code

# See all commands
make help
```

---

## 🗂️ Data Model

### Dimensional Model (Star Schema)

**Fact Tables** (metrics):
- `fct_validations_daily`: Daily ticket validations by station
- `fct_punctuality_monthly`: Monthly punctuality rates by line

**Dimension Tables** (context):
- `dim_stop`: Stations/stops with coordinates
- `dim_line`: Transport lines with colors
- `dim_date`: Date dimension (day, month, year, weekday)
- `dim_ticket_type`: Ticket categories

**Mart Tables** (analytics):
- `mart_network_scorecard_monthly`: Executive KPI dashboard
- `fct_data_health_daily`: Data quality monitoring

### Field Naming

- **Raw Layer**: French (preserves source: `jour`, `ligne`, `nb_vald`)
- **Analytics Layer**: English (standardized: `date`, `line`, `validation_count`)

See [docs/FIELD_DICTIONARY.md](docs/FIELD_DICTIONARY.md) for complete mappings.

---

## 📊 Sample Queries

```sql
-- Top 10 busiest stations (January 2024)
SELECT 
    s.stop_name,
    SUM(v.validation_count) AS total_validations
FROM transport_analytics.fct_validations_daily v
JOIN transport_core.dim_stop s ON v.stop_id = s.stop_id
WHERE v.date BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY s.stop_name
ORDER BY total_validations DESC
LIMIT 10;

-- Average punctuality by line
SELECT 
    l.line_name,
    AVG(p.punctuality_rate) AS avg_punctuality,
    AVG(p.cancelled_trains * 100.0 / p.scheduled_trains) AS cancellation_rate
FROM transport_analytics.fct_punctuality_monthly p
JOIN transport_core.dim_line l ON p.line_id = l.line_id
WHERE p.month = '2024-01'
GROUP BY l.line_name
ORDER BY avg_punctuality DESC;
```

---

## 🧪 Testing Strategy

### Python Tests (pytest)
```bash
pytest tests/ -v --cov=ingestion
```
- Unit tests for API client
- Integration tests for extraction scripts

### dbt Tests
```bash
cd warehouse/dbt
dbt test --target dev
```
- Not null on critical fields
- Unique primary keys
- Referential integrity (foreign keys)
- Accepted values for enums

### Data Quality (SLA Checks)
```bash
python scripts/check_sla.py
```
- Freshness: Data available within 24h
- Completeness: Expected row counts
- Validity: No nulls in required fields

---

## 🚀 Deployment

### CI/CD Pipeline (GitHub Actions)

**On Pull Request**:
1. Lint Python (black, isort, flake8)
2. Lint SQL (sqlfluff)
3. Run pytest
4. Compile dbt models

**On Merge to Main**:
1. Run dbt tests on production
2. Alert on failures

### Production Setup

For production, replace local Docker Compose with:
- **Google Cloud Composer** (managed Airflow), or
- **Kubernetes** with Helm chart

Set environment variables:
```bash
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT=...
AIRFLOW_VAR_GCP_PROJECT_ID=...
```

---

## 📈 Monitoring

### Airflow UI
- DAG run history
- Task duration trends
- Error logs

### BigQuery
```sql
-- Data health check results
SELECT * FROM transport_analytics.fct_data_health_daily
ORDER BY check_date DESC;
```

### Slack Alerts (Optional)
Configure webhook in Airflow:
- Success notifications
- Failure alerts with error details

---

## 🎓 Skills Demonstrated

This project showcases:

**Data Engineering**
- API ingestion with pagination and retry
- Batch processing patterns
- Data lake architecture (Bronze/Silver/Gold)

**Cloud Infrastructure**
- Google Cloud Platform (BigQuery, GCS)
- Infrastructure as code
- Cost optimization

**Data Modeling**
- Dimensional modeling (Kimball)
- Star schema design
- Slowly changing dimensions

**Modern Tools**
- dbt for transformations
- Airflow for orchestration
- Docker for containerization

**Best Practices**
- Unit testing
- CI/CD pipelines
- Documentation
- Version control with Git
- Code reviews

---

## 🤝 Contributing

1. Fork the repository
2. Create feature branch: `git checkout -b feature/my-feature`
3. Make changes with tests
4. Run quality checks: `make lint && make test`
5. Commit: `git commit -m "feat: add new feature"`
6. Push and create Pull Request

**Commit conventions**: `feat:`, `fix:`, `docs:`, `test:`, `refactor:`

---

## 📄 License

MIT License - See [LICENSE](LICENSE) file

---

## 🙏 Acknowledgments

- **IDFM** (Île-de-France Mobilités) for open data
- **SNCF** for Transilien APIs
- **dbt Labs** for dbt Core
- **Apache Airflow** community

---

## 📞 Contact

- 📧 Email: your.email@example.com
- 💼 LinkedIn: [Your LinkedIn](https://linkedin.com/in/yourprofile)
- 🐛 Issues: [GitHub Issues](https://github.com/your-username/idfm-analytics-dataops/issues)

---

**Built with ❤️ to demonstrate modern data engineering practices**
