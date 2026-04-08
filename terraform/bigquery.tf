# ─────────────────────────────────────────────────────────────
# BIGQUERY DATASETS — PROD
# ─────────────────────────────────────────────────────────────
# P2-B: renamed all datasets from transport_staging_* to transport_*
# to eliminate the double-staging naming artefact from generate_schema_name.
#
# Migration performed 2026-04-08:
#   transport_staging_staging   → transport_staging
#   transport_staging_core      → transport_core
#   transport_staging_analytics → transport_analytics
#
# Old datasets were deleted manually from BigQuery Console after
# Looker Studio datasources were re-pointed to the new names.
# ─────────────────────────────────────────────────────────────

resource "google_bigquery_dataset" "transport_raw" {
  dataset_id    = "transport_raw"
  friendly_name = "Transport Raw"
  description   = "Raw ingestion layer — IDFM raw data (validations, punctuality, reference data)"
  location      = var.location
  labels        = { env = "prod", layer = "raw" }
}

resource "google_bigquery_dataset" "transport_staging" {
  dataset_id    = "transport_staging"
  friendly_name = "Transport Staging"
  description   = "Staging layer — dbt cleaned and normalized views"
  location      = var.location
  labels        = { env = "prod", layer = "staging" }
}

resource "google_bigquery_dataset" "transport_core" {
  dataset_id    = "transport_core"
  friendly_name = "Transport Core"
  description   = "Core layer — dbt dimensions and facts (star schema)"
  location      = var.location
  labels        = { env = "prod", layer = "core" }
}

resource "google_bigquery_dataset" "transport_analytics" {
  dataset_id    = "transport_analytics"
  friendly_name = "Transport Analytics"
  description   = "Analytics layer — dbt business marts and monitoring"
  location      = var.location
  labels        = { env = "prod", layer = "analytics" }
}

resource "google_bigquery_dataset" "transport_snapshots" {
  dataset_id    = "transport_snapshots"
  friendly_name = "Transport Snapshots"
  description   = "SCD Type 2 snapshots — historical tracking of lines and stops"
  location      = var.location
  labels        = { env = "prod", layer = "snapshots" }
}

resource "google_bigquery_dataset" "transport_elementary" {
  dataset_id    = "transport"
  friendly_name = "Transport Elementary"
  description   = "Elementary data observability — test results, run history, lineage"
  location      = var.location
  labels        = { env = "prod", layer = "observability" }
}

# ─────────────────────────────────────────────────────────────
# BIGQUERY DATASETS — DEV (local development only)
# ─────────────────────────────────────────────────────────────
# P2-B: renamed from transport_staging_dev_* to transport_dev_*
# to match the new profiles.yml dev target (BQ_DATASET_BASE=transport_dev)
# ─────────────────────────────────────────────────────────────

resource "google_bigquery_dataset" "transport_dev_staging" {
  dataset_id    = "transport_dev_staging"
  friendly_name = "Transport Dev - Staging"
  description   = "Dev staging layer — local dbt development"
  location      = var.location
  labels        = { env = "dev", layer = "staging" }
}

resource "google_bigquery_dataset" "transport_dev_core" {
  dataset_id    = "transport_dev_core"
  friendly_name = "Transport Dev - Core"
  description   = "Dev core layer — local dbt development"
  location      = var.location
  labels        = { env = "dev", layer = "core" }
}

resource "google_bigquery_dataset" "transport_dev_analytics" {
  dataset_id    = "transport_dev_analytics"
  friendly_name = "Transport Dev - Analytics"
  description   = "Dev analytics layer — local dbt development"
  location      = var.location
  labels        = { env = "dev", layer = "analytics" }
}

resource "google_bigquery_dataset" "transport_dev_snapshots" {
  dataset_id    = "transport_dev_snapshots"
  friendly_name = "Transport Dev - Snapshots"
  description   = "Dev snapshots layer — local dbt development"
  location      = var.location
  labels        = { env = "dev", layer = "snapshots" }
}

# ─────────────────────────────────────────────────────────────
# NOTE: BigQuery tables are NOT managed by Terraform
# ─────────────────────────────────────────────────────────────
# Tables are managed by dbt (warehouse/dbt/models/) — intentional
# separation of concerns:
#
#   Terraform → Infrastructure (datasets, IAM, GCP project config)
#   dbt       → Tables, views, schemas, tests, documentation
#
# Active datasets managed by Terraform:
#   PROD : transport_raw, transport_staging, transport_core,
#          transport_analytics, transport_snapshots, transport (Elementary)
#   DEV  : transport_dev_staging, transport_dev_core,
#          transport_dev_analytics, transport_dev_snapshots
# ─────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────
# BIGQUERY TABLES — Terraform-managed exceptions
# ─────────────────────────────────────────────────────────────
# General rule: tables managed by dbt, not Terraform.
# Exception: dag_metrics is written directly by monitoring_dag (not dbt).
# ─────────────────────────────────────────────────────────────

resource "google_bigquery_table" "dag_metrics" {
  dataset_id          = google_bigquery_dataset.transport_raw.dataset_id
  table_id            = "dag_metrics"
  description         = "DAG execution metrics — monitoring, z-score, anomaly detection"
  deletion_protection = false

  schema = jsonencode([
    { name = "ingestion_ts", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "dag_id", type = "STRING", mode = "NULLABLE" },
    { name = "run_id", type = "STRING", mode = "NULLABLE" },
    { name = "task_id", type = "STRING", mode = "NULLABLE" },
    { name = "status", type = "STRING", mode = "NULLABLE" },
    { name = "duration_seconds", type = "FLOAT64", mode = "NULLABLE" },
    { name = "nb_records", type = "INTEGER", mode = "NULLABLE" },
    { name = "extra", type = "STRING", mode = "NULLABLE" },
    { name = "z_score", type = "FLOAT64", mode = "NULLABLE" },
    { name = "is_anomaly", type = "BOOL", mode = "NULLABLE" }
  ])
}
