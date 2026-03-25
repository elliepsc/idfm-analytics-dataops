# ─────────────────────────────────────────────────────────────
# BIGQUERY DATASETS — PROD
# ─────────────────────────────────────────────────────────────

resource "google_bigquery_dataset" "transport_raw" {
  dataset_id    = "transport_raw"
  friendly_name = "Transport Raw"
  description   = "Raw ingestion layer — IDFM raw data (validations, punctuality, reference data)"
  location      = var.location
  labels = { env = "prod", layer = "raw" }
}

resource "google_bigquery_dataset" "transport_staging_staging" {
  dataset_id    = "transport_staging_staging"
  friendly_name = "Transport Staging - Staging"
  description   = "Staging layer — dbt cleaned and normalized views"
  location      = var.location
  labels = { env = "prod", layer = "staging" }
}

resource "google_bigquery_dataset" "transport_staging_core" {
  dataset_id    = "transport_staging_core"
  friendly_name = "Transport Staging - Core"
  description   = "Core layer — dbt dimensions and facts (star schema)"
  location      = var.location
  labels = { env = "prod", layer = "core" }
}

resource "google_bigquery_dataset" "transport_staging_analytics" {
  dataset_id    = "transport_staging_analytics"
  friendly_name = "Transport Staging - Analytics"
  description   = "Analytics layer — dbt business marts and monitoring"
  location      = var.location
  labels = { env = "prod", layer = "analytics" }
}

resource "google_bigquery_dataset" "transport_snapshots" {
  dataset_id    = "transport_snapshots"
  friendly_name = "Transport Snapshots"
  description   = "SCD Type 2 snapshots — historical tracking of lines and stops"
  location      = var.location
  labels = { env = "prod", layer = "snapshots" }
}

# ─────────────────────────────────────────────────────────────
# BIGQUERY DATASETS — DEV (local development only)
# ─────────────────────────────────────────────────────────────

resource "google_bigquery_dataset" "transport_staging_dev_staging" {
  dataset_id    = "transport_staging_dev_staging"
  friendly_name = "Transport Dev - Staging"
  description   = "Dev staging layer — local dbt development"
  location      = var.location
  labels = { env = "dev", layer = "staging" }
}

resource "google_bigquery_dataset" "transport_staging_dev_core" {
  dataset_id    = "transport_staging_dev_core"
  friendly_name = "Transport Dev - Core"
  description   = "Dev core layer — local dbt development"
  location      = var.location
  labels = { env = "dev", layer = "core" }
}

resource "google_bigquery_dataset" "transport_staging_dev_analytics" {
  dataset_id    = "transport_staging_dev_analytics"
  friendly_name = "Transport Dev - Analytics"
  description   = "Dev analytics layer — local dbt development"
  location      = var.location
  labels = { env = "dev", layer = "analytics" }
}

resource "google_bigquery_dataset" "transport_staging_dev_snapshots" {
  dataset_id    = "transport_staging_dev_snapshots"
  friendly_name = "Transport Dev - Snapshots"
  description   = "Dev snapshots layer — local dbt development"
  location      = var.location
  labels = { env = "dev", layer = "snapshots" }
}

# ─────────────────────────────────────────────────────────────
# NOTE: BigQuery tables are NOT managed by Terraform
# ─────────────────────────────────────────────────────────────
# Tables are managed by dbt (warehouse/dbt/models/) — intentional
# separation of concerns:
#
#   Terraform → Infrastructure (datasets, IAM, GCP project)
#   dbt       → Tables, views, schemas, tests, documentation
#
# Active datasets managed by Terraform:
#   PROD : transport_raw, transport_staging_staging,
#          transport_staging_core, transport_staging_analytics,
#          transport_snapshots
#   DEV  : transport_staging_dev_staging, transport_staging_dev_core,
#          transport_staging_dev_analytics, transport_staging_dev_snapshots
#
# Deprecated datasets (to delete manually in BQ Console):
#   transport_analytics_staging, transport_analytics_core,
#   transport_analytics_analytics
# ─────────────────────────────────────────────────────────────
