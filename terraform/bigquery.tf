# ─────────────────────────────────────────────────────────────
# BIGQUERY DATASETS
# ─────────────────────────────────────────────────────────────

resource "google_bigquery_dataset" "transport_raw" {
  dataset_id    = "transport_raw"
  friendly_name = "Transport Raw"
  description   = "Raw ingestion layer — IDFM raw data (validations, punctuality, reference data)"
  location      = var.location

  labels = {
    env   = "dev"
    layer = "raw"
  }
}

resource "google_bigquery_dataset" "transport_analytics_staging" {
  dataset_id    = "transport_analytics_staging"
  friendly_name = "Transport Analytics - Staging"
  description   = "Staging layer — dbt cleaned and normalized views"
  location      = var.location

  labels = {
    env   = "dev"
    layer = "staging"
  }
}

resource "google_bigquery_dataset" "transport_analytics_core" {
  dataset_id    = "transport_analytics_core"
  friendly_name = "Transport Analytics - Core"
  description   = "Core layer — dbt dimensions and facts (star schema)"
  location      = var.location

  labels = {
    env   = "dev"
    layer = "core"
  }
}

resource "google_bigquery_dataset" "transport_analytics_analytics" {
  dataset_id    = "transport_analytics_analytics"
  friendly_name = "Transport Analytics - Analytics"
  description   = "Analytics layer — dbt business marts and monitoring"
  location      = var.location

  labels = {
    env   = "dev"
    layer = "analytics"
  }
}

resource "google_bigquery_dataset" "transport_snapshots" {
  dataset_id    = "transport_snapshots"
  friendly_name = "Transport Snapshots"
  description   = "SCD Type 2 snapshots — historical tracking of lines and stops reference data"
  location      = var.location

  labels = {
    env   = "dev"
    layer = "snapshots"
  }
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
# Benefits:
#   - dbt owns the full table lifecycle (CREATE, ALTER, partitioning,
#     clustering, incremental logic)
#   - Avoids state conflicts between Terraform and dbt
#   - Schemas are documented in schema.yml (single source of truth)
#
# Active tables managed by dbt:
#   transport_raw                : raw_validations, raw_punctuality,
#                                  raw_ref_stops, raw_ref_lines
#   transport_analytics_core     : fct_validations_daily (partitioned DAY,
#                                  clustered stop_id/ticket_type),
#                                  fct_punctuality_monthly (partitioned MONTH,
#                                  clustered line_id), dim_stop, dim_line,
#                                  dim_ticket_type, dim_date
#   transport_analytics_staging  : stg_* views
#   transport_analytics_analytics: mart_network_scorecard_monthly,
#                                  fct_data_health_daily, metrics_*
#   transport_snapshots          : snap_ref_lines, snap_ref_stops (SCD2)
# ─────────────────────────────────────────────────────────────
