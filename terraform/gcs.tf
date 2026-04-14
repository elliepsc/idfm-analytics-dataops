# ─────────────────────────────────────────────────────────────
# GCS LANDING ZONE — raw NDJSON files before BigQuery load
# ─────────────────────────────────────────────────────────────
# Architecture: API → GCS (NDJSON, persistent) → BigQuery RAW → dbt
#
# Layout:
#   gs://{bucket}/validations/validations_YYYY-MM-DD_YYYY-MM-DD.json
#   gs://{bucket}/punctuality/punctuality_YYYY-MM_YYYY-MM.json
#   gs://{bucket}/referentials/ref_stops_YYYYMMDD.json
#   gs://{bucket}/referentials/ref_lines_YYYYMMDD.json
#   gs://{bucket}/referentials/ref_stop_lines_YYYYMMDDTHHmmss.json
#   gs://{bucket}/referentials/ref_stations_YYYYMMDD.json
#   gs://{bucket}/referentials/ref_stop_id_mapping_YYYYMMDDTHHmmss.json
# ─────────────────────────────────────────────────────────────

resource "google_storage_bucket" "raw_landing" {
  name          = var.gcs_bucket_raw
  project       = var.project_id
  location      = var.region
  storage_class = "STANDARD"

  # Prevent accidental deletion of historical raw files
  force_destroy = false

  uniform_bucket_level_access = true

  labels = {
    env   = "prod"
    layer = "landing"
  }

  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
}
