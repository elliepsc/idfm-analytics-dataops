variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "europe-west1"
}

variable "location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "europe-west1"
}

variable "gcs_bucket_raw" {
  description = "GCS bucket name for the raw NDJSON landing zone"
  type        = string
  default     = "idfm-analytics-raw"
}
