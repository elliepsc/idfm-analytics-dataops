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
