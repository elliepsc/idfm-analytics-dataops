output "raw_dataset_id" {
  description = "BigQuery raw dataset ID"
  value       = google_bigquery_dataset.transport_raw.dataset_id
}

output "core_dataset_id" {
  description = "BigQuery core dataset ID (prod)"
  value       = google_bigquery_dataset.transport_staging_core.dataset_id
}

output "analytics_dataset_id" {
  description = "BigQuery analytics dataset ID (prod)"
  value       = google_bigquery_dataset.transport_staging_analytics.dataset_id
}

output "staging_dataset_id" {
  description = "BigQuery staging dataset ID (prod)"
  value       = google_bigquery_dataset.transport_staging_staging.dataset_id
}

output "snapshots_dataset_id" {
  description = "BigQuery snapshots dataset ID"
  value       = google_bigquery_dataset.transport_snapshots.dataset_id
}

output "dev_core_dataset_id" {
  description = "BigQuery core dataset ID (dev)"
  value       = google_bigquery_dataset.transport_staging_dev_core.dataset_id
}

output "dev_analytics_dataset_id" {
  description = "BigQuery analytics dataset ID (dev)"
  value       = google_bigquery_dataset.transport_staging_dev_analytics.dataset_id
}
