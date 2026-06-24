output "lakehouse_bucket" {
  description = "GCS bucket for all lakehouse layers"
  value       = google_storage_bucket.lakehouse.name
}

output "dataproc_cluster_name" {
  description = "Dataproc cluster name"
  value       = google_dataproc_cluster.spark.name
}

output "bigquery_dataset" {
  description = "BigQuery Gold dataset ID"
  value       = google_bigquery_dataset.gold.dataset_id
}

output "composer_airflow_uri" {
  description = "Cloud Composer Airflow web UI URL"
  value       = google_composer_environment.orchestrator.config[0].airflow_uri
}

output "dataplex_lake_name" {
  description = "DataPlex lake resource name"
  value       = google_dataplex_lake.lakehouse.name
}

output "dataproc_service_account" {
  description = "Service account email used by Dataproc"
  value       = google_service_account.dataproc.email
}
