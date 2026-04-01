output "environment_id" {
  value       = fabric_environment.spark_env.id
  description = "Fabric Spark Environment item ID"
}

output "airflow_variables" {
  description = "Airflow Variables to set after terraform apply"
  value = {
    bronze_ingest_file = fabric_notebook.notebooks["bronze_ingest_file"].id
    silver1_clean      = fabric_notebook.notebooks["silver1_clean"].id
  }
}
