output "environment_id" {
  value       = fabric_environment.spark_env.id
  description = "Fabric Spark Environment item ID"
}

# Set these in Airflow UI: Admin → Variables
output "airflow_variables" {
  description = "Airflow Variables to set manually after terraform apply"
  value = {
    notebook_id__bronze__ingest_file              = fabric_notebook.notebooks["ingest_file"].id
    notebook_id__silver1__process_invoice         = fabric_notebook.notebooks["process_invoice"].id
    notebook_id__silver2__build_carrier_invoice   = fabric_notebook.notebooks["build_carrier_invoice"].id
  }
}
