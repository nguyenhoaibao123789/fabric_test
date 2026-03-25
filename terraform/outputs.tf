output "environment_id" {
  value       = fabric_environment.spark_env.id
  description = "Fabric Spark Environment item ID"
}

output "airflow_id" {
  value       = fabric_apache_airflow_job.airflow.id
  description = "Fabric Apache Airflow Job item ID — replaces airflow_id in resource_ids_dev.json"
}

# Pass these to deploy.py: python deploy.py --env dev
# deploy.py will set them as Airflow Variables automatically
output "airflow_variables" {
  description = "Airflow Variables to set via deploy.py after terraform apply"
  value = {
    notebook_id__bronze__ingest_file            = fabric_notebook.notebooks["ingest_file"].id
    notebook_id__silver1__process_invoice       = fabric_notebook.notebooks["process_invoice"].id
    notebook_id__silver2__build_carrier_invoice = fabric_notebook.notebooks["build_carrier_invoice"].id
  }
}
