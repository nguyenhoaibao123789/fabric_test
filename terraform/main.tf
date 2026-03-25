locals {
  # Map notebook display name → source .ipynb path
  notebooks = {
    "shared_functions" = {
      ipynb_path = "${path.module}/../notebooks/shared/shared_functions.ipynb"
    }
    "ingest_file" = {
      ipynb_path = "${path.module}/../notebooks/bronze/ingest_file.ipynb"
    }
    "process_invoice" = {
      ipynb_path = "${path.module}/../notebooks/silver1/process_invoice.ipynb"
    }
    "build_carrier_invoice" = {
      ipynb_path = "${path.module}/../notebooks/silver2/build_carrier_invoice.ipynb"
    }
  }
}


# ── Spark Environment ──────────────────────────────────────────────────────────

resource "fabric_environment" "spark_env" {
  workspace_id = var.workspace_id
  display_name = var.spark_env_name
}

# ── Notebooks ──────────────────────────────────────────────────────────────────
# Note: PyPI library installation is managed by deploy.py via the Fabric REST API,
# not via Terraform (the microsoft/fabric provider v1.x has no resource for it).

resource "fabric_notebook" "notebooks" {
  for_each = local.notebooks

  workspace_id = var.workspace_id
  display_name = each.key
  format       = "ipynb"

  # Upload the notebook file. Environment attachment (environmentId in metadata)
  # is handled post-deploy by deploy.py, which patches the notebook via REST API.
  definition = {
    "notebook-content.ipynb" = {
      source          = each.value.ipynb_path
      processing_mode = "None"
    }
  }

  depends_on = [fabric_environment.spark_env]
}


# ── Airflow ─────────────────────────────────────────────────────────────────────

resource "fabric_apache_airflow_job" "airflow" {
  workspace_id = var.workspace_id
  display_name = "dev-medallion-airflow"
  format       = "Default"

  definition = {
    "apacheairflowjob-content.json" = {
      source          = "${path.module}/airflow-content.json"
      processing_mode = "GoTemplate"
      tokens = {
        git_repo_url    = var.git_repo_url
        git_branch      = var.git_branch
        git_dags_folder = var.git_dags_folder
      }
    }
  }
}
