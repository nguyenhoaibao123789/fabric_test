locals {
  # Map notebook display name → source .ipynb path
  notebooks = {
    "shared_functions" = {
      ipynb_path = "${path.module}/../notebooks/shared/shared_functions.ipynb"
    }
    "setup_create_silver_tables" = {
      ipynb_path = "${path.module}/../notebooks/setup/create_silver_tables.ipynb"
    }
    "bronze_ingest_file" = {
      ipynb_path = "${path.module}/../notebooks/bronze/ingest_file.ipynb"
    }
    "silver1_clean" = {
      ipynb_path = "${path.module}/../notebooks/silver1/clean.ipynb"
    }
    "silver2_combine" = {
      ipynb_path = "${path.module}/../notebooks/silver2/combine.ipynb"
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

