locals {
  # Read workspace/lakehouse IDs from the shared config — single source of truth
  config       = yamldecode(file("${path.module}/../dags/config/${var.env}.yaml"))
  workspace_id  = local.config["fabric_workspace_id"]
  lakehouse_id  = local.config["fabric_lakehouse_id"]
  warehouse_id  = local.config["fabric_warehouse_id"]

  # Map notebook display name → source .ipynb path + whether to inject lakehouse tokens
  notebooks = {
    "setup_create_silver_tables" = {
      ipynb_path       = "${path.module}/../notebooks/setup/create_silver_tables.ipynb"
      inject_lakehouse = true
    }
    "bronze_ingest_file" = {
      ipynb_path       = "${path.module}/../notebooks/bronze/ingest_file.ipynb"
      inject_lakehouse = true
    }
    "silver1_clean" = {
      ipynb_path       = "${path.module}/../notebooks/silver1/clean.ipynb"
      inject_lakehouse = true
    }
    "hello_world" = {
      ipynb_path       = "${path.module}/../notebooks/test/hello_world.ipynb"
      inject_lakehouse = false
    }
  }
}


# ── Notebooks ──────────────────────────────────────────────────────────────────

resource "fabric_notebook" "notebooks" {
  for_each = local.notebooks

  workspace_id = local.workspace_id
  display_name = each.key
  format       = "ipynb"

  definition = {
    "notebook-content.ipynb" = {
      source          = each.value.ipynb_path
      processing_mode = each.value.inject_lakehouse ? "GoTemplate" : "None"

      tokens = each.value.inject_lakehouse ? {
        lakehouse_id = local.lakehouse_id
        warehouse_id = local.warehouse_id
        workspace_id = local.workspace_id
      } : null
    }
  }
}
