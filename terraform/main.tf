locals {
  # Map notebook display name → source .ipynb path
  notebooks = {
    "setup_create_silver_tables" = {
      ipynb_path = "${path.module}/../notebooks/setup/create_silver_tables.ipynb"
    }
    "bronze_ingest_file" = {
      ipynb_path = "${path.module}/../notebooks/bronze/ingest_file.ipynb"
    }
    "silver1_clean" = {
      ipynb_path = "${path.module}/../notebooks/silver1/clean.ipynb"
    }
    "hello_world" = {
      ipynb_path = "${path.module}/../notebooks/test/hello_world.ipynb"
    }
  }
}


# ── Notebooks ──────────────────────────────────────────────────────────────────

resource "fabric_notebook" "notebooks" {
  for_each = local.notebooks

  workspace_id = var.workspace_id
  display_name = each.key
  format       = "ipynb"

  definition = {
    "notebook-content.ipynb" = {
      source          = each.value.ipynb_path
      processing_mode = "None"
    }
  }
}
