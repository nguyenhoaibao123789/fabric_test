locals {
  # Read pip packages from the same file deploy.py used
  pip_libraries = [
    for line in split("\n", file("${path.module}/../config/notebook_requirements.txt")) :
    trimspace(line)
    if trimspace(line) != "" && !startswith(trimspace(line), "#")
  ]

  # Map notebook display name → source .ipynb path
  notebooks = {
    "shared_functions" = {
      ipynb_path = "${path.module}/../fabric/notebooks/shared/shared_functions.ipynb"
    }
    "ingest_file" = {
      ipynb_path = "${path.module}/../fabric/notebooks/bronze/ingest_file.ipynb"
    }
    "process_invoice" = {
      ipynb_path = "${path.module}/../fabric/notebooks/silver1/process_invoice.ipynb"
    }
    "build_carrier_invoice" = {
      ipynb_path = "${path.module}/../fabric/notebooks/silver2/build_carrier_invoice.ipynb"
    }
  }
}


# ── Spark Environment ──────────────────────────────────────────────────────────

resource "fabric_environment" "spark_env" {
  workspace_id = var.workspace_id
  display_name = var.spark_env_name
}

resource "fabric_environment_spark_libraries" "pip" {
  workspace_id   = var.workspace_id
  environment_id = fabric_environment.spark_env.id

  pypi_libraries = [
    for pkg in local.pip_libraries : { package = pkg }
  ]
}


# ── Notebooks ──────────────────────────────────────────────────────────────────

resource "fabric_notebook" "notebooks" {
  for_each = local.notebooks

  workspace_id = var.workspace_id
  display_name = each.key

  # Build an ipynb payload matching the format Fabric expects.
  # Mirrors _encode_notebook() in deploy.py.
  definition = {
    "artifact.content.ipynb" = {
      # Inject the environment dependency into the notebook metadata at deploy time.
      # The .ipynb files in the repo do not contain environment IDs (they vary per env).
      source = base64encode(jsonencode(merge(
        jsondecode(file(each.value.ipynb_path)),
        {
          metadata = merge(
            jsondecode(file(each.value.ipynb_path)).metadata,
            {
              dependencies = {
                environment = {
                  environmentId = fabric_environment.spark_env.id
                  workspaceId   = var.workspace_id
                }
              }
            }
          )
        }
      )))
    }
  }

  depends_on = [fabric_environment_spark_libraries.pip]
}
