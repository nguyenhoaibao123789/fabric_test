# Fabric Medallion Pipeline

Carrier invoice data pipeline — Bronze → Silver → Gold — running on Microsoft Fabric.

---

## Architecture

```
Files/landing/<carrier>/       Files/bronze/<carrier>/     Tables/silver/               Gold Warehouse
────────────────────────  →    ──────────────────────  →   ──────────────────────  →    ──────────────
Client uploads here            Immutable raw archive        Delta tables                 dim_* / fact_invoice
                               ingest_file.py               process_invoice.py           dbt models
                                                            build_carrier_invoice.py
```

Orchestrated by **Managed Airflow** (created by Terraform). DAGs are synced directly from this Git repository — no manual file uploads needed. Gold (`dbt run`) triggers as soon as its upstream Silver 2 entity succeeds — it does not wait for unrelated Silver 2 tasks.

---

## What Terraform manages

```
terraform apply
  ├── fabric_environment          Spark Environment (packages installed separately via deploy.py)
  ├── fabric_notebook [x4]        shared_functions, ingest_file, process_invoice, build_carrier_invoice
  └── fabric_apache_airflow_job   Airflow instance + Git sync to this repo
```

Everything else (Lakehouse, Data Warehouse, Gold DDL) is created once manually.

---

## First-Time Setup

Complete these steps in order on a fresh environment.

### Step 1 — Create resources in the Fabric UI

Log in to [app.fabric.microsoft.com](https://app.fabric.microsoft.com) and create the following items manually inside a new workspace named `dev-fabric-data`:

| Item | Type | Name |
|---|---|---|
| Workspace | Workspace | `dev-fabric-data` |
| Medallion storage | Lakehouse | `fabric_lakehouse` |
| Gold storage | Data Warehouse | `fabric_gold_warehouse` |

> Airflow is created by Terraform in Step 5 — do not create it manually.

### Step 2 — Create landing zone folders

Inside `fabric_lakehouse` → `Files/`, create these folders manually via the Lakehouse UI:

```
Files/
├── landing/
│   ├── fedex/invoice/
│   ├── dhl/invoice/
│   └── ups/carrier/
├── bronze/          ← pipeline creates subfolders automatically
└── shared/          ← shared_functions.py goes here (deploy.py uploads it)
```

### Step 3 — Fill in config files

**`config/dev.json`** — workspace name and Gold Warehouse SQL endpoint:
```json
{
  "workspace_name": "dev-fabric-data",
  "lakehouse": "fabric_lakehouse",
  "gold_warehouse": "fabric_gold_warehouse",
  "gold_warehouse_sql_endpoint": "<workspace-id>.datawarehouse.fabric.microsoft.com"
}
```

> The workspace ID is visible in the browser URL when you are inside the workspace:
> `https://app.fabric.microsoft.com/groups/<workspace-id>/...`

### Step 4 — Run the Gold Warehouse DDL

Open `fabric_gold_warehouse` in the Fabric UI → SQL editor, then run:

```
warehouse/create_tables.sql
```

This creates the Gold dimension and fact tables with `IDENTITY` primary keys. **Do this before triggering the pipeline for the first time.** dbt will not create these tables itself — it only merges into them.

> **Never run `dbt run --full-refresh` in production** — it drops and recreates tables, losing `IDENTITY` definitions.

### Step 5 — Set up local Python environment

```bash
python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # Mac/Linux

pip install -r requirements.txt
nbstripout --install          # register git filter for .ipynb files (run once)
```

### Step 6 — Install Terraform and Azure CLI

These are system tools, not Python packages — install them separately:

- **Terraform** >= 1.6 → [developer.hashicorp.com/terraform/install](https://developer.hashicorp.com/terraform/install)
- **Azure CLI** → [learn.microsoft.com/en-us/cli/azure/install-azure-cli](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli)

Then log in once with your Fabric account (same credentials as `app.fabric.microsoft.com`):

```bash
az login
```

### Step 7 — Create your Terraform variables file

Copy the example and fill in your values:

```bash
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
```

Edit `terraform/terraform.tfvars`:

```hcl
workspace_id   = "<workspace-id>"          # from the Fabric portal URL
spark_env_name = "medallion-env-dev"

# Git sync — Airflow pulls DAGs directly from this repo
git_repo_url      = "https://github.com/your-org/your-repo"
git_branch        = "main"
git_dags_folder   = "fabric/dags"
git_connection_id = ""                     # leave empty for public repos
```

> `terraform.tfvars` is gitignored — never commit it.

### Step 8 — Deploy with Terraform

```bash
cd terraform
terraform init
terraform apply
```

This creates:
- Fabric Spark Environment
- All 4 notebooks
- Managed Airflow instance with Git sync pointed at `fabric/dags/`

After `apply` completes, note the output values — you will need the notebook IDs in the next step.

### Step 9 — Install Spark environment libraries

The Fabric Terraform provider does not support PyPI library management. Run `deploy.py` to install packages from `config/notebook_requirements.txt` into the Spark environment:

```bash
cd ..   # back to repo root
python deploy.py --env dev
```

This also sets the required Airflow Variables automatically (see list below).

### Step 10 — Set Airflow Variables

`deploy.py` sets most variables automatically. Verify in the Airflow UI (**Admin → Variables**) that these are present:

| Variable | Value |
|---|---|
| `fabric_workspace_id` | workspace GUID |
| `notebook_id__bronze__ingest_file` | from `terraform output` |
| `notebook_id__silver1__process_invoice` | from `terraform output` |
| `notebook_id__silver2__build_carrier_invoice` | from `terraform output` |
| `environment` | `dev` |
| `dbt_project_dir` | `/opt/airflow/dags/dbt` |
| `dbt_warehouse_server` | `<workspace-id>.datawarehouse.fabric.microsoft.com` |
| `lakehouse` | `fabric_lakehouse` |
| `azure_tenant_id` | Service principal tenant ID |
| `azure_client_id` | Service principal app (client) ID |
| `azure_client_secret` | Service principal secret |

---

## Re-deploying after changes

| Change | Command |
|---|---|
| Notebook code changed | `terraform apply` |
| DAG or dbt model changed | `git push` — Git sync picks it up automatically |
| New source added (`sources.json`) | Update file → `git push` |
| Added/removed Spark packages (`notebook_requirements.txt`) | `python deploy.py --env dev` |
| Gold schema changed | Update `create_tables.sql` → run DDL manually in Gold Warehouse SQL editor |

---

## dbt Gold Layer

The Silver → Gold transformation runs via **dbt** (`dbt-fabric` adapter) inside Airflow. To run it locally for testing:

```bash
export DBT_WAREHOUSE_SERVER="<workspace-id>.datawarehouse.fabric.microsoft.com"
export AZURE_TENANT_ID="..."
export AZURE_CLIENT_ID="..."
export AZURE_CLIENT_SECRET="..."

cd fabric/dags/dbt
dbt deps                              # first time only
dbt run  --profiles-dir . --target dev
dbt test --profiles-dir . --target dev
```

---

## Project Structure

```
fabric_test/
├── deploy.py                         # Installs Spark libraries + sets Airflow Variables
├── fabric.yaml                       # Project metadata
├── config/
│   ├── dev.json                      # workspace name, lakehouse name, endpoints
│   ├── prod.json                     # create manually — do not commit
│   ├── sources.json                  # one entry per carrier/file type
│   ├── schedules.json                # DAG schedule groups and cron expressions
│   ├── notebook_requirements.txt     # pip packages installed in the Fabric Spark Environment
│   └── resource_ids_dev.json         # workspace_id only — airflow_id now comes from terraform output
├── terraform/
│   ├── providers.tf                  # microsoft/fabric provider, preview mode enabled
│   ├── variables.tf                  # workspace_id, spark_env_name, git_* variables
│   ├── main.tf                       # Spark Environment, Notebooks, Airflow Job
│   ├── outputs.tf                    # environment_id, airflow_id, airflow_variables
│   ├── airflow-content.json          # Airflow Job definition (Git sync config)
│   └── terraform.tfvars.example      # copy to terraform.tfvars and fill in values
├── fabric/
│   ├── notebooks/
│   │   ├── shared/shared_functions.ipynb
│   │   ├── bronze/ingest_file.ipynb
│   │   ├── silver1/process_invoice.ipynb
│   │   └── silver2/build_carrier_invoice.ipynb
│   └── dags/                         # Git sync root → Airflow picks up changes on push
│       ├── medallion_pipeline.py     # DAG factory — one DAG per schedule group
│       ├── callbacks.py              # Teams failure/SLA alerts
│       └── dbt/                      # Gold layer (dbt-fabric adapter)
│           ├── dbt_project.yml
│           ├── profiles.yml
│           ├── packages.yml
│           └── models/
│               ├── sources.yml
│               └── gold/
│                   ├── dim_carrier.sql
│                   ├── dim_service_type.sql
│                   ├── dim_date.sql
│                   ├── fact_invoice.sql
│                   └── schema.yml
└── warehouse/
    └── create_tables.sql             # run once manually in Gold Warehouse SQL editor
```

### Lakehouse layout

```
fabric_lakehouse/
├── Files/
│   ├── landing/                      ← clients upload raw files here
│   │   ├── fedex/invoice/
│   │   ├── dhl/invoice/
│   │   └── ups/carrier/
│   ├── bronze/                       ← pipeline archives files here (never deleted)
│   │   ├── fedex/invoice/
│   │   ├── dhl/invoice/
│   │   └── ups/carrier/
│   └── shared/
│       └── shared_functions.py
└── Tables/
    └── silver/                       ← Delta tables (SQL-queryable as silver.*)
        ├── staging_fedex_direct_invoice/
        ├── staging_dhl_direct_invoice/
        ├── staging_ups_carrier_report/
        └── carrier_invoice/
```

---

## Notes

- `terraform/terraform.tfvars` and `config/prod.json` are gitignored — never commit them.
- Airflow is now fully managed by Terraform — do not create or delete it manually in the Fabric portal.
- The Terraform provider does not support PyPI library installation — always run `deploy.py` after `terraform apply` to install Spark packages.
- The exact JSON field names in `terraform/airflow-content.json` (Git sync config) may need adjustment if the Fabric API changes — check Microsoft docs if `terraform apply` returns a 400 on the Airflow resource.
- Gold layer is fully managed by dbt — see `fabric/dags/dbt/models/gold/`.
