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

Orchestrated by **Managed Airflow**. Gold (`dbt run`) triggers as soon as its upstream Silver 2 entity succeeds — it does not wait for unrelated Silver 2 tasks.

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
| Orchestration | Managed Airflow | `fabric-airflow-dev` |

> Your Fabric trial capacity is already provisioned — no Azure subscription needed.

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

Clients upload raw files to `Files/landing/<carrier>/`. The Bronze notebook moves them to `Files/bronze/<carrier>/` automatically.

### Step 3 — Collect IDs from the Fabric portal

You need two GUIDs before filling in config files.

**Workspace ID** — visible in the browser URL when you are inside the workspace:
```
https://app.fabric.microsoft.com/groups/<workspace-id>/...
```

**Airflow item ID** — open `fabric-airflow-dev` → Settings → copy the ID from the URL.

### Step 4 — Fill in config files

**`config/dev.json`** — workspace name and Gold Warehouse SQL endpoint:
```json
{
  "workspace_name": "dev-fabric-data",
  "lakehouse": "fabric_lakehouse",
  "gold_warehouse": "fabric_gold_warehouse",
  "gold_warehouse_sql_endpoint": "<workspace-id>.datawarehouse.fabric.microsoft.com"
}
```

**`config/resource_ids_dev.json`** — IDs used by `deploy.py` (create this file manually, do not commit):
```json
{
  "workspace_id": "<workspace-id>",
  "airflow_id":   "<airflow-item-id>"
}
```

### Step 5 — Run the Gold Warehouse DDL

Open `fabric_gold_warehouse` in the Fabric UI → SQL editor, then run:

```
fabric/warehouse/sql/create_tables.sql
```

This creates the Gold dimension and fact tables with `IDENTITY` primary keys. **Do this before triggering the pipeline for the first time.** dbt will not create these tables itself — it only merges into them.

> **Never run `dbt run --full-refresh` in production** — it drops and recreates tables, losing `IDENTITY` definitions.

### Step 6 — Set up local Python environment

```bash
python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # Mac/Linux

pip install -r requirements.txt
nbstripout --install          # register git filter for .ipynb files (run once)
```

### Step 7 — Install Terraform and Azure CLI

These are system tools, not Python packages — install them separately:

- **Terraform** >= 1.6 → [developer.hashicorp.com/terraform/install](https://developer.hashicorp.com/terraform/install)
- **Azure CLI** → [learn.microsoft.com/en-us/cli/azure/install-azure-cli](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli)

Then log in once with your Fabric account (same credentials as `app.fabric.microsoft.com`):

```bash
az login
```

### Step 8 — Deploy Spark Environment and Notebooks

```bash
cd terraform
terraform init
terraform apply -var-file=environments/dev.tfvars
```

This creates/updates:
- Fabric Spark Environment (`medallion-env-dev`) with packages from `config/notebook_requirements.txt`
- All notebooks with the environment attached

After `apply` completes, Terraform prints the notebook IDs needed in the next step.

### Step 9 — Connect DAGs via Git sync

In the Fabric portal, connect the workspace Git integration to your repo and set the sync path to `fabric/dags/`. On each push, Airflow will automatically pick up DAG and dbt changes.

### Step 10 — Set Airflow Variables manually

In the Airflow UI: **Admin → Variables**, add:

| Variable | Value |
|---|---|
| `fabric_workspace_id` | `8dac972c-0ff1-400f-bba3-e1e2302070b5` |
| `notebook_id__bronze__ingest_file` | *(from `terraform apply` output)* |
| `notebook_id__silver1__process_invoice` | *(from `terraform apply` output)* |
| `notebook_id__silver2__build_carrier_invoice` | *(from `terraform apply` output)* |
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
| Notebook code changed | `terraform apply -var-file=environments/dev.tfvars` |
| Added/removed notebook packages (`notebook_requirements.txt`) | `terraform apply -var-file=environments/dev.tfvars` |
| DAG or dbt model changed | `git push` — Git sync picks it up automatically |
| New source added (`sources.json`) | Update file → `git push` |
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
fabric/
├── deploy.py                         # Deploys Spark environment, notebooks, DAGs, Airflow variables
├── fabric.yaml                       # Spark env names, notebook paths, DAG paths
├── config/
│   ├── dev.json                      # workspace name, lakehouse name, endpoints
│   ├── prod.json                     # create manually — do not commit
│   ├── sources.json                  # one entry per carrier/file type (feeds_gold, schedule_group, etc.)
│   ├── schedules.json                # DAG schedule groups and cron expressions
│   ├── notebook_requirements.txt     # pip packages installed in the Fabric Spark Environment
│   ├── resource_ids_dev.json         # workspace_id + airflow_id — create manually, do not commit
│   └── resource_ids_prod.json        # same for prod — do not commit
└── fabric/
    ├── notebooks/
    │   ├── shared/shared_functions.py   # OneLake, DW connection, PySpark helpers
    │   ├── bronze/ingest_file.py        # landing/ → bronze/ file move
    │   ├── silver1/process_invoice.py   # clean + MERGE into Tables/silver/staging_*
    │   └── silver2/build_carrier_invoice.py  # UNION staging tables → Tables/silver/carrier_invoice
    ├── dags/                            # Git sync root → /opt/airflow/dags/
    │   ├── medallion_pipeline.py        # DAG factory — one DAG per schedule group
    │   ├── callbacks.py                 # Teams failure/SLA alerts
    │   └── dbt/                         # Gold layer (dbt-fabric adapter)
    │       ├── dbt_project.yml
    │       ├── profiles.yml
    │       ├── packages.yml
    │       └── models/
    │           ├── sources.yml          # declares silver.carrier_invoice from Lakehouse
    │           └── gold/
    │               ├── dim_carrier.sql
    │               ├── dim_service_type.sql
    │               ├── dim_date.sql
    │               ├── fact_invoice.sql
    │               └── schema.yml
    └── warehouse/sql/
        └── create_tables.sql            # run once manually in Gold Warehouse SQL editor
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

- `resource_ids_*.json` and `config/prod.json` are in `.gitignore` — never commit them.
- DAG and dbt file uploads use a preview Fabric API. If they fail, copy files manually via the Managed Airflow UI.
- Gold layer is fully managed by dbt — see `dbt/models/gold/`.
