# Fabric Medallion Pipeline

Carrier data pipeline — Bronze → Silver → Gold — running on Microsoft Fabric.

---

## Architecture

```
Files/landing/<carrier>/       Files/bronze/<carrier>/     Tables/silver/               Gold Warehouse
────────────────────────  →    ──────────────────────  →   ──────────────────────  →    ──────────────
Client uploads here            Immutable raw archive        Delta tables                 dim_* / fact_*
                               ingest_file notebook         clean notebook               dbt models
                                                            combine notebook
```

Orchestrated by **Apache Airflow Job** (created by Terraform). DAGs are synced directly from this Git repository — no manual file uploads needed.

### DAGs generated

| DAG | Schedule | Trigger |
|---|---|---|
| `medallion_carrier_invoice` | `0 6 * * *` | cron |
| `medallion_carrier_tracking` | `0 * * * *` | cron |
| `medallion_rate_card` | `0 6 * * 0` | cron |
| `gold_fact_invoice` | — | Dataset: `carrier_invoice` Silver 2 updated |
| `gold_dim_carrier` | — | Dataset: `carrier_rate_card` AND `carrier_tracking` Silver 2 updated |

Gold DAGs use **Airflow Datasets** — they fire automatically when all required Silver 2 tasks complete, with no cron needed.

---

## What Terraform manages

```
terraform apply
  ├── fabric_environment          Spark Environment
  ├── fabric_notebook [x4]        shared_functions, bronze_ingest_file, silver1_clean, silver2_combine
  └── fabric_apache_airflow_job   Airflow instance (Git sync configured separately via portal UI)
```

Everything else (Lakehouse, Data Warehouse, Gold DDL, Git sync, Airflow Variables) is set up manually.

---

## First-Time Setup

### Step 1 — Create resources in the Fabric UI

Log in to [app.fabric.microsoft.com](https://app.fabric.microsoft.com) and create:

| Item | Type | Name |
|---|---|---|
| Workspace | Workspace | `dev-fabric-data` |
| Medallion storage | Lakehouse | `fabric_lakehouse` |
| Gold storage | Data Warehouse | `fabric_gold_warehouse` |

### Step 2 — Create landing zone folders

Inside `fabric_lakehouse` → `Files/`, create these folders manually:

```
Files/
├── landing/
│   ├── fedex/invoice/
│   ├── fedex/rate_card/
│   ├── dhl/invoice/
│   ├── ups/carrier/
│   └── usps/tracking/
├── bronze/
└── shared/
```

### Step 3 — Fill in config files

**`dags/config/dev.yaml`** — workspace name, workspace ID, and Gold Warehouse SQL endpoint:

```yaml
environment: dev
workspace_name: dev-fabric-data
fabric_workspace_id: "<workspace-id>"
fabric_conn_id: fabric_default
lakehouse: fabric_lakehouse
gold_warehouse: fabric_gold_warehouse
gold_warehouse_sql_endpoint: "<workspace-id>.datawarehouse.fabric.microsoft.com"
```

> The workspace ID is in the browser URL: `https://app.fabric.microsoft.com/groups/<workspace-id>/...`

### Step 4 — Run the Gold Warehouse DDL

Open `fabric_gold_warehouse` → SQL editor, run `warehouse/create_tables.sql`.

### Step 4b — Create Silver Lakehouse tables

After `terraform apply`, open the `setup_create_silver_tables` notebook in Fabric and run it once.
This creates all Silver 1 staging and Silver 2 Delta tables in the Lakehouse before any DAGs run.
Safe to re-run — existing tables and data are never modified.

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

- **Terraform** >= 1.6 → [developer.hashicorp.com/terraform/install](https://developer.hashicorp.com/terraform/install)
- **Azure CLI** → [learn.microsoft.com/en-us/cli/azure/install-azure-cli](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli)

```bash
az login
```

### Step 7 — Create your Terraform variables file

```bash
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
```

Edit `terraform/terraform.tfvars`:

```hcl
workspace_id   = "<workspace-id>"
spark_env_name = "dev-spark-env"
```

> `terraform.tfvars` is gitignored — never commit it.

### Step 8 — Deploy with Terraform

```bash
cd terraform
terraform init
terraform apply
```

This creates the Spark Environment, all 4 notebooks, and the Airflow job.

### Step 9 — Configure Git sync on the Airflow job

In the Fabric portal, open the Airflow job → **Settings → File storage → Git sync**:

| Field | Value |
|---|---|
| Git service type | Github |
| Git credential type | None (public repo) |
| Repository | `https://github.com/your-org/your-repo.git` |
| Branch | `main` |

Click **Apply**. Airflow will sync DAGs from `dags/` in the repo.

### Step 10 — Configure Airflow environment

In the Fabric portal → Airflow job → **Settings → Environment configuration**:

**Apache Airflow requirements:**
```
apache-airflow-microsoft-fabric-plugin
```

Click **Apply**, then **Stop** and **Start** the Airflow job.

### Step 11 — Create Fabric connection in Airflow

In the Airflow UI (**Monitor Airflow** → **Admin → Connections → + Add**):

| Field | Value |
|---|---|
| Conn Id | `fabric_default` |
| Conn Type | Microsoft Fabric |

Authentication uses the workspace managed identity — no Azure credentials required.

### Step 12 — Import Airflow Variables

After `terraform apply`, generate the variables file:

```bash
cd terraform
terraform output -json airflow_variables > ../airflow-variables.json
```

Then in the Airflow UI → **Admin → Variables → Import Variables**, upload `airflow-variables.json`.

> `airflow-variables.json` is gitignored — it contains notebook IDs tied to your specific workspace.

---

## Re-deploying after changes

| Change | Action |
|---|---|
| Notebook code changed | `terraform apply` |
| DAG, config, or dbt model changed | `git push` — Git sync picks it up automatically |
| New source added (`sources.yaml`) | Update file → `git push` |
| New subject/schedule added | Add block to `sources.yaml` → `git push` |
| Gold schema changed | Update `warehouse/create_tables.sql` → run DDL manually |

---

## dbt Gold Layer

To run locally for testing:

```bash
export DBT_WAREHOUSE_SERVER="<workspace-id>.datawarehouse.fabric.microsoft.com"
export AZURE_TENANT_ID="..."
export AZURE_CLIENT_ID="..."
export AZURE_CLIENT_SECRET="..."

cd dags/dbt
dbt deps                              # first time only
dbt run  --profiles-dir . --target dev
dbt test --profiles-dir . --target dev
```

---

## Project Structure

```
fabric_test/
├── dags/                             # Git-synced to Airflow automatically on push
│   ├── medallion_pipeline.py         # DAG factory — generates medallion + gold DAGs from YAML
│   ├── callbacks.py                  # Teams failure/SLA alerts
│   ├── requirements.txt
│   ├── config/
│   │   ├── dev.yaml                  # workspace name, IDs, endpoints per environment
│   │   └── sources.yaml              # subjects, sources, cron schedules, gold_tables
│   └── dbt/                          # Gold layer (dbt-fabric adapter)
│       ├── dbt_project.yml
│       ├── profiles.yml
│       ├── packages.yml
│       └── models/
│           ├── sources.yml
│           └── gold/
│               ├── dim_carrier.sql
│               ├── dim_service_type.sql
│               ├── dim_date.sql
│               ├── fact_invoice.sql
│               └── schema.yml
├── notebooks/                        # Uploaded to Fabric via Terraform
│   ├── shared/shared_functions.ipynb
│   ├── bronze/ingest_file.ipynb
│   ├── silver1/clean.ipynb
│   └── silver2/combine.ipynb
├── terraform/
│   ├── providers.tf                  # microsoft/fabric provider
│   ├── variables.tf
│   ├── main.tf                       # Spark Environment, Notebooks, Airflow Job
│   ├── outputs.tf
│   ├── airflow-content.json          # Airflow Job definition
│   └── terraform.tfvars.example
└── warehouse/
    └── create_tables.sql             # run once manually in Gold Warehouse SQL editor
```

### Lakehouse layout

```
fabric_lakehouse/
├── Files/
│   ├── landing/                      ← clients upload raw files here
│   │   ├── fedex/invoice/
│   │   ├── fedex/rate_card/
│   │   ├── dhl/invoice/
│   │   ├── ups/carrier/
│   │   └── usps/tracking/
│   ├── bronze/                       ← pipeline archives files here (never deleted)
│   └── shared/
│       └── shared_functions.py
└── Tables/
    └── silver/                       ← Delta tables (SQL-queryable as silver.*)
        ├── staging_fedex_direct_invoice/
        ├── staging_dhl_direct_invoice/
        ├── staging_ups_carrier_report/
        ├── staging_usps_tracking/
        ├── staging_fedex_rate_card/
        ├── carrier_invoice/
        ├── carrier_tracking/
        └── carrier_rate_card/
```

---

## Notes

- `terraform/terraform.tfvars` is gitignored — never commit it.
- Git sync is configured via the Fabric portal UI (Settings → File storage) — not managed by Terraform.
- Spark package installation is not supported by the Terraform provider — add packages via the portal (Settings → Environment configuration → Apache Airflow requirements).
- Gold layer is fully managed by dbt — see `dags/dbt/models/gold/`.
- Notebook IDs are resolved at Airflow parse time via the Fabric REST API — no Airflow Variables needed for notebook mappings.
