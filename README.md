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

Orchestrated by **Apache Airflow Job** (created by Terraform). DAGs are synced directly from this Git repository via Git sync — no manual file uploads needed. Gold (`dbt run`) triggers as soon as its upstream Silver 2 entity succeeds.

---

## What Terraform manages

```
terraform apply
  ├── fabric_environment          Spark Environment
  ├── fabric_notebook [x4]        shared_functions, ingest_file, process_invoice, build_carrier_invoice
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
│   ├── dhl/invoice/
│   └── ups/carrier/
├── bronze/
└── shared/
```

### Step 3 — Fill in config files

**`dags/config/dev.json`** — workspace name and Gold Warehouse SQL endpoint:
```json
{
  "workspace_name": "dev-fabric-data",
  "lakehouse": "fabric_lakehouse",
  "gold_warehouse": "fabric_gold_warehouse",
  "gold_warehouse_sql_endpoint": "<workspace-id>.datawarehouse.fabric.microsoft.com"
}
```

> The workspace ID is in the browser URL: `https://app.fabric.microsoft.com/groups/<workspace-id>/...`

### Step 4 — Run the Gold Warehouse DDL

Open `fabric_gold_warehouse` → SQL editor, run `warehouse/create_tables.sql`.

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

git_repo_url    = "https://github.com/your-org/your-repo.git"
git_branch      = "main"
git_dags_folder = "fabric"
```

> `terraform.tfvars` is gitignored — never commit it.

### Step 8 — Deploy with Terraform

```bash
cd terraform
terraform init
terraform apply
```

This creates the Spark Environment, all 4 notebooks, and the Airflow job.

After apply, get the notebook IDs:

```bash
terraform output airflow_variables
```

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
| Tenant ID | your Azure tenant ID |
| Client ID | your Service Principal client ID |
| Client Secret | your Service Principal secret |

### Step 12 — Set Airflow Variables

In the Airflow UI → **Admin → Variables**, add:

| Variable | Value |
|---|---|
| `fabric_workspace_id` | your workspace GUID |
| `notebook_id__bronze__ingest_file` | from `terraform output airflow_variables` |
| `notebook_id__silver1__process_invoice` | from `terraform output airflow_variables` |
| `notebook_id__silver2__build_carrier_invoice` | from `terraform output airflow_variables` |
| `dbt_project_dir` | `/opt/airflow/git/<repo-name>.git/dags/dbt` |
| `dbt_warehouse_server` | `<workspace-id>.datawarehouse.fabric.microsoft.com` |
| `azure_tenant_id` | your Azure tenant ID |
| `azure_client_id` | your Service Principal client ID |
| `azure_client_secret` | your Service Principal secret |

---

## Re-deploying after changes

| Change | Action |
|---|---|
| Notebook code changed | `terraform apply` |
| DAG, config, or dbt model changed | `git push` — Git sync picks it up automatically |
| New source added (`sources.json`) | Update file → `git push` |
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
│   ├── medallion_pipeline.py         # DAG factory — one DAG per schedule group
│   ├── callbacks.py                  # Teams failure/SLA alerts
│   ├── requirements.txt              # (unused by Fabric — set packages via portal UI)
│   ├── config/
│   │   ├── dev.json                  # workspace name, lakehouse name, endpoints
│   │   ├── sources.json              # one entry per carrier/file type
│   │   └── schedules.json            # DAG schedule groups and cron expressions
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
│   ├── silver1/process_invoice.ipynb
│   └── silver2/build_carrier_invoice.ipynb
├── terraform/
│   ├── providers.tf                  # microsoft/fabric provider
│   ├── variables.tf
│   ├── main.tf                       # Spark Environment, Notebooks, Airflow Job
│   ├── outputs.tf                    # notebook IDs via airflow_variables output
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
│   │   ├── dhl/invoice/
│   │   └── ups/carrier/
│   ├── bronze/                       ← pipeline archives files here (never deleted)
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

- `terraform/terraform.tfvars` is gitignored — never commit it.
- Git sync is configured via the Fabric portal UI (Settings → File storage) — `gitSyncProperties` in the Terraform definition JSON is stored but not acted on by Fabric.
- Spark package installation is not supported by the Terraform provider — add packages via the portal (Settings → Environment configuration → Apache Airflow requirements).
- Gold layer is fully managed by dbt — see `dags/dbt/models/gold/`.
