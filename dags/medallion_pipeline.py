"""
Fabric Managed Airflow — Medallion Pipeline DAG Factory
=========================================================
Reads config/sources.yaml at parse time.
Generates one independent DAG per subject — each DAG processes
only the sources belonging to that subject.

DAGs produced (example):
  medallion_carrier_invoice   → cron: 0 6 * * *
  medallion_carrier_tracking  → cron: 0 * * * *
  medallion_rate_card         → cron: 0 6 * * 0

Adding a new subject/schedule: add a subject block to sources.yaml, no code change needed.
Adding a new source:           add a source entry under the relevant subject, no code change needed.
"""
import json
import yaml
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.datasets import Dataset
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.bash import BashOperator

# Fabric custom operators — installed in the Managed Airflow environment
# pip install apache-airflow-microsoft-fabric-plugin
from apache_airflow_microsoft_fabric_plugin.operators.fabric import FabricRunItemOperator

from callbacks import on_failure_teams_alert, on_sla_miss_teams_alert

log = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────
_HERE = Path(__file__).parent
_CONFIG = _HERE / "config"  # dags/config/ — synced with git

# ── Load config at parse time ─────────────────────────────────────────
ENV = Variable.get("environment", default_var="dev")

_env_config: dict = yaml.safe_load((_CONFIG / f"{ENV}.yaml").read_text())
_sources_cfg: dict  = yaml.safe_load((_CONFIG / "sources.yaml").read_text())
_subjects:    list[dict] = _sources_cfg["subjects"]
_gold_tables: list[dict] = _sources_cfg.get("gold_tables", [])

WORKSPACE_ID  = Variable.get("fabric_workspace_id", default_var="")   # set in Airflow Variables
FABRIC_CONN_ID = Variable.get("fabric_conn_id", default_var="fabric_default")
GOLD_WAREHOUSE = _env_config["gold_warehouse"]

# ── Default task args ─────────────────────────────────────────────────
DEFAULT_ARGS = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "on_failure_callback": on_failure_teams_alert,
    "sla": timedelta(hours=4),
}


# ── Notebook item IDs (set in Airflow Variables after deploy.py runs) ──
def notebook_id(name: str) -> str:
    """Look up Fabric item ID for a notebook by its Airflow Variable key."""
    return Variable.get(name, default_var="")


# ── DAG factory ───────────────────────────────────────────────────────
def create_medallion_dag(subject: dict):
    """
    Build one Airflow DAG for the given subject.
    Only sources in subject['sources'] that are enabled for the current ENV are included.
    """
    subject_name = subject["subject"]
    dag_id = subject["dag_id"]
    cron = subject["cron"]

    sources = [
        s for s in subject["sources"]
        if s.get("environment", ENV) == ENV
    ]

    if not sources:
        log.warning("subject '%s' has no enabled sources — skipping DAG", subject_name)
        return None

    # Group sources by their silver2_table for scoped Silver 2 wiring
    silver2_entities: dict[str, list] = {}
    for src in sources:
        silver2_entities.setdefault(src["silver2_table"], []).append(src["source_name"])

    @dag(
        dag_id=dag_id,
        schedule_interval=cron,
        start_date=datetime(2026, 1, 1),
        default_args=DEFAULT_ARGS,
        catchup=False,
        sla_miss_callback=on_sla_miss_teams_alert,
        tags=["medallion", "fabric", ENV, subject_name],
        doc_md=f"""
            ## Medallion Pipeline — {subject_name}

            **Schedule:** `{cron}`
            **Sources:** {len(sources)}
            **Silver 2 entities:** {list(silver2_entities.keys())}

            Each source runs Bronze → Silver 1 independently in parallel.
            Silver 2 per entity runs after all its Silver 1 sources succeed (all_success).
            Silver 2 emits a Dataset event consumed by the standalone Gold DAG.
                """,
    )
    def dag_fn():

        # ── Bookkeeping ────────────────────────────────────────────────
        # Maps source_name → the silver1 FabricRunItemOperator task
        silver1_task_by_source: dict[str, FabricRunItemOperator] = {}

        # ── Phase 1 + 2: Bronze + Silver 1 per source ─────────────────
        for src in sources:
            # Bronze: copy raw file into Bronze Lakehouse / Files/
            bronze = FabricRunItemOperator(
                task_id=f"src_to_brz__{src['source_name']}",
                fabric_conn_id=FABRIC_CONN_ID,
                workspace_id=WORKSPACE_ID,
                item_id=notebook_id("bronze_ingest_file"),
                job_type="RunNotebook",
                job_params={
                    "configuration": {
                        "parameters": {
                            "source_config": json.dumps(src),
                            "env": ENV,
                        }
                    }
                },
                deferrable=True,
            )

            # Silver 1: validate + clean + MERGE into staging_{source_name}
            silver1 = FabricRunItemOperator(
                task_id=f"brz_to_sil1__{src['source_name']}",
                fabric_conn_id=FABRIC_CONN_ID,
                workspace_id=WORKSPACE_ID,
                item_id=notebook_id("silver1_clean"),
                job_type="RunNotebook",
                job_params={
                    "configuration": {
                        "parameters": {
                            "source_config": json.dumps(src),
                            "env": ENV,
                        }
                    }
                },
                outlets=[Dataset(f"silver1://{src['silver1_table']}")],
                deferrable=True,
            )

            bronze.ui_color  = "#faebd4"; bronze.ui_fgcolor  = "#4d2c00"
            silver1.ui_color = "#e8f4ff"; silver1.ui_fgcolor = "#1a4a80"

            bronze >> silver1
            silver1_task_by_source[src["source_name"]] = silver1

        # ── Phase 3: Silver 2 per entity ──────────────────────────────
        # All Silver 1 tasks for an entity must succeed before Silver 2
        # for that entity runs. Keyed by entity name for selective wiring.
        silver2_task_by_entity: dict[str, FabricRunItemOperator] = {}

        for entity, source_names in silver2_entities.items():
            silver2 = FabricRunItemOperator(
                task_id=f"sil1_to_sil2__{entity}",
                fabric_conn_id=FABRIC_CONN_ID,
                workspace_id=WORKSPACE_ID,
                item_id=notebook_id("silver2_combine"),
                job_type="RunNotebook",
                job_params={
                    "configuration": {
                        "parameters": {
                            "entity": entity,
                            "source_names": json.dumps(source_names),
                            "env": ENV,
                        }
                    }
                },
                outlets=[Dataset(f"{dag_id}/silver2://{entity}")],
                deferrable=True,
            )

            silver2.ui_color  = "#eae8fd"; silver2.ui_fgcolor = "#302880"

            # Wire: all silver1 tasks for this entity → silver2
            # Silver2 emits a Dataset event consumed by the standalone Gold DAG
            upstream_tasks = [silver1_task_by_source[sn] for sn in source_names]
            upstream_tasks >> silver2
            silver2_task_by_entity[entity] = silver2

    return dag_fn()


# ── Gold DAG factory ──────────────────────────────────────────────────
def create_gold_dag(gold_cfg: dict):
    """
    Build a dataset-triggered Gold DAG for a single dbt model.
    Fires only when ALL upstream Silver 2 datasets have emitted a new event.
    """
    gold_dag_id = gold_cfg["dag_id"]
    dbt_model   = gold_cfg["dbt_model"]
    wait_for    = gold_cfg["wait_for"]

    dataset_schedule = [
        Dataset(f"{w['dag_id']}/silver2://{w['silver2_table']}")
        for w in wait_for
    ]

    @dag(
        dag_id=gold_dag_id,
        schedule_interval=dataset_schedule,
        start_date=datetime(2026, 1, 1),
        default_args=DEFAULT_ARGS,
        catchup=False,
        tags=["gold", "dbt", ENV],
        doc_md=f"""
            ## Gold DAG — {dbt_model}

            Triggered by datasets: {[w['silver2_table'] for w in wait_for]}
            Runs only when ALL upstream Silver 2 tables have new data.
            Executes: `dbt run --select {dbt_model}`
                """,
    )
    def gold_dag_fn():
        gold = BashOperator(
            task_id=f"dbt_run__{dbt_model}",
            bash_command=(
                "set -e && "
                "cd \"${DBT_PROJECT_DIR}\" && "
                "dbt deps --profiles-dir . --quiet && "
                f"dbt run  --profiles-dir . --target \"${{DBT_TARGET}}\" --select {dbt_model} && "
                f"dbt test --profiles-dir . --target \"${{DBT_TARGET}}\" --select {dbt_model}"
            ),
            env={
                "DBT_PROJECT_DIR": Variable.get("dbt_project_dir", default_var="/opt/airflow/dags/dbt"),
                "DBT_TARGET": ENV,
                "DBT_WAREHOUSE_SERVER": Variable.get("dbt_warehouse_server", default_var=""),
                "DBT_LAKEHOUSE":        Variable.get("lakehouse", default_var="fabric_lakehouse"),
                "AZURE_TENANT_ID":      Variable.get("azure_tenant_id",     default_var=os.getenv("AZURE_TENANT_ID", "")),
                "AZURE_CLIENT_ID":      Variable.get("azure_client_id",     default_var=os.getenv("AZURE_CLIENT_ID", "")),
                "AZURE_CLIENT_SECRET":  Variable.get("azure_client_secret", default_var=os.getenv("AZURE_CLIENT_SECRET", "")),
            },
            append_env=True,
            on_failure_callback=on_failure_teams_alert,
        )
        gold.ui_color  = "#fff8e0"; gold.ui_fgcolor = "#7a5800"

    return gold_dag_fn()


# ── Generate one DAG per subject ──────────────────────────────────────
# Airflow discovers any DAG object assigned at module level.
for _subject in _subjects:
    _dag_obj = create_medallion_dag(_subject)
    if _dag_obj is not None:
        globals()[_subject["dag_id"]] = _dag_obj

# ── Generate one Gold DAG per gold_tables entry ───────────────────────
for _gold_cfg in _gold_tables:
    _gold_dag_obj = create_gold_dag(_gold_cfg)
    if _gold_dag_obj is not None:
        globals()[_gold_cfg["dag_id"]] = _gold_dag_obj
