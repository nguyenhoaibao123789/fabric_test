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
from datetime import datetime, timedelta
from pathlib import Path

from airflow.datasets import Dataset
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from apache_airflow_microsoft_fabric_plugin.operators.fabric import FabricRunItemOperator
from apache_airflow_microsoft_fabric_plugin.hooks.fabric import FabricHook


class FabricRunNotebookOperator(FabricRunItemOperator):
    """
    Extends FabricRunItemOperator to fix notebook parameter injection.
    The base plugin wraps job_params as {"executionData": {"parameters": ...}}
    but Fabric expects parameters at the top level of the request body.
    """

    def execute(self, context):
        hook = FabricHook(fabric_conn_id=self.fabric_conn_id)
        headers = hook.get_headers()

        url = (
            f"https://api.fabric.microsoft.com/v1/workspaces/{self.workspace_id}"
            f"/items/{self.item_id}/jobs/instances?jobType={self.job_type}"
        )

        body = {}
        if self.job_params:
            body["parameters"] = self.job_params

        import requests
        response = requests.post(url, headers=headers, json=body)
        response.raise_for_status()

        location = response.headers.get("Location")
        if not location:
            raise Exception("No Location header in Fabric response")

        succeeded = hook.wait_for_item_run_status(
            location,
            target_status="Completed",
            check_interval=self.check_interval if hasattr(self, "check_interval") else 60,
            timeout=self.timeout if hasattr(self, "timeout") else 3600,
        )
        if not succeeded:
            raise Exception(f"Notebook run failed for item_id={self.item_id}")

log = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────
here = Path(__file__).parent
config_path = here / "config"  # dags/config/ — synced with git

# ── Load config at parse time ─────────────────────────────────────────
env = Variable.get("environment", default_var="dev")

env_config: dict     = yaml.safe_load((config_path / f"{env}.yaml").read_text())
sources_cfg: dict    = yaml.safe_load((config_path / "sources.yaml").read_text())
subjects:    list[dict] = sources_cfg["subjects"]
gold_tables: list[dict] = sources_cfg.get("gold_tables", [])

workspace_id  = env_config["fabric_workspace_id"]
fabric_conn_id = env_config["fabric_conn_id"]
gold_warehouse = env_config["gold_warehouse"]

# ── Default task args ─────────────────────────────────────────────────
default_args = {
    "retries": 0,
    "sla": timedelta(hours=4),
}


# ── Notebook item IDs ─────────────────────────────────────────────────
def notebook_id(name: str) -> str:
    """Look up Fabric notebook item ID from an Airflow Variable."""
    return Variable.get(name, default_var="")


# ── DAG factory ───────────────────────────────────────────────────────
def create_medallion_dag(subject: dict):
    """
    Build one Airflow DAG for the given subject.
    Only sources in subject['sources'] that are enabled for the current env are included.
    """
    subject_name = subject["subject"]
    dag_id = subject["dag_id"]
    cron = subject["cron"]

    sources = [
        s for s in subject["sources"]
        if s.get("is_enabled", True)
        and s.get("environment", env) == env
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
        default_args=default_args,
        catchup=False,
        tags=["medallion", "fabric", env, subject_name],
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
        silver1_task_by_source: dict[str, FabricRunNotebookOperator] = {}

        # ── Phase 1 + 2: Bronze + Silver 1 per source ─────────────────
        for src in sources:
            src_with_ids = {
                **src,
                "workspace_id": env_config["fabric_workspace_id"],
                "lakehouse_id": env_config["fabric_lakehouse_id"],
                "warehouse_id": env_config["fabric_warehouse_id"],
                "gold_warehouse_sql_endpoint": env_config["gold_warehouse_sql_endpoint"],
                "gold_warehouse": env_config["gold_warehouse"],
            }

            # Bronze: copy raw file into Bronze Lakehouse / Files/
            bronze = FabricRunNotebookOperator(
                task_id=f"src_to_brz__{src['source_name']}",
                fabric_conn_id=fabric_conn_id,
                workspace_id=workspace_id,
                item_id=notebook_id("bronze_ingest_file"),
                job_type="RunNotebook",
                job_params=[
                    {"name": "source_config", "value": json.dumps(src_with_ids), "type": "Text"},
                    {"name": "env",           "value": env,                       "type": "Text"},
                ],
                deferrable=False,
            )

            # Silver 1: validate + clean + MERGE into staging_{source_name}
            silver1 = FabricRunNotebookOperator(
                task_id=f"brz_to_sil1__{src['source_name']}",
                fabric_conn_id=fabric_conn_id,
                workspace_id=workspace_id,
                item_id=notebook_id("silver1_clean"),
                job_type="RunNotebook",
                job_params=[
                    {"name": "source_config", "value": json.dumps(src_with_ids), "type": "Text"},
                    {"name": "env",           "value": env,                       "type": "Text"},
                ],
                deferrable=False,
            )

            bronze.ui_color  = "#faebd4"; bronze.ui_fgcolor  = "#4d2c00"
            silver1.ui_color = "#e8f4ff"; silver1.ui_fgcolor = "#1a4a80"

            bronze >> silver1
            silver1_task_by_source[src["source_name"]] = silver1

        # ── Phase 3: Silver 2 per entity (dbt) ──────────────────────────
        # All Silver 1 tasks for an entity must succeed before Silver 2
        # dbt model runs. Keyed by entity name for selective wiring.
        silver2_task_by_entity: dict[str, BashOperator] = {}

        for entity, source_names in silver2_entities.items():
            # Map silver2_table name to dbt model name
            # e.g. "silver_carrier_invoice" → "carrier_invoice"
            dbt_model = entity.removeprefix("silver_") if entity.startswith("silver_") else entity

            silver2 = BashOperator(
                task_id=f"sil1_to_sil2__{entity}",
                bash_command=(
                    "set -e && "
                    "cd \"${DBT_PROJECT_DIR}\" && "
                    "dbt deps --profiles-dir . --quiet && "
                    f"dbt run  --profiles-dir . --target \"${{DBT_TARGET}}\" --select {dbt_model} && "
                    f"dbt test --profiles-dir . --target \"${{DBT_TARGET}}\" --select {dbt_model}"
                ),
                env={
                    "DBT_PROJECT_DIR":      "/opt/airflow/dags/dbt",
                    "DBT_TARGET":           env,
                    "DBT_LAKEHOUSE":        env_config["lakehouse"],
                },
                append_env=True,
                outlets=[Dataset(f"{dag_id}/silver2://{entity}")],
            )

            silver2.ui_color  = "#eae8fd"; silver2.ui_fgcolor = "#302880"

            # Wire: all silver1 tasks for this entity → silver2
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
        default_args=default_args,
        catchup=False,
        tags=["gold", "dbt", env],
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
                "DBT_PROJECT_DIR": "/opt/airflow/dags/dbt",
                "DBT_TARGET":           env,
                "DBT_LAKEHOUSE":        env_config["lakehouse"],
            },
            append_env=True,
        )
        gold.ui_color  = "#fff8e0"; gold.ui_fgcolor = "#7a5800"

    return gold_dag_fn()


# ── Generate one DAG per subject ──────────────────────────────────────
for subject in subjects:
    dag_obj = create_medallion_dag(subject)
    if dag_obj is not None:
        globals()[subject["dag_id"]] = dag_obj

# ── Generate one Gold DAG per gold_tables entry ───────────────────────
for gold_cfg in gold_tables:
    gold_dag_obj = create_gold_dag(gold_cfg)
    if gold_dag_obj is not None:
        globals()[gold_cfg["dag_id"]] = gold_dag_obj
