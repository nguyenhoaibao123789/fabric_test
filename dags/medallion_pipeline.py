"""
Fabric Managed Airflow — Medallion Pipeline DAG Factory
=========================================================
Reads config/schedules.json and config/sources.json at parse time.
Generates one independent DAG per schedule group — each DAG processes
only the sources assigned to that group.

DAGs produced (example):
  medallion_morning_batch   → cron: 0 6 * * *
  medallion_hourly          → cron: 0 * * * *
  medallion_weekly_sunday   → cron: 0 6 * * 0

Adding a new schedule: add a row to schedules.json, no code change needed.
Adding a new source:   add a row to sources.json,   no code change needed.
"""
import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator

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

_env_config: dict = json.loads((_CONFIG / f"{ENV}.json").read_text())
_all_sources: list[dict] = json.loads((_CONFIG / "sources.json").read_text())
_schedules: list[dict] = json.loads((_CONFIG / "schedules.json").read_text())

WORKSPACE_ID = Variable.get("fabric_workspace_id", default_var="")   # set in Airflow Variables
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
    """Look up Fabric item ID for a notebook by its logical name."""
    return Variable.get(f"notebook_id__{name.replace('/', '__')}", default_var="")


# ── DAG factory ───────────────────────────────────────────────────────
def create_medallion_dag(schedule_group: str, cron: str, dag_id: str):
    """
    Build one Airflow DAG for the given schedule_group.
    Only sources where source['schedule_group'] == schedule_group are included.
    """
    sources = [
        s for s in _all_sources
        if s["schedule_group"] == schedule_group
        and s["is_enabled"]
        and s["environment"] == ENV
    ]

    if not sources:
        log.warning("schedule_group '%s' has no enabled sources — skipping DAG", schedule_group)
        return None

    # Group sources by their silver2_entity for scoped Silver 2 wiring
    silver2_entities: dict[str, list] = {}
    for src in sources:
        silver2_entities.setdefault(src["silver2_entity"], []).append(src["source_name"])

    @dag(
        dag_id=dag_id,
        schedule_interval=cron,
        start_date=datetime(2026, 1, 1),
        default_args=DEFAULT_ARGS,
        catchup=False,
        sla_miss_callback=on_sla_miss_teams_alert,
        tags=["medallion", "fabric", ENV, schedule_group],
        doc_md=f"""
            ## Medallion Pipeline — {schedule_group}

            **Schedule:** `{cron}`
            **Sources:** {len(sources)}
            **Silver 2 entities:** {list(silver2_entities.keys())}

            Each source runs Bronze → Silver 1 independently in parallel.
            Silver 2 per entity runs after all its Silver 1 sources succeed (all_success).
            Gold runs once after its upstream Silver 2 tasks (feeds_gold=true) succeed — does not wait for unrelated Silver 2 tasks.
                """,
    )
    def dag_fn():

        # ── Bookkeeping ────────────────────────────────────────────────
        # Maps source_name → the silver1 FabricRunItemOperator task
        silver1_task_by_source: dict[str, FabricRunItemOperator] = {}

        # ── Phase 1 + 2: Bronze + Silver 1 per source ─────────────────
        for src in sources:
            with TaskGroup(group_id=f"group_{src['source_name']}") as grp:

                # Bronze: copy raw file into Bronze Lakehouse / Files/
                bronze = FabricRunItemOperator(
                    task_id=f"bronze_{src['source_name']}",
                    workspace_id=WORKSPACE_ID,
                    fabric_item_id=notebook_id("bronze/ingest_file"),
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
                    task_id=f"silver1_{src['source_name']}",
                    workspace_id=WORKSPACE_ID,
                    fabric_item_id=notebook_id(f"silver1/{src['silver1_notebook']}"),
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

                bronze >> silver1

            silver1_task_by_source[src["source_name"]] = silver1

        # ── Phase 3: Silver 2 per entity ──────────────────────────────
        # All Silver 1 tasks for an entity must succeed before Silver 2
        # for that entity runs. Keyed by entity name for selective wiring.
        silver2_task_by_entity: dict[str, FabricRunItemOperator] = {}

        for entity, source_names in silver2_entities.items():
            silver2 = FabricRunItemOperator(
                task_id=f"silver2_{entity}",
                workspace_id=WORKSPACE_ID,
                fabric_item_id=notebook_id(f"silver2/build_{entity}"),
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
                deferrable=True,
            )

            # Wire: all silver1 tasks for this entity → silver2
            upstream_tasks = [silver1_task_by_source[sn] for sn in source_names]
            upstream_tasks >> silver2
            silver2_task_by_entity[entity] = silver2

        # ── Phase 4: Gold — dbt run ────────────────────────────────────
        # Gold depends only on Silver 2 entities where feeds_gold=true in sources.json.
        # This means Gold triggers as soon as its required entities succeed —
        # it does NOT wait for unrelated Silver 2 entities in the same DAG.
        gold_entities = {
            s["silver2_entity"]
            for s in sources
            if s.get("feeds_gold", False)
        }
        gold_upstream = [
            silver2_task_by_entity[e]
            for e in gold_entities
            if e in silver2_task_by_entity
        ]

        if not gold_upstream:
            log.warning(
                "No feeds_gold=true sources found for schedule_group '%s' — "
                "Gold task will not be added to this DAG.",
                schedule_group,
            )
            return

        gold = BashOperator(
            task_id="gold_fact_invoice",
            bash_command=(
                "set -e && "
                "cd \"${DBT_PROJECT_DIR}\" && "
                "dbt deps --profiles-dir . --quiet && "
                "dbt run  --profiles-dir . --target \"${DBT_TARGET}\" && "
                "dbt test --profiles-dir . --target \"${DBT_TARGET}\""
            ),
            env={
                "DBT_PROJECT_DIR": Variable.get("dbt_project_dir", default_var="/opt/airflow/dags/dbt"),
                "DBT_TARGET": ENV,
                "DBT_WAREHOUSE_SERVER":  Variable.get("dbt_warehouse_server", default_var=""),
                "DBT_LAKEHOUSE":         Variable.get("lakehouse", default_var="fabric_lakehouse"),
                "AZURE_TENANT_ID":       Variable.get("azure_tenant_id",    default_var=os.getenv("AZURE_TENANT_ID", "")),
                "AZURE_CLIENT_ID":       Variable.get("azure_client_id",    default_var=os.getenv("AZURE_CLIENT_ID", "")),
                "AZURE_CLIENT_SECRET":   Variable.get("azure_client_secret",default_var=os.getenv("AZURE_CLIENT_SECRET", "")),
            },
            append_env=True,
            on_failure_callback=on_failure_teams_alert,
        )

        gold_upstream >> gold

    return dag_fn()


# ── Generate one DAG per schedule group ───────────────────────────────
# Airflow discovers any DAG object assigned at module level.
for _sched in _schedules:
    _dag_obj = create_medallion_dag(
        schedule_group=_sched["schedule_group"],
        cron=_sched["cron"],
        dag_id=_sched["dag_id"],
    )
    if _dag_obj is not None:
        globals()[_sched["dag_id"]] = _dag_obj
