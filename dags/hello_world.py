"""
Simple DAG to test Fabric connectivity — triggers a hello_world notebook.
"""
from datetime import datetime

import yaml
from pathlib import Path
from airflow.decorators import dag
from airflow.models import Variable
from apache_airflow_microsoft_fabric_plugin.operators.fabric import FabricRunItemOperator

here = Path(__file__).parent
env = Variable.get("environment", default_var="dev")
env_config = yaml.safe_load((here / "config" / f"{env}.yaml").read_text())

workspace_id   = env_config["fabric_workspace_id"]
fabric_conn_id = env_config["fabric_conn_id"]


@dag(
    dag_id="hello_world",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["test"],
)
def hello_world_dag():
    FabricRunItemOperator(
        task_id="run_hello_world_notebook",
        fabric_conn_id=fabric_conn_id,
        workspace_id=workspace_id,
        item_id=Variable.get("hello_world", default_var=""),
        job_type="RunNotebook",
        deferrable=False,
    )


hello_world_dag()
