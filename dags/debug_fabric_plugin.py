"""Throwaway diagnostic DAG — delete after running once."""
import subprocess
from datetime import datetime
from airflow.decorators import dag
from airflow.operators.python import PythonOperator


def inspect_plugin():
    result = subprocess.run(
        ["pip", "show", "-f", "apache-airflow-providers-microsoft-fabric-plugin"],
        capture_output=True, text=True
    )
    print(result.stdout)
    print(result.stderr)


@dag(
    dag_id="debug_fabric_plugin",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["debug"],
)
def debug_dag():
    PythonOperator(task_id="inspect", python_callable=inspect_plugin)


debug_dag()
