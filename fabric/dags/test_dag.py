from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="test_git_sync",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
):
    EmptyOperator(task_id="ping")
