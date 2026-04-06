from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from datetime import datetime


@dag(schedule=None, start_date=datetime(2024, 1, 1), catchup=False, tags=["debug"])
def check_env():
    BashOperator(
        task_id="list_packages",
        bash_command="pip list -v",
    )


check_env()
