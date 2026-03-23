from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="1b-bashoperator",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    # Task 1: print hello
    task_hello = BashOperator(
        task_id="hello_task",
        bash_command="echo 'Hello from BashOperator'"
    )

    # Task 2: print date
    task_date = BashOperator(
        task_id="date_task",
        bash_command="python3 hello_world.py"
    )

    # Dependency (sequential)
    task_hello >> task_date