from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# fungsi yang akan dijalankan
def hello_world():
    print("Hello World from Airflow!")

# definisi DAG
with DAG(
    dag_id="1a-hello_world",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    # definisi task
    task_hello = PythonOperator(
        task_id="hello_task",
        python_callable=hello_world
    )