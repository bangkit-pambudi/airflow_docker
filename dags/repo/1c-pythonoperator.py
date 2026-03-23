from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Function Python
def extract():
    print("Extract data...")

def transform():
    print("Transform data...")

def load():
    print("Load data...")

with DAG(
    dag_id="1c-pythonoperator",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    task_extract = PythonOperator(
        task_id="extract_task",
        python_callable=extract
    )

    task_transform = PythonOperator(
        task_id="transform_task",
        python_callable=transform
    )

    task_load = PythonOperator(
        task_id="load_task",
        python_callable=load
    )

    # Dependency (ETL flow)
    task_extract >> task_transform >> task_load