from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def task_a():
    print("Task A")

def task_b():
    print("Task B")

def task_c():
    print("Task C")

with DAG(
    dag_id="2a-sequential-job",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    t1 = PythonOperator(task_id="task_a", python_callable=task_a)
    t2 = PythonOperator(task_id="task_b", python_callable=task_b)
    t3 = PythonOperator(task_id="task_c", python_callable=task_c)

    # Sequential
    t1 >> t2 >> t3

    # Parallel (contoh alternatif)
    # t1 >> [t2, t3]