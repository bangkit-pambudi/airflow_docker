from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def push_function(**kwargs):
    # Mengambil Task Instance (ti) dari kwargs
    ti = kwargs['ti']
    data = "Ini pesan rahasia"
    # Melakukan push ke XCom dengan key 'my_message'
    ti.xcom_push(key='my_message', value=data)

def pull_function(**kwargs):
    ti = kwargs['ti']
    # Melakukan pull dari XCom menggunakan ID task sebelumnya dan key-nya
    pulled_data = ti.xcom_pull(task_ids='push_task', key='my_message')
    print(f"Pesan yang diterima: {pulled_data}")

with DAG('4b-simple-xcom', start_date=datetime(2024, 1, 1), schedule_interval=None) as dag:
    
    push_task = PythonOperator(
        task_id='push_task',
        python_callable=push_function,
    )

    pull_task = PythonOperator(
        task_id='pull_task',
        python_callable=pull_function,
    )

    push_task >> pull_task