from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
from datetime import datetime

def send_failure_email(context):
    """Fungsi kustom untuk mengirim email dengan detail error"""
    ti = context.get('task_instance')
    dag_id = ti.dag_id
    task_id = ti.task_id
    log_url = ti.log_url
    exception = context.get('exception')
    
    subject = f"🚨 Airflow Alert: Task Gagal - {dag_id}.{task_id}"
    
    body = f"""
    <h3>Task Failure Alert</h3>
    <p><b>DAG:</b> {dag_id}</p>
    <p><b>Task:</b> {task_id}</p>
    <p><b>Execution Date:</b> {context.get('execution_date')}</p>
    <p><b>Error:</b> {exception}</p>
    <br>
    <p><a href='{log_url}'>Klik di sini untuk cek Log Airflow</a></p>
    """
    
    send_email(to=['tim_data@perusahaan.com'], subject=subject, html_content=body)

with DAG(
    '5a-callback-failure',
    start_date=datetime(2024, 1, 1),
    on_failure_callback=send_failure_email, # Dipasang di level DAG
    schedule_interval='@daily',
    catchup=False
) as dag:

    task_test = PythonOperator(
        task_id='cek_koneksi_database',
        python_callable=lambda: Exception("Koneksi Database Timeout!")
    )