from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
from datetime import datetime, timedelta
import time

# Fungsi yang akan dipanggil saat SLA terlampaui
def my_sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """
    dag: Objek DAG
    task_list: List task yang kena SLA miss
    blocking_task_list: List task yang menyebabkan delay
    slas: Detail SLA yang terlampaui
    blocking_tis: Task Instance yang memblokir
    """
    subject = f"⚠️ Airflow SLA Miss: {dag.dag_id}"
    
    # Membuat daftar task yang terlambat untuk isi email
    tasks_delayed = ", ".join([task.task_id for task in task_list])
    
    body = f"""
    <h3>SLA Terlampaui!</h3>
    <p><b>DAG:</b> {dag.dag_id}</p>
    <p><b>Tasks yang melampaui batas:</b> {tasks_delayed}</p>
    <p><b>Batas SLA:</b> 1 Jam</p>
    <p><b>Waktu Kejadian:</b> {datetime.now()}</p>
    <br>
    <p>Mohon segera cek Airflow UI untuk investigasi bottleneck.</p>
    """
    
    # Kirim email
    send_email(to=['admin_data@perusahaan.com'], subject=subject, html_content=body)

# Konfigurasi default
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'email': ['admin_data@perusahaan.com'],
    'email_on_failure': True,
}

with DAG(
    'pipeline_monitoring_sla',
    default_args=default_args,
    schedule_interval='0 * * * *', # Jalan tiap jam
    sla_miss_callback=my_sla_miss_callback, # Callback di level DAG
    catchup=False
) as dag:

    # Task yang berjalan lama (simulasi lebih dari 1 jam)
    task_lama = PythonOperator(
        task_id='proses_data_berat',
        python_callable=lambda: time.sleep(4000), # Tidur selama 4000 detik (> 1 jam)
        sla=timedelta(hours=1) # Set limit SLA 1 jam
    )

    # Task normal
    task_cepat = PythonOperator(
        task_id='proses_ringan',
        python_callable=lambda: print("Selesai cepat!"),
        sla=timedelta(minutes=30)
    )