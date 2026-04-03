from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Connection
from airflow.exceptions import AirflowTaskTimeout
from datetime import datetime, timedelta
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from os.path import basename

# --- FUNGSI PENGIRIM EMAIL ---
def send_email_logic(conn_id, to_address, cc, subject, html_content, file_path=None):
    connection = Connection.get_connection_from_secrets(conn_id)
    smtp_server = connection.host
    smtp_port = connection.port
    smtp_username = connection.login
    smtp_password = connection.password

    msg = MIMEMultipart('mixed')
    msg['Subject'] = subject
    msg['From'] = smtp_username
    msg['To'] = ", ".join(to_address)
    msg['Cc'] = ", ".join(cc)

    msg.attach(MIMEText(html_content, 'html'))

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.ehlo()
        server.starttls()
        if smtp_username and smtp_password:
            server.login(smtp_username, smtp_password)
        server.sendmail(smtp_username, to_address + cc, msg.as_string())
    print("Email sent successfully!")

# --- CALLBACK SAAT TIMEOUT / GAGAL ---
def timeout_or_failure_callback(context):
    ti = context.get('task_instance')
    exception = context.get('exception')
    
    # Cek apakah gagalnya karena timeout atau error biasa
    if isinstance(exception, AirflowTaskTimeout):
        alasan = "Task kelamaan (Timeout)!"
    else:
        alasan = f"Error biasa: {str(exception)}"

    subject = f"🚨 Alert Task Failed/Timeout: {ti.dag_id}.{ti.task_id}"
    
    body = f"""
    <h3>Alert Airflow!</h3>
    <p><b>DAG:</b> {ti.dag_id}</p>
    <p><b>Task:</b> {ti.task_id}</p>
    <p><b>Alasan Gagal:</b> <code style='color:red;'>{alasan}</code></p>
    <br>
    <p><a href='{ti.log_url}'>Cek Log Task di Sini</a></p>
    """

    send_email_logic(
        conn_id='email_test', 
        to_address=['demodibimbing@gmail.com', 'bagaspambudibudi@gmail.com'],
        cc=[],
        subject=subject,
        html_content=body
    )

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0, # Jangan di-retry kalau mau langsung dapet email pas gagal
}

with DAG(
    '5b-sla_alert',
    default_args=default_args,
    schedule_interval='@once', 
    catchup=False
) as dag:

    task_bisa_timeout = PythonOperator(
        task_id='proses_yang_kelamaan',
        python_callable=lambda: time.sleep(20), # Sengaja dibuat lama (20 detik)
        execution_timeout=timedelta(seconds=10), # BATAS MAKSIMAL CUMA 10 DETIK
        on_failure_callback=timeout_or_failure_callback # Panggil fungsi email kalau gagal/timeout
    )