from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Connection
from datetime import datetime, timedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from os.path import basename

# --- KONFIGURASI DEFAULT ---
default_args = {
    'owner': 'airflow',
    'retries': 1, # Default retry untuk semua task di DAG ini adalah 1
    'retry_delay': timedelta(minutes=5),
}

# --- FUNGSI PENGIRIM EMAIL (LOGIKA KUSTOM) ---
def send_email_logic(conn_id, to_address, cc, subject, html_content, file_path=None):
    # Mengambil koneksi dari Airflow Metadata DB
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

    if file_path:
        with open(file_path, 'rb') as fp:
            part = MIMEApplication(fp.read(), Name=basename(file_path))
        part['Content-Disposition'] = f'attachment; filename="{basename(file_path)}"'
        msg.attach(part)

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.ehlo()
        server.starttls()
        if smtp_username and smtp_password:
            server.login(smtp_username, smtp_password)
        server.sendmail(smtp_username, to_address + cc, msg.as_string())
    
    print("Email sent successfully!")

# --- CALLBACK UNTUK KONDISI GAGAL ---
def custom_failure_callback(context):
    """
    Fungsi ini dipanggil otomatis oleh Airflow saat task gagal.
    Ia mengekstrak info error lalu memanggil fungsi send_email_logic.
    """
    ti = context.get('task_instance')
    dag_id = ti.dag_id
    task_id = ti.task_id
    log_url = ti.log_url
    exception = context.get('exception')
    execution_date = context.get('execution_date')

    subject = f"🚨 Alert Gagal: {dag_id}.{task_id}"
    
    body = f"""
    <h3>Task Failure Alert</h3>
    <p><b>DAG:</b> {dag_id}</p>
    <p><b>Task:</b> {task_id}</p>
    <p><b>Execution Date:</b> {execution_date}</p>
    <p><b>Error Detail:</b> <br><code style='color:red;'>{exception}</code></p>
    <br>
    <p><a href='{log_url}'>Klik di sini untuk cek Log Airflow</a></p>
    <p>Regards,<br>Data Team</p>
    """

    send_email_logic(
        conn_id='email_test', # Pastikan ID ini ada di Airflow Connections
        to_address=['demodibimbing@gmail.com', 'bagaspambudibudi@gmail.com'],
        cc=[],
        subject=subject,
        html_content=body
    )

# --- DEFINISI DAG ---
with DAG(
    dag_id='5a-callback-failure-custom-smtp',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    on_failure_callback=custom_failure_callback, # Callback dipasang di level DAG
    schedule_interval=None,
    catchup=False,
    tags=['demo', 'callback']
) as dag:

    start = DummyOperator(task_id="start")

    # Task ini sengaja dibuat error untuk memicu callback
    task_test_error = PythonOperator(
        task_id='cek_koneksi_database',
        python_callable=lambda: exec('raise Exception("Koneksi Database Timeout!")'),
        retries=0  # <--- DITAMBAHKAN DI SINI: Override default_args agar langsung FAIL tanpa retry
    )

    end = DummyOperator(task_id="end")

    start >> task_test_error >> end