from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from airflow.models import Connection
from os.path import basename
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def send_email(conn_id, to_address, cc, subject, html_content, file_path=None):
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
        part['Content-Disposition'] = 'attachment; filename="%s"' % basename(file_path)
        msg.attach(part)

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.ehlo()
        server.starttls()
        if smtp_username and smtp_password:
            server.login(smtp_username, smtp_password)
        server.sendmail(smtp_username, to_address + cc, msg.as_string())

    print("Email sent successfully!")

def send_email_plain_text():
    plain_text = """
    Halo,

    Ini adalah email demo tanpa template HTML.
    Dikirim otomatis oleh Airflow pada: {}

    Regards,
    Data Team
    """.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    send_email(
        conn_id='email_test',
        to_address=['bagaspambudibudi@gmail.com'],
        cc=[],
        subject='[DEMO] Email Plain Text',
        html_content="<pre>{}</pre>".format(plain_text),
    )

# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id='3a-email-test',
    default_args=default_args,
    description='Demo DAG',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['demo', 'email'],
) as dag:

    start = DummyOperator(
        task_id="start",
        dag=dag
    )

    task_email = PythonOperator(
        task_id='send_email_plain_text',
        python_callable=send_email_plain_text,
    )

    end = DummyOperator(
        task_id="end",
        dag=dag
    )

    start >> task_email >> end