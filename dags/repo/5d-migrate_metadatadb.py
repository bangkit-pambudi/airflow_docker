import os
import subprocess
import boto3
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from botocore.client import Config

# --- Konfigurasi Menggunakan Airflow Variables ---
MINIO_ENDPOINT = Variable.get("MINIO_ENDPOINT")      # host.docker.internal:9000
MINIO_ACCESS   = Variable.get("MINIO_ACCESS_KEY")    # minioadmin
MINIO_SECRET   = Variable.get("MINIO_SECRET_KEY")    # minioadmin123
MINIO_BUCKET   = Variable.get("MINIO_BUCKET") # airflow-db-backups (sesuaikan key-nya)
MINIO_SECURE   = False

AIRFLOW_DB_CONN_ID = 'airflow_db' # ID koneksi database metadata di UI Airflow

def dump_db_and_upload_to_minio(ds, **kwargs):
    # 1. Ambil kredensial database dari Airflow Connections
    db_conn = BaseHook.get_connection(AIRFLOW_DB_CONN_ID)
    
    # Inisialisasi S3 Client untuk Minio
    s3_client = boto3.client(
        's3',
        endpoint_url=f"http://{MINIO_ENDPOINT}" if not MINIO_SECURE else f"https://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        config=Config(signature_version='s3v4'),
        verify=MINIO_SECURE
    )
    
    # 2. Setup nama file dan direktori sementara
    date_partition = ds.replace('-', '/') # Contoh: 2024/03/31
    backup_filename = f"metadatadb_{ds}.dump"
    local_filepath = f"/tmp/{backup_filename}"
    
    print(f"Memulai proses pg_dump untuk database: {db_conn.schema}")
    
    # 3. Siapkan environment variable untuk PGPASSWORD
    env = os.environ.copy()
    env['PGPASSWORD'] = db_conn.password
    
    # Command pg_dump
    dump_command = [
        'pg_dump',
        '-h', db_conn.host,
        '-p', str(db_conn.port or 5432), # Default 5432 jika port tidak diisi
        '-U', db_conn.login,
        '-F', 'c',             # Format Custom (compressed)
        '-f', local_filepath,  # Output file
        db_conn.schema         # Nama database
    ]
    
    # Eksekusi command pg_dump
    try:
        subprocess.run(dump_command, env=env, check=True)
        print(f"Backup berhasil dibuat di: {local_filepath}")
    except subprocess.CalledProcessError as e:
        raise Exception(f"Gagal melakukan pg_dump: {e}")

    # 4. Upload ke MinIO menggunakan Boto3
    dest_key = f"{date_partition}/{backup_filename}"
    print(f"Mengunggah backup ke Minio: s3://{MINIO_BUCKET}/{dest_key}")
    
    try:
        s3_client.upload_file(local_filepath, MINIO_BUCKET, dest_key)
        print("Upload ke MinIO berhasil.")
    except Exception as e:
        raise Exception(f"Gagal mengunggah ke MinIO: {e}")
    finally:
        # 5. Cleanup file lokal
        if os.path.exists(local_filepath):
            os.remove(local_filepath)
            print(f"File sementara {local_filepath} telah dihapus.")

# --- Definisi DAG ---
default_args = {
    'owner': 'data_engineering',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    '5d-migrate_metadatadb',
    default_args=default_args,
    description='Backup harian Airflow Metadata DB ke MinIO via Variables',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
) as dag:

    backup_task = PythonOperator(
        task_id='dump_and_upload_db',
        python_callable=dump_db_and_upload_to_minio,
        provide_context=True,
    )