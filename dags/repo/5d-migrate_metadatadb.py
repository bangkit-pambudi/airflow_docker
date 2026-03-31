import os
import subprocess
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook

# --- Konfigurasi ---
MINIO_CONN_ID = 'minio_conn'
MINIO_BUCKET = 'airflow-db-backups'
AIRFLOW_DB_CONN_ID = 'airflow_db' # ID koneksi database metadatadb kamu

def dump_db_and_upload_to_minio(ds, **kwargs):
    # 1. Ambil kredensial database dengan aman dari Airflow Connections
    db_conn = BaseHook.get_connection(AIRFLOW_DB_CONN_ID)
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    
    # 2. Setup nama file dan direktori sementara
    date_partition = ds.replace('-', '/') # Contoh: 2024/03/31
    backup_filename = f"metadatadb_{ds}.dump"
    local_filepath = f"/tmp/{backup_filename}"
    
    print(f"Memulai proses pg_dump untuk database: {db_conn.schema}")
    
    # 3. Siapkan environment variable agar password tidak bocor di log command
    env = os.environ.copy()
    env['PGPASSWORD'] = db_conn.password
    
    # Command pg_dump dengan format custom (-F c) yang sudah otomatis terkompresi
    dump_command = [
        'pg_dump',
        '-h', db_conn.host,
        '-p', str(db_conn.port),
        '-U', db_conn.login,
        '-F', 'c',             # Format Custom (compressed & suitable for pg_restore)
        '-f', local_filepath,  # Output file
        db_conn.schema         # Nama database
    ]
    
    # Eksekusi command pg_dump
    try:
        subprocess.run(dump_command, env=env, check=True)
        print(f"Backup berhasil dibuat di: {local_filepath}")
    except subprocess.CalledProcessError as e:
        raise Exception(f"Gagal melakukan pg_dump: {e}")

    # 4. Upload ke MinIO
    dest_key = f"{date_partition}/{backup_filename}"
    print(f"Mengunggah backup ke Minio: s3://{MINIO_BUCKET}/{dest_key}")
    
    try:
        s3_hook.load_file(
            filename=local_filepath,
            key=dest_key,
            bucket_name=MINIO_BUCKET,
            replace=True
        )
        print("Upload ke MinIO berhasil.")
    except Exception as e:
        raise Exception(f"Gagal mengunggah ke MinIO: {e}")
    finally:
        # 5. Auto-Cleanup: Hapus file dump lokal bagaimanapun hasil uploadnya
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
    'backup_metadatadb_to_minio',
    default_args=default_args,
    description='Backup harian Airflow Metadata DB ke MinIO',
    schedule_interval='@daily', # Berjalan setiap hari
    catchup=False,
    max_active_runs=1,
) as dag:

    backup_task = PythonOperator(
        task_id='dump_and_upload_db',
        python_callable=dump_db_and_upload_to_minio,
        provide_context=True,
    )