import os
import gzip
import shutil
import boto3
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from botocore.client import Config

# --- Konfigurasi Menggunakan Airflow Variables ---
LOG_SOURCE_DIR = '/opt/airflow/logs/'
MINIO_ENDPOINT = Variable.get("MINIO_ENDPOINT")      # host.docker.internal:9000
MINIO_ACCESS   = Variable.get("MINIO_ACCESS_KEY")    # minioadmin
MINIO_SECRET   = Variable.get("MINIO_SECRET_KEY")    # minioadmin123
MINIO_BUCKET   = Variable.get("MINIO_BUCKET")        # adventureworks-elt
MINIO_SECURE   = False  # Gunakan False jika HTTP, True jika HTTPS

def compress_and_upload_to_minio(ds, **kwargs):
    """
    Fungsi untuk mencari log, mengompresnya menjadi .gz, 
    mengunggah ke Minio menggunakan Boto3, lalu menghapus file lokal.
    """
    
    # Inisialisasi S3 Client untuk Minio
    s3_client = boto3.client(
        's3',
        endpoint_url=f"http://{MINIO_ENDPOINT}" if not MINIO_SECURE else f"https://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        config=Config(signature_version='s3v4'),
        verify=MINIO_SECURE
    )
    
    # Gunakan tanggal logical date dari Airflow (ds) sebagai folder partisi di Minio
    date_partition = ds.replace('-', '/') # Contoh: 2024/03/31
    
    if not os.path.exists(LOG_SOURCE_DIR):
        print(f"Directory {LOG_SOURCE_DIR} tidak ditemukan!")
        return

    for root, dirs, files in os.walk(LOG_SOURCE_DIR):
        for file in files:
            # Lewati file yang sudah dikompresi sebelumnya
            if file.endswith('.gz'):
                continue
                
            local_file_path = os.path.join(root, file)
            gzipped_file_path = f"{local_file_path}.gz"
            
            try:
                # 1. Kompresi file menggunakan GZIP
                print(f"Mengompresi {local_file_path}...")
                with open(local_file_path, 'rb') as f_in:
                    with gzip.open(gzipped_file_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                
                # 2. Upload ke Minio
                relative_path = os.path.relpath(local_file_path, LOG_SOURCE_DIR)
                dest_key = f"{date_partition}/{relative_path}.gz"
                
                print(f"Mengunggah ke Minio: s3://{MINIO_BUCKET}/{dest_key}")
                s3_client.upload_file(gzipped_file_path, MINIO_BUCKET, dest_key)
                
                # 3. Cleanup: Hapus file asli dan file .gz lokal
                os.remove(local_file_path)
                os.remove(gzipped_file_path)
                print(f"Cleanup berhasil untuk {local_file_path}")
                
            except Exception as e:
                print(f"Gagal memproses {file}: {str(e)}")
                # Hapus file gz sementara jika gagal upload agar tidak menumpuk
                if os.path.exists(gzipped_file_path):
                    os.remove(gzipped_file_path)

# --- Definisi DAG ---
default_args = {
    'owner': 'data_engineering_team',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['alert-data@perusahaan.com']
}

with DAG(
    '5c-migrate_log_data',
    default_args=default_args,
    description='Kompresi dan Migrasi log Airflow ke Minio S3 via Variables',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
) as dag:

    migrate_logs_task = PythonOperator(
        task_id='compress_upload_cleanup',
        python_callable=compress_and_upload_to_minio,
        provide_context=True,
        execution_timeout=timedelta(hours=2)
    )