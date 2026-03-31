import os
import gzip
import shutil
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# --- Konfigurasi ---
LOG_SOURCE_DIR = '/opt/airflow/logs/'
MINIO_BUCKET = 'airflow-archive-logs'
MINIO_CONN_ID = 'minio_conn'

def compress_and_upload_to_minio(ds, **kwargs):
    """
    Fungsi untuk mencari log, mengompresnya menjadi .gz, 
    mengunggah ke Minio, lalu menghapus file lokal.
    """
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    
    # Gunakan tanggal logical date dari Airflow (ds) sebagai folder partisi di Minio
    date_partition = ds.replace('-', '/') # Contoh: 2024/03/31
    
    if not os.path.exists(LOG_SOURCE_DIR):
        print(f"Directory {LOG_SOURCE_DIR} tidak ditemukan!")
        return

    for root, dirs, files in os.walk(LOG_SOURCE_DIR):
        for file in files:
            # Lewati file yang sudah dikompresi sebelumnya (jika ada sisa)
            if file.endswith('.gz'):
                continue
                
            local_file_path = os.path.join(root, file)
            gzipped_file_path = f"{local_file_path}.gz"
            
            # 1. Kompresi file menggunakan GZIP
            print(f"Mengompresi {local_file_path}...")
            with open(local_file_path, 'rb') as f_in:
                with gzip.open(gzipped_file_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            # 2. Upload ke Minio
            # Mempertahankan struktur folder asli log Airflow di dalam Minio
            relative_path = os.path.relpath(local_file_path, LOG_SOURCE_DIR)
            dest_key = f"{date_partition}/{relative_path}.gz"
            
            print(f"Mengunggah ke Minio: s3://{MINIO_BUCKET}/{dest_key}")
            s3_hook.load_file(
                filename=gzipped_file_path,
                key=dest_key,
                bucket_name=MINIO_BUCKET,
                replace=True
            )
            
            # 3. Cleanup: Hapus file asli dan file .gz lokal setelah sukses upload
            os.remove(local_file_path)
            os.remove(gzipped_file_path)
            print(f"Cleanup berhasil untuk {local_file_path}")

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
    'advanced_log_migration_to_minio',
    default_args=default_args,
    description='Kompresi dan Migrasi log Airflow ke Minio S3',
    schedule_interval='@daily', # Jalan setiap tengah malam
    catchup=False,
    max_active_runs=1,
) as dag:

    migrate_logs_task = PythonOperator(
        task_id='compress_upload_cleanup',
        python_callable=compress_and_upload_to_minio,
        provide_context=True,
        execution_timeout=timedelta(hours=2) # Beri waktu lebih untuk kompresi data besar
    )