from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# 1. Fungsi biasa untuk menghasilkan data dinamis (Dinamis -> op_kwargs)
def get_regions_func():
    """
    Menghasilkan list berisi dictionary.
    Setiap dictionary akan disalurkan ke parameter bernama 'region'.
    """
    return [
        {"region": "US"},
        {"region": "EU"},
        {"region": "APAC"},
        {"region": "LATAM"}
    ]

# 2. Fungsi biasa untuk memproses data
# ⚠️ PENTING: Susunan argumen diubah. 
# Yang statis di depan (untuk op_args), yang dinamis di belakang.
def process_regional_data_func(target_bucket: str, file_format: str, region: str):
    print(f"📥 Mengambil data untuk region: {region}...")
    print(f"💾 Menyimpan data {region} ke bucket '{target_bucket}' dalam format .{file_format}...")
    return f"Berhasil memproses {region}"

# 3. Definisi DAG gaya klasik (Classic Way)
with DAG(
    dag_id="4a-simple_dynamic_task_mapping",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "dynamic_mapping", "partial", "classic"]
) as dag:

    # Task 1: Mengambil list region
    task_get_regions = PythonOperator(
        task_id="get_regions",
        python_callable=get_regions_func
    )

    # Task 2: Memetakan (mapping) task secara dinamis
    task_process_data = PythonOperator.partial(
        task_id="process_regional_data",
        python_callable=process_regional_data_func,
        # ✅ STATIS diletakkan di op_args (berupa List/Array)
        op_args=["global-sales-datalake", "parquet"]
    ).expand(
        # ✅ DINAMIS diletakkan di op_kwargs (mengambil dari task sebelumnya)
        op_kwargs=task_get_regions.output
    )