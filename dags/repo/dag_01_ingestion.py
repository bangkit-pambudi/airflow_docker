"""
DAG 1 — INGESTION
AdventureWorks (PostgreSQL) → MinIO (raw Parquet)
"""

import io
import logging
from datetime import datetime, timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────
AW_CONN_ID     = "adventure_works"  # Connection ID untuk PostgreSQL di Airflow
MINIO_ENDPOINT = Variable.get("MINIO_ENDPOINT") # host.docker.internal:9000	
MINIO_ACCESS   = Variable.get("MINIO_ACCESS_KEY") # minioadmin
MINIO_SECRET   = Variable.get("MINIO_SECRET_KEY") # minioadmin123
MINIO_BUCKET   = Variable.get("MINIO_BUCKET") #adventureworks-elt
MINIO_SECURE   = False

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

# ── Tabel yang akan di-ingest ─────────────────────────────────
# DIUBAH: Menjadi List of Dictionaries agar siap masuk ke .expand(op_kwargs)
INGEST_TABLES = [
    {"schema": "Sales",      "table": "SalesOrderHeader",   "folder": "sales_order_header"},
    {"schema": "Sales",      "table": "SalesOrderDetail",   "folder": "sales_order_detail"},
    {"schema": "Sales",      "table": "SalesTerritory",     "folder": "sales_territory"},
    {"schema": "Production", "table": "Product",            "folder": "product"},
    {"schema": "Production", "table": "ProductSubcategory", "folder": "product_subcategory"},
    {"schema": "Production", "table": "ProductCategory",    "folder": "product_category"},
]


# ── Helpers ───────────────────────────────────────────────────
def get_minio() -> Minio:
    return Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS,
        secret_key=MINIO_SECRET,
        secure=MINIO_SECURE,
    )


def df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    table  = pa.Table.from_pandas(df, preserve_index=False)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    return buffer.getvalue()


def upload_parquet(client: Minio, object_path: str, data: bytes) -> None:
    client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_path,
        data=io.BytesIO(data),
        length=len(data),
        content_type="application/octet-stream",
    )
    log.info(f"Uploaded → s3://{MINIO_BUCKET}/{object_path} ({len(data)/1024:.1f} KB)")


# ════════════════════════════════════════════════════════════
# TASK FUNCTIONS
# ════════════════════════════════════════════════════════════

def check_connections(**context):
    """Verifikasi koneksi PostgreSQL dan MinIO."""
    hook = PostgresHook(postgres_conn_id=AW_CONN_ID)
    conn = hook.get_conn()
    cur  = conn.cursor()
    cur.execute("SELECT version();")
    ver = cur.fetchone()[0]
    cur.close()
    conn.close()
    log.info(f"✅ PostgreSQL OK → {ver[:50]}")

    client = get_minio()
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
        log.info(f"✅ Bucket '{MINIO_BUCKET}' dibuat.")
    else:
        log.info(f"✅ MinIO OK → bucket '{MINIO_BUCKET}' exists.")


def ingest_table(schema: str, table: str, folder: str, **context):
    """
    Ingest satu tabel dari PostgreSQL ke MinIO sebagai Parquet.
    Fungsi ini akan dijalankan paralel oleh Dynamic Task Mapping.
    """
    logical_date = context["ds"]
    object_path  = f"raw/{folder}/dt={logical_date}/data.parquet"

    hook = PostgresHook(postgres_conn_id=AW_CONN_ID)
    sql  = f'SELECT * FROM "{schema}"."{table}"'

    log.info(f"Extracting {schema}.{table}...")
    df = hook.get_pandas_df(sql)
    log.info(f"  → {len(df):,} rows, {len(df.columns)} columns")

    for col in df.select_dtypes(include=["datetimetz"]).columns:
        df[col] = df[col].dt.tz_localize(None)

    data   = df_to_parquet_bytes(df)
    client = get_minio()
    upload_parquet(client, object_path, data)

    log.info(f"✅ {table}: {len(df):,} rows → {object_path}")

    # DIUBAH: Cukup return dictionary, Airflow otomatis menyimpannya di XCom
    return {
        "table": table,
        "path": object_path,
        "rows": len(df)
    }


def validate_ingestion(**context):
    """Validasi semua file Parquet di MinIO menggunakan hasil dari task sebelumnya."""
    client = get_minio()
    ti     = context["ti"]

    # DIUBAH: xcom_pull dari task yang di-expand akan mengembalikan LIST of Dictionaries
    ingest_results = ti.xcom_pull(task_ids="ingest_tables")
    
    if not ingest_results:
        raise ValueError("❌ Tidak ada data dari task 'ingest_tables' di XCom!")

    results_summary = {}
    
    # Looping langsung dari hasil XCom, tidak perlu lagi looping INGEST_TABLES
    for item in ingest_results:
        table = item["table"]
        path  = item["path"]
        rows  = item["rows"]

        try:
            resp   = client.get_object(MINIO_BUCKET, path)
            buf    = io.BytesIO(resp.read())
            schema_pq = pq.read_schema(buf)

            results_summary[table] = {
                "status": "✅ OK",
                "info": f"{rows:,} rows | {len(schema_pq.names)} cols | {path}"
            }
            log.info(f"✅ {table}: {results_summary[table]['info']}")

        except Exception as e:
            results_summary[table] = {"status": "❌ FAILED", "info": str(e)}
            log.error(f"❌ {table}: {e}")

    # Summary
    log.info("=" * 60)
    log.info("INGESTION VALIDATION SUMMARY")
    log.info("=" * 60)
    for tbl, info in results_summary.items():
        log.info(f"  {tbl}: {info['status']}")

    failed = [t for t, i in results_summary.items() if "FAILED" in i["status"]]
    if failed:
        raise ValueError(f"Validation failed untuk: {failed}")

    log.info("🎉 Semua tabel berhasil diingest ke MinIO!")


# ════════════════════════════════════════════════════════════
# DAG DEFINITION
# ════════════════════════════════════════════════════════════
with DAG(
    dag_id="elt_01_ingestion",
    default_args=default_args,
    description="DAG 1 — Ingest raw tables dari AdventureWorks ke MinIO (Parquet)",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["elt", "ingestion", "minio", "parquet"],
) as dag:

    task_check = PythonOperator(
        task_id="check_connections",
        python_callable=check_connections,
    )

    # Implementasi Dynamic Task Mapping (Jauh lebih bersih tanpa for loop)
    task_ingest = PythonOperator.partial(
        task_id="ingest_tables",
        python_callable=ingest_table,
    ).expand(
        op_kwargs=INGEST_TABLES
    )

    task_validate = PythonOperator(
        task_id="validate_ingestion",
        python_callable=validate_ingestion,
    )

    # Dependency yang sangat mudah dibaca
    task_check >> task_ingest >> task_validate