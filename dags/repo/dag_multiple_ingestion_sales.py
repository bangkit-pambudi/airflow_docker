"""
DAG 1 — INGESTION
AdventureWorks (PostgreSQL) → MinIO (raw Parquet)
"""

import io
import os
import logging
from datetime import datetime, timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable # <-- Tambahkan import ini

log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────
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
    """Ingest satu tabel dari PostgreSQL ke MinIO sebagai Parquet."""
    logical_date = context["ds"]
    client       = get_minio()
    hook         = PostgresHook(postgres_conn_id=AW_CONN_ID)

    log.info(f"Extracting {schema}.{table}...")
    df = hook.get_pandas_df(f'SELECT * FROM "{schema}"."{table}"')
    log.info(f"  → {len(df):,} rows, {len(df.columns)} columns")

    for col in df.select_dtypes(include=["datetimetz"]).columns:
        df[col] = df[col].dt.tz_localize(None)

    object_path = f"raw/{folder}/dt={logical_date}/data.parquet"
    upload_parquet(client, object_path, df_to_parquet_bytes(df))
    log.info(f"✅ {table}: {len(df):,} rows → {object_path}")


# ════════════════════════════════════════════════════════════
# DAG DEFINITION
# ════════════════════════════════════════════════════════════
with DAG(
    dag_id="dag_multiple_ingestion_sales",
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

    task_ingest = PythonOperator.partial(
        task_id="ingest_table",
        python_callable=ingest_table,
    ).expand(
        # ✅ Ubah expand_kwargs menjadi expand(op_kwargs=...)
        op_kwargs=INGEST_TABLES
    )

    task_check >> task_ingest