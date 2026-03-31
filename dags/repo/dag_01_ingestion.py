"""
DAG 1 — INGESTION
AdventureWorks (PostgreSQL) → MinIO (raw Parquet)

Tujuan:
  Hanya LOAD data mentah ke MinIO sebagai Parquet.
  TIDAK ADA transformasi apapun di DAG ini.
  Data diambil apa adanya dari source.

Output MinIO layout:
  adventureworks-elt/
  └── raw/
      ├── sales_order_header/
      │   └── dt=YYYY-MM-DD/data.parquet
      ├── sales_order_detail/
      │   └── dt=YYYY-MM-DD/data.parquet
      ├── sales_territory/
      │   └── dt=YYYY-MM-DD/data.parquet
      ├── product/
      │   └── dt=YYYY-MM-DD/data.parquet
      ├── product_subcategory/
      │   └── dt=YYYY-MM-DD/data.parquet
      └── product_category/
          └── dt=YYYY-MM-DD/data.parquet

Pipeline:
  check_connections
        ↓
  ingest_sales_order_header
  ingest_sales_order_detail    ← semua paralel
  ingest_sales_territory
  ingest_product
  ingest_product_subcategory
  ingest_product_category
        ↓
  validate_ingestion
"""

import io
import os
import logging
from datetime import datetime, timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from minio.error import S3Error

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────
AW_CONN_ID     = "adventure_works"
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET   = os.getenv("MINIO_BUCKET", "adventureworks-elt")
MINIO_SECURE   = os.getenv("MINIO_SECURE", "false").lower() == "true"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

# ── Tabel yang akan di-ingest ─────────────────────────────────
# Format: (task_id, schema, table, minio_folder)
INGEST_TABLES = [
    ("ingest_sales_order_header",   "Sales",      "SalesOrderHeader",      "sales_order_header"),
    ("ingest_sales_order_detail",   "Sales",      "SalesOrderDetail",      "sales_order_detail"),
    ("ingest_sales_territory",      "Sales",      "SalesTerritory",        "sales_territory"),
    ("ingest_product",              "Production", "Product",               "product"),
    ("ingest_product_subcategory",  "Production", "ProductSubcategory",    "product_subcategory"),
    ("ingest_product_category",     "Production", "ProductCategory",       "product_category"),
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
    """Konversi DataFrame ke bytes Parquet (in-memory, snappy compression)."""
    table  = pa.Table.from_pandas(df, preserve_index=False)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    return buffer.getvalue()


def upload_parquet(client: Minio, object_path: str, data: bytes) -> None:
    """Upload bytes Parquet ke MinIO."""
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
    # Check PostgreSQL
    hook = PostgresHook(postgres_conn_id=AW_CONN_ID)
    conn = hook.get_conn()
    cur  = conn.cursor()
    cur.execute("SELECT version();")
    ver = cur.fetchone()[0]
    cur.close()
    conn.close()
    log.info(f"✅ PostgreSQL OK → {ver[:50]}")

    # Check MinIO & pastikan bucket ada
    client = get_minio()
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
        log.info(f"✅ Bucket '{MINIO_BUCKET}' dibuat.")
    else:
        log.info(f"✅ MinIO OK → bucket '{MINIO_BUCKET}' exists.")


def make_ingest_fn(schema: str, table: str, folder: str):
    """
    Factory function: membuat fungsi ingest untuk satu tabel.
    Extract SEMUA kolom dari tabel source tanpa filter apapun.
    Upload sebagai raw Parquet ke MinIO dengan partisi per tanggal.
    """
    def _ingest(**context):
        logical_date = context["ds"]   # format YYYY-MM-DD dari Airflow
        object_path  = f"raw/{folder}/dt={logical_date}/data.parquet"

        # Extract dari PostgreSQL — ambil semua kolom, tidak ada WHERE
        hook = PostgresHook(postgres_conn_id=AW_CONN_ID)
        sql  = f'SELECT * FROM "{schema}"."{table}"'

        log.info(f"Extracting {schema}.{table}...")
        df = hook.get_pandas_df(sql)
        log.info(f"  → {len(df):,} rows, {len(df.columns)} columns")

        # Konversi datetime columns agar kompatibel dengan Parquet
        for col in df.select_dtypes(include=["datetimetz"]).columns:
            df[col] = df[col].dt.tz_localize(None)

        # Upload ke MinIO
        data   = df_to_parquet_bytes(df)
        client = get_minio()
        upload_parquet(client, object_path, data)

        log.info(f"✅ {table}: {len(df):,} rows → {object_path}")

        # Simpan metadata ke XCom
        context["ti"].xcom_push(key=f"{folder}_path", value=object_path)
        context["ti"].xcom_push(key=f"{folder}_rows", value=len(df))

    _ingest.__name__ = f"ingest_{folder}"
    return _ingest


def validate_ingestion(**context):
    """
    Validasi semua file Parquet di MinIO:
    - File ada
    - Bisa dibaca
    - Jumlah kolom > 0
    """
    client = get_minio()
    ti     = context["ti"]

    results = {}
    for task_id, schema, table, folder in INGEST_TABLES:
        path = ti.xcom_pull(key=f"{folder}_path", task_ids=task_id)
        rows = ti.xcom_pull(key=f"{folder}_rows", task_ids=task_id)

        try:
            # Baca kembali dari MinIO untuk validasi
            resp   = client.get_object(MINIO_BUCKET, path)
            buf    = io.BytesIO(resp.read())
            schema_pq = pq.read_schema(buf)

            results[table] = {
                "path": path,
                "rows": rows,
                "columns": len(schema_pq.names),
                "status": "✅ OK",
            }
            log.info(f"✅ {table}: {rows:,} rows | {len(schema_pq.names)} cols | {path}")

        except Exception as e:
            results[table] = {"status": f"❌ FAILED: {e}"}
            log.error(f"❌ {table}: {e}")

    # Summary
    log.info("=" * 60)
    log.info("INGESTION VALIDATION SUMMARY")
    log.info("=" * 60)
    for tbl, info in results.items():
        log.info(f"  {tbl}: {info['status']}")

    failed = [t for t, i in results.items() if "FAILED" in i["status"]]
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
    doc_md="""
## DAG 1 — Ingestion

**Tujuan:** Load data mentah dari AdventureWorks PostgreSQL ke MinIO sebagai Parquet.

**Tidak ada transformasi** — data diambil apa adanya (raw).

**Tabel yang di-ingest:**
- `Sales.SalesOrderHeader`
- `Sales.SalesOrderDetail`
- `Sales.SalesTerritory`
- `Production.Product`
- `Production.ProductSubcategory`
- `Production.ProductCategory`

**Output MinIO:** `raw/{table_name}/dt=YYYY-MM-DD/data.parquet`

Setelah DAG ini selesai, jalankan **DAG 2 (elt_02_transform_duckdb)**.
    """,
) as dag:

    task_check = PythonOperator(
        task_id="check_connections",
        python_callable=check_connections,
    )

    # Buat task ingest secara dinamis per tabel
    ingest_tasks = []
    for task_id, schema, table, folder in INGEST_TABLES:
        t = PythonOperator(
            task_id=task_id,
            python_callable=make_ingest_fn(schema, table, folder),
        )
        ingest_tasks.append(t)

    task_validate = PythonOperator(
        task_id="validate_ingestion",
        python_callable=validate_ingestion,
    )

    # check → semua ingest paralel → validate
    task_check >> ingest_tasks >> task_validate
