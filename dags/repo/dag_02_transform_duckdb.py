"""
DAG 2 — TRANSFORM (DuckDB)
MinIO (raw Parquet) → DuckDB [bronze → silver → gold]

Arsitektur DuckDB:
  ┌─────────────────────────────────────────────────────┐
  │                   DuckDB                            │
  │                                                     │
  │  bronze.*   ← baca langsung Parquet dari MinIO      │
  │     ↓                                               │
  │  silver.*   ← stg_* dan trf_* (transform SQL)       │
  │     ↓                                               │
  │  gold.*     ← fact_sales_performance (final)        │
  └─────────────────────────────────────────────────────┘
        ↓
  Export gold.fact_sales_performance → MinIO Parquet

DuckDB S3 Extension digunakan untuk baca/tulis Parquet di MinIO
tanpa perlu download file ke local terlebih dahulu.

Pipeline:
  setup_duckdb_s3
        ↓
  create_bronze_layer      ← VIEW ke Parquet di MinIO
        ↓
  create_silver_layer      ← stg_sales_orders, stg_products, trf_sales_summary
        ↓
  create_gold_layer        ← fact_sales_performance
        ↓
  export_gold_to_minio     ← simpan fact sebagai Parquet di MinIO
        ↓
  run_analytic_queries     ← validasi + analytic queries demo
"""

import io
import os
import logging
from datetime import datetime, timedelta

import duckdb
import pandas as pd
import pyarrow.parquet as pq
from minio import Minio

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable # <-- Tambahkan import ini

log = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────
AW_CONN_ID     = "adventure_works"  # Connection ID untuk PostgreSQL di Airflow
MINIO_ENDPOINT = Variable.get("MINIO_ENDPOINT") # host.docker.internal:9000	
MINIO_ACCESS   = Variable.get("MINIO_ACCESS_KEY") # minioadmin
MINIO_SECRET   = Variable.get("MINIO_SECRET_KEY") # minioadmin123
MINIO_BUCKET   = Variable.get("MINIO_BUCKET") #adventureworks-elt
MINIO_SECURE   = False
DUCKDB_PATH    = "/opt/airflow/dags/repo/dwh.duckdb"

# S3 endpoint untuk DuckDB (http karena MinIO lokal tidak pakai HTTPS)
S3_ENDPOINT    = f"http://{MINIO_ENDPOINT}"
S3_PREFIX      = f"s3://{MINIO_BUCKET}"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


# ── Helpers ───────────────────────────────────────────────────
def get_minio() -> Minio:
    return Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS,
        secret_key=MINIO_SECRET,
        secure=MINIO_SECURE,
    )


def get_duckdb_conn() -> duckdb.DuckDBPyConnection:
    """
    Buka koneksi DuckDB dan konfigurasi S3 extension
    agar bisa baca/tulis Parquet di MinIO.
    """
    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
    conn = duckdb.connect(DUCKDB_PATH)

    # Install & load S3 extension
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")

    # Konfigurasi S3 → MinIO
    conn.execute(f"SET s3_endpoint='{MINIO_ENDPOINT}';")
    conn.execute(f"SET s3_access_key_id='{MINIO_ACCESS}';")
    conn.execute(f"SET s3_secret_access_key='{MINIO_SECRET}';")
    conn.execute("SET s3_use_ssl=false;")
    conn.execute("SET s3_url_style='path';")   # MinIO pakai path-style, bukan virtual-hosted

    return conn


def get_latest_partition(folder: str, logical_date: str) -> str:
    """
    Return path Parquet di MinIO untuk partisi tanggal tertentu.
    Format: raw/{folder}/dt={logical_date}/data.parquet
    """
    return f"{S3_PREFIX}/raw/{folder}/dt={logical_date}/data.parquet"


# ════════════════════════════════════════════════════════════
# TASK FUNCTIONS
# ════════════════════════════════════════════════════════════

def setup_duckdb_s3(**context):
    """
    Setup DuckDB:
    - Test koneksi S3/MinIO
    - Buat schema bronze, silver, gold
    - Verifikasi file Parquet dari DAG 1 bisa diakses
    """
    logical_date = context["ds"]
    conn = get_duckdb_conn()

    try:
        # Buat schema
        for schema in ("bronze", "silver", "gold"):
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            log.info(f"✅ Schema '{schema}' ready.")

        # Verifikasi Parquet dari DAG 1 bisa dibaca via S3
        tables_to_check = [
            ("sales_order_header", "SalesOrderID"),
            ("sales_order_detail", "SalesOrderDetailID"),
            ("sales_territory",    "TerritoryID"),
            ("product",            "ProductID"),
            ("product_subcategory","ProductSubcategoryID"),
            ("product_category",   "ProductCategoryID"),
        ]

        for folder, pk in tables_to_check:
            path = get_latest_partition(folder, logical_date)
            print(f"🔍 Verifying access to {path}...")
            result = conn.execute(
                f"SELECT COUNT(*) AS cnt FROM read_parquet('{path}')"
            ).fetchone()
            log.info(f"✅ {folder}: {result[0]:,} rows accessible via S3")

        log.info("✅ DuckDB S3 setup complete.")
    finally:
        conn.close()


def create_bronze_layer(**context):
    """
    Bronze Layer: Buat VIEW di DuckDB yang membaca langsung
    Parquet mentah dari MinIO. Tidak ada transformasi.

    Prinsip bronze:
    - Data apa adanya dari source
    - Schema sesuai source, tidak ada kolom baru
    - Hanya VIEW, bukan tabel fisik (hemat storage)
    """
    logical_date = context["ds"]
    conn = get_duckdb_conn()

    try:
        bronze_views = {
            "sales_order_header":  ("sales_order_header",  logical_date),
            "sales_order_detail":  ("sales_order_detail",  logical_date),
            "sales_territory":     ("sales_territory",      logical_date),
            "product":             ("product",              logical_date),
            "product_subcategory": ("product_subcategory",  logical_date),
            "product_category":    ("product_category",     logical_date),
        }

        for view_name, (folder, dt) in bronze_views.items():
            path = get_latest_partition(folder, dt)

            # DROP dan recreate VIEW agar selalu fresh
            conn.execute(f"DROP VIEW IF EXISTS bronze.{view_name};")
            conn.execute(f"""
                CREATE VIEW bronze.{view_name} AS
                SELECT * FROM read_parquet('{path}');
            """)

            # Verifikasi VIEW bisa diquery
            cnt = conn.execute(f"SELECT COUNT(*) FROM bronze.{view_name}").fetchone()[0]
            log.info(f"✅ bronze.{view_name}: {cnt:,} rows (VIEW → {path})")

        log.info("✅ Bronze layer complete — semua VIEW siap.")
    finally:
        conn.close()


def create_silver_layer(**context):
    """
    Silver Layer: Transform data dari bronze menjadi:
    - silver.stg_sales_orders  : JOIN SalesOrderHeader + Detail + Territory
    - silver.stg_products      : JOIN Product + Subcategory + Category
    - silver.trf_sales_summary : Agregasi + kalkulasi Margin & MarginPct

    Prinsip silver:
    - Data sudah bersih dan terstruktur
    - JOIN dilakukan di sini
    - Kalkulasi dasar (margin, agregasi)
    - Disimpan sebagai TABLE fisik di DuckDB (bukan VIEW)
    """
    conn = get_duckdb_conn()

    try:
        # ── stg_sales_orders ─────────────────────────────────
        log.info("Creating silver.stg_sales_orders...")
        conn.execute("DROP TABLE IF EXISTS silver.stg_sales_orders;")
        conn.execute("""
            CREATE TABLE silver.stg_sales_orders AS
            SELECT
                soh."SalesOrderID",
                sod."SalesOrderDetailID",
                soh."OrderDate"::DATE               AS "OrderDate",
                sod."ProductID",
                sod."OrderQty"::INTEGER             AS "OrderQty",
                sod."UnitPrice"::DOUBLE             AS "UnitPrice",
                sod."LineTotal"::DOUBLE             AS "LineTotal",
                soh."TerritoryID",
                st."Name"                           AS "TerritoryName",
                st."CountryRegionCode"
            FROM bronze.sales_order_header AS soh
            INNER JOIN bronze.sales_order_detail AS sod
                ON soh."SalesOrderID" = sod."SalesOrderID"
            INNER JOIN bronze.sales_territory AS st
                ON soh."TerritoryID" = st."TerritoryID"
            -- Filter hanya order yang sudah selesai
            WHERE soh."Status" = 5;
        """)
        cnt = conn.execute("SELECT COUNT(*) FROM silver.stg_sales_orders").fetchone()[0]
        log.info(f"✅ silver.stg_sales_orders: {cnt:,} rows")

        # ── stg_products ──────────────────────────────────────
        log.info("Creating silver.stg_products...")
        conn.execute("DROP TABLE IF EXISTS silver.stg_products;")
        conn.execute("""
            CREATE TABLE silver.stg_products AS
            SELECT
                p."ProductID",
                p."Name"                            AS "ProductName",
                p."ProductNumber",
                p."ListPrice"::DOUBLE               AS "ListPrice",
                p."StandardCost"::DOUBLE            AS "StandardCost",
                COALESCE(pc."Name", 'N/A')          AS "Category",
                COALESCE(ps."Name", 'N/A')          AS "Subcategory"
            FROM bronze.product AS p
            LEFT JOIN bronze.product_subcategory AS ps
                ON p."ProductSubcategoryID" = ps."ProductSubcategoryID"
            LEFT JOIN bronze.product_category AS pc
                ON ps."ProductCategoryID" = pc."ProductCategoryID"
            WHERE p."ListPrice" > 0;
        """)
        cnt = conn.execute("SELECT COUNT(*) FROM silver.stg_products").fetchone()[0]
        log.info(f"✅ silver.stg_products: {cnt:,} rows")

        # ── trf_sales_summary ─────────────────────────────────
        log.info("Creating silver.trf_sales_summary...")
        conn.execute("DROP TABLE IF EXISTS silver.trf_sales_summary;")
        conn.execute("""
            CREATE TABLE silver.trf_sales_summary AS
            SELECT
                p."ProductID",
                p."ProductName",
                p."ProductNumber",
                p."Category",
                p."Subcategory",
                o."TerritoryID",
                o."TerritoryName",
                o."CountryRegionCode",
                -- Agregasi
                ROUND(SUM(o."LineTotal"), 2)                    AS "TotalRevenue",
                SUM(o."OrderQty")                               AS "TotalQty",
                ROUND(AVG(o."UnitPrice"), 2)                    AS "AvgUnitPrice",
                -- Atribut produk
                p."ListPrice",
                p."StandardCost",
                -- Kalkulasi margin
                ROUND(p."ListPrice" - p."StandardCost", 2)      AS "Margin",
                ROUND(
                    (p."ListPrice" - p."StandardCost")
                    / NULLIF(p."ListPrice", 0) * 100
                , 2)                                            AS "MarginPct"
            FROM silver.stg_sales_orders AS o
            INNER JOIN silver.stg_products AS p
                ON o."ProductID" = p."ProductID"
            GROUP BY
                p."ProductID",
                p."ProductName",
                p."ProductNumber",
                p."Category",
                p."Subcategory",
                o."TerritoryID",
                o."TerritoryName",
                o."CountryRegionCode",
                p."ListPrice",
                p."StandardCost";
        """)
        cnt = conn.execute("SELECT COUNT(*) FROM silver.trf_sales_summary").fetchone()[0]
        log.info(f"✅ silver.trf_sales_summary: {cnt:,} rows")

        log.info("✅ Silver layer complete.")
    finally:
        conn.close()


def create_gold_layer(**context):
    """
    Gold Layer: Buat fact table final dari silver.trf_sales_summary.
    Tambahkan metadata kolom: load_timestamp, batch_date.

    Prinsip gold:
    - Data final, siap dikonsumsi BI / analitik
    - Schema stabil dan terdokumentasi
    - Ada audit trail (load_timestamp)
    - PRIMARY KEY jelas (ProductID, TerritoryID)
    """
    logical_date = context["ds"]
    conn = get_duckdb_conn()

    try:
        log.info("Creating gold.fact_sales_performance...")
        conn.execute("DROP TABLE IF EXISTS gold.fact_sales_performance;")
        conn.execute(f"""
            CREATE TABLE gold.fact_sales_performance AS
            SELECT
                "ProductID",
                "ProductName",
                "ProductNumber",
                "Category",
                "Subcategory",
                "TerritoryID",
                "TerritoryName",
                "CountryRegionCode",
                "TotalRevenue",
                "TotalQty",
                "AvgUnitPrice",
                "ListPrice",
                "StandardCost",
                "Margin",
                "MarginPct",
                -- Metadata audit
                CURRENT_TIMESTAMP                   AS "LoadTimestamp",
                '{logical_date}'::DATE              AS "BatchDate"
            FROM silver.trf_sales_summary;
        """)

        cnt = conn.execute(
            "SELECT COUNT(*) FROM gold.fact_sales_performance"
        ).fetchone()[0]
        log.info(f"✅ gold.fact_sales_performance: {cnt:,} rows")
    finally:
        conn.close()



# ════════════════════════════════════════════════════════════
# DAG DEFINITION
# ════════════════════════════════════════════════════════════
with DAG(
    dag_id="elt_02_transform_duckdb",
    default_args=default_args,
    description="DAG 2 — Transform di DuckDB: bronze → silver → gold",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["elt", "duckdb", "transform", "bronze", "silver", "gold"],
    doc_md="""
## DAG 2 — Transform with DuckDB

**Tujuan:** Baca Parquet dari MinIO (hasil DAG 1), transform di DuckDB
menjadi 3 layer: bronze → silver → gold.

**Arsitektur DuckDB:**
```
bronze.*  → VIEW langsung ke Parquet di MinIO (no copy)
silver.*  → stg_sales_orders, stg_products, trf_sales_summary
gold.*    → fact_sales_performance (final, dengan audit trail)
```

**Pipeline:**
```
setup_duckdb_s3
      ↓
create_bronze_layer   ← VIEW ke MinIO Parquet
      ↓
create_silver_layer   ← JOIN + kalkulasi
      ↓
create_gold_layer     ← fact table final
      ↓
export_gold_to_minio  ← simpan balik ke MinIO
      ↓
run_analytic_queries  ← demo 5 query analitik
```

**Jalankan setelah DAG 1 (elt_01_ingestion) selesai.**
    """,
) as dag:

    task_setup = PythonOperator(
        task_id="setup_duckdb_s3",
        python_callable=setup_duckdb_s3,
    )

    task_bronze = PythonOperator(
        task_id="create_bronze_layer",
        python_callable=create_bronze_layer,
    )

    task_silver = PythonOperator(
        task_id="create_silver_layer",
        python_callable=create_silver_layer,
    )

    task_gold = PythonOperator(
        task_id="create_gold_layer",
        python_callable=create_gold_layer,
    )

    # Pipeline sekuensial (tiap layer bergantung ke sebelumnya)
    (
        task_setup
        >> task_bronze
        >> task_silver
        >> task_gold
    )
