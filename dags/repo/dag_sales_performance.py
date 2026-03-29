"""
DAG: adventure_works_sales_performance
Deskripsi: Pipeline ETL Sales Performance dari AdventureWorks
           ke Data Warehouse schema dwh.
           Skenario: "Produk apa yang paling laris per wilayah?"

Pipeline  : check_connection → extract → transform → load
Source    : Sales, Production schema (AdventureWorks)
Target    : schema dwh
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

# ──────────────────────────────────────────────
# Konstanta
# ──────────────────────────────────────────────
CONN_ID       = "adventure_works"
TARGET_SCHEMA = "example"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


# ──────────────────────────────────────────────
# Helper
# ──────────────────────────────────────────────
def get_hook():
    return PostgresHook(postgres_conn_id=CONN_ID)


# ──────────────────────────────────────────────
# Task 1 — Check Connection
# ──────────────────────────────────────────────
def check_connection(**kwargs):
    """Verifikasi koneksi ke PostgreSQL adventure_works."""
    hook = get_hook()
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT version();")
    version = cursor.fetchone()
    print(f"[check_connection] Koneksi berhasil!")
    print(f"[check_connection] {version[0]}")

    cursor.close()
    conn.close()


# ──────────────────────────────────────────────
# Task 2 — Extract
# Source  → dwh.stg_sales_orders
#         → dwh.stg_products
# ──────────────────────────────────────────────
def extract(**kwargs):
    """
    Ekstrak data transaksi penjualan dan produk dari AdventureWorks.
    Hasil disimpan ke:
      - dwh.stg_sales_orders  (data order mentah)
      - dwh.stg_products      (data produk mentah)
    """
    hook = get_hook()
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # ── Setup schema & tabel staging ──────────────────────────────
        cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{TARGET_SCHEMA}";')

        # stg_sales_orders
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS "{TARGET_SCHEMA}"."stg_sales_orders" (
                "SalesOrderID"       INT,
                "SalesOrderDetailID" INT,
                "OrderDate"          DATE,
                "ProductID"          INT,
                "OrderQty"           SMALLINT,
                "UnitPrice"          NUMERIC(10, 2),
                "LineTotal"          NUMERIC(12, 2),
                "TerritoryID"        INT,
                "TerritoryName"      VARCHAR(100),
                "CountryRegionCode"  VARCHAR(10)
            );
        """)

        # stg_products
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS "{TARGET_SCHEMA}"."stg_products" (
                "ProductID"      INT,
                "ProductName"    VARCHAR(255),
                "ProductNumber"  VARCHAR(50),
                "ListPrice"      NUMERIC(10, 2),
                "StandardCost"   NUMERIC(10, 2),
                "Category"       VARCHAR(100),
                "Subcategory"    VARCHAR(100)
            );
        """)

        # ── Truncate sebelum load ──────────────────────────────────────
        cursor.execute(f'TRUNCATE TABLE "{TARGET_SCHEMA}"."stg_sales_orders";')
        cursor.execute(f'TRUNCATE TABLE "{TARGET_SCHEMA}"."stg_products";')

        # ── Extract: Sales Orders ──────────────────────────────────────
        # JOIN: SalesOrderHeader → SalesOrderDetail → SalesTerritory
        cursor.execute(f"""
            INSERT INTO "{TARGET_SCHEMA}"."stg_sales_orders" (
                "SalesOrderID",
                "SalesOrderDetailID",
                "OrderDate",
                "ProductID",
                "OrderQty",
                "UnitPrice",
                "LineTotal",
                "TerritoryID",
                "TerritoryName",
                "CountryRegionCode"
            )
            SELECT
                soh."SalesOrderID",
                sod."SalesOrderDetailID",
                soh."OrderDate",
                sod."ProductID",
                sod."OrderQty",
                sod."UnitPrice",
                sod."LineTotal",
                soh."TerritoryID",
                st."Name",
                st."CountryRegionCode"
            FROM "Sales"."SalesOrderHeader" AS soh
            INNER JOIN "Sales"."SalesOrderDetail" AS sod
                ON soh."SalesOrderID" = sod."SalesOrderID"
            INNER JOIN "Sales"."SalesTerritory" AS st
                ON soh."TerritoryID" = st."TerritoryID"
            WHERE soh."OnlineOrderFlag" = false
              AND soh."Status" = 5;
        """)

        # ── Extract: Products ──────────────────────────────────────────
        # JOIN: Product → ProductSubcategory → ProductCategory
        cursor.execute(f"""
            INSERT INTO "{TARGET_SCHEMA}"."stg_products" (
                "ProductID",
                "ProductName",
                "ProductNumber",
                "ListPrice",
                "StandardCost",
                "Category",
                "Subcategory"
            )
            SELECT
                p."ProductID",
                p."Name",
                p."ProductNumber",
                p."ListPrice",
                p."StandardCost",
                COALESCE(pc."Name", 'N/A'),
                COALESCE(ps."Name", 'N/A')
            FROM "Production"."Product" AS p
            LEFT JOIN "Production"."ProductSubcategory" AS ps
                ON p."ProductSubcategoryID" = ps."ProductSubcategoryID"
            LEFT JOIN "Production"."ProductCategory" AS pc
                ON ps."ProductCategoryID" = pc."ProductCategoryID"
            WHERE p."ListPrice" > 0;
        """)

        conn.commit()
        print(f"[extract] stg_sales_orders & stg_products berhasil di-load.")

    except Exception as e:
        conn.rollback()
        print(f"[extract] ERROR: {e}")
        raise

    finally:
        cursor.close()
        conn.close()


# ──────────────────────────────────────────────
# Task 3 — Transform
# stg_sales_orders + stg_products → dwh.trf_sales_summary
# ──────────────────────────────────────────────
def transform(**kwargs):
    """
    Gabungkan stg_sales_orders & stg_products, lalu hitung:
      - total_revenue    : total pendapatan per produk per wilayah
      - total_qty        : total unit terjual
      - avg_unit_price   : rata-rata harga jual
      - margin           : ListPrice - StandardCost
      - margin_pct       : margin dalam %
    Hasil disimpan ke dwh.trf_sales_summary
    """
    hook = get_hook()
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS "{TARGET_SCHEMA}"."trf_sales_summary" (
                "ProductID"       INT,
                "ProductName"     VARCHAR(255),
                "ProductNumber"   VARCHAR(50),
                "Category"        VARCHAR(100),
                "Subcategory"     VARCHAR(100),
                "TerritoryID"     INT,
                "TerritoryName"   VARCHAR(100),
                "CountryRegionCode" VARCHAR(10),
                "TotalRevenue"    NUMERIC(15, 2),
                "TotalQty"        INT,
                "AvgUnitPrice"    NUMERIC(10, 2),
                "ListPrice"       NUMERIC(10, 2),
                "StandardCost"    NUMERIC(10, 2),
                "Margin"          NUMERIC(10, 2),
                "MarginPct"       NUMERIC(6, 2)
            );
        """)

        cursor.execute(f'TRUNCATE TABLE "{TARGET_SCHEMA}"."trf_sales_summary";')

        # Transformasi: JOIN staging + agregasi + kalkulasi margin
        cursor.execute(f"""
            INSERT INTO "{TARGET_SCHEMA}"."trf_sales_summary" (
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
                "MarginPct"
            )
            SELECT
                p."ProductID",
                p."ProductName",
                p."ProductNumber",
                p."Category",
                p."Subcategory",
                o."TerritoryID",
                o."TerritoryName",
                o."CountryRegionCode",
                ROUND(SUM(o."LineTotal"), 2)                                         AS "TotalRevenue",
                SUM(o."OrderQty")                                                    AS "TotalQty",
                ROUND(AVG(o."UnitPrice"), 2)                                         AS "AvgUnitPrice",
                p."ListPrice",
                p."StandardCost",
                ROUND(p."ListPrice" - p."StandardCost", 2)                           AS "Margin",
                ROUND((p."ListPrice" - p."StandardCost") / p."ListPrice" * 100, 2)   AS "MarginPct"
            FROM "{TARGET_SCHEMA}"."stg_sales_orders" AS o
            INNER JOIN "{TARGET_SCHEMA}"."stg_products" AS p
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

        conn.commit()
        print(f"[transform] trf_sales_summary berhasil dibuat.")

    except Exception as e:
        conn.rollback()
        print(f"[transform] ERROR: {e}")
        raise

    finally:
        cursor.close()
        conn.close()


# ──────────────────────────────────────────────
# Task 4 — Load
# trf_sales_summary → dwh.fact_sales_performance
# ──────────────────────────────────────────────
def load(**kwargs):
    """
    Load data final ke dwh.fact_sales_performance.
    Tambahkan load_timestamp untuk audit trail.
    """
    hook = get_hook()
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS "{TARGET_SCHEMA}"."fact_sales_performance" (
                "ProductID"          INT            NOT NULL,
                "ProductName"        VARCHAR(255),
                "ProductNumber"      VARCHAR(50),
                "Category"           VARCHAR(100),
                "Subcategory"        VARCHAR(100),
                "TerritoryID"        INT            NOT NULL,
                "TerritoryName"      VARCHAR(100),
                "CountryRegionCode"  VARCHAR(10),
                "TotalRevenue"       NUMERIC(15, 2),
                "TotalQty"           INT,
                "AvgUnitPrice"       NUMERIC(10, 2),
                "ListPrice"          NUMERIC(10, 2),
                "StandardCost"       NUMERIC(10, 2),
                "Margin"             NUMERIC(10, 2),
                "MarginPct"          NUMERIC(6, 2),
                "LoadTimestamp"      TIMESTAMP      DEFAULT NOW(),
                PRIMARY KEY ("ProductID", "TerritoryID")
            );
        """)

        cursor.execute(f'TRUNCATE TABLE "{TARGET_SCHEMA}"."fact_sales_performance";')

        cursor.execute(f"""
            INSERT INTO "{TARGET_SCHEMA}"."fact_sales_performance" (
                "ProductID", "ProductName", "ProductNumber",
                "Category", "Subcategory",
                "TerritoryID", "TerritoryName", "CountryRegionCode",
                "TotalRevenue", "TotalQty", "AvgUnitPrice",
                "ListPrice", "StandardCost", "Margin", "MarginPct",
                "LoadTimestamp"
            )
            SELECT
                "ProductID", "ProductName", "ProductNumber",
                "Category", "Subcategory",
                "TerritoryID", "TerritoryName", "CountryRegionCode",
                "TotalRevenue", "TotalQty", "AvgUnitPrice",
                "ListPrice", "StandardCost", "Margin", "MarginPct",
                NOW()
            FROM "{TARGET_SCHEMA}"."trf_sales_summary";
        """)

        cursor.execute(f'SELECT COUNT(*) FROM "{TARGET_SCHEMA}"."fact_sales_performance";')
        total_rows = cursor.fetchone()[0]

        conn.commit()
        print(f"[load] Berhasil memuat {total_rows} baris ke {TARGET_SCHEMA}.fact_sales_performance.")

    except Exception as e:
        conn.rollback()
        print(f"[load] ERROR: {e}")
        raise

    finally:
        cursor.close()
        conn.close()


# ──────────────────────────────────────────────
# DAG definition
# ──────────────────────────────────────────────
with DAG(
    dag_id="adventure_works_sales_performance",
    default_args=default_args,
    description="ETL Sales Performance AdventureWorks → dwh.fact_sales_performance",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["postgres", "adventure_works", "etl", "sales"],
) as dag:

    task_check_connection = PythonOperator(
        task_id="check_connection",
        python_callable=check_connection,
    )

    task_extract = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    task_transform = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    task_load = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    task_check_connection >> task_extract >> task_transform >> task_load
