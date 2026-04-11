"""
DAG 3 — weather_bronze_to_silver
Trigger: Dataset dari DAG 2 (Bronze DuckDB selesai di-load)

Tugas:
  Bronze → Silver: cleaning, normalisasi, enrich dimensi
  SEMUA transformasi dilakukan langsung di DuckDB SQL (zero Python business logic)

Silver = data bersih, terstruktur, siap untuk analitik.
  - Tabel: silver_weather_daily, silver_weather_hourly
  - Berisi: data yang sudah dibersihkan, field-field yang sudah dinormalisasi,
            FK ke dim tables, flag-flag dasar (is_wet_season, dll)

Downstream: weather_4_silver_to_gold
"""

import logging
import os
from datetime import datetime, timedelta

import duckdb
from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

logger = logging.getLogger(__name__)

# ─── Constants ─────────────────────────────────────────────────
DB_PATH = os.getenv("DUCKDB_PATH", "/opt/airflow/dags/repo/weather_indonesia.duckdb")

DUCKDB_BRONZE_DATASET = Dataset("duckdb://weather_indonesia/bronze")
DUCKDB_SILVER_DATASET = Dataset("duckdb://weather_indonesia/silver")

default_args = {
    "owner":            "weather-pipeline",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}


def get_conn() -> duckdb.DuckDBPyConnection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return duckdb.connect(DB_PATH)


# ═══════════════════════════════════════════════════════════════
# TASK FUNCTIONS — semua SQL dijalankan di DuckDB
# ═══════════════════════════════════════════════════════════════

def task_init_silver_schema(**ctx):
    """Buat tabel Silver jika belum ada."""
    conn = get_conn()

    # --- DROP SEMENTARA UNTUK RESET SCHEMA SILVER (DATA BRONZE AMAN) ---
    conn.execute("DROP TABLE IF EXISTS silver_weather_daily;")
    conn.execute("DROP TABLE IF EXISTS silver_weather_hourly;")
    # -------------------------------------------------------------------

    # Silver Daily — data bersih + FK ke dim
    conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_silver_daily;")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS silver_weather_daily (
            silver_id           INTEGER PRIMARY KEY DEFAULT nextval('seq_silver_daily'),
            -- FK
            location_key        VARCHAR NOT NULL,
            obs_date            DATE    NOT NULL,
            run_date            DATE    NOT NULL,
            -- Cleaned measurements
            temp_max_c          DOUBLE,
            temp_min_c          DOUBLE,
            temp_avg_c          DOUBLE,
            apparent_temp_max_c DOUBLE,
            precip_mm           DOUBLE,
            windspeed_kmh       DOUBLE,
            humidity_pct        DOUBLE,
            uv_index_max        DOUBLE,
            evapotranspiration  DOUBLE,
            weathercode         INTEGER,
            -- Derived time features (dari obs_date)
            year                INTEGER,
            month               INTEGER,
            quarter             INTEGER,
            day_of_week         INTEGER,
            week_of_year        INTEGER,
            season              VARCHAR,    -- Kemarau / Hujan
            monsoon_phase       VARCHAR,
            is_el_nino_period   BOOLEAN,
            is_peak_dry         BOOLEAN,
            is_peak_wet         BOOLEAN,
            -- Data lineage
            bronze_id           INTEGER,
            ingested_at         TIMESTAMP DEFAULT now(),
            transformed_at      TIMESTAMP DEFAULT now(),
            UNIQUE (location_key, obs_date, run_date)
        )
    """)

    # Silver Hourly
    conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_silver_hourly;")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS silver_weather_hourly (
            silver_id       INTEGER PRIMARY KEY DEFAULT nextval('seq_silver_hourly'),
            location_key    VARCHAR   NOT NULL,
            hour_timestamp  TIMESTAMP NOT NULL,
            run_date        DATE      NOT NULL,
            -- Cleaned fields
            temperature_c   DOUBLE,
            precipitation_mm DOUBLE,
            windspeed_kmh   DOUBLE,
            humidity_pct    DOUBLE,
            apparent_temp_c DOUBLE,
            precip_prob_pct DOUBLE,
            cloudcover_pct  DOUBLE,
            soil_moisture   DOUBLE,
            -- Derived
            obs_date        DATE,
            obs_hour        INTEGER,
            -- Lineage
            bronze_id       INTEGER,
            ingested_at     TIMESTAMP DEFAULT now(),
            transformed_at  TIMESTAMP DEFAULT now(),
            UNIQUE (location_key, hour_timestamp, run_date)
        )
    """)

    conn.close()
    logger.info("[SILVER] Schema siap.")


def task_transform_daily_to_silver(**ctx):
    """
    Bronze → Silver (daily) menggunakan DuckDB SQL murni.

    Transformasi yang dilakukan:
    1. Cleaning: clamp nilai ekstrim, isi NULL dengan default
    2. Derived: temp_avg, field-field waktu (season, monsoon, dll)
    3. Filter: hanya row yang belum diproses (is_processed = FALSE)
    """
    run_date = ctx["ds"]
    conn     = get_conn()

    result = conn.execute("""
        INSERT OR IGNORE INTO silver_weather_daily (
            location_key, obs_date, run_date,
            temp_max_c, temp_min_c, temp_avg_c, apparent_temp_max_c,
            precip_mm, windspeed_kmh, humidity_pct,
            uv_index_max, evapotranspiration, weathercode,
            year, month, quarter, day_of_week, week_of_year,
            season, monsoon_phase,
            is_el_nino_period, is_peak_dry, is_peak_wet,
            bronze_id
        )
        SELECT
            b.location_key,
            b.obs_date,
            b.run_date,

            -- ── Cleaning: clamp suhu ke rentang wajar Indonesia ──
            CASE
                WHEN b.temp_max < 10 OR b.temp_max > 50 THEN NULL
                ELSE ROUND(b.temp_max, 2)
            END AS temp_max_c,

            CASE
                WHEN b.temp_min < 5 OR b.temp_min > 45 THEN NULL
                ELSE ROUND(b.temp_min, 2)
            END AS temp_min_c,

            -- temp_avg = rata-rata max & min, fallback ke max jika min NULL
            ROUND(
                (
                    COALESCE(
                        CASE WHEN b.temp_max < 10 OR b.temp_max > 50 THEN NULL ELSE b.temp_max END,
                        30.0
                    ) +
                    COALESCE(
                        CASE WHEN b.temp_min < 5  OR b.temp_min > 45 THEN NULL ELSE b.temp_min END,
                        25.0
                    )
                ) / 2.0,
                2
            ) AS temp_avg_c,

            ROUND(COALESCE(b.apparent_temp_max, b.temp_max, 30.0), 2) AS apparent_temp_max_c,

            -- Presipitasi: clamp negatif ke 0
            ROUND(GREATEST(COALESCE(b.precip_sum, 0), 0), 2)  AS precip_mm,
            ROUND(GREATEST(COALESCE(b.windspeed_max, 0), 0), 2) AS windspeed_kmh,
            ROUND(LEAST(GREATEST(COALESCE(b.humidity_max, 70), 0), 100), 2) AS humidity_pct,

            COALESCE(b.uv_index_max, 0)        AS uv_index_max,
            COALESCE(b.evapotranspiration, 0)  AS evapotranspiration,
            b.weathercode,

            -- ── Time features langsung dari obs_date ──
            YEAR(b.obs_date)                                AS year,
            MONTH(b.obs_date)                               AS month,
            ((MONTH(b.obs_date) - 1) / 3) + 1              AS quarter,
            DAYOFWEEK(b.obs_date)                           AS day_of_week,
            WEEKOFYEAR(b.obs_date)                          AS week_of_year,

            CASE
                WHEN MONTH(b.obs_date) IN (4,5,6,7,8,9,10) THEN 'Kemarau'
                ELSE 'Hujan'
            END AS season,

            CASE
                WHEN MONTH(b.obs_date) IN (12,1,2)  THEN 'Puncak Hujan'
                WHEN MONTH(b.obs_date) IN (3,4)     THEN 'Akhir Hujan'
                WHEN MONTH(b.obs_date) IN (5,6)     THEN 'Awal Kemarau'
                WHEN MONTH(b.obs_date) IN (7,8,9)   THEN 'Puncak Kemarau'
                ELSE 'Awal Hujan'
            END AS monsoon_phase,

            b.obs_date >= DATE '2026-04-01'     AS is_el_nino_period,
            MONTH(b.obs_date) IN (7,8,9)        AS is_peak_dry,
            MONTH(b.obs_date) IN (12,1,2)       AS is_peak_wet,

            b.bronze_id

        FROM bronze_weather_daily b
        WHERE b.run_date = ?
          AND b.is_processed = FALSE
    """, [run_date])

    # Tandai bronze sebagai processed
    conn.execute("""
        UPDATE bronze_weather_daily
        SET is_processed = TRUE
        WHERE run_date = ?
          AND is_processed = FALSE
    """, [run_date])

    row_count = conn.execute("""
        SELECT COUNT(*) FROM silver_weather_daily WHERE run_date = ?
    """, [run_date]).fetchone()[0]

    conn.close()
    logger.info(f"[SILVER] Daily transform selesai: {row_count} baris untuk {run_date}")


def task_transform_hourly_to_silver(**ctx):
    """
    Bronze → Silver (hourly) menggunakan DuckDB SQL murni.
    """
    run_date = ctx["ds"]
    conn     = get_conn()

    conn.execute("""
        INSERT OR IGNORE INTO silver_weather_hourly (
            location_key, hour_timestamp, run_date,
            temperature_c, precipitation_mm, windspeed_kmh, humidity_pct,
            apparent_temp_c, precip_prob_pct, cloudcover_pct, soil_moisture,
            obs_date, obs_hour,
            bronze_id
        )
        SELECT
            b.location_key,
            b.hour_timestamp,
            b.run_date,

            -- Cleaning
            CASE
                WHEN b.temperature_2m < 0 OR b.temperature_2m > 50 THEN NULL
                ELSE ROUND(b.temperature_2m, 2)
            END AS temperature_c,

            ROUND(GREATEST(COALESCE(b.precipitation, 0), 0), 3)  AS precipitation_mm,
            ROUND(GREATEST(COALESCE(b.windspeed_10m, 0), 0), 2)  AS windspeed_kmh,
            ROUND(LEAST(GREATEST(COALESCE(b.humidity_2m, 70), 0), 100), 2) AS humidity_pct,
            ROUND(COALESCE(b.apparent_temp, b.temperature_2m, 28.0), 2) AS apparent_temp_c,
            ROUND(LEAST(GREATEST(COALESCE(b.precip_prob, 0), 0), 100), 2) AS precip_prob_pct,
            ROUND(LEAST(GREATEST(COALESCE(b.cloudcover, 0), 0), 100), 2) AS cloudcover_pct,
            ROUND(GREATEST(COALESCE(b.soil_moisture, 0), 0), 4) AS soil_moisture,

            -- Derived
            CAST(b.hour_timestamp AS DATE)                        AS obs_date,
            HOUR(b.hour_timestamp)                                AS obs_hour,

            b.bronze_id

        FROM bronze_weather_hourly b
        WHERE b.run_date = ?
          AND b.is_processed = FALSE
    """, [run_date])

    conn.execute("""
        UPDATE bronze_weather_hourly
        SET is_processed = TRUE
        WHERE run_date = ?
          AND is_processed = FALSE
    """, [run_date])

    row_count = conn.execute("""
        SELECT COUNT(*) FROM silver_weather_hourly WHERE run_date = ?
    """, [run_date]).fetchone()[0]

    conn.close()
    logger.info(f"[SILVER] Hourly transform selesai: {row_count} baris untuk {run_date}")


def task_silver_quality_check(**ctx):
    """
    DQ check Silver:
    - Jumlah kota & baris
    - Suhu tidak di luar rentang
    - Tidak ada obs_date NULL
    """
    run_date = ctx["ds"]
    conn     = get_conn()
    checks   = []

    city_count = conn.execute("""
        SELECT COUNT(DISTINCT location_key)
        FROM silver_weather_daily
        WHERE run_date = ?
    """, [run_date]).fetchone()[0]
    checks.append(("Kota Silver", city_count >= 10, f"{city_count}/15 kota"))

    bad_temp = conn.execute("""
        SELECT COUNT(*) FROM silver_weather_daily
        WHERE run_date = ?
          AND (temp_max_c < 10 OR temp_max_c > 50)
          AND temp_max_c IS NOT NULL
    """, [run_date]).fetchone()[0]
    checks.append(("Suhu Silver valid", bad_temp == 0, f"{bad_temp} anomali"))

    null_date = conn.execute("""
        SELECT COUNT(*) FROM silver_weather_daily
        WHERE run_date = ? AND obs_date IS NULL
    """, [run_date]).fetchone()[0]
    checks.append(("obs_date tidak NULL", null_date == 0, f"{null_date} NULL"))

    conn.close()

    all_pass = True
    for name, passed, detail in checks:
        status = "PASS" if passed else "FAIL"
        logger.info(f"  [SILVER DQ {status}] {name}: {detail}")
        if not passed:
            all_pass = False

    if not all_pass:
        raise ValueError("[SILVER DQ] Ada check yang gagal — lihat log.")
    logger.info("[SILVER DQ] Semua check lulus.")


# ─── DAG Definition ────────────────────────────────────────────

with DAG(
    dag_id          = "weather_3_bronze_to_silver",
    default_args    = default_args,
    description     = "Bronze → Silver: cleaning & normalisasi via DuckDB SQL",
    schedule        = None,
    start_date      = datetime(2026, 1, 1),
    catchup         = False,
    max_active_runs = 1,
    tags            = ["weather", "silver", "duckdb", "layer-silver"],
) as dag:

    start = EmptyOperator(task_id="start")

    init_schema = PythonOperator(
        task_id         = "init_silver_schema",
        python_callable = task_init_silver_schema,
    )

    transform_daily = PythonOperator(
        task_id         = "transform_daily_to_silver",
        python_callable = task_transform_daily_to_silver,
    )

    transform_hourly = PythonOperator(
        task_id         = "transform_hourly_to_silver",
        python_callable = task_transform_hourly_to_silver,
    )

    dq_check = PythonOperator(
        task_id         = "silver_quality_check",
        python_callable = task_silver_quality_check,
    )

    end = EmptyOperator(
        task_id  = "end",
        outlets  = [DUCKDB_SILVER_DATASET],
    )

    start >> init_schema >> transform_daily >> transform_hourly >> dq_check >> end