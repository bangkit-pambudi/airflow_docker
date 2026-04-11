"""
DAG 2 — weather_minio_to_bronze
Trigger: Dataset dari DAG 1 (manifest ditulis ke MinIO)
Fallback schedule: setiap hari jam 23:30 UTC (06:30 WIB)

Tugas:
  Baca raw JSON dari MinIO → parse → insert ke Bronze layer DuckDB
  (raw_weather_daily & raw_weather_hourly)

Bronze = data as-is dari sumber, hanya parsing + penambahan metadata.
Tidak ada transformasi bisnis di tahap ini.

Downstream: weather_3_bronze_to_silver
"""

import json
import logging
import os
from datetime import datetime, timedelta

import duckdb
import pandas as pd
from airflow import DAG
from airflow.datasets import Dataset
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)

# ─── Constants ─────────────────────────────────────────────────
MINIO_BUCKET      = "adventureworks-elt"
DB_PATH           = os.getenv("DUCKDB_PATH", "/opt/airflow/dags/repo/weather_indonesia.duckdb")
SQL_DIR           = os.getenv("SQL_DIR",     "/opt/airflow/sql")

# Dataset trigger dari DAG 1
MINIO_RAW_DATASET    = Dataset("minio://adventureworks-elt/weather/raw/_manifests/")
# Dataset outlet untuk trigger DAG 3
DUCKDB_BRONZE_DATASET = Dataset("duckdb://weather_indonesia/bronze")

LOCATIONS = [
    "jakarta", "surabaya", "bandung", "semarang", "yogyakarta",
    "medan", "palembang", "pekanbaru", "palangkaraya", "samarinda",
    "makassar", "manado", "jayapura", "kupang", "mataram",
]

default_args = {
    "owner":            "weather-pipeline",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}


# ─── Helpers ───────────────────────────────────────────────────

def get_minio_client() -> Minio:
    try:
        endpoint   = Variable.get("MINIO_ENDPOINT")
        access_key = Variable.get("MINIO_ACCESS_KEY")
        secret_key = Variable.get("MINIO_SECRET_KEY")
    except Exception:
        endpoint   = os.getenv("MINIO_ENDPOINT",   "localhost:9000")
        access_key = os.getenv("MINIO_ACCESS_KEY",  "minioadmin")
        secret_key = os.getenv("MINIO_SECRET_KEY",  "minioadmin123")
    return Minio(endpoint=endpoint, access_key=access_key, secret_key=secret_key, secure=False)


def get_conn() -> duckdb.DuckDBPyConnection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return duckdb.connect(DB_PATH)


# ─── Task Functions ────────────────────────────────────────────

def task_init_bronze_schema(**ctx):
    """
    Buat tabel Bronze jika belum ada.
    Bronze hanya menerima data apa adanya dari sumber.
    """
    conn = get_conn()

    # 1. Buat sequence dan tabel untuk DAILY
    conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_bronze_daily;")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze_weather_daily (
            bronze_id        INTEGER PRIMARY KEY DEFAULT nextval('seq_bronze_daily'),
            location_key     VARCHAR NOT NULL,
            run_date         DATE    NOT NULL,
            obs_date         DATE    NOT NULL,
            extracted_at     TIMESTAMP,
            ingested_at      TIMESTAMP DEFAULT now(),
            -- Raw fields dari Open-Meteo
            temp_max         DOUBLE,
            temp_min         DOUBLE,
            apparent_temp_max DOUBLE,
            precip_sum       DOUBLE,
            windspeed_max    DOUBLE,
            humidity_max     DOUBLE,
            sunrise          VARCHAR,
            sunset           VARCHAR,
            uv_index_max     DOUBLE,
            evapotranspiration DOUBLE,
            weathercode      INTEGER,
            -- Control
            is_processed     BOOLEAN DEFAULT FALSE,
            source_file      VARCHAR,
            UNIQUE (location_key, run_date, obs_date)
        )
    """)

    # 2. Buat sequence dan tabel untuk HOURLY
    conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_bronze_hourly;")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze_weather_hourly (
            bronze_id       INTEGER PRIMARY KEY DEFAULT nextval('seq_bronze_hourly'),
            location_key    VARCHAR NOT NULL,
            run_date        DATE    NOT NULL,
            hour_timestamp  TIMESTAMP NOT NULL,
            extracted_at    TIMESTAMP,
            ingested_at     TIMESTAMP DEFAULT now(),
            -- Raw fields
            temperature_2m  DOUBLE,
            precipitation   DOUBLE,
            windspeed_10m   DOUBLE,
            humidity_2m     DOUBLE,
            apparent_temp   DOUBLE,
            precip_prob     DOUBLE,
            cloudcover      DOUBLE,
            soil_moisture   DOUBLE,
            -- Control
            is_processed    BOOLEAN DEFAULT FALSE,
            source_file     VARCHAR,
            UNIQUE (location_key, run_date, hour_timestamp)
        )
    """)

    # Jalankan seed schema utama (dim tables, sequences, dll) kalau belum
    for fname in ["01_schema.sql", "02_seed.sql"]:
        fpath = os.path.join(SQL_DIR, fname)
        if os.path.exists(fpath):
            with open(fpath) as f:
                sql = f.read()
            for stmt in sql.split(";"):
                stmt = stmt.strip()
                if stmt:
                    try:
                        conn.execute(stmt)
                    except Exception as e:
                        if "already exists" not in str(e).lower():
                            logger.warning(f"SQL [{fname}]: {e}")

    conn.close()
    logger.info("[BRONZE] Schema siap.")


def task_load_daily_to_bronze(location_key: str, **ctx):
    """
    Baca raw JSON daily dari MinIO → insert ke bronze_weather_daily.
    Tidak ada transformasi bisnis — hanya parsing field mentah.
    """
    run_date = ctx["ds"]
    client   = get_minio_client()
    conn     = get_conn()

    object_key = f"weather/raw/daily/{location_key}/{run_date}.json"

    try:
        resp      = client.get_object(MINIO_BUCKET, object_key)
        raw_bytes = resp.read()
        resp.close()
        resp.release_conn()
    except S3Error as e:
        if e.code == "NoSuchKey":
            logger.warning(f"[BRONZE] {object_key} tidak ditemukan di MinIO. Skip.")
            conn.close()
            return
        raise

    payload = json.loads(raw_bytes.decode("utf-8"))
    meta    = payload.get("_meta", {})
    data    = payload.get("data", payload)
    daily   = data.get("daily", {})

    extracted_at = meta.get("extracted_at", datetime.utcnow().isoformat())

    # Helper function untuk mencegah IndexError jika array tidak dikembalikan utuh
    def safe_get(key, index):
        arr = daily.get(key)
        if isinstance(arr, list) and index < len(arr):
            return arr[index]
        return None

    rows = []
    times = daily.get("time", [])
    for i, obs_date in enumerate(times):
        rows.append({
            "location_key":       location_key,
            "run_date":           run_date,
            "obs_date":           obs_date,
            "extracted_at":       extracted_at,
            "temp_max":           safe_get("temperature_2m_max", i),
            "temp_min":           safe_get("temperature_2m_min", i),
            "apparent_temp_max":  safe_get("apparent_temperature_max", i),
            "precip_sum":         safe_get("precipitation_sum", i),
            "windspeed_max":      safe_get("wind_speed_10m_max", i),
            "humidity_max":       safe_get("relative_humidity_2m_max", i),
            "sunrise":            safe_get("sunrise", i),
            "sunset":             safe_get("sunset", i),
            "uv_index_max":       safe_get("uv_index_max", i),
            "evapotranspiration": safe_get("et0_fao_evapotranspiration", i),
            "weathercode":        safe_get("weather_code", i),
            "source_file":        object_key,
        })

    if not rows:
        logger.warning(f"[BRONZE] {location_key} tidak ada data daily. Skip.")
        conn.close()
        return

    df = pd.DataFrame(rows)
    conn.execute("""
        INSERT OR IGNORE INTO bronze_weather_daily
            (location_key, run_date, obs_date, extracted_at, ingested_at,
             temp_max, temp_min, apparent_temp_max, precip_sum, windspeed_max,
             humidity_max, sunrise, sunset, uv_index_max, evapotranspiration,
             weathercode, is_processed, source_file)
        SELECT
            location_key, run_date::DATE, obs_date::DATE, extracted_at::TIMESTAMP, now(),
            temp_max, temp_min, apparent_temp_max, precip_sum, windspeed_max,
            humidity_max, sunrise, sunset, uv_index_max, evapotranspiration,
            weathercode, FALSE, source_file
        FROM df
    """)

    conn.close()
    logger.info(f"[BRONZE] {location_key}: {len(rows)} baris daily → bronze_weather_daily")


def task_load_hourly_to_bronze(location_key: str, **ctx):
    """
    Baca raw JSON hourly dari MinIO → insert ke bronze_weather_hourly.
    """
    run_date = ctx["ds"]
    client   = get_minio_client()
    conn     = get_conn()

    object_key = f"weather/raw/hourly/{location_key}/{run_date}.json"

    try:
        resp      = client.get_object(MINIO_BUCKET, object_key)
        raw_bytes = resp.read()
        resp.close()
        resp.release_conn()
    except S3Error as e:
        if e.code == "NoSuchKey":
            logger.warning(f"[BRONZE] {object_key} tidak ditemukan. Skip.")
            conn.close()
            return
        raise

    payload = json.loads(raw_bytes.decode("utf-8"))
    meta    = payload.get("_meta", {})
    data    = payload.get("data", payload)
    hourly  = data.get("hourly", {})

    extracted_at = meta.get("extracted_at", datetime.utcnow().isoformat())

    # Helper function untuk mencegah IndexError jika array tidak dikembalikan utuh
    def safe_get(key, index):
        arr = hourly.get(key)
        if isinstance(arr, list) and index < len(arr):
            return arr[index]
        return None

    rows = []
    for i, ts in enumerate(hourly.get("time", [])):
        rows.append({
            "location_key":   location_key,
            "run_date":       run_date,
            "hour_timestamp": ts,
            "extracted_at":   extracted_at,
            "temperature_2m": safe_get("temperature_2m", i),
            "precipitation":  safe_get("precipitation", i),
            "windspeed_10m":  safe_get("wind_speed_10m", i),
            "humidity_2m":    safe_get("relative_humidity_2m", i),
            "apparent_temp":  safe_get("apparent_temperature", i),
            "precip_prob":    safe_get("precipitation_probability", i),
            "cloudcover":     safe_get("cloud_cover", i),
            "soil_moisture":  safe_get("soil_moisture_0_to_1cm", i),
            "source_file":    object_key,
        })

    if not rows:
        conn.close()
        return

    df = pd.DataFrame(rows)
    conn.execute("""
        INSERT OR IGNORE INTO bronze_weather_hourly
            (location_key, run_date, hour_timestamp, extracted_at, ingested_at,
             temperature_2m, precipitation, windspeed_10m, humidity_2m, apparent_temp,
             precip_prob, cloudcover, soil_moisture, is_processed, source_file)
        SELECT
            location_key, run_date::DATE, hour_timestamp::TIMESTAMP, extracted_at::TIMESTAMP, now(),
            temperature_2m, precipitation, windspeed_10m, humidity_2m, apparent_temp,
            precip_prob, cloudcover, soil_moisture, FALSE, source_file
        FROM df
    """)

    conn.close()
    logger.info(f"[BRONZE] {location_key}: {len(rows)} baris hourly → bronze_weather_hourly")


def task_bronze_quality_check(**ctx):
    """
    DQ check minimal untuk Bronze:
    - Semua kota harus ada data untuk run_date ini
    - Tidak ada semua field NULL (indikasi payload rusak)
    """
    run_date = ctx["ds"]
    conn     = get_conn()
    issues   = []

    # Check: jumlah kota yang masuk
    city_count = conn.execute("""
        SELECT COUNT(DISTINCT location_key)
        FROM bronze_weather_daily
        WHERE run_date = ?
    """, [run_date]).fetchone()[0]

    if city_count < len(LOCATIONS):
        issues.append(f"Hanya {city_count}/{len(LOCATIONS)} kota masuk ke Bronze untuk {run_date}")

    # Check: tidak ada baris dengan temp_max NULL semua
    null_rows = conn.execute("""
        SELECT COUNT(*) FROM bronze_weather_daily
        WHERE run_date = ?
          AND temp_max IS NULL AND temp_min IS NULL AND precip_sum IS NULL
    """, [run_date]).fetchone()[0]

    if null_rows > 0:
        issues.append(f"{null_rows} baris bronze dengan semua field utama NULL")

    conn.close()

    if issues:
        for issue in issues:
            logger.warning(f"[BRONZE DQ] {issue}")
        # Bronze DQ hanya warning, tidak raise — data tetap dilanjutkan
    else:
        logger.info(f"[BRONZE DQ] OK — {city_count} kota, run_date={run_date}")


# ─── DAG Definition ────────────────────────────────────────────

with DAG(
    dag_id          = "weather_2_minio_to_bronze",
    default_args    = default_args,
    description     = "Baca raw JSON dari MinIO → load ke Bronze DuckDB (no transformation)",
    schedule        = None,   # trigger saat DAG 1 selesai menulis manifest
    start_date      = datetime(2026, 1, 1),
    catchup         = False,
    max_active_runs = 1,
    tags            = ["weather", "bronze", "duckdb", "layer-bronze"],
) as dag:

    start = EmptyOperator(task_id="start")

    init_schema = PythonOperator(
        task_id         = "init_bronze_schema",
        python_callable = task_init_bronze_schema,
    )

    city_groups = []
    prev_tg = None
    for loc_key in LOCATIONS:
        with TaskGroup(group_id=f"city_{loc_key}") as city_tg:
            load_daily = PythonOperator(
                task_id         = "load_daily",
                python_callable = task_load_daily_to_bronze,
                op_kwargs       = {"location_key": loc_key},
            )
            load_hourly = PythonOperator(
                task_id         = "load_hourly",
                python_callable = task_load_hourly_to_bronze,
                op_kwargs       = {"location_key": loc_key},
            )
            # daily & hourly berjalan serial per kota
            load_daily >> load_hourly
            
        # Bikin antar kota (TaskGroup) berjalan serial
        if prev_tg is not None:
            prev_tg >> city_tg
            
        # Update prev_tg dengan TaskGroup yang baru saja dibuat
        prev_tg = city_tg 
        
        city_groups.append(city_tg)


    dq_check = PythonOperator(
        task_id         = "bronze_quality_check",
        python_callable = task_bronze_quality_check,
        trigger_rule    = "all_done",   # tetap jalan walau ada kota yang gagal
    )

    # Outlet Dataset → trigger DAG 3
    end = EmptyOperator(
        task_id  = "end",
        outlets  = [DUCKDB_BRONZE_DATASET],
    )

    # Menggabungkan flow utama
    start >> init_schema >> city_groups[0]
    city_groups[-1] >> dq_check >> end