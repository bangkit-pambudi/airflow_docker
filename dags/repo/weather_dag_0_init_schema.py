"""
DAG 0 — weather_0_init_schema
Schedule: None (jalankan MANUAL sekali saat setup awal)

Tugas:
  1. Buat semua tabel (raw, bronze, silver, gold, dim, fact)
  2. Seed data statik ke semua dim tables:
       - dim_location
       - dim_date (2024-2030)
       - dim_weather_condition
       - dim_risk_level
"""

import logging
import os
from datetime import datetime, timedelta

import duckdb
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

logger = logging.getLogger(__name__)

DB_PATH = os.getenv("DUCKDB_PATH", "/opt/airflow/dags/repo/weather_indonesia.duckdb")

default_args = {
    "owner":            "weather-pipeline",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=2),
}

def get_conn() -> duckdb.DuckDBPyConnection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return duckdb.connect(DB_PATH)

# ═══════════════════════════════════════════════════════════════
# TASK: Create Schema
# ═══════════════════════════════════════════════════════════════
def task_create_dim_tables(**ctx):
    conn = get_conn()

    conn.execute("CREATE SEQUENCE IF NOT EXISTS dim_location_seq START 1")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dim_location (
            location_id   INTEGER PRIMARY KEY DEFAULT nextval('dim_location_seq'),
            location_key  VARCHAR UNIQUE NOT NULL,
            city_name     VARCHAR NOT NULL,
            province      VARCHAR NOT NULL,
            island        VARCHAR NOT NULL,
            region_type   VARCHAR,
            climate_zone  VARCHAR,
            latitude      DOUBLE NOT NULL,
            longitude     DOUBLE NOT NULL,
            elevation_m   DOUBLE,
            timezone      VARCHAR DEFAULT 'Asia/Jakarta',
            is_active     BOOLEAN DEFAULT TRUE,
            created_at    TIMESTAMP DEFAULT now()
        )
    """)

    conn.execute("CREATE SEQUENCE IF NOT EXISTS dim_date_seq START 1")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dim_date (
            date_id           INTEGER PRIMARY KEY DEFAULT nextval('dim_date_seq'),
            full_date         DATE UNIQUE NOT NULL,
            year              INTEGER,
            month             INTEGER,
            quarter           INTEGER,
            day_of_year       INTEGER,
            day_of_week       INTEGER,
            week_of_year      INTEGER,
            season            VARCHAR,
            monsoon_phase     VARCHAR,
            is_el_nino_period BOOLEAN DEFAULT FALSE,
            is_peak_dry       BOOLEAN DEFAULT FALSE,
            is_peak_wet       BOOLEAN DEFAULT FALSE
        )
    """)

    conn.execute("CREATE SEQUENCE IF NOT EXISTS dim_condition_seq START 1")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dim_weather_condition (
            condition_id    INTEGER PRIMARY KEY DEFAULT nextval('dim_condition_seq'),
            condition_code  VARCHAR UNIQUE NOT NULL,
            condition_label VARCHAR NOT NULL,
            precip_category VARCHAR,
            is_extreme      BOOLEAN DEFAULT FALSE,
            has_thunder     BOOLEAN DEFAULT FALSE,
            risk_multiplier DOUBLE DEFAULT 1.0
        )
    """)

    conn.execute("CREATE SEQUENCE IF NOT EXISTS dim_risk_seq START 1")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dim_risk_level (
            risk_id           INTEGER PRIMARY KEY DEFAULT nextval('dim_risk_seq'),
            risk_code         VARCHAR UNIQUE NOT NULL,
            risk_category     VARCHAR NOT NULL,
            risk_score        INTEGER,
            flood_risk        VARCHAR,
            drought_risk      VARCHAR,
            wildfire_risk     VARCHAR,
            heat_risk         VARCHAR,
            mitigation_action TEXT,
            alert_color       VARCHAR
        )
    """)

    conn.close()
    logger.info("[INIT] Dim tables OK.")

# ═══════════════════════════════════════════════════════════════
# TASK: Seed Data
# ═══════════════════════════════════════════════════════════════
def task_seed_dim_location(**ctx):
    conn = get_conn()
    conn.execute("""
        INSERT OR IGNORE INTO dim_location
            (location_key, city_name, province, island, region_type, climate_zone, latitude, longitude, elevation_m)
        VALUES
            ('jakarta',      'Jakarta',      'DKI Jakarta',           'Jawa',       'urban',    'Pesisir Utara Jawa',      -6.2088,  106.8456,   8.0),
            ('surabaya',     'Surabaya',     'Jawa Timur',            'Jawa',       'urban',    'Pesisir Utara Jawa',      -7.2575,  112.7521,   5.0),
            ('bandung',      'Bandung',      'Jawa Barat',            'Jawa',       'highland', 'Dataran Tinggi Jawa',     -6.9175,  107.6191, 768.0),
            ('semarang',     'Semarang',     'Jawa Tengah',           'Jawa',       'coastal',  'Pesisir Utara Jawa',      -6.9932,  110.4203,   5.0),
            ('yogyakarta',   'Yogyakarta',   'DI Yogyakarta',         'Jawa',       'urban',    'Lembah Jawa Selatan',     -7.7956,  110.3695, 113.0),
            ('medan',        'Medan',        'Sumatera Utara',        'Sumatera',   'urban',    'Delta Sumatera',           3.5952,   98.6722,  23.0),
            ('palembang',    'Palembang',    'Sumatera Selatan',      'Sumatera',   'peatland', 'Lahan Gambut Sumatera',   -2.9761,  104.7754,  11.0),
            ('pekanbaru',    'Pekanbaru',    'Riau',                  'Sumatera',   'peatland', 'Lahan Gambut Sumatera',    0.5103,  101.4478,  30.0),
            ('palangkaraya', 'Palangkaraya', 'Kalimantan Tengah',     'Kalimantan', 'peatland', 'Lahan Gambut Kalimantan', -2.2136,  113.9108,  27.0),
            ('samarinda',    'Samarinda',    'Kalimantan Timur',      'Kalimantan', 'urban',    'Kalimantan Timur',        -0.4948,  117.1436,  13.0),
            ('makassar',     'Makassar',     'Sulawesi Selatan',      'Sulawesi',   'coastal',  'Pesisir Sulawesi',        -5.1477,  119.4327,   2.0),
            ('manado',       'Manado',       'Sulawesi Utara',        'Sulawesi',   'coastal',  'Sulawesi Utara',           1.4748,  124.8421,   5.0),
            ('jayapura',     'Jayapura',     'Papua',                 'Papua',      'coastal',  'Papua Utara',             -2.5337,  140.7181,  15.0),
            ('kupang',       'Kupang',       'Nusa Tenggara Timur',   'NTT',        'coastal',  'NTT Kering',             -10.1772,  123.6070,  80.0),
            ('mataram',      'Mataram',      'Nusa Tenggara Barat',   'Lombok',     'urban',    'Lombok',                  -8.5833,  116.1167,  23.0)
    """)
    conn.close()
    logger.info("[SEED] dim_location berhasil.")

def task_seed_dim_date(**ctx):
    conn = get_conn()
    # Generate tanggal dari 2024 sampai 2030
    conn.execute("""
        INSERT OR IGNORE INTO dim_date (date_id, full_date)
        SELECT
            CAST(strftime(d, '%Y%m%d') AS INTEGER),
            d::DATE
        FROM generate_series(DATE '2024-01-01', DATE '2030-12-31', INTERVAL 1 DAY) AS t(d);
    """)
    conn.close()
    logger.info("[SEED] dim_date berhasil.")

def task_seed_dim_weather_condition(**ctx):
    conn = get_conn()
    conn.execute("""
        INSERT OR IGNORE INTO dim_weather_condition
            (condition_code, condition_label, precip_category, is_extreme, has_thunder, risk_multiplier)
        VALUES
            ('CLEAR',         'Cerah',               'Tidak Hujan',           FALSE, FALSE, 0.8),
            ('PARTLY_CLOUDY', 'Cerah Berawan',        'Tidak Hujan',           FALSE, FALSE, 0.9),
            ('CLOUDY',        'Berawan',              'Tidak Hujan',           FALSE, FALSE, 1.0),
            ('DRIZZLE',       'Gerimis',              'Ringan',                FALSE, FALSE, 1.0),
            ('LIGHT_RAIN',    'Hujan Ringan',         'Ringan (<20mm)',         FALSE, FALSE, 1.1),
            ('MODERATE_RAIN', 'Hujan Sedang',         'Sedang (20-50mm)',       FALSE, FALSE, 1.2),
            ('HEAVY_RAIN',    'Hujan Lebat',          'Lebat (50-100mm)',       FALSE, FALSE, 1.5),
            ('VERY_HEAVY',    'Hujan Sangat Lebat',   'Sangat Lebat (100-150mm)', TRUE, FALSE, 1.8),
            ('EXTREME_RAIN',  'Hujan Ekstrem',        'Ekstrem (>150mm)',       TRUE,  TRUE,  2.5),
            ('THUNDERSTORM',  'Hujan Badai',          'Ekstrem',               TRUE,  TRUE,  3.0),
            ('FOG',           'Kabut',                'Tidak Hujan',           FALSE, FALSE, 1.0),
            ('HAZE',          'Asap/Kabut Asap',      'Tidak Hujan',           TRUE,  FALSE, 1.5)
    """)
    conn.close()
    logger.info("[SEED] dim_weather_condition berhasil.")

def task_seed_dim_risk_level(**ctx):
    conn = get_conn()
    conn.execute("""
        INSERT OR IGNORE INTO dim_risk_level
            (risk_code, risk_category, risk_score, flood_risk, drought_risk,
             wildfire_risk, heat_risk, alert_color, mitigation_action)
        VALUES
            ('R_LOW',      'RENDAH', 10, 'RENDAH', 'RENDAH', 'RENDAH', 'RENDAH', 'GREEN',  'Monitor rutin.'),
            ('R_MODERATE', 'SEDANG', 40, 'SEDANG', 'SEDANG', 'SEDANG', 'SEDANG', 'YELLOW', 'Siaga Level 2.'),
            ('R_HIGH',     'TINGGI', 70, 'TINGGI', 'TINGGI', 'TINGGI', 'TINGGI', 'ORANGE', 'Siaga Level 1.'),
            ('R_EXTREME',  'EKSTREM', 95, 'EKSTREM', 'EKSTREM', 'EKSTREM', 'EKSTREM', 'RED', 'TANGGAP DARURAT.')
    """)
    conn.close()
    logger.info("[SEED] dim_risk_level berhasil.")

# ═══════════════════════════════════════════════════════════════
# DAG Definition
# ═══════════════════════════════════════════════════════════════
with DAG(
    dag_id          = "weather_0_init_schema",
    default_args    = default_args,
    schedule        = None,
    start_date      = datetime(2026, 1, 1),
    catchup         = False,
    max_active_runs = 1,
) as dag:

    start = EmptyOperator(task_id="start")

    create_dim    = PythonOperator(task_id="create_dim_tables",          python_callable=task_create_dim_tables)
    
    # Task Seed
    seed_location  = PythonOperator(task_id="seed_dim_location",         python_callable=task_seed_dim_location)
    seed_date      = PythonOperator(task_id="seed_dim_date",             python_callable=task_seed_dim_date)
    seed_condition = PythonOperator(task_id="seed_dim_weather_condition",python_callable=task_seed_dim_weather_condition)
    seed_risk      = PythonOperator(task_id="seed_dim_risk_level",       python_callable=task_seed_dim_risk_level)

    end = EmptyOperator(task_id="end")

    # Flow
    start >> create_dim 
    create_dim >> seed_location >> seed_date >> seed_condition >> seed_risk >> end