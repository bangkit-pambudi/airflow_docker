"""
Weather ELT Pipeline — Indonesia
Tahap 2: Baca raw JSON dari MinIO → Transform → Load ke DuckDB fact table

Arsitektur:
  Open-Meteo API
       │  DAG: weather_ingestion  (jam 05:00 WIB)
       ▼
  MinIO  adventureworks-elt/weather/raw/daily/{kota}/{date}.json
       │  DAG: weather_elt_indonesia  (jam 06:00 WIB)  ← file ini
       ▼
  DuckDB  fact_weather_daily + dim tables
"""

import os
import json
import logging
from datetime import datetime, date, timedelta
from io import BytesIO
from typing import Optional

import duckdb
import pandas as pd
from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)

# ─── Config ────────────────────────────────────────────────────
DB_PATH      = os.getenv("DUCKDB_PATH", "/opt/airflow/data/weather_indonesia.duckdb")
SQL_DIR      = os.getenv("SQL_DIR",     "/opt/airflow/sql")
MINIO_BUCKET = "adventureworks-elt"

LOCATIONS = {
    "jakarta":      {"lat": -6.2088,  "lon": 106.8456},
    "surabaya":     {"lat": -7.2575,  "lon": 112.7521},
    "bandung":      {"lat": -6.9175,  "lon": 107.6191},
    "semarang":     {"lat": -6.9932,  "lon": 110.4203},
    "yogyakarta":   {"lat": -7.7956,  "lon": 110.3695},
    "medan":        {"lat":  3.5952,  "lon":  98.6722},
    "palembang":    {"lat": -2.9761,  "lon": 104.7754},
    "pekanbaru":    {"lat":  0.5103,  "lon": 101.4478},
    "palangkaraya": {"lat": -2.2136,  "lon": 113.9108},
    "samarinda":    {"lat": -0.4948,  "lon": 117.1436},
    "makassar":     {"lat": -5.1477,  "lon": 119.4327},
    "manado":       {"lat":  1.4748,  "lon": 124.8421},
    "jayapura":     {"lat": -2.5337,  "lon": 140.7181},
    "kupang":       {"lat": -10.1772, "lon": 123.6070},
    "mataram":      {"lat": -8.5833,  "lon": 116.1167},
}


def get_minio_client() -> Minio:
    """Buat MinIO client. Coba Airflow Variables dulu, fallback ke env vars."""
    try:
        from airflow.models import Variable
        endpoint   = Variable.get("MINIO_ENDPOINT")
        access_key = Variable.get("MINIO_ACCESS_KEY")
        secret_key = Variable.get("MINIO_SECRET_KEY")
    except Exception:
        endpoint   = os.getenv("MINIO_ENDPOINT",   "localhost:9000")
        access_key = os.getenv("MINIO_ACCESS_KEY",  "minioadmin")
        secret_key = os.getenv("MINIO_SECRET_KEY",  "minioadmin123")

    return Minio(
        endpoint   = endpoint,
        access_key = access_key,
        secret_key = secret_key,
        secure     = False,
    )


# ─── Database ──────────────────────────────────────────────────

def get_conn() -> duckdb.DuckDBPyConnection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return duckdb.connect(DB_PATH)


def init_db():
    conn = get_conn()
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
    logger.info("Database initialized")


# ─── EXTRACT dari MinIO ────────────────────────────────────────

def extract_from_minio(location_key: str, run_date: str) -> Optional[dict]:
    """
    Baca raw JSON dari MinIO.
    Path: adventureworks-elt/weather/raw/daily/{location_key}/{run_date}.json
    """
    client     = get_minio_client()
    object_key = f"weather/raw/daily/{location_key}/{run_date}.json"

    try:
        resp      = client.get_object(MINIO_BUCKET, object_key)
        raw_bytes = resp.read()
        resp.close()
        resp.release_conn()

        payload = json.loads(raw_bytes.decode("utf-8"))
        logger.info(
            f"[EXTRACT] {location_key} ← MinIO {object_key} "
            f"({len(raw_bytes):,} bytes)"
        )
        return payload

    except S3Error as e:
        if e.code == "NoSuchKey":
            logger.warning(
                f"[EXTRACT] {object_key} tidak ada di MinIO. "
                f"Jalankan DAG weather_ingestion terlebih dahulu."
            )
            return None
        raise


def list_available_dates(location_key: str) -> list:
    """List semua date yang tersedia di MinIO untuk satu kota."""
    client  = get_minio_client()
    prefix  = f"weather/raw/daily/{location_key}/"
    objects = client.list_objects(MINIO_BUCKET, prefix=prefix)
    return sorted([
        obj.object_name.split("/")[-1].replace(".json", "")
        for obj in objects
        if obj.object_name.endswith(".json")
    ])


# ─── LOAD ke staging ───────────────────────────────────────────

def load_to_staging(location_key: str, payload: dict, run_date: str):
    """Parse MinIO payload → insert ke raw_weather_daily & raw_weather_hourly."""
    conn = get_conn()
    now  = datetime.now()
    data = payload.get("data", payload)
    daily  = data.get("daily",  {})
    hourly = data.get("hourly", {})

    # Daily
    daily_rows = []
    for i, obs_date in enumerate(daily.get("time", [])):
        daily_rows.append({
            "location_key":      location_key,
            "extracted_at":      now,
            "obs_date":          obs_date,
            "temp_max":          daily.get("temperature_2m_max",       [None])[i],
            "temp_min":          daily.get("temperature_2m_min",       [None])[i],
            "precip_sum":        daily.get("precipitation_sum",         [None])[i],
            "windspeed_max":     daily.get("windspeed_10m_max",         [None])[i],
            "humidity_max":      daily.get("relativehumidity_2m_max",   [None])[i],
            "apparent_temp_max": daily.get("apparent_temperature_max",  [None])[i],
            "sunrise":           daily.get("sunrise",                   [None])[i],
            "sunset":            daily.get("sunset",                    [None])[i],
            "is_processed":      False,
        })

    if daily_rows:
        df = pd.DataFrame(daily_rows)
        conn.execute("""
            INSERT INTO raw_weather_daily
                (location_key, extracted_at, obs_date, temp_max, temp_min,
                 precip_sum, windspeed_max, humidity_max, apparent_temp_max,
                 sunrise, sunset, is_processed)
            SELECT location_key, extracted_at, obs_date::DATE, temp_max, temp_min,
                   precip_sum, windspeed_max, humidity_max, apparent_temp_max,
                   sunrise, sunset, is_processed
            FROM df
        """)
        logger.info(f"[LOAD] {location_key}: {len(daily_rows)} baris daily → staging")

    # Hourly
    hourly_rows = []
    for i, ts in enumerate(hourly.get("time", [])):
        hourly_rows.append({
            "location_key":   location_key,
            "extracted_at":   now,
            "hour_timestamp": ts,
            "temperature_2m": hourly.get("temperature_2m",       [None])[i],
            "precipitation":  hourly.get("precipitation",         [None])[i],
            "windspeed_10m":  hourly.get("windspeed_10m",         [None])[i],
            "humidity_2m":    hourly.get("relativehumidity_2m",   [None])[i],
            "apparent_temp":  hourly.get("apparent_temperature",  [None])[i],
            "is_processed":   False,
        })

    if hourly_rows:
        df_h = pd.DataFrame(hourly_rows)
        conn.execute("""
            INSERT INTO raw_weather_hourly
                (location_key, extracted_at, hour_timestamp, temperature_2m,
                 precipitation, windspeed_10m, humidity_2m, apparent_temp, is_processed)
            SELECT location_key, extracted_at, hour_timestamp::TIMESTAMP,
                   temperature_2m, precipitation, windspeed_10m, humidity_2m,
                   apparent_temp, is_processed
            FROM df_h
        """)
        logger.info(f"[LOAD] {location_key}: {len(hourly_rows)} baris hourly → staging")

    conn.close()


# ─── TRANSFORM ─────────────────────────────────────────────────

def _get_or_create_date_id(conn, obs_date: date) -> int:
    month   = obs_date.month
    season  = "Kemarau" if month in (4,5,6,7,8,9,10) else "Hujan"
    monsoon = (
        "Puncak Hujan"   if month in (12,1,2) else
        "Akhir Hujan"    if month in (3,4)    else
        "Awal Kemarau"   if month in (5,6)    else
        "Puncak Kemarau" if month in (7,8,9)  else
        "Awal Hujan"
    )
    conn.execute("""
        INSERT OR IGNORE INTO dim_date
            (full_date, year, month, quarter, day_of_year, day_of_week,
             week_of_year, season, monsoon_phase,
             is_el_nino_period, is_peak_dry, is_peak_wet)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """, [
        obs_date, obs_date.year, month, (month-1)//3+1,
        obs_date.timetuple().tm_yday, obs_date.weekday(),
        obs_date.isocalendar()[1], season, monsoon,
        obs_date >= date(2026, 4, 1),
        month in (7,8,9),
        month in (12,1,2),
    ])
    return conn.execute(
        "SELECT date_id FROM dim_date WHERE full_date = ?", [obs_date]
    ).fetchone()[0]


def _classify_condition(precip_mm) -> str:
    p = precip_mm or 0
    if p >= 150: return "EXTREME_RAIN"
    if p >= 100: return "VERY_HEAVY"
    if p >= 50:  return "HEAVY_RAIN"
    if p >= 20:  return "MODERATE_RAIN"
    if p >= 5:   return "LIGHT_RAIN"
    if p >= 1:   return "DRIZZLE"
    return "CLEAR"


def _drought_index(precip_mm, temp_max, humidity) -> float:
    p, t, h = precip_mm or 0, temp_max or 30, humidity or 70
    return round(
        max(0, 1-(p/20)) * 0.50 +
        min(1, max(0,(t-28)/12)) * 0.30 +
        max(0, 1-(h/100)) * 0.20,
        4
    )


def _fire_index(temp_max, humidity, windspeed) -> float:
    t, h, w = temp_max or 30, humidity or 70, windspeed or 0
    return round((t/40)*0.35 + ((100-h)/100)*0.40 + (w/80)*0.25, 4)


def _risk_code(precip_mm, drought_idx, fire_idx) -> str:
    p = precip_mm or 0
    if p>=150 or drought_idx>=0.75 or fire_idx>=0.75: return "R_EXTREME"
    if p>=100 or drought_idx>=0.55 or fire_idx>=0.55: return "R_HIGH"
    if p>=50  or drought_idx>=0.35 or fire_idx>=0.35: return "R_MODERATE"
    return "R_LOW"


def transform(location_key: str) -> int:
    """Proses staging → fact table. Return jumlah baris diproses."""
    conn = get_conn()

    loc_row = conn.execute(
        "SELECT location_id FROM dim_location WHERE location_key = ?",
        [location_key]
    ).fetchone()
    if not loc_row:
        logger.error(f"[TRANSFORM] '{location_key}' tidak ada di dim_location")
        conn.close()
        return 0
    location_id = loc_row[0]

    staging = conn.execute("""
        SELECT ingestion_id, obs_date, temp_max, temp_min,
               precip_sum, windspeed_max, humidity_max, apparent_temp_max
        FROM raw_weather_daily
        WHERE location_key = ? AND is_processed = FALSE
        ORDER BY obs_date
    """, [location_key]).fetchall()

    if not staging:
        conn.close()
        return 0

    processed = 0
    for row in staging:
        (ingestion_id, obs_date_str, temp_max, temp_min,
         precip_sum, windspeed_max, humidity_max, apparent_temp_max) = row

        obs_date  = date.fromisoformat(str(obs_date_str))
        precip_mm = precip_sum or 0
        temp_avg  = round(((temp_max or 0) + (temp_min or 0)) / 2, 2)

        # Dry streak dari hari sebelumnya
        prev = conn.execute("""
            SELECT MAX(f.dry_day_streak)
            FROM fact_weather_daily f
            JOIN dim_date d ON f.date_id = d.date_id
            WHERE f.location_id = ? AND d.full_date = ?
        """, [location_id, obs_date - timedelta(days=1)]).fetchone()
        prev_streak = (prev[0] or 0) if prev else 0
        dry_streak  = (prev_streak + 1) if precip_mm < 1 else 0

        d_idx     = _drought_index(precip_mm, temp_max, humidity_max)
        f_idx     = _fire_index(temp_max, humidity_max, windspeed_max)
        heat_idx  = round(apparent_temp_max or temp_max or 30, 2)
        crop_s    = round(max(0, d_idx*0.6 + max(0,(temp_avg-30)/10)*0.4), 4) if d_idx > 0.3 else 0

        date_id = _get_or_create_date_id(conn, obs_date)

        cond = conn.execute(
            "SELECT condition_id FROM dim_weather_condition WHERE condition_code = ?",
            [_classify_condition(precip_mm)]
        ).fetchone()
        risk = conn.execute(
            "SELECT risk_id FROM dim_risk_level WHERE risk_code = ?",
            [_risk_code(precip_mm, d_idx, f_idx)]
        ).fetchone()

        conn.execute("""
            INSERT OR REPLACE INTO fact_weather_daily
                (location_id, date_id, condition_id, risk_id,
                 temp_max_c, temp_min_c, temp_avg_c,
                 precip_mm, windspeed_kmh, humidity_pct, apparent_temp_max,
                 drought_index, dry_day_streak, heat_index,
                 fire_weather_idx, crop_stress_score,
                 is_extreme_weather, is_drought_alert, is_flood_alert, is_fire_alert,
                 ingested_at, transformed_at, source_api)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,now(),now(),'open-meteo-via-minio')
        """, [
            location_id, date_id,
            cond[0] if cond else 1,
            risk[0] if risk else 1,
            temp_max, temp_min, temp_avg,
            precip_mm, windspeed_max, humidity_max, apparent_temp_max,
            d_idx, dry_streak, heat_idx, f_idx, crop_s,
            precip_mm >= 100 or (temp_max or 0) > 38 or f_idx > 0.7,
            d_idx >= 0.55,
            precip_mm >= 100,
            f_idx >= 0.55,
        ])
        conn.execute(
            "UPDATE raw_weather_daily SET is_processed = TRUE WHERE ingestion_id = ?",
            [ingestion_id]
        )
        processed += 1

    conn.close()
    logger.info(f"[TRANSFORM] {location_key}: {processed} baris → fact table")
    return processed


# ─── Summary Report ────────────────────────────────────────────

def generate_summary_report() -> pd.DataFrame:
    conn = get_conn()
    df = conn.execute("""
        SELECT l.city_name, l.island, d.full_date,
               f.temp_max_c, f.precip_mm,
               f.drought_index, f.dry_day_streak, f.fire_weather_idx,
               r.risk_category, r.alert_color,
               f.is_extreme_weather, f.is_drought_alert, f.is_fire_alert
        FROM fact_weather_daily f
        JOIN dim_location    l ON f.location_id = l.location_id
        JOIN dim_date        d ON f.date_id     = d.date_id
        JOIN dim_risk_level  r ON f.risk_id     = r.risk_id
        WHERE d.full_date = (
            SELECT MAX(d2.full_date) FROM fact_weather_daily f2
            JOIN dim_date d2 ON f2.date_id = d2.date_id
        )
        ORDER BY f.drought_index DESC
    """).df()
    conn.close()
    return df


# ─── CLI ───────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run_date = sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat()
    logger.info(f"Run date: {run_date}")
    init_db()
    for loc_key in LOCATIONS:
        payload = extract_from_minio(loc_key, run_date)
        if payload:
            load_to_staging(loc_key, payload, run_date)
            transform(loc_key)
    df = generate_summary_report()
    if not df.empty:
        print("\n=== RINGKASAN TERKINI ===")
        print(df.to_string(index=False))
