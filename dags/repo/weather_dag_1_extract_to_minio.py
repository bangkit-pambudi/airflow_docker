"""
DAG 1 — weather_extract
Jadwal: setiap hari jam 05:00 WIB (22:00 UTC)

Tugas:
  Extract data cuaca dari Open-Meteo API → simpan raw JSON ke MinIO (landing zone)

Output MinIO:
  adventureworks-elt/
  └── weather/
      └── raw/
          ├── daily/{kota}/{YYYY-MM-DD}.json
          ├── hourly/{kota}/{YYYY-MM-DD}.json
          └── _manifests/{YYYY-MM-DD}.json

Downstream: dag_2_minio_to_bronze (trigger via dataset / ExternalTaskSensor)
"""

import json
import logging
from datetime import datetime, timedelta
from io import BytesIO

import requests
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
MINIO_BUCKET = "adventureworks-elt"
API_BASE     = "https://api.open-meteo.com/v1/forecast"

# Dataset outlet — akan men-trigger DAG 2 secara otomatis
MINIO_RAW_DATASET = Dataset("minio://adventureworks-elt/weather/raw/_manifests/")

LOCATIONS = {
    "jakarta":      {"lat": -6.2088,  "lon": 106.8456, "province": "DKI Jakarta",      "island": "Jawa"},
    "surabaya":     {"lat": -7.2575,  "lon": 112.7521, "province": "Jawa Timur",        "island": "Jawa"},
    "bandung":      {"lat": -6.9175,  "lon": 107.6191, "province": "Jawa Barat",        "island": "Jawa"},
    "semarang":     {"lat": -6.9932,  "lon": 110.4203, "province": "Jawa Tengah",       "island": "Jawa"},
    "yogyakarta":   {"lat": -7.7956,  "lon": 110.3695, "province": "DI Yogyakarta",     "island": "Jawa"},
    "medan":        {"lat":  3.5952,  "lon":  98.6722, "province": "Sumatera Utara",    "island": "Sumatera"},
    "palembang":    {"lat": -2.9761,  "lon": 104.7754, "province": "Sumatera Selatan",  "island": "Sumatera"},
    "pekanbaru":    {"lat":  0.5103,  "lon": 101.4478, "province": "Riau",              "island": "Sumatera"},
    "palangkaraya": {"lat": -2.2136,  "lon": 113.9108, "province": "Kalimantan Tengah", "island": "Kalimantan"},
    "samarinda":    {"lat": -0.4948,  "lon": 117.1436, "province": "Kalimantan Timur",  "island": "Kalimantan"},
    "makassar":     {"lat": -5.1477,  "lon": 119.4327, "province": "Sulawesi Selatan",  "island": "Sulawesi"},
    "manado":       {"lat":  1.4748,  "lon": 124.8421, "province": "Sulawesi Utara",    "island": "Sulawesi"},
    "jayapura":     {"lat": -2.5337,  "lon": 140.7181, "province": "Papua",             "island": "Papua"},
    "kupang":       {"lat": -10.1772, "lon": 123.6070, "province": "NTT",               "island": "NTT"},
    "mataram":      {"lat": -8.5833,  "lon": 116.1167, "province": "NTB",               "island": "Lombok"},
}

# ─── Default args ──────────────────────────────────────────────
default_args = {
    "owner":            "weather-pipeline",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          3,
    "retry_delay":      timedelta(minutes=3),
}


# ─── Helpers ───────────────────────────────────────────────────

def get_minio_client() -> Minio:
    try:
        endpoint   = Variable.get("MINIO_ENDPOINT")
        access_key = Variable.get("MINIO_ACCESS_KEY")
        secret_key = Variable.get("MINIO_SECRET_KEY")
    except Exception:
        endpoint   = "localhost:9000"
        access_key = "minioadmin"
        secret_key = "minioadmin123"
    return Minio(endpoint=endpoint, access_key=access_key, secret_key=secret_key, secure=False)


# ─── Task Functions ────────────────────────────────────────────

def task_ensure_bucket(**ctx):
    client = get_minio_client()
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
        logger.info(f"Bucket '{MINIO_BUCKET}' dibuat.")
    else:
        logger.info(f"Bucket '{MINIO_BUCKET}' sudah ada.")


def task_extract_daily(location_key: str, **ctx):
    loc      = LOCATIONS[location_key]
    run_date = ctx["ds"]
    client   = get_minio_client()

    params = {
        "latitude":        loc["lat"],
        "longitude":       loc["lon"],
        "daily": ",".join([
            "temperature_2m_max", "temperature_2m_min", "apparent_temperature_max",
            "precipitation_sum", "wind_speed_10m_max", "relative_humidity_2m_max",
            "sunrise", "sunset", "uv_index_max", "et0_fao_evapotranspiration", "weather_code",
        ]),
        "hourly": ",".join([
            "temperature_2m", "precipitation", "wind_speed_10m", "relative_humidity_2m",
            "apparent_temperature", "precipitation_probability", "cloud_cover", "soil_moisture_0_to_1cm",
        ]),
        "current_weather": True,
        "timezone":        "Asia/Jakarta",
        "forecast_days":   7,
        "past_days":       1,
    }

    resp = requests.get(API_BASE, params=params, timeout=30)
    resp.raise_for_status()
    raw = resp.json()

    payload = {
        "_meta": {
            "location_key": location_key,
            "province":     loc["province"],
            "island":       loc["island"],
            "latitude":     loc["lat"],
            "longitude":    loc["lon"],
            "extracted_at": datetime.utcnow().isoformat() + "Z",
            "run_date":     run_date,
            "source":       "open-meteo.com",
            "api_url":      resp.url,
        },
        "data": raw,
    }

    object_key    = f"weather/raw/daily/{location_key}/{run_date}.json"
    payload_bytes = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")

    client.put_object(
        bucket_name=MINIO_BUCKET, object_name=object_key,
        data=BytesIO(payload_bytes), length=len(payload_bytes),
        content_type="application/json",
    )

    ctx["ti"].xcom_push(key=f"ingested_{location_key}", value={
        "location_key":  location_key,
        "run_date":      run_date,
        "object_key":    object_key,
        "size_bytes":    len(payload_bytes),
        "days_returned": len(raw.get("daily", {}).get("time", [])),
    })
    logger.info(f"[EXTRACT-DAILY] {location_key} OK → {object_key}")


def task_extract_hourly(location_key: str, **ctx):
    loc      = LOCATIONS[location_key]
    run_date = ctx["ds"]
    client   = get_minio_client()

    params = {
        "latitude":  loc["lat"],
        "longitude": loc["lon"],
        "hourly": ",".join([
            "temperature_2m", "precipitation", "wind_speed_10m", "relative_humidity_2m",
            "apparent_temperature", "precipitation_probability", "cloud_cover",
            "visibility", "soil_moisture_0_to_1cm", "uv_index",
        ]),
        "timezone":      "Asia/Jakarta",
        "forecast_days": 7,
        "past_days":     1,
    }

    resp = requests.get(API_BASE, params=params, timeout=30)
    resp.raise_for_status()
    raw = resp.json()

    payload = {
        "_meta": {
            "location_key": location_key,
            "province":     loc["province"],
            "island":       loc["island"],
            "latitude":     loc["lat"],
            "longitude":    loc["lon"],
            "extracted_at": datetime.utcnow().isoformat() + "Z",
            "run_date":     run_date,
            "source":       "open-meteo.com",
            "resolution":   "hourly",
        },
        "data": raw,
    }

    object_key    = f"weather/raw/hourly/{location_key}/{run_date}.json"
    payload_bytes = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")

    client.put_object(
        bucket_name=MINIO_BUCKET, object_name=object_key,
        data=BytesIO(payload_bytes), length=len(payload_bytes),
        content_type="application/json",
    )
    logger.info(f"[EXTRACT-HOURLY] {location_key} OK → {object_key}")


def task_validate_ingestion(**ctx):
    run_date = ctx["ds"]
    client   = get_minio_client()
    missing  = []

    for loc_key in LOCATIONS:
        for resolution in ("daily", "hourly"):
            object_key = f"weather/raw/{resolution}/{loc_key}/{run_date}.json"
            try:
                client.stat_object(MINIO_BUCKET, object_key)
                logger.info(f"  [OK] {object_key}")
            except S3Error as e:
                if e.code == "NoSuchKey":
                    logger.error(f"  [MISSING] {object_key}")
                    missing.append(object_key)
                else:
                    raise

    if missing:
        raise ValueError(
            f"Validasi GAGAL — {len(missing)} file tidak ditemukan:\n"
            + "\n".join(f"  - {m}" for m in missing)
        )
    logger.info(f"[VALIDATE] Semua {len(LOCATIONS)*2} file terverifikasi untuk {run_date}.")


def task_write_manifest(**ctx):
    run_date = ctx["ds"]
    client   = get_minio_client()
    ti       = ctx["ti"]

    entries = []
    for loc_key in LOCATIONS:
        info = ti.xcom_pull(
            task_ids=f"city_{loc_key}.extract_daily",
            key=f"ingested_{loc_key}",
        )
        if info:
            entries.append(info)

    manifest = {
        "run_date":        run_date,
        "created_at":      datetime.utcnow().isoformat() + "Z",
        "dag_run_id":      ctx["run_id"],
        "total_locations": len(LOCATIONS),
        "files_ingested":  len(entries) * 2,
        "locations":       entries,
        "bucket":          MINIO_BUCKET,
        "status":          "completed",
    }

    manifest_key   = f"weather/raw/_manifests/{run_date}.json"
    manifest_bytes = json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")

    client.put_object(
        bucket_name=MINIO_BUCKET, object_name=manifest_key,
        data=BytesIO(manifest_bytes), length=len(manifest_bytes),
        content_type="application/json",
    )
    logger.info(f"[MANIFEST] Ditulis: s3://{MINIO_BUCKET}/{manifest_key}")


# ─── DAG Definition ────────────────────────────────────────────

with DAG(
    dag_id          = "weather_1_extract_to_minio",
    default_args    = default_args,
    description     = "Extract cuaca dari Open-Meteo API → simpan raw JSON ke MinIO",
    schedule        = None,   # 05:00 WIB = 22:00 UTC
    start_date      = datetime(2026, 1, 1),
    catchup         = False,
    max_active_runs = 1,
    tags            = ["weather", "extract", "minio", "layer-raw"],
) as dag:

    start = EmptyOperator(task_id="start")

    ensure_bucket = PythonOperator(
        task_id         = "ensure_bucket",
        python_callable = task_ensure_bucket,
    )

    city_groups = []
    for loc_key in LOCATIONS.keys():
        with TaskGroup(group_id=f"city_{loc_key}") as city_tg:
            extract_daily = PythonOperator(
                task_id         = "extract_daily",
                python_callable = task_extract_daily,
                op_kwargs       = {"location_key": loc_key},
            )
            extract_hourly = PythonOperator(
                task_id         = "extract_hourly",
                python_callable = task_extract_hourly,
                op_kwargs       = {"location_key": loc_key},
            )
            # daily & hourly paralel
            [extract_daily, extract_hourly]
        city_groups.append(city_tg)

    validate = PythonOperator(
        task_id         = "validate_ingestion",
        python_callable = task_validate_ingestion,
        trigger_rule    = "all_success",
    )

    # write_manifest men-update Dataset → otomatis trigger DAG 2
    manifest = PythonOperator(
        task_id         = "write_manifest",
        python_callable = task_write_manifest,
        outlets         = [MINIO_RAW_DATASET],
    )

    end = EmptyOperator(task_id="end")

    start >> ensure_bucket >> city_groups >> validate >> manifest >> end