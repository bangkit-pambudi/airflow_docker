"""
DAG: weather_ingestion
Tahap 1 — Extract dari Open-Meteo API → simpan raw JSON ke MinIO (data lake)

Struktur bucket MinIO:
  adventureworks-elt/
  └── weather/
      └── raw/
          └── daily/
              └── {location_key}/
                  └── {YYYY-MM-DD}.json        ← 1 file per kota per hari
          └── hourly/
              └── {location_key}/
                  └── {YYYY-MM-DD}.json

Jadwal: setiap hari jam 05:00 WIB (22:00 UTC)
        — 1 jam sebelum DAG transform berjalan
"""

import json
import logging
from datetime import datetime, timedelta
from io import BytesIO

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)

# ─── Config dari Airflow Variables ────────────────────────────
def get_minio_client() -> Minio:
    return Minio(
        endpoint  = Variable.get("MINIO_ENDPOINT"),   # host.docker.internal:9000
        access_key= Variable.get("MINIO_ACCESS_KEY"), # minioadmin
        secret_key= Variable.get("MINIO_SECRET_KEY"), # minioadmin123
        secure    = False,
    )

MINIO_BUCKET = "adventureworks-elt"
API_BASE     = "https://api.open-meteo.com/v1/forecast"

LOCATIONS = {
    "jakarta":      {"lat": -6.2088,  "lon": 106.8456, "province": "DKI Jakarta",         "island": "Jawa"},
    "surabaya":     {"lat": -7.2575,  "lon": 112.7521, "province": "Jawa Timur",           "island": "Jawa"},
    "bandung":      {"lat": -6.9175,  "lon": 107.6191, "province": "Jawa Barat",           "island": "Jawa"},
    "semarang":     {"lat": -6.9932,  "lon": 110.4203, "province": "Jawa Tengah",          "island": "Jawa"},
    "yogyakarta":   {"lat": -7.7956,  "lon": 110.3695, "province": "DI Yogyakarta",        "island": "Jawa"},
    "medan":        {"lat":  3.5952,  "lon":  98.6722, "province": "Sumatera Utara",       "island": "Sumatera"},
    "palembang":    {"lat": -2.9761,  "lon": 104.7754, "province": "Sumatera Selatan",     "island": "Sumatera"},
    "pekanbaru":    {"lat":  0.5103,  "lon": 101.4478, "province": "Riau",                 "island": "Sumatera"},
    "palangkaraya": {"lat": -2.2136,  "lon": 113.9108, "province": "Kalimantan Tengah",    "island": "Kalimantan"},
    "samarinda":    {"lat": -0.4948,  "lon": 117.1436, "province": "Kalimantan Timur",     "island": "Kalimantan"},
    "makassar":     {"lat": -5.1477,  "lon": 119.4327, "province": "Sulawesi Selatan",     "island": "Sulawesi"},
    "manado":       {"lat":  1.4748,  "lon": 124.8421, "province": "Sulawesi Utara",       "island": "Sulawesi"},
    "jayapura":     {"lat": -2.5337,  "lon": 140.7181, "province": "Papua",                "island": "Papua"},
    "kupang":       {"lat": -10.1772, "lon": 123.6070, "province": "NTT",                  "island": "NTT"},
    "mataram":      {"lat": -8.5833,  "lon": 116.1167, "province": "NTB",                  "island": "Lombok"},
}

# ─── Default args ──────────────────────────────────────────────
default_args = {
    "owner":            "weather-elt",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          3,
    "retry_delay":      timedelta(minutes=3),
}


# ══════════════════════════════════════════════════════════════
# TASK FUNCTIONS
# ══════════════════════════════════════════════════════════════

def task_ensure_bucket(**ctx):
    """Pastikan bucket MinIO sudah ada. Buat jika belum."""
    client = get_minio_client()
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
        logger.info(f"[MINIO] Bucket '{MINIO_BUCKET}' dibuat.")
    else:
        logger.info(f"[MINIO] Bucket '{MINIO_BUCKET}' sudah ada.")


def task_extract_daily(location_key: str, **ctx):
    """
    Extract data harian dari Open-Meteo API untuk satu kota.
    Simpan sebagai raw JSON ke MinIO:
      weather/raw/daily/{location_key}/{YYYY-MM-DD}.json
    """
    loc        = LOCATIONS[location_key]
    run_date   = ctx["ds"]          # format: YYYY-MM-DD (dari Airflow execution_date)
    client     = get_minio_client()

    # ── 1. Build API request ──────────────────────────────────
    params = {
        "latitude":   loc["lat"],
        "longitude":  loc["lon"],
        "daily": ",".join([
            "temperature_2m_max",
            "temperature_2m_min",
            "apparent_temperature_max",
            "precipitation_sum",
            "windspeed_10m_max",
            "relativehumidity_2m_max",
            "sunrise",
            "sunset",
            "uv_index_max",
            "et0_fao_evapotranspiration",
            "weathercode",
        ]),
        "hourly": ",".join([
            "temperature_2m",
            "precipitation",
            "windspeed_10m",
            "relativehumidity_2m",
            "apparent_temperature",
            "precipitation_probability",
            "cloudcover",
            "soil_moisture_0_1cm",
        ]),
        "current_weather": True,
        "timezone":        "Asia/Jakarta",
        "forecast_days":   7,
        "past_days":       1,   # sertakan kemarin untuk backfill
    }

    logger.info(f"[EXTRACT] {location_key} — memanggil Open-Meteo API...")
    resp = requests.get(API_BASE, params=params, timeout=30)
    resp.raise_for_status()
    raw = resp.json()

    # ── 2. Tambah metadata ────────────────────────────────────
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

    # ── 3. Upload ke MinIO ────────────────────────────────────
    object_key   = f"weather/raw/daily/{location_key}/{run_date}.json"
    payload_bytes = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    payload_io    = BytesIO(payload_bytes)

    client.put_object(
        bucket_name  = MINIO_BUCKET,
        object_name  = object_key,
        data         = payload_io,
        length       = len(payload_bytes),
        content_type = "application/json",
    )

    logger.info(
        f"[MINIO] Upload OK: s3://{MINIO_BUCKET}/{object_key} "
        f"({len(payload_bytes):,} bytes)"
    )

    # ── 4. Push ringkasan ke XCom ─────────────────────────────
    ctx["ti"].xcom_push(key=f"ingested_{location_key}", value={
        "location_key": location_key,
        "run_date":     run_date,
        "object_key":   object_key,
        "size_bytes":   len(payload_bytes),
        "days_returned": len(raw.get("daily", {}).get("time", [])),
    })

    logger.info(
        f"[EXTRACT] {location_key}: "
        f"{len(raw.get('daily', {}).get('time', []))} hari, "
        f"{len(raw.get('hourly', {}).get('time', []))} jam data OK."
    )


def task_extract_hourly(location_key: str, **ctx):
    """
    Extract data hourly terpisah dan simpan ke:
      weather/raw/hourly/{location_key}/{YYYY-MM-DD}.json

    Dipisah dari daily supaya bisa diproses & di-query independen.
    """
    loc       = LOCATIONS[location_key]
    run_date  = ctx["ds"]
    client    = get_minio_client()

    params = {
        "latitude":  loc["lat"],
        "longitude": loc["lon"],
        "hourly": ",".join([
            "temperature_2m",
            "precipitation",
            "windspeed_10m",
            "relativehumidity_2m",
            "apparent_temperature",
            "precipitation_probability",
            "cloudcover",
            "visibility",
            "soil_moisture_0_1cm",
            "uv_index",
        ]),
        "timezone":      "Asia/Jakarta",
        "forecast_days": 7,
        "past_days":     1,
    }

    logger.info(f"[EXTRACT-HOURLY] {location_key} — memanggil Open-Meteo API (hourly)...")
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
        bucket_name  = MINIO_BUCKET,
        object_name  = object_key,
        data         = BytesIO(payload_bytes),
        length       = len(payload_bytes),
        content_type = "application/json",
    )

    logger.info(
        f"[MINIO] Upload hourly OK: s3://{MINIO_BUCKET}/{object_key} "
        f"({len(payload_bytes):,} bytes)"
    )


def task_validate_ingestion(**ctx):
    """
    Cek semua file sudah terupload ke MinIO.
    Gagal jika ada kota yang file-nya tidak ditemukan.
    """
    run_date = ctx["ds"]
    client   = get_minio_client()
    missing  = []

    for loc_key in LOCATIONS:
        for resolution in ("daily", "hourly"):
            object_key = f"weather/raw/{resolution}/{loc_key}/{run_date}.json"
            try:
                stat = client.stat_object(MINIO_BUCKET, object_key)
                logger.info(
                    f"  [OK] {object_key} "
                    f"({stat.size:,} bytes, {stat.last_modified})"
                )
            except S3Error as e:
                if e.code == "NoSuchKey":
                    logger.error(f"  [MISSING] {object_key}")
                    missing.append(object_key)
                else:
                    raise

    if missing:
        raise ValueError(
            f"Validasi GAGAL — {len(missing)} file tidak ditemukan di MinIO:\n"
            + "\n".join(f"  - {m}" for m in missing)
        )

    total = len(LOCATIONS) * 2
    logger.info(f"[VALIDATE] Semua {total} file terverifikasi di MinIO untuk {run_date}.")


def task_write_manifest(**ctx):
    """
    Tulis file manifest JSON untuk tanggal ini.
    Berguna untuk downstream DAG (transform) agar tahu
    file mana saja yang harus diproses.

    Path: weather/raw/_manifests/{YYYY-MM-DD}.json
    """
    run_date = ctx["ds"]
    client   = get_minio_client()

    # Kumpulkan XCom dari semua task extract
    ti       = ctx["ti"]
    entries  = []
    for loc_key in LOCATIONS:
        info = ti.xcom_pull(
            task_ids=f"city_{loc_key}.extract_daily",
            key=f"ingested_{loc_key}"
        )
        if info:
            entries.append(info)

    manifest = {
        "run_date":       run_date,
        "created_at":     datetime.utcnow().isoformat() + "Z",
        "dag_run_id":     ctx["run_id"],
        "total_locations": len(LOCATIONS),
        "files_ingested": len(entries) * 2,   # daily + hourly per kota
        "locations":      entries,
        "bucket":         MINIO_BUCKET,
        "prefix":         f"weather/raw/daily/",
        "status":         "completed",
    }

    manifest_key   = f"weather/raw/_manifests/{run_date}.json"
    manifest_bytes = json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")

    client.put_object(
        bucket_name  = MINIO_BUCKET,
        object_name  = manifest_key,
        data         = BytesIO(manifest_bytes),
        length       = len(manifest_bytes),
        content_type = "application/json",
    )

    logger.info(f"[MANIFEST] Ditulis: s3://{MINIO_BUCKET}/{manifest_key}")
    logger.info(f"[MANIFEST] {len(entries)} kota, {len(entries)*2} file, run_date={run_date}")


# ══════════════════════════════════════════════════════════════
# DAG DEFINITION
# ══════════════════════════════════════════════════════════════

with DAG(
    dag_id          = "weather_ingestion",
    default_args    = default_args,
    description     = "Ingest cuaca dari Open-Meteo API → MinIO (raw data lake)",
    schedule        = "0 22 * * *",   # 05:00 WIB = 22:00 UTC (1 jam sebelum transform)
    start_date      = datetime(2026, 1, 1),
    catchup         = False,
    max_active_runs = 1,
    tags            = ["weather", "ingestion", "minio", "open-meteo"],
    doc_md          = """
## Weather Ingestion DAG

Mengambil data cuaca dari Open-Meteo API untuk 15 kota di Indonesia
dan menyimpannya sebagai raw JSON ke MinIO data lake.

**Output path:**
```
adventureworks-elt/
└── weather/raw/
    ├── daily/{kota}/{YYYY-MM-DD}.json
    ├── hourly/{kota}/{YYYY-MM-DD}.json
    └── _manifests/{YYYY-MM-DD}.json
```

**Downstream:** `weather_transform` DAG membaca dari path di atas.
    """,
) as dag:

    start = EmptyOperator(task_id="start")

    ensure_bucket = PythonOperator(
        task_id         = "ensure_bucket",
        python_callable = task_ensure_bucket,
    )

    # ── Per-city task groups ───────────────────────────────────
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

            # Daily dan hourly bisa jalan paralel
            [extract_daily, extract_hourly]

        city_groups.append(city_tg)

    # ── Validasi & Manifest ───────────────────────────────────
    validate = PythonOperator(
        task_id         = "validate_ingestion",
        python_callable = task_validate_ingestion,
        trigger_rule    = "all_success",
    )

    manifest = PythonOperator(
        task_id         = "write_manifest",
        python_callable = task_write_manifest,
    )

    end = EmptyOperator(task_id="end")

    # ── Dependencies ──────────────────────────────────────────
    start >> ensure_bucket >> city_groups >> validate >> manifest >> end
