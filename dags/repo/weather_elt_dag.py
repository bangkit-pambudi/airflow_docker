"""
Airflow DAG — Weather ELT Indonesia
Jadwal: setiap hari jam 06:00 WIB (23:00 UTC)
"""

import sys
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

sys.path.insert(0, "/opt/airflow/scripts")
from elt_pipeline import (
    init_db, extract, load_to_staging,
    transform, generate_summary_report, LOCATIONS
)

logger = logging.getLogger(__name__)

# ─── Default args ──────────────────────────────────────────────
default_args = {
    "owner":            "weather-elt",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

# ─── Task Functions ────────────────────────────────────────────

def task_init_db(**ctx):
    logger.info("Initializing DuckDB schema...")
    init_db()
    logger.info("Schema ready.")


def task_extract(location_key: str, **ctx):
    logger.info(f"[EXTRACT] Starting: {location_key}")
    raw = extract(location_key, days=7)
    if raw is None:
        raise ValueError(f"Extract gagal untuk {location_key}")
    # Push ke XCom (disimpan sementara)
    ctx["ti"].xcom_push(key=f"raw_{location_key}", value={"status": "ok", "days": len(raw["daily"]["time"])})
    logger.info(f"[EXTRACT] Done: {location_key}")
    return raw


def task_load(location_key: str, **ctx):
    logger.info(f"[LOAD] Starting: {location_key}")
    # Re-extract untuk load (XCom tidak cocok untuk payload besar)
    raw = extract(location_key, days=7)
    if raw:
        load_to_staging(location_key, raw)
        logger.info(f"[LOAD] Done: {location_key}")
    else:
        raise ValueError(f"Load gagal — extract kosong untuk {location_key}")


def task_transform(location_key: str, **ctx):
    logger.info(f"[TRANSFORM] Starting: {location_key}")
    processed = transform(location_key)
    logger.info(f"[TRANSFORM] Done: {location_key} — {processed} baris")


def task_quality_check(**ctx):
    """
    Data Quality Check sederhana:
    - Pastikan tidak ada fact tanpa risk_id
    - Pastikan tidak ada suhu yang abnormal
    - Pastikan jumlah kota sesuai ekspektasi
    """
    import duckdb, os
    db_path = os.getenv("DUCKDB_PATH", "/opt/airflow/data/weather_indonesia.duckdb")
    conn    = duckdb.connect(db_path)

    checks = []

    # Check 1: Jumlah kota
    city_count = conn.execute("""
        SELECT COUNT(DISTINCT location_id)
        FROM fact_weather_daily f
        JOIN dim_date d ON f.date_id = d.date_id
        WHERE d.full_date >= CURRENT_DATE - 1
    """).fetchone()[0]
    checks.append(("Kota terupdate", city_count >= 10, f"{city_count}/15 kota"))

    # Check 2: Tidak ada suhu nol atau null
    bad_temp = conn.execute("""
        SELECT COUNT(*)
        FROM fact_weather_daily
        WHERE temp_max_c IS NULL OR temp_max_c < 10 OR temp_max_c > 50
    """).fetchone()[0]
    checks.append(("Suhu valid", bad_temp == 0, f"{bad_temp} baris anomali suhu"))

    # Check 3: Risk level terisi semua
    missing_risk = conn.execute("""
        SELECT COUNT(*) FROM fact_weather_daily WHERE risk_id IS NULL
    """).fetchone()[0]
    checks.append(("Risk level lengkap", missing_risk == 0, f"{missing_risk} baris tanpa risk"))

    # Check 4: El Niño flag correct (Apr 2026+)
    el_nino_check = conn.execute("""
        SELECT COUNT(*) FROM dim_date
        WHERE full_date >= '2026-04-01'
          AND is_el_nino_period = FALSE
    """).fetchone()[0]
    checks.append(("El Niño flag correct", el_nino_check == 0, f"{el_nino_check} tanggal salah flag"))

    conn.close()

    logger.info("=== DATA QUALITY REPORT ===")
    all_pass = True
    for name, passed, detail in checks:
        status = "PASS" if passed else "FAIL"
        logger.info(f"  [{status}] {name}: {detail}")
        if not passed:
            all_pass = False

    if not all_pass:
        raise ValueError("Data Quality Check GAGAL — lihat log untuk detail")

    logger.info("Semua DQ checks lulus.")


def task_report(**ctx):
    """Log ringkasan cuaca terbaru + alert kota berisiko tinggi."""
    df = generate_summary_report()
    if df.empty:
        logger.warning("Report kosong — tidak ada data terbaru")
        return

    logger.info("\n=== RINGKASAN CUACA INDONESIA HARI INI ===")
    for _, row in df.iterrows():
        flag = ""
        if row.get("is_fire_alert"):   flag += " [KARHUTLA]"
        if row.get("is_drought_alert"): flag += " [KEKERINGAN]"
        if row.get("is_extreme_weather"): flag += " [EKSTREM]"
        logger.info(
            f"  {row['city_name']:<14} | {row['alert_color']:<6} | "
            f"Suhu: {row['temp_max_c']:.1f}°C | "
            f"Hujan: {row['precip_mm']:.1f}mm | "
            f"Drought: {row['drought_index']:.2f} | "
            f"Fire: {row['fire_weather_idx']:.2f}{flag}"
        )

    # Alert kota dengan risiko tinggi
    high_risk = df[df["risk_category"].isin(["TINGGI", "EKSTREM"])]
    if not high_risk.empty:
        logger.warning(f"=== {len(high_risk)} KOTA RISIKO TINGGI/EKSTREM ===")
        for _, row in high_risk.iterrows():
            logger.warning(f"  !! {row['city_name']} — {row['risk_category']} !!")


# ─── DAG Definition ───────────────────────────────────────────

with DAG(
    dag_id          = "weather_elt_indonesia",
    default_args    = default_args,
    description     = "ELT pipeline cuaca Indonesia dari Open-Meteo API ke DuckDB",
    schedule        = "0 23 * * *",   # 06:00 WIB = 23:00 UTC
    start_date      = datetime(2026, 1, 1),
    catchup         = False,
    max_active_runs = 1,
    tags            = ["weather", "indonesia", "el-nino", "elt"],
) as dag:

    start = EmptyOperator(task_id="start")

    init = PythonOperator(
        task_id         = "init_database",
        python_callable = task_init_db,
    )

    # ── Per-city task groups ───────────────────────────────────
    city_groups = []

    for loc_key in LOCATIONS.keys():
        with TaskGroup(group_id=f"city_{loc_key}") as city_tg:

            extract_task = PythonOperator(
                task_id         = "extract",
                python_callable = task_extract,
                op_kwargs       = {"location_key": loc_key},
            )

            load_task = PythonOperator(
                task_id         = "load",
                python_callable = task_load,
                op_kwargs       = {"location_key": loc_key},
            )

            transform_task = PythonOperator(
                task_id         = "transform",
                python_callable = task_transform,
                op_kwargs       = {"location_key": loc_key},
            )

            extract_task >> load_task >> transform_task

        city_groups.append(city_tg)

    # ── Quality Check & Report ────────────────────────────────
    dq_check = PythonOperator(
        task_id         = "data_quality_check",
        python_callable = task_quality_check,
    )

    report = PythonOperator(
        task_id         = "generate_report",
        python_callable = task_report,
    )

    end = EmptyOperator(task_id="end")

    # ── Dependencies ──────────────────────────────────────────
    start >> init >> city_groups >> dq_check >> report >> end
