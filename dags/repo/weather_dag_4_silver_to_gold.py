"""
DAG 4 — weather_silver_to_gold
Trigger: Dataset dari DAG 3 (Silver DuckDB selesai)

Tugas:
  Silver → Gold: kalkulasi indeks bisnis, agregasi, fact table, laporan akhir
  SEMUA logika dilakukan murni di DuckDB SQL — tidak ada Python business logic

Gold layer terdiri dari:
  - fact_weather_daily          : fact table utama (existing, dipertahankan)
  - gold_weather_daily_agg      : agregasi harian per pulau
  - gold_risk_summary           : ringkasan risiko per kota per hari
  - gold_alert_dashboard        : view untuk dashboard alert

Indeks yang dihitung di DuckDB SQL:
  - drought_index       : formula kekeringan berbasis hujan, suhu, kelembaban
  - fire_weather_idx    : indeks bahaya kebakaran hutan
  - heat_index          : apparent temperature adjusted
  - crop_stress_score   : stres tanaman
  - dry_day_streak      : berapa hari berturut-turut tidak hujan (window function)
  - risk_category       : RENDAH / SEDANG / TINGGI / EKSTREM
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

DUCKDB_SILVER_DATASET = Dataset("duckdb://weather_indonesia/silver")
DUCKDB_GOLD_DATASET   = Dataset("duckdb://weather_indonesia/gold")

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
# TASK 1 — Init Gold Schema
# ═══════════════════════════════════════════════════════════════

def task_init_gold_schema(**ctx):
    """Buat tabel-tabel Gold jika belum ada."""
    conn = get_conn()

    # --- DROP SEMENTARA UNTUK RESET SCHEMA GOLD ---
    conn.execute("DROP TABLE IF EXISTS gold_weather_daily;")
    conn.execute("DROP TABLE IF EXISTS gold_weather_island_daily;")
    # ----------------------------------------------

    # ── Gold: fact table utama (diperkaya dari Silver) ──────────
    conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_gold_daily;")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gold_weather_daily (
            gold_id             INTEGER PRIMARY KEY DEFAULT nextval('seq_gold_daily'),
            -- Dimensi
            location_key        VARCHAR NOT NULL,
            location_id         INTEGER,   -- FK ke dim_location
            obs_date            DATE    NOT NULL,
            run_date            DATE    NOT NULL,
            -- Measurements (dari Silver)
            temp_max_c          DOUBLE,
            temp_min_c          DOUBLE,
            temp_avg_c          DOUBLE,
            apparent_temp_max_c DOUBLE,
            precip_mm           DOUBLE,
            windspeed_kmh       DOUBLE,
            humidity_pct        DOUBLE,
            uv_index_max        DOUBLE,
            -- ── Indeks bisnis (dihitung di DuckDB) ──
            drought_index       DOUBLE,   -- 0..1, makin tinggi makin kering
            fire_weather_idx    DOUBLE,   -- 0..1, makin tinggi makin berbahaya
            heat_index          DOUBLE,   -- apparent heat stress
            crop_stress_score   DOUBLE,   -- 0..1, dampak ke pertanian
            dry_day_streak      INTEGER,  -- berapa hari berturut-turut tanpa hujan
            -- Klasifikasi
            condition_label     VARCHAR,  -- CLEAR / DRIZZLE / LIGHT_RAIN / ... / EXTREME_RAIN
            risk_category       VARCHAR,  -- RENDAH / SEDANG / TINGGI / EKSTREM
            risk_code           VARCHAR,  -- R_LOW / R_MODERATE / R_HIGH / R_EXTREME
            alert_color         VARCHAR,  -- GREEN / YELLOW / ORANGE / RED
            -- Alert flags
            is_extreme_weather  BOOLEAN,
            is_drought_alert    BOOLEAN,
            is_flood_alert      BOOLEAN,
            is_fire_alert       BOOLEAN,
            -- Time context (dari Silver)
            season              VARCHAR,
            monsoon_phase       VARCHAR,
            is_el_nino_period   BOOLEAN,
            is_peak_dry         BOOLEAN,
            is_peak_wet         BOOLEAN,
            -- Lineage
            silver_id           INTEGER,
            transformed_at      TIMESTAMP DEFAULT now(),
            UNIQUE (location_key, obs_date, run_date)
        )
    """)

    # ── Gold: agregasi per pulau per hari ───────────────────────
    conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_gold_island;")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gold_weather_island_daily (
            agg_id              INTEGER PRIMARY KEY DEFAULT nextval('seq_gold_island'),
            island              VARCHAR NOT NULL,
            obs_date            DATE    NOT NULL,
            run_date            DATE    NOT NULL,
            -- Agregasi
            city_count          INTEGER,
            avg_temp_max_c      DOUBLE,
            avg_temp_min_c      DOUBLE,
            total_precip_mm     DOUBLE,
            max_precip_mm       DOUBLE,
            avg_humidity_pct    DOUBLE,
            avg_windspeed_kmh   DOUBLE,
            avg_drought_index   DOUBLE,
            max_drought_index   DOUBLE,
            avg_fire_idx        DOUBLE,
            max_fire_idx        DOUBLE,
            -- Jumlah kota per kategori risiko
            cities_rendah       INTEGER,
            cities_sedang       INTEGER,
            cities_tinggi       INTEGER,
            cities_ekstrem      INTEGER,
            -- Alert flags (TRUE jika ada >= 1 kota yang alert)
            has_fire_alert      BOOLEAN,
            has_drought_alert   BOOLEAN,
            has_flood_alert     BOOLEAN,
            has_extreme_weather BOOLEAN,
            transformed_at      TIMESTAMP DEFAULT now(),
            UNIQUE (island, obs_date, run_date)
        )
    """)

    # ── Fact table lama (backward compatibility) ────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fact_weather_daily (
            location_id         INTEGER,
            date_id             INTEGER,
            condition_id        INTEGER,
            risk_id             INTEGER,
            temp_max_c          DOUBLE,
            temp_min_c          DOUBLE,
            temp_avg_c          DOUBLE,
            precip_mm           DOUBLE,
            windspeed_kmh       DOUBLE,
            humidity_pct        DOUBLE,
            apparent_temp_max   DOUBLE,
            drought_index       DOUBLE,
            dry_day_streak      INTEGER,
            heat_index          DOUBLE,
            fire_weather_idx    DOUBLE,
            crop_stress_score   DOUBLE,
            is_extreme_weather  BOOLEAN,
            is_drought_alert    BOOLEAN,
            is_flood_alert      BOOLEAN,
            is_fire_alert       BOOLEAN,
            ingested_at         TIMESTAMP,
            transformed_at      TIMESTAMP,
            source_api          VARCHAR,
            PRIMARY KEY (location_id, date_id)
        )
    """)

    # ── Gold: dashboard alert ringkasan ─────────────────────────
    conn.execute("""
        CREATE OR REPLACE VIEW gold_alert_dashboard AS
        SELECT
            g.location_key,
            l.city_name,
            l.province,
            l.island,
            g.obs_date,
            g.temp_max_c,
            g.precip_mm,
            g.drought_index,
            g.fire_weather_idx,
            g.dry_day_streak,
            g.risk_category,
            g.alert_color,
            g.is_fire_alert,
            g.is_drought_alert,
            g.is_flood_alert,
            g.is_extreme_weather,
            g.season,
            g.monsoon_phase,
            g.is_el_nino_period
        FROM gold_weather_daily g
        LEFT JOIN dim_location l ON g.location_key = l.location_key
        WHERE g.obs_date = (
            SELECT MAX(obs_date) FROM gold_weather_daily
        )
        ORDER BY
            CASE g.risk_category
                WHEN 'EKSTREM' THEN 1
                WHEN 'TINGGI'  THEN 2
                WHEN 'SEDANG'  THEN 3
                ELSE 4
            END,
            g.drought_index DESC
    """)

    conn.close()
    logger.info("[GOLD] Schema siap.")


# ═══════════════════════════════════════════════════════════════
# TASK 2 — Silver → Gold (fact utama, semua kalkulasi di SQL)
# ═══════════════════════════════════════════════════════════════

def task_transform_silver_to_gold(**ctx):
    run_date = ctx["ds"]
    conn     = get_conn()

    # 1. Hapus data run_date ini (agar idempoten & tidak error multiple constraint)
    conn.execute("DELETE FROM gold_weather_daily WHERE run_date = ?", [run_date])

    # 2. Insert ulang dengan data baru
    conn.execute("""
        INSERT INTO gold_weather_daily (
            location_key, location_id, obs_date, run_date,
            temp_max_c, temp_min_c, temp_avg_c, apparent_temp_max_c,
            precip_mm, windspeed_kmh, humidity_pct, uv_index_max,
            drought_index, fire_weather_idx, heat_index, crop_stress_score,
            dry_day_streak,
            condition_label, risk_category, risk_code, alert_color,
            is_extreme_weather, is_drought_alert, is_flood_alert, is_fire_alert,
            season, monsoon_phase, is_el_nino_period, is_peak_dry, is_peak_wet,
            silver_id, transformed_at
        )
        WITH base AS (
            -- Ambil semua Silver untuk run_date ini beserta FK lokasi
            SELECT
                s.*,
                l.location_id
            FROM silver_weather_daily s
            LEFT JOIN dim_location l ON s.location_key = l.location_key
            WHERE s.run_date = ?
        ),

        with_indices AS (
            -- Hitung semua indeks bisnis
            SELECT
                b.*,

                -- Drought Index (0..1)
                ROUND(
                    GREATEST(0.0, 1.0 - COALESCE(b.precip_mm, 0) / 20.0) * 0.50 +
                    LEAST(1.0, GREATEST(0.0, (COALESCE(b.temp_max_c, 30.0) - 28.0) / 12.0)) * 0.30 +
                    GREATEST(0.0, 1.0 - COALESCE(b.humidity_pct, 70.0) / 100.0) * 0.20,
                    4
                ) AS drought_index,

                -- Fire Weather Index (0..1)
                ROUND(
                    (COALESCE(b.temp_max_c, 30.0) / 40.0) * 0.35 +
                    ((100.0 - COALESCE(b.humidity_pct, 70.0)) / 100.0) * 0.40 +
                    (COALESCE(b.windspeed_kmh, 0.0) / 80.0) * 0.25,
                    4
                ) AS fire_weather_idx,

                -- Heat Index (apparent temp adjusted, simple version)
                ROUND(COALESCE(b.apparent_temp_max_c, b.temp_max_c, 30.0), 2) AS heat_index

            FROM base b
        ),

        with_streak AS (
            -- Dry day streak: jumlah hari berturut-turut tanpa hujan (precip < 1mm)
            -- Menggunakan gap-island window technique di DuckDB
            SELECT
                w.*,
                -- Beri nomor urut hari per kota
                ROW_NUMBER() OVER (PARTITION BY w.location_key ORDER BY w.obs_date) AS rn,
                -- Beri nomor urut hari basah per kota
                COUNT(CASE WHEN w.precip_mm >= 1 THEN 1 END)
                    OVER (PARTITION BY w.location_key ORDER BY w.obs_date
                          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS wet_count
            FROM with_indices w
        ),

        with_streak_calc AS (
            SELECT
                ws.*,
                -- dry_streak = rn - wet_count (reset setiap ada hujan)
                CASE
                    WHEN ws.precip_mm < 1
                    THEN ws.rn - ws.wet_count
                    ELSE 0
                END AS dry_day_streak_today,
                -- Juga ambil streak yang sudah ada di Gold (hari sebelumnya)
                COALESCE((
                    SELECT g_prev.dry_day_streak
                    FROM gold_weather_daily g_prev
                    WHERE g_prev.location_key = ws.location_key
                      AND g_prev.obs_date = ws.obs_date - INTERVAL '1 day'
                    LIMIT 1
                ), 0) AS prev_streak
            FROM with_streak ws
        ),

        with_risk AS (
            -- Klasifikasi risiko & condition label
            SELECT
                sc.*,

                -- Final dry streak (gunakan prev+1 jika tidak ada data historis di Silver batch ini)
                CASE
                    WHEN sc.precip_mm < 1 THEN
                        CASE
                            WHEN sc.dry_day_streak_today > 0 THEN sc.dry_day_streak_today
                            ELSE sc.prev_streak + 1
                        END
                    ELSE 0
                END AS dry_day_streak,

                -- Crop stress (hanya aktif jika drought_index > 0.3)
                CASE
                    WHEN sc.drought_index > 0.3
                    THEN ROUND(
                        GREATEST(0,
                            sc.drought_index * 0.6 +
                            GREATEST(0, (sc.temp_avg_c - 30.0) / 10.0) * 0.4
                        ), 4)
                    ELSE 0.0
                END AS crop_stress_score,

                -- Condition label (berdasarkan presipitasi)
                CASE
                    WHEN COALESCE(sc.precip_mm, 0) >= 150 THEN 'EXTREME_RAIN'
                    WHEN COALESCE(sc.precip_mm, 0) >= 100 THEN 'VERY_HEAVY'
                    WHEN COALESCE(sc.precip_mm, 0) >= 50  THEN 'HEAVY_RAIN'
                    WHEN COALESCE(sc.precip_mm, 0) >= 20  THEN 'MODERATE_RAIN'
                    WHEN COALESCE(sc.precip_mm, 0) >= 5   THEN 'LIGHT_RAIN'
                    WHEN COALESCE(sc.precip_mm, 0) >= 1   THEN 'DRIZZLE'
                    ELSE 'CLEAR'
                END AS condition_label,

                -- Risk code
                CASE
                    WHEN COALESCE(sc.precip_mm,0)>=150 OR sc.drought_index>=0.75 OR sc.fire_weather_idx>=0.75
                        THEN 'R_EXTREME'
                    WHEN COALESCE(sc.precip_mm,0)>=100 OR sc.drought_index>=0.55 OR sc.fire_weather_idx>=0.55
                        THEN 'R_HIGH'
                    WHEN COALESCE(sc.precip_mm,0)>=50  OR sc.drought_index>=0.35 OR sc.fire_weather_idx>=0.35
                        THEN 'R_MODERATE'
                    ELSE 'R_LOW'
                END AS risk_code

            FROM with_streak_calc sc
        )

        SELECT
            r.location_key,
            r.location_id,
            r.obs_date,
            r.run_date,
            r.temp_max_c,
            r.temp_min_c,
            r.temp_avg_c,
            r.apparent_temp_max_c,
            r.precip_mm,
            r.windspeed_kmh,
            r.humidity_pct,
            r.uv_index_max,
            r.drought_index,
            r.fire_weather_idx,
            r.heat_index,
            r.crop_stress_score,
            r.dry_day_streak,
            r.condition_label,

            -- risk_category & alert_color dari risk_code
            CASE r.risk_code
                WHEN 'R_EXTREME'  THEN 'EKSTREM'
                WHEN 'R_HIGH'     THEN 'TINGGI'
                WHEN 'R_MODERATE' THEN 'SEDANG'
                ELSE 'RENDAH'
            END AS risk_category,

            r.risk_code,

            CASE r.risk_code
                WHEN 'R_EXTREME'  THEN 'RED'
                WHEN 'R_HIGH'     THEN 'ORANGE'
                WHEN 'R_MODERATE' THEN 'YELLOW'
                ELSE 'GREEN'
            END AS alert_color,

            -- Alert flags
            (COALESCE(r.precip_mm,0) >= 100 OR COALESCE(r.temp_max_c,0) > 38 OR r.fire_weather_idx > 0.7)
                AS is_extreme_weather,
            r.drought_index >= 0.55     AS is_drought_alert,
            COALESCE(r.precip_mm,0) >= 100 AS is_flood_alert,
            r.fire_weather_idx >= 0.55  AS is_fire_alert,

            r.season,
            r.monsoon_phase,
            r.is_el_nino_period,
            r.is_peak_dry,
            r.is_peak_wet,
            r.silver_id,
            now() AS transformed_at

        FROM with_risk r
    """, [run_date])

    row_count = conn.execute("""
        SELECT COUNT(*) FROM gold_weather_daily WHERE run_date = ?
    """, [run_date]).fetchone()[0]

    conn.close()
    logger.info(f"[GOLD] silver_to_gold selesai: {row_count} baris untuk {run_date}")


# ═══════════════════════════════════════════════════════════════
# TASK 3 — Agregasi per Pulau (Gold agg)
# ═══════════════════════════════════════════════════════════════

def task_aggregate_by_island(**ctx):
    run_date = ctx["ds"]
    conn     = get_conn()

    # 1. Hapus data run_date ini (sama seperti task di atas)
    conn.execute("DELETE FROM gold_weather_island_daily WHERE run_date = ?", [run_date])

    # 2. Insert ulang dengan data baru
    conn.execute("""
        INSERT INTO gold_weather_island_daily (
            island, obs_date, run_date,
            city_count,
            avg_temp_max_c, avg_temp_min_c,
            total_precip_mm, max_precip_mm,
            avg_humidity_pct, avg_windspeed_kmh,
            avg_drought_index, max_drought_index,
            avg_fire_idx, max_fire_idx,
            cities_rendah, cities_sedang, cities_tinggi, cities_ekstrem,
            has_fire_alert, has_drought_alert, has_flood_alert, has_extreme_weather,
            transformed_at
        )
        SELECT
            l.island,
            g.obs_date,
            g.run_date,
            COUNT(*)                                   AS city_count,
            ROUND(AVG(g.temp_max_c), 2)                AS avg_temp_max_c,
            ROUND(AVG(g.temp_min_c), 2)                AS avg_temp_min_c,
            ROUND(SUM(g.precip_mm), 2)                 AS total_precip_mm,
            ROUND(MAX(g.precip_mm), 2)                 AS max_precip_mm,
            ROUND(AVG(g.humidity_pct), 2)              AS avg_humidity_pct,
            ROUND(AVG(g.windspeed_kmh), 2)             AS avg_windspeed_kmh,
            ROUND(AVG(g.drought_index), 4)             AS avg_drought_index,
            ROUND(MAX(g.drought_index), 4)             AS max_drought_index,
            ROUND(AVG(g.fire_weather_idx), 4)          AS avg_fire_idx,
            ROUND(MAX(g.fire_weather_idx), 4)          AS max_fire_idx,
            COUNT(CASE WHEN g.risk_category = 'RENDAH'  THEN 1 END) AS cities_rendah,
            COUNT(CASE WHEN g.risk_category = 'SEDANG'  THEN 1 END) AS cities_sedang,
            COUNT(CASE WHEN g.risk_category = 'TINGGI'  THEN 1 END) AS cities_tinggi,
            COUNT(CASE WHEN g.risk_category = 'EKSTREM' THEN 1 END) AS cities_ekstrem,
            BOOL_OR(g.is_fire_alert)                   AS has_fire_alert,
            BOOL_OR(g.is_drought_alert)                AS has_drought_alert,
            BOOL_OR(g.is_flood_alert)                  AS has_flood_alert,
            BOOL_OR(g.is_extreme_weather)              AS has_extreme_weather,
            now()                                      AS transformed_at
        FROM gold_weather_daily g
        LEFT JOIN dim_location l ON g.location_key = l.location_key
        WHERE g.run_date = ?
        GROUP BY l.island, g.obs_date, g.run_date
    """, [run_date])

    islands = conn.execute("""
        SELECT island, city_count, max_drought_index, max_fire_idx, cities_ekstrem
        FROM gold_weather_island_daily
        WHERE run_date = ?
        ORDER BY max_drought_index DESC
    """, [run_date]).fetchall()

    conn.close()
    logger.info(f"[GOLD AGG] Agregasi per pulau untuk {run_date}:")
    for row in islands:
        logger.info(f"  {row[0]:<14} | {row[1]} kota | drought_max={row[2]:.3f} | fire_max={row[3]:.3f} | ekstrem={row[4]}")


# ═══════════════════════════════════════════════════════════════
# TASK 4 — Sync ke fact_weather_daily (FULL INGEST)
# ═══════════════════════════════════════════════════════════════

def task_sync_to_fact_table(**ctx):
    """
    Sinkronisasi Gold → fact_weather_daily (tabel lama).
    FULL INGEST: Memasukkan semua data dari gold_weather_daily tanpa filter run_date.
    Mempertahankan kompatibilitas dengan query downstream yang sudah ada.
    Menggunakan DuckDB SQL murni untuk mapping.
    """
    conn = get_conn()

    conn.execute("""
        INSERT OR REPLACE INTO fact_weather_daily (
            location_id, date_id, condition_id, risk_id,
            temp_max_c, temp_min_c, temp_avg_c,
            precip_mm, windspeed_kmh, humidity_pct, apparent_temp_max,
            drought_index, dry_day_streak, heat_index,
            fire_weather_idx, crop_stress_score,
            is_extreme_weather, is_drought_alert, is_flood_alert, is_fire_alert,
            ingested_at, transformed_at, source_api
        )
        SELECT
            g.location_id,
            d.date_id,
            COALESCE(wc.condition_id, 1),
            COALESCE(rl.risk_id,      1),
            g.temp_max_c,
            g.temp_min_c,
            g.temp_avg_c,
            g.precip_mm,
            g.windspeed_kmh,
            g.humidity_pct,
            g.apparent_temp_max_c,
            g.drought_index,
            g.dry_day_streak,
            g.heat_index,
            g.fire_weather_idx,
            g.crop_stress_score,
            g.is_extreme_weather,
            g.is_drought_alert,
            g.is_flood_alert,
            g.is_fire_alert,
            now(),
            now(),
            'open-meteo-via-minio-gold'
        FROM gold_weather_daily g
        LEFT JOIN dim_date              d  ON d.full_date     = g.obs_date
        LEFT JOIN dim_weather_condition wc ON wc.condition_code = g.condition_label
        LEFT JOIN dim_risk_level        rl ON rl.risk_code    = g.risk_code
        WHERE g.location_id IS NOT NULL
          AND d.date_id     IS NOT NULL
    """)

    synced = conn.execute("SELECT COUNT(*) FROM fact_weather_daily").fetchone()[0]

    conn.close()
    logger.info(f"[GOLD SYNC] fact_weather_daily FULL INGEST selesai. Total semua baris: {synced}")


# ═══════════════════════════════════════════════════════════════
# TASK 5 — Gold Quality Check & Report
# ═══════════════════════════════════════════════════════════════

def task_gold_quality_check(**ctx):
    """DQ check final untuk Gold layer."""
    run_date = ctx["ds"]
    conn     = get_conn()
    checks   = []

    city_count = conn.execute("""
        SELECT COUNT(DISTINCT location_key)
        FROM gold_weather_daily WHERE run_date = ?
    """, [run_date]).fetchone()[0]
    checks.append(("Kota Gold", city_count >= 10, f"{city_count}/15"))

    bad_risk = conn.execute("""
        SELECT COUNT(*) FROM gold_weather_daily
        WHERE run_date = ? AND risk_category IS NULL
    """, [run_date]).fetchone()[0]
    checks.append(("Risk tidak NULL", bad_risk == 0, f"{bad_risk} NULL"))

    bad_drought = conn.execute("""
        SELECT COUNT(*) FROM gold_weather_daily
        WHERE run_date = ?
          AND (drought_index < 0 OR drought_index > 1)
    """, [run_date]).fetchone()[0]
    checks.append(("Drought index 0-1", bad_drought == 0, f"{bad_drought} out of range"))

    el_nino_wrong = conn.execute("""
        SELECT COUNT(*) FROM gold_weather_daily
        WHERE run_date = ?
          AND obs_date >= DATE '2026-04-01'
          AND is_el_nino_period = FALSE
    """, [run_date]).fetchone()[0]
    checks.append(("El Niño flag benar", el_nino_wrong == 0, f"{el_nino_wrong} salah"))

    conn.close()

    all_pass = True
    for name, passed, detail in checks:
        status = "PASS" if passed else "FAIL"
        logger.info(f"  [GOLD DQ {status}] {name}: {detail}")
        if not passed:
            all_pass = False

    if not all_pass:
        raise ValueError("[GOLD DQ] Ada check yang gagal!")
    logger.info("[GOLD DQ] Semua check lulus.")


def task_generate_report(**ctx):
    """
    Cetak ringkasan harian dari Gold layer ke log.
    Highlight kota dengan risiko TINGGI / EKSTREM.
    """
    run_date = ctx["ds"]
    conn     = get_conn()

    rows = conn.execute("""
        SELECT
            COALESCE(l.city_name, g.location_key) AS city_name,
            g.alert_color,
            g.temp_max_c,
            g.precip_mm,
            g.drought_index,
            g.fire_weather_idx,
            g.risk_category,
            g.is_fire_alert,
            g.is_drought_alert,
            g.is_extreme_weather
        FROM gold_weather_daily g
        LEFT JOIN dim_location l ON g.location_key = l.location_key
        WHERE g.run_date = ?
          AND g.obs_date = (
              SELECT MAX(obs_date) FROM gold_weather_daily WHERE run_date = ?
          )
        ORDER BY
            CASE g.risk_category
                WHEN 'EKSTREM' THEN 1 WHEN 'TINGGI' THEN 2
                WHEN 'SEDANG'  THEN 3 ELSE 4
            END,
            g.drought_index DESC
    """, [run_date, run_date]).fetchall()

    conn.close()

    if not rows:
        logger.warning("[REPORT] Tidak ada data Gold untuk dilaporkan.")
        return

    logger.info("\n=== RINGKASAN CUACA INDONESIA (GOLD) ===")
    for row in rows:
        city, color, tmax, precip, drought, fire, risk, is_fire, is_drought, is_extreme = row
        flags = ""
        if is_fire:    flags += " [KARHUTLA]"
        if is_drought: flags += " [KEKERINGAN]"
        if is_extreme: flags += " [EKSTREM]"
        logger.info(
            f"  {city:<14} | {color:<6} | {risk:<8} | "
            f"Suhu: {tmax:.1f}°C | Hujan: {precip:.1f}mm | "
            f"Drought: {drought:.3f} | Fire: {fire:.3f}{flags}"
        )

    high_risk = [r for r in rows if r[6] in ("TINGGI", "EKSTREM")]
    if high_risk:
        logger.warning(f"\n⚠️  {len(high_risk)} KOTA RISIKO TINGGI/EKSTREM:")
        for r in high_risk:
            logger.warning(f"   !! {r[0]} — {r[6]} !!")


# ─── DAG Definition ────────────────────────────────────────────

with DAG(
    dag_id          = "weather_4_silver_to_gold",
    default_args    = default_args,
    description     = "Silver → Gold: kalkulasi indeks bisnis & agregasi via DuckDB SQL",
    schedule        = None,
    start_date      = datetime(2026, 1, 1),
    catchup         = False,
    max_active_runs = 1,
    tags            = ["weather", "gold", "duckdb", "layer-gold"],
) as dag:

    start = EmptyOperator(task_id="start")

    init_schema = PythonOperator(
        task_id         = "init_gold_schema",
        python_callable = task_init_gold_schema,
    )

    silver_to_gold = PythonOperator(
        task_id         = "transform_silver_to_gold",
        python_callable = task_transform_silver_to_gold,
    )

    aggregate_island = PythonOperator(
        task_id         = "aggregate_by_island",
        python_callable = task_aggregate_by_island,
    )

    sync_fact = PythonOperator(
        task_id         = "sync_to_fact_table",
        python_callable = task_sync_to_fact_table,
    )

    dq_check = PythonOperator(
        task_id         = "gold_quality_check",
        python_callable = task_gold_quality_check,
    )

    report = PythonOperator(
        task_id         = "generate_report",
        python_callable = task_generate_report,
    )

    end = EmptyOperator(
        task_id  = "end",
        outlets  = [DUCKDB_GOLD_DATASET],
    )

    (
        start
        >> init_schema
        >> silver_to_gold
        >> aggregate_island 
        >> sync_fact
        >> dq_check
        >> report
        >> end
    )