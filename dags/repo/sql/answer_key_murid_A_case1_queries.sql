-- ============================================================
-- ANSWER KEY — ANALYTIC QUERIES
-- Case 1: HR Attrition & Workforce Analytics
-- Murid A  |  Filter: Department = Production & Engineering
-- Source  : dwh.fact_hr_workforce
-- ============================================================


-- ──────────────────────────────────────────────────────────
-- QUERY WAJIB 1
-- Business Question: Department mana yang memiliki rata-rata
-- tenure lebih tinggi — Production vs Engineering?
-- ──────────────────────────────────────────────────────────
SELECT
    "DepartmentName",
    COUNT(*)                            AS "TotalKaryawan",
    ROUND(AVG("TenureYears"), 2)        AS "AvgTenureYears",
    ROUND(MIN("TenureYears"), 2)        AS "MinTenureYears",
    ROUND(MAX("TenureYears"), 2)        AS "MaxTenureYears"
FROM "dwh"."fact_hr_workforce"
GROUP BY "DepartmentName"
ORDER BY "AvgTenureYears" DESC;


-- ──────────────────────────────────────────────────────────
-- QUERY WAJIB 2
-- Business Question: Top 5 karyawan dengan pay rate tertinggi
-- beserta tenure mereka
-- ──────────────────────────────────────────────────────────
SELECT
    "FullName",
    "JobTitle",
    "DepartmentName",
    "LatestPayRate",
    "TenureYears",
    -- Rank berdasarkan pay rate per department
    RANK() OVER (
        PARTITION BY "DepartmentName"
        ORDER BY "LatestPayRate" DESC
    )                                   AS "RankInDept"
FROM "dwh"."fact_hr_workforce"
ORDER BY "LatestPayRate" DESC
LIMIT 5;


-- ──────────────────────────────────────────────────────────
-- QUERY WAJIB 3
-- Business Question: Berapa % karyawan yang pernah pindah
-- departemen (IsMover = TRUE)?
-- ──────────────────────────────────────────────────────────
SELECT
    "DepartmentName",
    COUNT(*)                                        AS "TotalKaryawan",
    SUM(CASE WHEN "IsMover" = TRUE THEN 1 ELSE 0 END)
                                                    AS "TotalMover",
    ROUND(
        SUM(CASE WHEN "IsMover" = TRUE THEN 1 ELSE 0 END)
        * 100.0 / COUNT(*), 2
    )                                               AS "MoverPct"
FROM "dwh"."fact_hr_workforce"
GROUP BY "DepartmentName"
ORDER BY "MoverPct" DESC;


-- ──────────────────────────────────────────────────────────
-- QUERY BONUS 1
-- Korelasi pay rate vs tenure: apakah yang tenure lebih lama
-- cenderung dapat pay rate lebih tinggi?
-- Dibagi ke dalam 4 bracket tenure
-- ──────────────────────────────────────────────────────────
SELECT
    CASE
        WHEN "TenureYears" < 5  THEN '0-5 tahun'
        WHEN "TenureYears" < 10 THEN '5-10 tahun'
        WHEN "TenureYears" < 15 THEN '10-15 tahun'
        ELSE '15+ tahun'
    END                                 AS "TenureBracket",
    COUNT(*)                            AS "JumlahKaryawan",
    ROUND(AVG("LatestPayRate"), 2)      AS "AvgPayRate",
    ROUND(MIN("LatestPayRate"), 2)      AS "MinPayRate",
    ROUND(MAX("LatestPayRate"), 2)      AS "MaxPayRate"
FROM "dwh"."fact_hr_workforce"
GROUP BY "TenureBracket"
ORDER BY MIN("TenureYears");


-- ──────────────────────────────────────────────────────────
-- QUERY BONUS 2
-- Distribusi JobTitle per departemen — siapa saja yang ada?
-- ──────────────────────────────────────────────────────────
SELECT
    "DepartmentName",
    "JobTitle",
    COUNT(*)                            AS "JumlahKaryawan",
    ROUND(AVG("LatestPayRate"), 2)      AS "AvgPayRate",
    ROUND(AVG("TenureYears"), 2)        AS "AvgTenure"
FROM "dwh"."fact_hr_workforce"
GROUP BY "DepartmentName", "JobTitle"
ORDER BY "DepartmentName", "JumlahKaryawan" DESC;


-- ──────────────────────────────────────────────────────────
-- QUERY BONUS 3
-- Karyawan bertenure panjang tapi pay rate rendah
-- (kandidat review salary)
-- ──────────────────────────────────────────────────────────
SELECT
    "FullName",
    "DepartmentName",
    "JobTitle",
    "TenureYears",
    "LatestPayRate"
FROM "dwh"."fact_hr_workforce"
WHERE "TenureYears" > 10
  AND "LatestPayRate" < (
      SELECT AVG("LatestPayRate")
      FROM "dwh"."fact_hr_workforce"
  )
ORDER BY "TenureYears" DESC;
