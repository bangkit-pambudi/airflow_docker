-- ============================================================
-- ANSWER KEY — ANALYTIC QUERIES
-- Case 2: Procurement & Vendor Performance
-- Murid B  |  Filter: ShipMethod = CARGO TRANSPORT 5 & OVERNIGHT J-FAST
-- Source  : dwh.fact_vendor_performance
-- ============================================================


-- ──────────────────────────────────────────────────────────
-- QUERY WAJIB 1
-- Business Question: Ranking vendor berdasarkan VendorScore
-- (tertinggi ke terendah) — siapa yang paling reliable?
-- ──────────────────────────────────────────────────────────
SELECT
    RANK() OVER (ORDER BY "VendorScore" DESC)   AS "Rank",
    "VendorName",
    "TotalOrders",
    "OnTimeRate",
    "AvgLeadTimeDays",
    "AvgPriceVariance",
    "VendorScore"
FROM "dwh"."fact_vendor_performance"
ORDER BY "VendorScore" DESC;


-- ──────────────────────────────────────────────────────────
-- QUERY WAJIB 2
-- Business Question: Perbandingan OnTimeRate antara vendor
-- yang pakai CARGO TRANSPORT 5 vs OVERNIGHT J-FAST
-- (join balik ke staging untuk tahu ShipMethod per vendor)
-- ──────────────────────────────────────────────────────────
SELECT
    stg."ShipMethodName",
    COUNT(DISTINCT stg."VendorID")              AS "JumlahVendor",
    ROUND(AVG(f."OnTimeRate"), 2)               AS "AvgOnTimeRate",
    ROUND(AVG(f."VendorScore"), 2)              AS "AvgVendorScore",
    ROUND(AVG(f."AvgLeadTimeDays"), 1)          AS "AvgLeadTime"
FROM "dwh"."fact_vendor_performance" AS f
INNER JOIN "dwh"."stg_purchase_orders" AS stg
    ON f."VendorID" = stg."VendorID"
GROUP BY stg."ShipMethodName"
ORDER BY "AvgOnTimeRate" DESC;


-- ──────────────────────────────────────────────────────────
-- QUERY WAJIB 3
-- Business Question: Vendor dengan AvgPriceVariance negatif
-- (beli lebih murah dari StandardCost = menguntungkan)
-- ──────────────────────────────────────────────────────────
SELECT
    "VendorName",
    "AvgPriceVariance",
    "OnTimeRate",
    "VendorScore",
    CASE
        WHEN "AvgPriceVariance" < 0 THEN 'Lebih murah dari Standard ✓'
        WHEN "AvgPriceVariance" = 0 THEN 'Sama dengan Standard'
        ELSE 'Lebih mahal dari Standard ✗'
    END                                         AS "PriceStatus"
FROM "dwh"."fact_vendor_performance"
ORDER BY "AvgPriceVariance" ASC;


-- ──────────────────────────────────────────────────────────
-- QUERY BONUS 1
-- Vendor yang on-time rate tinggi (>= 80%) tapi
-- price variance juga kompetitif (< 10) — best of both worlds
-- ──────────────────────────────────────────────────────────
SELECT
    "VendorName",
    "OnTimeRate",
    "AvgPriceVariance",
    "VendorScore",
    "TotalOrders"
FROM "dwh"."fact_vendor_performance"
WHERE "OnTimeRate" >= 80
  AND "AvgPriceVariance" < 10
ORDER BY "VendorScore" DESC;


-- ──────────────────────────────────────────────────────────
-- QUERY BONUS 2
-- Distribusi performa vendor: berapa yang masuk kategori
-- Excellent, Good, Fair, Poor berdasarkan VendorScore
-- ──────────────────────────────────────────────────────────
SELECT
    CASE
        WHEN "VendorScore" >= 80 THEN 'Excellent (80+)'
        WHEN "VendorScore" >= 60 THEN 'Good (60-79)'
        WHEN "VendorScore" >= 40 THEN 'Fair (40-59)'
        ELSE 'Poor (<40)'
    END                                     AS "PerformanceCategory",
    COUNT(*)                                AS "JumlahVendor",
    ROUND(AVG("OnTimeRate"), 2)             AS "AvgOnTimeRate",
    ROUND(AVG("AvgLeadTimeDays"), 1)        AS "AvgLeadTime"
FROM "dwh"."fact_vendor_performance"
GROUP BY "PerformanceCategory"
ORDER BY MIN("VendorScore") DESC;


-- ──────────────────────────────────────────────────────────
-- QUERY BONUS 3
-- Cek kapan data terakhir di-load (audit trail)
-- ──────────────────────────────────────────────────────────
SELECT
    MAX("LoadTimestamp")    AS "LastLoadTime",
    COUNT(*)                AS "TotalVendor",
    ROUND(AVG("VendorScore"), 2) AS "AvgScoreKeseluruhan"
FROM "dwh"."fact_vendor_performance";
