-- ============================================================
-- QUERY ANALITIK: Sales Performance Dashboard
-- AdventureWorks → dwh.fact_sales_performance
-- Untuk keperluan demo & latihan murid
-- ============================================================


-- ──────────────────────────────────────────────────────────
-- 0. Cek isi tabel hasil ETL
-- ──────────────────────────────────────────────────────────
SELECT *
FROM "dwh"."fact_sales_performance"
LIMIT 20;


-- ──────────────────────────────────────────────────────────
-- 1. TOP 10 Produk dengan Revenue Tertinggi (semua wilayah)
-- ──────────────────────────────────────────────────────────
SELECT
    "ProductName",
    "Category",
    "Subcategory",
    SUM("TotalRevenue")  AS "TotalRevenue",
    SUM("TotalQty")      AS "TotalQty"
FROM "dwh"."fact_sales_performance"
GROUP BY
    "ProductName",
    "Category",
    "Subcategory"
ORDER BY "TotalRevenue" DESC
LIMIT 10;


-- ──────────────────────────────────────────────────────────
-- 2. Revenue per Wilayah (Territory)
-- ──────────────────────────────────────────────────────────
SELECT
    "TerritoryName",
    "CountryRegionCode",
    SUM("TotalRevenue")  AS "TotalRevenue",
    SUM("TotalQty")      AS "TotalQty",
    COUNT(DISTINCT "ProductID") AS "TotalProducts"
FROM "dwh"."fact_sales_performance"
GROUP BY
    "TerritoryName",
    "CountryRegionCode"
ORDER BY "TotalRevenue" DESC;


-- ──────────────────────────────────────────────────────────
-- 3. TOP 5 Produk per Wilayah
--    (pakai window function RANK)
-- ──────────────────────────────────────────────────────────
SELECT
    "TerritoryName",
    "ProductName",
    "Category",
    "TotalRevenue",
    "TotalQty",
    RANK() OVER (
        PARTITION BY "TerritoryName"
        ORDER BY "TotalRevenue" DESC
    ) AS "RankInTerritory"
FROM "dwh"."fact_sales_performance"
QUALIFY RANK() OVER (
    PARTITION BY "TerritoryName"
    ORDER BY "TotalRevenue" DESC
) <= 5
ORDER BY "TerritoryName", "RankInTerritory";

-- ⚠️ Catatan: Kalau PostgreSQL tidak support QUALIFY, pakai subquery:
SELECT *
FROM (
    SELECT
        "TerritoryName",
        "ProductName",
        "Category",
        "TotalRevenue",
        "TotalQty",
        RANK() OVER (
            PARTITION BY "TerritoryName"
            ORDER BY "TotalRevenue" DESC
        ) AS "RankInTerritory"
    FROM "dwh"."fact_sales_performance"
) ranked
WHERE "RankInTerritory" <= 5
ORDER BY "TerritoryName", "RankInTerritory";


-- ──────────────────────────────────────────────────────────
-- 4. Perbandingan Margin per Kategori Produk
-- ──────────────────────────────────────────────────────────
SELECT
    "Category",
    "Subcategory",
    ROUND(AVG("MarginPct"), 2)      AS "AvgMarginPct",
    ROUND(AVG("Margin"), 2)         AS "AvgMargin",
    ROUND(SUM("TotalRevenue"), 2)   AS "TotalRevenue",
    SUM("TotalQty")                 AS "TotalQty"
FROM "dwh"."fact_sales_performance"
GROUP BY
    "Category",
    "Subcategory"
ORDER BY "AvgMarginPct" DESC;


-- ──────────────────────────────────────────────────────────
-- 5. Produk dengan Margin Tinggi tapi Qty Rendah
--    (potensi pricing strategy improvement)
-- ──────────────────────────────────────────────────────────
SELECT
    "ProductName",
    "Category",
    "Subcategory",
    SUM("TotalQty")      AS "TotalQty",
    AVG("MarginPct")     AS "AvgMarginPct",
    SUM("TotalRevenue")  AS "TotalRevenue"
FROM "dwh"."fact_sales_performance"
GROUP BY
    "ProductName",
    "Category",
    "Subcategory"
HAVING
    AVG("MarginPct") > 40
    AND SUM("TotalQty") < 100
ORDER BY "AvgMarginPct" DESC;


-- ──────────────────────────────────────────────────────────
-- 6. Kontribusi Revenue per Kategori (% dari total)
-- ──────────────────────────────────────────────────────────
SELECT
    "Category",
    SUM("TotalRevenue") AS "CategoryRevenue",
    ROUND(
        SUM("TotalRevenue") * 100.0
        / SUM(SUM("TotalRevenue")) OVER (), 2
    ) AS "RevenueSharePct"
FROM "dwh"."fact_sales_performance"
GROUP BY "Category"
ORDER BY "CategoryRevenue" DESC;


-- ──────────────────────────────────────────────────────────
-- 7. Cek kapan data terakhir di-load (audit trail)
-- ──────────────────────────────────────────────────────────
SELECT
    MAX("LoadTimestamp") AS "LastLoadTime",
    COUNT(*)             AS "TotalRows"
FROM "dwh"."fact_sales_performance";
