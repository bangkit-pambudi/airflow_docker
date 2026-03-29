-- ============================================================
-- ANSWER KEY — ANALYTIC QUERIES
-- Case 3: Customer Segmentation & RFM Analysis
-- Murid C  |  Filter: Territory = Northwest & Southwest
-- Source  : dwh.fact_customer_rfm
-- ============================================================


-- ──────────────────────────────────────────────────────────
-- QUERY WAJIB 1
-- Business Question: Top 10 customer Champions di territory
-- Northwest dan Southwest
-- ──────────────────────────────────────────────────────────
SELECT
    "CustomerName",
    "TerritoryName",
    "Recency",
    "Frequency",
    "Monetary",
    "RFM_Score",
    "Segment"
FROM "dwh"."fact_customer_rfm"
WHERE "Segment" = 'Champions'
ORDER BY "RFM_Score" DESC, "Monetary" DESC
LIMIT 10;


-- ──────────────────────────────────────────────────────────
-- QUERY WAJIB 2
-- Business Question: Distribusi jumlah customer per Segment
-- Champions / Loyal / At Risk / Lost
-- ──────────────────────────────────────────────────────────
SELECT
    "Segment",
    COUNT(*)                                    AS "JumlahCustomer",
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()
    , 2)                                        AS "PctDariTotal",
    ROUND(AVG("Monetary"), 2)                   AS "AvgMonetary",
    ROUND(AVG("Frequency"), 1)                  AS "AvgFrequency",
    ROUND(AVG("Recency"), 0)                    AS "AvgRecencyDays"
FROM "dwh"."fact_customer_rfm"
GROUP BY "Segment"
ORDER BY
    CASE "Segment"
        WHEN 'Champions' THEN 1
        WHEN 'Loyal'     THEN 2
        WHEN 'At Risk'   THEN 3
        WHEN 'Lost'      THEN 4
    END;


-- ──────────────────────────────────────────────────────────
-- QUERY WAJIB 3
-- Business Question: Apakah ada perbedaan profil RFM antara
-- customer Northwest vs Southwest?
-- ──────────────────────────────────────────────────────────
SELECT
    "TerritoryName",
    COUNT(*)                                    AS "TotalCustomer",
    ROUND(AVG("Recency"), 0)                    AS "AvgRecency",
    ROUND(AVG("Frequency"), 1)                  AS "AvgFrequency",
    ROUND(AVG("Monetary"), 2)                   AS "AvgMonetary",
    ROUND(AVG("RFM_Score"), 2)                  AS "AvgRFMScore",
    -- Breakdown segment per territory
    SUM(CASE WHEN "Segment" = 'Champions' THEN 1 ELSE 0 END) AS "Champions",
    SUM(CASE WHEN "Segment" = 'Loyal'     THEN 1 ELSE 0 END) AS "Loyal",
    SUM(CASE WHEN "Segment" = 'At Risk'   THEN 1 ELSE 0 END) AS "AtRisk",
    SUM(CASE WHEN "Segment" = 'Lost'      THEN 1 ELSE 0 END) AS "Lost"
FROM "dwh"."fact_customer_rfm"
GROUP BY "TerritoryName"
ORDER BY "AvgRFMScore" DESC;


-- ──────────────────────────────────────────────────────────
-- QUERY BONUS 1
-- Customer dengan Recency > 365 hari tapi Monetary tinggi
-- → kandidat win-back campaign (dulu beli banyak, sekarang hilang)
-- ──────────────────────────────────────────────────────────
SELECT
    "CustomerName",
    "TerritoryName",
    "Recency"                                   AS "RecencyDays",
    "Frequency",
    "Monetary",
    "Segment"
FROM "dwh"."fact_customer_rfm"
WHERE "Recency" > 365
  AND "Monetary" > (
      SELECT AVG("Monetary")
      FROM "dwh"."fact_customer_rfm"
  )
ORDER BY "Monetary" DESC;


-- ──────────────────────────────────────────────────────────
-- QUERY BONUS 2
-- Average RFM Score per territory
-- + persentase Champions per territory
-- ──────────────────────────────────────────────────────────
SELECT
    "TerritoryName",
    ROUND(AVG("RFM_Score"), 2)                  AS "AvgRFMScore",
    ROUND(AVG("R_Score"), 2)                    AS "AvgR",
    ROUND(AVG("F_Score"), 2)                    AS "AvgF",
    ROUND(AVG("M_Score"), 2)                    AS "AvgM",
    ROUND(
        SUM(CASE WHEN "Segment" = 'Champions' THEN 1 ELSE 0 END)
        * 100.0 / COUNT(*), 2
    )                                           AS "ChampionsPct"
FROM "dwh"."fact_customer_rfm"
GROUP BY "TerritoryName"
ORDER BY "AvgRFMScore" DESC;


-- ──────────────────────────────────────────────────────────
-- QUERY BONUS 3
-- Heatmap RFM: distribusi customer berdasarkan kombinasi
-- R_Score dan F_Score — untuk memahami pola perilaku
-- ──────────────────────────────────────────────────────────
SELECT
    "R_Score",
    "F_Score",
    COUNT(*)                                    AS "JumlahCustomer",
    ROUND(AVG("Monetary"), 2)                   AS "AvgMonetary",
    ROUND(AVG("RFM_Score"), 2)                  AS "AvgRFMScore"
FROM "dwh"."fact_customer_rfm"
GROUP BY "R_Score", "F_Score"
ORDER BY "R_Score" DESC, "F_Score" DESC;


-- ──────────────────────────────────────────────────────────
-- QUERY BONUS 4
-- Audit trail: kapan data terakhir di-load
-- ──────────────────────────────────────────────────────────
SELECT
    MAX("LoadTimestamp")        AS "LastLoadTime",
    COUNT(*)                    AS "TotalCustomer",
    COUNT(DISTINCT "TerritoryName") AS "TotalTerritory",
    SUM(CASE WHEN "Segment" = 'Champions' THEN 1 ELSE 0 END) AS "Champions",
    SUM(CASE WHEN "Segment" = 'Loyal'     THEN 1 ELSE 0 END) AS "Loyal",
    SUM(CASE WHEN "Segment" = 'At Risk'   THEN 1 ELSE 0 END) AS "AtRisk",
    SUM(CASE WHEN "Segment" = 'Lost'      THEN 1 ELSE 0 END) AS "Lost"
FROM "dwh"."fact_customer_rfm";
