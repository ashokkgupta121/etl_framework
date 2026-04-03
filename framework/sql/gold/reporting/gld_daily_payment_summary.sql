-- =============================================================================
-- FILE    : framework/sql/gold/reporting/gld_daily_payment_summary.sql
-- LAYER   : GOLD
-- SOURCE  : Silver consolidated tables
-- TABLE   : daily_payment_summary (Gold)
-- STRATEGY: FULL (rebuilt daily)
-- PURPOSE : Gold consumption-ready aggregate for BI / reporting tools.
--           Reads ONLY from Silver. No joins to Bronze or source directly.
--
-- GOLD LAYER PRINCIPLES:
--   - Business-level aggregations and metrics
--   - Optimised for query performance (pre-aggregated, wide tables)
--   - Named for business consumers, not technical tables
--   - Reads only from Silver layer
-- =============================================================================

-- query_001_base_payments
-- Pull the business_date slice from Silver consolidated payments
SELECT
    ACCT_ID,
    PMT_DT,
    PMT_AMT,
    PMT_TYPE_DESC,
    PMT_STATUS_DESC,
    CURR_CD,
    SRC_SYSTEM_NM,
    SRC_BUSINESS_DT
FROM silver.consolidated_payments
WHERE SRC_BUSINESS_DT = CAST('${business_date}' AS DATE)
  AND PMT_STATUS_DESC  <> 'VOID'
;


-- query_002_aggregate
-- Daily payment summary by account, type, and currency
SELECT
    SRC_BUSINESS_DT                     AS BUSINESS_DT,
    ACCT_ID,
    PMT_TYPE_DESC,
    CURR_CD,
    SRC_SYSTEM_NM,

    COUNT(*)                            AS PAYMENT_COUNT,
    SUM(PMT_AMT)                        AS TOTAL_PMT_AMT,
    AVG(PMT_AMT)                        AS AVG_PMT_AMT,
    MAX(PMT_AMT)                        AS MAX_PMT_AMT,
    MIN(PMT_AMT)                        AS MIN_PMT_AMT,

    COUNT(CASE WHEN PMT_STATUS_DESC = 'POSTED'    THEN 1 END) AS POSTED_COUNT,
    COUNT(CASE WHEN PMT_STATUS_DESC = 'HOLD'      THEN 1 END) AS HOLD_COUNT,
    COUNT(CASE WHEN PMT_STATUS_DESC = 'CANCELLED' THEN 1 END) AS CANCELLED_COUNT,

    current_timestamp()                 AS GLD_LOAD_TS

FROM query_001_base_payments
GROUP BY
    SRC_BUSINESS_DT,
    ACCT_ID,
    PMT_TYPE_DESC,
    CURR_CD,
    SRC_SYSTEM_NM
;


-- query_003_final
SELECT * FROM query_002_aggregate
;

-- dbutils.notebook.exit("query_003_final")
