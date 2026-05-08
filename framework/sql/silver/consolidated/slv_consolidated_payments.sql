-- =============================================================================
-- FILE    : framework/sql/silver/consolidated/slv_consolidated_payments.sql
-- LAYER   : SILVER
-- SOURCE  : Consolidated from multiple Bronze tables
-- TABLE   : consolidated_payments (Silver)
-- STRATEGY: INC (merge on ACCT_ID + SEQ_NO + SRC_SYSTEM_NM)
-- PURPOSE : Silver layer consolidates similar payment data from different
--           source systems (VisionPlus, SAP, etc.) into a unified model.
--           Applies business rules: standardised currency, date formats,
--           status code mapping, and deduplication.
--
-- SILVER LAYER PRINCIPLES:
--   - Reads ONLY from Bronze tables (never directly from source)
--   - Applies business-rule transformations and standardisation
--   - Deduplicated and validated
--   - Common data model alignment
-- =============================================================================

-- query_001_union_sources
-- Consolidate payments from all source-system bronze tables.
SELECT
    ACCT_ID,
    SEQ_NO,
    PMT_DT,
    PMT_AMT,
    PMT_TYPE_CD,
    PMT_STATUS_CD,
    ORIG_PMT_AMT,
    CURR_CD,
    POST_DT,
    EFFECTIVE_DT,
    SRC_SYSTEM_NM,
    SRC_BUSINESS_DT,
    UPDATE_DT           AS SRC_UPDATE_DT
FROM cdm_visionplus.ATH2_LRPMT_TGT
WHERE SRC_BUSINESS_DT  = CAST('${business_date}' AS DATE)

UNION ALL

-- Add other source system bronze tables here as they are onboarded:
-- SELECT ... FROM cdm_sap.SAP_PAYMENTS_BRZ WHERE SRC_BUSINESS_DT = ...
;


-- query_002_standardise
-- Apply Silver business rules:
--   1. Standardise status codes to enterprise code set
--   2. Normalise currency codes to ISO-4217
--   3. Compute derived fields
SELECT
    ACCT_ID,
    SEQ_NO,
    PMT_DT,
    PMT_AMT,
    ORIG_PMT_AMT,

    -- Standardise payment type
    CASE PMT_TYPE_CD
        WHEN 'C'  THEN 'CREDIT'
        WHEN 'D'  THEN 'DEBIT'
        WHEN 'R'  THEN 'REVERSAL'
        ELSE PMT_TYPE_CD
    END                                     AS PMT_TYPE_DESC,

    -- Standardise status
    CASE PMT_STATUS_CD
        WHEN 'P'  THEN 'POSTED'
        WHEN 'V'  THEN 'VOID'
        WHEN 'H'  THEN 'HOLD'
        WHEN 'C'  THEN 'CANCELLED'
        ELSE PMT_STATUS_CD
    END                                     AS PMT_STATUS_DESC,

    UPPER(CURR_CD)                          AS CURR_CD,     -- ISO-4217
    COALESCE(PMT_AMT - ORIG_PMT_AMT, 0)    AS ADJUSTMENT_AMT,
    POST_DT,
    EFFECTIVE_DT,
    SRC_SYSTEM_NM,
    SRC_BUSINESS_DT,
    SRC_UPDATE_DT,
    current_timestamp()                     AS SLV_LOAD_TS

FROM query_001_union_sources
;


-- query_003_deduplicate
-- Keep the most recent record per natural key across source systems.
SELECT *
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ACCT_ID, SEQ_NO
            ORDER BY SRC_UPDATE_DT DESC, SRC_SYSTEM_NM ASC
        ) AS rn
    FROM query_002_standardise
) ranked
WHERE rn = 1
;


-- query_004_final
SELECT
    ACCT_ID,
    SEQ_NO,
    PMT_DT,
    PMT_AMT,
    ORIG_PMT_AMT,
    ADJUSTMENT_AMT,
    PMT_TYPE_DESC,
    PMT_STATUS_DESC,
    CURR_CD,
    POST_DT,
    EFFECTIVE_DT,
    SRC_SYSTEM_NM,
    SRC_BUSINESS_DT,
    SRC_UPDATE_DT,
    SLV_LOAD_TS
FROM query_003_deduplicate
;

-- dbutils.notebook.exit("query_004_final")
