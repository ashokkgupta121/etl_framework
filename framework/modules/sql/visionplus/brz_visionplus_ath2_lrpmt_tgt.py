# =============================================================================
# FILE    : framework/sql/bronze/visionplus/brz_visionplus_ath2_lrpmt_tgt.py
# LAYER   : BRONZE
# SOURCE  : VisionPlus
# TABLE   : ATH2_LRPMT_TGT
# STRATEGY: FULL
# PURPOSE : Raw extraction of ATH2_LRPMT from VisionPlus source.
#           Bronze = as-is from source, no business transformation.
#           Only adds ETL audit columns (handled by transformation engine).
#
# NAMING CONVENTION:
#   {layer_prefix}_{source_system}_{target_table_name}.py
#   brz = bronze | slv = silver | gld = gold
#
# MULTI-QUERY CONVENTION:
#   Each logical step is prefixed with: # query_NNN_step_name
#   The LAST query must produce the final result DataFrame / temp view.
#   The notebook must exit with the temp view name:
#       dbutils.notebook.exit("query_003_final")
# =============================================================================

class BrzVisionplusAth2LrpmtTgt:
    def __init__(self, params: dict):
        self.watermark_value = params["watermark_value"]
        self.business_date = params["business_date"]
        
        self.queries = [
        {
            "name": "query_001_extract",
            "sql": """
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
                    CREATE_DT,
                    UPDATE_DT,
                    LOAD_DT
                FROM src_ath2_lrpmt
            """
        },
        {
            "name": "query_002_cleanse",
            "sql": """
                SELECT
                    TRIM(ACCT_ID)           AS ACCT_ID,
                    TRIM(SEQ_NO)            AS SEQ_NO,
                    PMT_DT,
                    PMT_AMT,
                    TRIM(PMT_TYPE_CD)       AS PMT_TYPE_CD,
                    TRIM(PMT_STATUS_CD)     AS PMT_STATUS_CD,
                    ORIG_PMT_AMT,
                    TRIM(CURR_CD)           AS CURR_CD,
                    POST_DT,
                    EFFECTIVE_DT,
                    CREATE_DT,
                    UPDATE_DT,
                    LOAD_DT,
                    'VisionPlus'            AS SRC_SYSTEM_NM,
                    CAST('2026-01-01' AS DATE)  AS SRC_BUSINESS_DT
                FROM query_001_extract
            """
        },
        {
            "name": "query_003_final",
            "sql": """
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
                    CREATE_DT,
                    UPDATE_DT,
                    LOAD_DT,
                    SRC_SYSTEM_NM,
                    SRC_BUSINESS_DT
                FROM query_002_cleanse
            """
        }
    ]