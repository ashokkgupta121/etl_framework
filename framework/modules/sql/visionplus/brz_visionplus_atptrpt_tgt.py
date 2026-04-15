class BrzVisionplusAtptrptTgt:
    def __init__(self, params: dict):
        self.watermark_value = params["watermark_value"]
        self.business_date = params["business_date"]
        self.queries = [
            {
                "name": "query_001_extract_incremental",
                "sql": f"""
                    SELECT
                        ACCT_ID,
                        RPT_DT,
                        RPT_TYPE_CD,
                        RPT_STATUS_CD,
                        RPT_AMT,
                        CURR_CD,
                        POSTING_DT,
                        REVERSAL_IND,
                        ORIG_RPT_ID,
                        CREATE_DT,
                        UPDATE_DT
                    FROM src_atptrpt
                    WHERE UPDATE_DT > CAST('2026-01-01' AS TIMESTAMP)
                """
            },
            {
                "name": "query_002_cleanse",
                "sql": f"""
                    SELECT
                        TRIM(ACCT_ID)           AS ACCT_ID,
                        RPT_DT,
                        TRIM(RPT_TYPE_CD)       AS RPT_TYPE_CD,
                        TRIM(RPT_STATUS_CD)     AS RPT_STATUS_CD,
                        RPT_AMT,
                        TRIM(CURR_CD)           AS CURR_CD,
                        POSTING_DT,
                        COALESCE(REVERSAL_IND, 'N')         AS REVERSAL_IND,
                        ORIG_RPT_ID,
                        CREATE_DT,
                        UPDATE_DT,
                        'VisionPlus'                         AS SRC_SYSTEM_NM,
                        CAST('2026-01-01' AS DATE) AS SRC_BUSINESS_DT
                    FROM query_001_extract_incremental
                """
            },
            {
                "name": "query_003_final",
                "sql": """
                    SELECT * FROM query_002_cleanse
                """
            }
        ]