# new-repytdf

1
SELECT 
    column_id,
    column_name,
    data_type,
    data_length,
    data_precision,
    data_scale,
    nullable,
    data_default,
    identity_column
FROM all_tab_columns
WHERE owner = 'CUR_IBS'
  AND table_name = 'VW_RC_OZ7_EVT_DAY_TME_ARD'
ORDER BY column_id;


2
SELECT 
    column_id,
    column_name AS name,
    data_type || 
        CASE 
            WHEN data_precision IS NOT NULL 
                THEN '(' || data_precision || ',' || data_scale || ')'
            WHEN data_type IN ('VARCHAR2','CHAR','NVARCHAR2') 
                THEN '(' || data_length || ')'
            ELSE ''
        END AS type,
    'COLUMN' AS kind,
    nullable AS "null?",
    NVL(TO_CHAR(data_default), 'null') AS "default"

This script performs multi-layer count validation for DDW applications. It validates Oracle counts against Snowflake Curated counts and Snowflake Curated counts against Shard counts. When mismatches occur, it performs drilldown analysis using SCD expiry dates, CUR_REC_IND records, FMT files, and UPD files. It generates detailed validation reports, query reference files, and optionally loads results into Snowflake metadata dashboards.
