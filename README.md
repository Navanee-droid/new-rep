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

This script performs hash-based field-level reconciliation between Oracle source tables and Snowflake target tables for ADS applications. It first compares aggregate table hashes for fast validation. If a mismatch is detected, it performs row-level hash comparison using business keys or primary keys to identify missing, extra, or modified records. The script generates summary, CSV, debug SQL, and difference reports, and can load validation results into Snowflake metadata tables for dashboard reporting.

This script performs count validation between the Curated layer (CUR_IBS) and the tenant-specific Application database within Snowflake. It validates that record counts match for each table for a given tenant and process date. For SCD tables it additionally validates SOR_EXP_DTE records. The script supports sharded tenant databases, generates validation reports and query logs, and optionally loads the results into Snowflake metadata tables for dashboard reporting.
