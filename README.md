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

"Curated_Shard_Validation.py validates data parity between the CUR_IBS curated layer and APP_IBS_SHARD databases. It first performs tenant-wise grouped count validation and then uses HASH_AGG-based comparison on common business columns. If mismatches are found, it drills down to tenant-level and row-level analysis to identify missing records, differing columns, and sample data discrepancies. It supports TB_C2 tables, shard-specific tenant routing, date filtering, and loads results into Snowflake validation dashboards."


