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

HDVCountValidation.py performs end-to-end record count reconciliation across Oracle DW, Snowflake CUR_IBS, and Snowflake APP_IBS_SHARD layers. It validates table counts, supports TB_C2 and DTM objects, automatically routes tenants to shard databases, performs date-level drilldowns for mismatches, generates detailed reports and query logs, and loads validation results into the Snowflake validation dashboard.

