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

DateFormatCheckDDW.py validates that all Oracle timestamp columns identified from control card (.pos/.pos1) definitions are created with valid TIMESTAMP datatypes in Snowflake. It automatically discovers timestamp fields, compares them against Snowflake metadata, validates both application and TB_C2 tables, generates validation reports, and loads results into the validation dashboard. Its purpose is to ensure date and timestamp fields are migrated with the correct datatype semantics.


