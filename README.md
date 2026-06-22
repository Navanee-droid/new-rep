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

BalanceValidation.py performs aggregate business validation between Oracle and Snowflake. It automatically discovers balance-related numeric columns such as balances, amounts, counts, quantities, and rates, calculates SUM values in both systems, and compares them for a given tenant and process date. This ensures that financial totals and business measures are preserved during migration, even when record counts match.


