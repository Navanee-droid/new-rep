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


Thank you for sharing the MOM and the action items.
I have reviewed the notes and acknowledge the follow-up tasks assigned to me. I will:
Share the pending access items along with the relevant RTM/DASH ticket details.
Send weekly status updates covering progress, deliverables, and automation learning activities.
Prepare and share the 4-week automation learning plan and forecast.
Verify Pluralsight access and update you on the status.
Compile and communicate any KT/support requirements related to automation deployment and question processes.
Please let me know if I have missed anything or if there are any additional expectations.
Thanks & Regards,
Navaneedhan


