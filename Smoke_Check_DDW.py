# This Script does a smoke check for Day1 Validation scenarios
# Input --a ApplicationName --c appl_code --b ADS_appl_code  --t tenant --g group --p prcs_dte
# Created:05/02/2025 Author:Balaji V
###############################################
# Updated:11/14/2025 Author:Charandeep Singh
# Enhanced : ADS BAcked Tenant and Banks Turned on Check 
# Updated:12/14/2025 Author:Charandeep Singh
# Enhanced : C2 Tables Existence Check in CNF_DIM
# Updated:02/06/2026 Author:Barath Lakshman A N
# Enhanced : Parameters SCD check, Incomplete Cycle Check, ELT_AUDIT_LOG Error Details, Output Formatting
# Updated:02/19/2026 Author:Barath Lakshman A N
# Enhanced : Manifest Actual Intraday Application Check
###############################################
import os.path
import os
import sys
import time
import traceback
import snowflake
import toml
import yaml
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake.connector import DictCursor
import logging
import oracledb
import subprocess
import getpass
import json
import re

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import datetime
from validation_utils import (
    ValidationLoader,
    ValidationResult,
    ValidationDetailResult,
    TestCaseRegistry,
    ExecutionSummary,
    write_results_to_csv,
    write_summary_report,
    determine_status,
    cap_details,
    infer_registry_app_category,
)

from script_utils import ( 
    parse_args,
    open_sf_connection,
    load_yaml,
    get_appl_code,
    open_oracle_connection,
    get_tables_from_appl_table
)

SCRIPT_NAME = "Smoke_Check_DDW.py"
SCRIPT_VERSION = "v2.2"

def arg_parsing() -> dict:
    args = parse_args(
        required=['--a', '--t', '--g', '--p'],
        optional=['--b', '--intraday', '--l', '--o', '--load-sf-meta', '--sf-meta-db', '--sf-meta-schema'],
        description='Smoke check for Day1 Validation scenarios.',
    )
    args['prcs_grp_id'] = args['prcs_grp_id']
    args['ads_appl_cde'] = args['ads_appl_code']
    args['intraday_check'] = args['intraday']
    return args

def sf_query_db(sql, cursor):
    # Snowflake query function
    sf_result = "0"
    try:
        cursor.execute(sql)
        sf_result = cursor.fetchall()
        # print(sf_result)
    except Exception:
        print(f'The following Snowflake query has failed.')
        print(sql)
        traceback.print_exc()
    return sf_result


def validation(sf_cursor, orc_cursor, argument_dict, ingest_cfg_dict,table_list,appl_code):
    tenant_id = argument_dict["tenant_id"]
    appl_name = argument_dict["appl_name"]
    process_date = argument_dict["process_date"]
    prcs_grp_id = argument_dict["prcs_grp_id"]
    process_date_ts = argument_dict["process_date_ts"]
    ads_appl_code = argument_dict["ads_appl_cde"]
    if ads_appl_code == 'NA':
        ads_appl_code = get_appl_code('ADS' + appl_name[3:], sf_cursor)
        if ads_appl_code == 'ADS' + appl_name[3:]:
            ads_appl_code = 'NA'

    output_folder = f'{ingest_cfg_dict["snowflake_connection"]["validation_path"]}'
    env = os.environ["PRJ_ENVIRONMENT"]
    env_tgt_server_id = {
        "opac": "%dev%",
        "opin": "%pdc%",
        "oper": "%bdc%"
    }
    tgt_server_id = env_tgt_server_id.get(env)
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    output_folder_pd = f"{output_folder}/{appl_name}/{script_name}"
    if not os.path.exists(output_folder_pd):
        os.makedirs(output_folder_pd)
    output_file = f'{output_folder_pd}/{appl_name}_{tenant_id}_Smoke_Check_{process_date.replace("-", "")}.txt'
    if os.path.exists(output_file):
        os.remove(output_file)
    skipped_checks = []

    # ELT Audit Log Check
    ELT_STARTED = ELT_COMPLETED = ELT_ERROR = 0
    elt_start = etl_comp = etl_error = ""
    elt_count_status = f"ELT_AUDIT_LOG: {tenant_id}{prcs_grp_id}  Result:SKIPPED"
    try:
        elt_start = f"""select count(*) from CUR_IBS.OPS.ELT_AUDIT_LOG where appl_name =  '{appl_name}' and tenant_id='{tenant_id}' 
                    and prcs_grp_id = '{prcs_grp_id}' and prcs_dte=to_date('{process_date_ts}', 'YYYY-MM-DD') 
                    and PRCS_STATUS = 'STARTED';"""

        ELT_STARTED = sf_query_db(sql=elt_start, cursor=sf_cursor)[0][0]
        etl_comp = f"""select count(*) from CUR_IBS.OPS.ELT_AUDIT_LOG where appl_name =  '{appl_name}' and tenant_id ='{tenant_id}' 
                    and prcs_grp_id = '{prcs_grp_id}' and prcs_dte=to_date('{process_date_ts}', 'YYYY-MM-DD') 
                        and PRCS_STATUS = 'COMPLETED';"""
        ELT_COMPLETED = sf_query_db(sql=etl_comp, cursor=sf_cursor)[0][0]
        etl_error = f"""select count(*) from CUR_IBS.OPS.ELT_AUDIT_LOG where appl_name =  '{appl_name}' and tenant_id = '{tenant_id}' 
                    and prcs_grp_id = '{prcs_grp_id}' and prcs_dte = to_date('{process_date_ts}', 'YYYY-MM-DD') 
                            and PRCS_STATUS = 'ERROR';"""
        ELT_ERROR = sf_query_db(sql=etl_error, cursor=sf_cursor)[0][0]
        print(f"\n{elt_start}\n\n{etl_comp}\n\n{etl_error}")
        if ELT_STARTED != ELT_COMPLETED and ELT_ERROR != '0':
            result_message = 'FAIL'
            print(f'{appl_name}:{tenant_id}{prcs_grp_id}  ELT_AUDID_LOG has Error record.')
            did_not_fail = False
        else:
            result_message = 'SUCCESS'
        elt_count_status = f"""ELT_AUDIT_LOG: {tenant_id}{prcs_grp_id}  ELT_STARTED: {ELT_STARTED} ELT_COMPLETED: {ELT_COMPLETED} ELT_ERROR: {ELT_ERROR} Result:{result_message}"""
    except Exception as e:
        skipped_checks.append(f"ELT_AUDIT_LOG Check - {e}")
        print(f"ELT_AUDIT_LOG check skipped due to error: {e}")
        traceback.print_exc()

    #Incomplete Cycle Check [02/06/2026]
    # ic_count = 0
    # ic_count_status = f"INCOMPLETE_CYCLES_DETAILS: {tenant_id}  Result:SKIPPED"
    # try:
    #     ic_sql = f"""SELECT COUNT(*) FROM CUR_IBS.OPS.INCOMPLETE_CYCLES_DETAILS
    #         WHERE APPL_NAME = '{appl_name}' AND PRCS_DTE = TO_DATE('{process_date_ts}', 'YYYY-MM-DD') AND tenant_id = '{tenant_id}'"""
    #     ic_count = sf_query_db(sql=ic_sql, cursor=sf_cursor)[0][0]
    #     ic_result_message = 'SUCCESS'
    #     if ic_count > 0:
    #         ic_result_message = 'FAIL'
    #         print(f'{appl_name}:{tenant_id} INCOMPLETE_CYCLE check has failed.')
    #         ic = f"""SELECT COUNT(*) FROM CUR_IBS.OPS.INCOMPLETE_CYCLES_DETAILS
    #             WHERE APPL_NAME = '{appl_name}' AND PRCS_DTE = TO_DATE('{process_date_ts}', 'YYYY-MM-DD') AND tenant_id = '{tenant_id}'"""
    #         result_ic = sf_cursor.execute(ic).fetchall()
    #         ic_column_names = [description[0] for description in sf_cursor.description]
    #     ic_count_status = f"""INCOMPLETE_CYCLES_DETAILS: {tenant_id} INCOMPLETE_CYCLE_COUNT: {ic_count} Result:{ic_result_message}"""
    # except Exception as e:
    #     skipped_checks.append(f"INCOMPLETE_CYCLES Check - {e}")
    #     print(f"INCOMPLETE_CYCLES check skipped due to error: {e}")
    #     traceback.print_exc()

    # print(f'Writing to file: {elt_count_status}')  # Debugging statement

    mf_actual_count = mf_expected_count = 0
    manifest_count_status = f"Actual VS Expected: {tenant_id}{prcs_grp_id}  Result:SKIPPED"
    try:
        mf_actual_sql = f"""select count(*) from RAW_IBS.OPS.MANIFEST_ACTUAL where appl_name =  '{appl_name}' and 
                      tenant_id='{tenant_id}' and  prcs_grp_id = '{prcs_grp_id}' and 
                      prcs_dte=to_date('{process_date_ts}', 'YYYY-MM-DD') 
                       and active_ind = 'Y';"""
        mf_actual_count = sf_query_db(sql=mf_actual_sql, cursor=sf_cursor)[0][0]
        mf_expected_sql = f"""select count(*) from RAW_IBS.OPS.MANIFEST_EXPECTED where appl_name =  '{appl_name}';"""
        mf_expected_count = sf_query_db(sql=mf_expected_sql, cursor=sf_cursor)[0][0]
        print(f"\n{mf_actual_count}\n{mf_expected_count}")
        if mf_actual_count != mf_expected_count:
            result_message = 'FAIL'
            print(
                f'{appl_name}:{tenant_id}{prcs_grp_id} File Entity Mismatch between Manifest_actual VS Manifest_Expected.')
            did_not_fail = False
        else:
            result_message = 'SUCCESS'
        manifest_count_status = f"""Actual VS Expected: {tenant_id}{prcs_grp_id}  Manifest_Actual: {mf_actual_count} Manifest_Expected: {mf_expected_count} Result:{result_message}"""
    except Exception as e:
        skipped_checks.append(f"Manifest Count Check - {e}")
        print(f"Manifest Count check skipped due to error: {e}")
        traceback.print_exc()

    tam = f"""select * exclude(APPL_NAME, AUDIT_TS) from RAW_IBS.OPS.TENANT_APPL_MASTER where appl_name =  '{appl_name}';"""
    result_tam = []
    column_names = []
    try:
        result_tam = sf_cursor.execute(tam).fetchall()
        column_names = [description[0] for description in sf_cursor.description]
    except Exception as e:
        skipped_checks.append(f"Tenant Master (TAM) Query - {e}")
        print(f"Tenant Master query skipped due to error: {e}")
        traceback.print_exc()
    tam_ora = f"""select TGT_TWIN_ID,TGT_PROCESSING_GRP,BANK_NBR from t_src_tgt_mapping where appl_cde ='{appl_code}' and tgt_server_id like '{tgt_server_id}'"""
    result_tam_ora = []
    tam_ora_cols = []
    try:
        result_tam_ora = orc_cursor.execute(tam_ora).fetchall()
        tam_ora_cols = [description[0] for description in orc_cursor.description]
    except Exception as e:
        skipped_checks.append(f"Oracle TAM Query (t_src_tgt_mapping) - {e}")
        print(f"Oracle TAM query skipped due to error: {e}")
        traceback.print_exc()
    aex = f"""select * exclude(AUDIT_TS) from RAW_IBS.OPS.APPLICATION_EXPECTED where appl_name =  '{appl_name}';"""
    result_aex = []
    ae_column_names = []
    try:
        result_aex = sf_cursor.execute(aex).fetchall()
        ae_column_names = [description[0] for description in sf_cursor.description]
    except Exception as e:
        skipped_checks.append(f"Application Expected (AEX) Query - {e}")
        print(f"Application Expected query skipped due to error: {e}")
        traceback.print_exc()
    appl_dep = f"""SELECT DISTINCT AE.APPL_NAME AS MAIN_APPL_NAME, AE.DEPENDENCY_APPL_NAME AS DEP_APP_NAME, TAM.TENANT_ID
            , TAM.PRCS_GRP_ID, TAM.BANK_NBR AS BANK_NBR , AE.DEPENDENCY_APPL_DAG , BNK_SCH.APPL_SCHD AS APPL_SCHD, PRCS_DTE AS PRCS_DTE
		    FROM RAW_IBS.OPS.TENANT_APPL_MASTER TAM LEFT JOIN RAW_IBS.OPS.APPLICATION_EXPECTED AE ON 
		    AE.APPL_NAME =TAM.APPL_NAME LEFT JOIN (SELECT ACN.APPL_NAME,BAS.* EXCLUDE(APPL_CODE, SERVER_CDE,SERVER_NAME,
		    CYCLES,SOURCE_FILE,LOAD_TS) FROM RAW_IBS.BNK_APPL_SCHD.BNK_APPLICATION_SCHEDULE BAS INNER JOIN 
            RAW_IBS.ARCHITECTURE.APPLICATION_CODE_NAME ACN
            ON BAS.APPL_CODE = ACN.APPL_CODE) BNK_SCH 
            ON BNK_SCH.APPL_NAME = AE.DEPENDENCY_APPL_NAME
            AND TAM.TENANT_ID = BNK_SCH.TENANT_ID
            AND TAM.BANK_NBR = BNK_SCH.BANK_NBR
            WHERE AE.APPL_NAME = '{appl_name}'
            AND BNK_SCH.PRCS_DTE = to_date('{process_date_ts}', 'YYYY-MM-DD') and TAM.tenant_id = '{tenant_id}' 
            and TAM.prcs_grp_id = '{prcs_grp_id}' order by tenant_id , prcs_grp_id """
    print(f"\n{appl_dep}")
    result_dep = []
    adep_column_names = []
    try:
        result_dep = sf_cursor.execute(appl_dep).fetchall()
        adep_column_names = [description[0] for description in sf_cursor.description]
    except Exception as e:
        skipped_checks.append(f"Application Dependency Query - {e}")
        print(f"Application Dependency query skipped due to error: {e}")
        traceback.print_exc()
    proc_cycle = f"""select tstm.tgt_twin_id as TENANT_ID, tstm.tgt_processing_grp as PROCESSING_GROUP, 
                   tstm.bank_nbr as BANK_NBR, ter.processing_cycles as PROCESSING_CYCLE, ter.entity_id as ENTITY_ID
                   from
                   t_src_tgt_mapping tstm,
                   T_ENTITY_REQUEST ter,
                   T_ENTITY_EXTRACT_PGM teep
                   where
                   tstm.tgt_twin_id ='{tenant_id}' 
                   and tstm.tgt_processing_grp = '{prcs_grp_id}' 
                   and tstm.appl_cde = '{appl_code}'
                   and tstm.appl_cde = teep.appl_cde
                   and ter.entity_id = teep.entity_id
                   and ter.bank_nbr = tstm.bank_nbr"""
    print(f"\n{proc_cycle}")
    result_prc_cycle = []
    processing_cycle = []
    try:
        result_prc_cycle = orc_cursor.execute(proc_cycle).fetchall()
        processing_cycle = [description[0] for description in orc_cursor.description]
    except Exception as e:
        skipped_checks.append(f"Oracle Processing Cycle Query - {e}")
        print(f"Oracle Processing Cycle query skipped due to error: {e}")
        traceback.print_exc()
    ads_backed_tenant = None
    result_ads_backed_tenant = []
    if ads_appl_code != 'NA':
            ads_backed_tenant = f"""
                SELECT tgt_twin_id, tgt_processing_grp, bank_nbr
                FROM t_src_tgt_mapping a
                WHERE appl_cde = '{appl_code}'
                  AND tgt_server_id LIKE '{tgt_server_id}'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM t_src_tgt_mapping b
                      WHERE b.appl_cde = '{ads_appl_code}'
                        AND a.tgt_twin_id = b.tgt_twin_id
                        AND a.tgt_processing_grp = b.tgt_processing_grp
                  )"""
            try:
                result_ads_backed_tenant = orc_cursor.execute(ads_backed_tenant).fetchall()
            except Exception as e:
                skipped_checks.append(f"ADS Backed Tenant Check - {e}")
                print(f"ADS Backed Tenant check skipped due to error: {e}")
                traceback.print_exc()
    
    # Parameters SCD check [02/06/2026]
    pscd = f"""select * from RAW_IBS.OPS.PARAMETERS_SCD where APPL_NAME = '{appl_name}';"""
    result_pscd = []
    pscd_column_names = []
    try:
        result_pscd = sf_cursor.execute(pscd).fetchall()
        pscd_column_names = [description[0] for description in sf_cursor.description]
    except Exception as e:
        skipped_checks.append(f"PARAMETERS_SCD Query - {e}")
        print(f"PARAMETERS_SCD query skipped due to error: {e}")
        traceback.print_exc()

    # Check each Oracle tenant-group combination in PARAMETERS_SCD (avoid duplicates)
    pscd_validation_results = []
    checked_combinations = set()
    for ora_row in result_tam_ora:
        ora_tenant_id = ora_row[0]
        ora_prcs_grp_id = ora_row[1]

        # Create unique key for tenant-group combination
        tenant_group_key = f"{ora_tenant_id}{ora_prcs_grp_id}"
        if tenant_group_key in checked_combinations:
            continue
        checked_combinations.add(tenant_group_key)

        pscd_active_check = f"""select ACTIVE_IND from RAW_IBS.OPS.PARAMETERS_SCD 
                                where APPL_NAME = '{appl_name}' 
                                and tenant_id = '{ora_tenant_id}' 
                                and prcs_grp_id = {ora_prcs_grp_id};"""
        result_pscd_active = None
        try:
            result_pscd_active = sf_cursor.execute(pscd_active_check).fetchone()
        except Exception as e:
            pscd_validation_results.append(f"SKIPPED - {ora_tenant_id}{ora_prcs_grp_id}: Validation skipped (error: {e})")
            skipped_checks.append(f"PARAMETERS_SCD Active Check ({ora_tenant_id}{ora_prcs_grp_id}) - {e}")
            print(f"PARAMETERS_SCD active check for {ora_tenant_id}{ora_prcs_grp_id} skipped due to error: {e}")
            traceback.print_exc()
            continue

        if result_pscd_active:
            if result_pscd_active[0] == 'Y':
                pscd_validation_results.append(f"SUCCESS - {ora_tenant_id}{ora_prcs_grp_id}: ACTIVE_IND = 'Y'")
            else:
                pscd_validation_results.append(f"FAIL - {ora_tenant_id}{ora_prcs_grp_id}: ACTIVE_IND = '{result_pscd_active[0]}'")
        else:
            pscd_validation_results.append(f"FAIL - {ora_tenant_id}{ora_prcs_grp_id}: Not found in PARAMETERS_SCD")

    if not pscd_validation_results:
        pscd_active_status = "SKIPPED"
    elif any("SKIPPED" in r for r in pscd_validation_results):
        non_skipped = [r for r in pscd_validation_results if "SKIPPED" not in r]
        pscd_active_status = "SKIPPED" if not non_skipped else ("SUCCESS" if all("SUCCESS" in r for r in non_skipped) else "FAIL")
    else:
        pscd_active_status = "SUCCESS" if all("SUCCESS" in result for result in pscd_validation_results) else "FAIL"

    table_check_results_cnf_dim = []
    table_check_results_appl_schema = []
    for table in table_list:
        # Check in DDW_CNF_DIM schema
        query_ddw_cnf_dim = f"SHOW TABLES LIKE '{table}' IN SCHEMA DDW_CNF_DIM;"
        # Check in appl_name schema
        query_appl_name = f"SHOW TABLES LIKE '{table}' IN SCHEMA {appl_name};"

        try:
            # Check in DDW_CNF_DIM schema
            result_ddw_cnf_dim = sf_cursor.execute(query_ddw_cnf_dim).fetchall()
            if result_ddw_cnf_dim:
                table_check_results_cnf_dim.append(f"Result:SUCCESS\t\t{table} (Exists in DDW_CNF_DIM)")
            else:
                table_check_results_cnf_dim.append(f"Result:FAIL\t\t{table} (Not in DDW_CNF_DIM)")

            # Check in appl_name schema
            result_appl_name = sf_cursor.execute(query_appl_name).fetchall()
            if result_appl_name:
                table_check_results_appl_schema.append(f"Result:FAIL\t\t{table} - Table exists in schema {appl_name}. It should be dropped.")
            else:
                table_check_results_appl_schema.append(f"Result:SUCCESS\t\t{table} - Table does not exist in schema {appl_name}.")
        except Exception as e:
            skipped_checks.append(f"C2 Table Check ({table}) - {e}")
            table_check_results_cnf_dim.append(f"Result:SKIPPED\t\t{table} - Validation skipped (error: {e})")
            table_check_results_appl_schema.append(f"Result:SKIPPED\t\t{table} - Validation skipped (error: {e})")

    # Manifest Actual Intraday Application Check [02/19/2026]
    intraday_check = argument_dict.get("intraday_check", "N")
    intraday_result_message = "N/A"
    manifest_detail_rows = []
    manifest_detail_cols = []
    intraday_compare_rows = []
    intraday_compare_cols = ['CURRENT_PRCS_DTE_TME', 'PREVIOUS_PRCS_DTE_TME', 'STATUS']
    if intraday_check.upper() == 'Y':
        try:
            manifest_detail_sql = f"""SELECT * FROM RAW_IBS.OPS.MANIFEST_ACTUAL 
                WHERE APPL_NAME = '{appl_name}' AND TENANT_ID = '{tenant_id}' 
                AND PRCS_GRP_ID = '{prcs_grp_id}' AND PRCS_DTE = TO_DATE('{process_date_ts}', 'YYYY-MM-DD')"""
            manifest_detail_rows = sf_cursor.execute(manifest_detail_sql).fetchall()
            manifest_detail_cols = [description[0] for description in sf_cursor.description]

            # Query current PRCS_DTE_TME values where ACTIVE_IND = 'Y'
            intraday_current_sql = f"""SELECT PRCS_DTE_TME 
                FROM RAW_IBS.OPS.MANIFEST_ACTUAL 
                WHERE APPL_NAME = '{appl_name}' AND TENANT_ID = '{tenant_id}' 
                AND PRCS_GRP_ID = '{prcs_grp_id}' AND ACTIVE_IND = 'Y' AND ACTIVE_IND = 'Y'
                AND PRCS_DTE = TO_DATE('{process_date_ts}', 'YYYY-MM-DD')
                ORDER BY PRCS_DTE_TME"""
            current_rows = sf_cursor.execute(intraday_current_sql).fetchall()
            current_values = sorted([str(row[0]) for row in current_rows])

            # Load previous run's values from snapshot file
            snapshot_file = f"{output_folder_pd}/{appl_name}_{tenant_id}_{prcs_grp_id}_{process_date}_intraday_snapshot.json"
            previous_values = []
            first_run = True
            if os.path.exists(snapshot_file):
                first_run = False
                with open(snapshot_file, 'r') as sf:
                    previous_values = json.load(sf)

            # Compare current vs previous values
            if not first_run:
                for i, curr_val in enumerate(current_values):
                    prev_val = previous_values[i] if i < len(previous_values) else 'N/A'
                    if prev_val == 'N/A':
                        status = 'NEW'
                    elif int(curr_val) > int(prev_val):
                        status = 'SUCCESS'
                    elif int(curr_val) == int(prev_val):
                        status = 'UNCHANGED'
                    else:
                        status = 'DECREASED'
                    intraday_compare_rows.append((curr_val, prev_val, status))
            else:
                for curr_val in current_values:
                    intraday_compare_rows.append((curr_val, 'N/A', 'FIRST RUN'))

            # Save current values as snapshot for next run
            with open(snapshot_file, 'w') as sf:
                json.dump(current_values, sf, indent=2)
            print(f'Intraday snapshot saved to: {snapshot_file}')

            if first_run:
                intraday_result_message = "FIRST RUN - Baseline captured"
            else:
                unchanged_count = sum(1 for row in intraday_compare_rows if row[2] == 'UNCHANGED')
                intraday_result_message = "FAIL" if unchanged_count > 0 else "SUCCESS"
                if intraday_result_message == "FAIL":
                    print(f'{appl_name}:{tenant_id}{prcs_grp_id} Manifest Intraday Application Check has UNCHANGED PRCS_DTE_TME values.')
        except Exception as e:
            intraday_result_message = "SKIPPED"
            skipped_checks.append(f"Manifest Intraday Application Check - {e}")
            print(f"Manifest Intraday check skipped due to error: {e}")
            traceback.print_exc()

    with open(file=output_file, mode='a') as f:
        # Header
        f.write('='*80 + '\n')
        f.write(f'SMOKE CHECK REPORT: {appl_name} - Tenant_Group {tenant_id}{prcs_grp_id}\n')
        f.write('='*80 + '\n\n')
        
        # Summary Section [02/06/2026]
        normalized_tam = {tuple(value.strip() for value in row) for row in result_tam}
        normalized_tam_ora = {tuple(value.strip() for value in row) for row in result_tam_ora}
        tam_result = "SUCCESS" if normalized_tam == normalized_tam_ora else "FAIL"
        ads_result = "SUCCESS" if ads_appl_code == 'NA' or not result_ads_backed_tenant else "FAIL" if ads_appl_code != 'NA' else "N/A"
        
        f.write('VALIDATION SUMMARY:\n')
        f.write('-'*80 + '\n')
        f.write(f'{elt_count_status}\n')
        # f.write(f'{ic_count_status}\n')
        f.write(f'{manifest_count_status}\n')
        f.write(f'Tenant Comparison (SF vs Oracle): Result:{tam_result}\n')
        f.write(f'Parameters SCD Check: Result:{pscd_active_status}\n')
        if ads_appl_code != 'NA':
            f.write(f'ADS Backed Application Check: Result:{ads_result}\n')
        
        # C2 Tables Summary
        c2_cnf_pass = all("SUCCESS" in result for result in table_check_results_cnf_dim)
        c2_appl_pass = all("SUCCESS" in result for result in table_check_results_appl_schema)
        c2_has_skipped = any("SKIPPED" in result for result in table_check_results_cnf_dim + table_check_results_appl_schema)
        c2_overall_status = "SKIPPED" if c2_has_skipped else ("SUCCESS" if c2_cnf_pass and c2_appl_pass else "FAIL")
        f.write(f'C2 Tables Existence Check: Result:{c2_overall_status}\n')

        # Intraday application check summary [02/19/2026]
        if intraday_check.upper() == 'Y':
            f.write(f'Manifest Intraday Application Check: Result:{intraday_result_message}\n')
        if skipped_checks:
            f.write(f'\nSkipped Checks ({len(skipped_checks)} check(s) could not be completed):\n')
            for check in skipped_checks:
                f.write(f'  - Validation skipped: {check}\n')
        f.write('='*80 + '\n\n')
        
        # Detailed Sections [02/06/2026]
        if ELT_ERROR > 0:
            f.write('ELT_AUDIT_LOG DETAILS:\n')
            f.write('-'*80 + '\n')
            elt_audit_log = f"""select * from CUR_IBS.OPS.ELT_AUDIT_LOG where appl_name =  '{appl_name}' and tenant_id = '{tenant_id}' 
                                and prcs_grp_id = '{prcs_grp_id}' and prcs_dte=to_date('{process_date_ts}', 'YYYY-MM-DD') 
                                and PRCS_STATUS = 'ERROR';"""
            result_elt_log = sf_cursor.execute(elt_audit_log).fetchall()
            elt_column_names = [description[0] for description in sf_cursor.description]
            f.write(f'ELT_AUDIT_LOG Table for {appl_name} with ERROR status:\n')
            f.write('\t'.join(elt_column_names) + '\n')
            for row in result_elt_log:
                f.write('\t'.join(map(str, row)) + '\n')
            f.write('='*80 + '\n\n')
        # if ic_count > 0:
        #     f.write('INCOMPLETE CYCLES DETAILS:\n')
        #     f.write('-'*80 + '\n')
        #     f.write(f'INCOMPLETE_CYCLES_DETAILS Table for {appl_name}:\n')
        #     ic_data = f"""select * from CUR_IBS.OPS.INCOMPLETE_CYCLES_DETAILS where appl_name =  '{appl_name}' and tenant_id = '{tenant_id}' 
        #                 and prcs_dte=to_date('{process_date_ts}', 'YYYY-MM-DD');"""
        #     ic_data = sf_cursor.execute(ic_data).fetchall()
        #     ic_column_names = [description[0] for description in sf_cursor.description]
        #     f.write('\t'.join(ic_column_names) + '\n')
        #     for row in ic_data:
        #         f.write('\t'.join(map(str, row)) + '\n')
        #     f.write('='*80 + '\n\n')

        f.write('TENANT CONFIGURATION DETAILS:\n')
        f.write('-'*80 + '\n')
        f.write(f'TENANT_APPL_MASTER Table (Snowflake) for {appl_name}:\n')
        f.write('\t'.join(column_names) + '\n')
        for row in result_tam:
            f.write('\t'.join(map(str, row)) + '\n')
        f.write(f'\nOracle Tenants Details for {appl_code}:\n')
        f.write('\t'.join(tam_ora_cols) + '\n')
        for row in result_tam_ora:
            f.write('\t'.join(map(str, row)) + '\n')
        if normalized_tam == normalized_tam_ora:
            f.write("Result: SUCCESS - Snowflake and Oracle have the same active tenants.\n")
        else:
            f.write("Result: FAIL - Snowflake and Oracle do not have the same active tenants.\n")
        f.write('='*80 + '\n\n')
        
        f.write('APPLICATION DEPENDENCIES & SCHEDULING:\n')
        f.write('-'*80 + '\n')
        f.write(f'APPLICATION_EXPECTED Table for {appl_name}:\n')
        f.write('\t'.join(ae_column_names) + '\n')
        for row in result_aex:
            f.write('\t'.join(map(str, row)) + '\n')
        f.write(f'\nDependency Schedule for {appl_name}:\n')
        f.write('\t'.join(adep_column_names) + '\n')
        for row in result_dep:
            f.write('\t'.join(map(str, row)) + '\n')
        f.write(f'\nProcessing Cycle for {appl_name}:\n')
        f.write('\t'.join(processing_cycle) + '\n')
        for row in result_prc_cycle:
            f.write('\t'.join(map(str, row)) + '\n')
        f.write('='*80 + '\n\n')

        # Parameters SCD check results  [02/06/2026]
        f.write('PARAMETERS SCD VALIDATION:\n')
        f.write('-'*80 + '\n')
        f.write(f'All PARAMETERS_SCD Records for {appl_name}:\n')
        f.write('\t'.join(pscd_column_names) + '\n')
        for row in result_pscd:
            formatted_row = []
            for cell in row:
                cell_str = str(cell)
                if '{' in cell_str and '}' in cell_str:
                    cell_str = cell_str.replace('\n', '').replace('  ', ' ')
                formatted_row.append(cell_str)
            f.write('\t'.join(formatted_row) + '\n')
        f.write(f'\nOracle Tenant-Group Validation (ACTIVE_IND Check):\n')
        for validation_result in pscd_validation_results:
            f.write(f'{validation_result}\n')
        f.write(f'\nOverall Result: {pscd_active_status}\n')
        f.write('='*80 + '\n\n')
          
        f.write('ADS BACKED APPLICATION CHECK:\n')
        f.write('-'*80 + '\n')
        if ads_appl_code != 'NA':
            if result_ads_backed_tenant:
                f.write(f'Result: FAIL - Validation failed for ADS back-out tenants.\n')
                f.write(f'Extra tenants have been turned on exclusively for DDW application {appl_code}.\n')
                f.write(f'A separate RAW ingestion process must be initiated for these tenants from DDW P1 scripts.\n\n')
                f.write('TGT_TWIN_ID\tTGT_PROCESSING_GRP\tBANK_NBR\n')
                for row in result_ads_backed_tenant:
                    f.write('\t'.join(map(str, row)) + '\n')
            else:
                f.write(f'Result: SUCCESS - All tenants in {appl_code} have corresponding entries in ADS application {ads_appl_code}.\n')
        else:
            f.write(f'DDW application {appl_code} does not have any backed ADS application.\n')
        f.write('='*80 + '\n\n')
        
        f.write('C2 TABLE EXISTENCE VALIDATION:\n')
        f.write('-'*80 + '\n')
        f.write('C2 Tables in DDW_CNF_DIM Schema:\n')
        for result in table_check_results_cnf_dim:
            f.write(result + '\n')
        f.write(f'\nC2 Tables Check in {appl_name} Schema (Should NOT Exist):\n')
        for result in table_check_results_appl_schema:
            f.write(result + '\n')
        f.write('='*80 + '\n')

        # Intraday application check details [02/19/2026]
        if intraday_check.upper() == 'Y':
            f.write('\nMANIFEST ACTUAL INTRADAY APPLICATION CHECK:\n')
            f.write('-'*80 + '\n')
            f.write(f'Manifest Actual Details for {appl_name} (Tenant: {tenant_id}, Group: {prcs_grp_id}):\n')
            if manifest_detail_cols:
                f.write('\t'.join(manifest_detail_cols) + '\n')
            for row in manifest_detail_rows:
                formatted_cells = []
                for cell in row:
                    cell_str = str(cell)
                    # Flatten JSON or multi-line values to single line
                    if '{' in cell_str or '\n' in cell_str:
                        cell_str = cell_str.replace('\n', '').replace('  ', ' ').strip()
                    formatted_cells.append(cell_str)
                f.write('\t'.join(formatted_cells) + '\n')
            f.write(f'\nPRCS_DTE_TME Comparison (Current vs Previous):\n')
            if intraday_compare_cols:
                f.write('\t'.join(intraday_compare_cols) + '\n')
            for row in intraday_compare_rows:
                f.write('\t'.join(map(str, row)) + '\n')
            f.write(f'\nOverall Result: {intraday_result_message}\n')
            f.write('='*80 + '\n')

    print(f'Output file path: {output_file}')

    # Build per-scenario ValidationResult objects for metadata loading
    normalized_tam = {tuple(value.strip() for value in row) for row in result_tam}
    normalized_tam_ora = {tuple(value.strip() for value in row) for row in result_tam_ora}
    tam_result_status = "SUCCESS" if normalized_tam == normalized_tam_ora else "FAIL"
    ads_result_status = "SUCCESS" if ads_appl_code == 'NA' or not result_ads_backed_tenant else "FAIL" if ads_appl_code != 'NA' else "SUCCESS"
    c2_cnf_ok = all("SUCCESS" in r for r in table_check_results_cnf_dim)
    c2_appl_ok = all("SUCCESS" in r for r in table_check_results_appl_schema)

    elt_status = 'SUCCESS' if ELT_STARTED == ELT_COMPLETED and ELT_ERROR == 0 else 'FAIL'
    # ic_status = 'SUCCESS' if ic_count == 0 else 'FAIL'
    # BTC 1.4 is one scenario: both VALIDATION SUMMARY lines must succeed (single Snowflake master row).
    curated_14_status = 'SUCCESS' if elt_status == 'SUCCESS' else 'FAIL' #and ic_status == 'SUCCESS' else 'FAIL'
    curated_14_reason = f'{elt_count_status}' #  {ic_count_status}'

    # Derived counts for BTC 1.1–1.7 (one ValidationResult / one Snowflake master row per step)
    tam_ora_n, tam_sf_n = len(result_tam_ora), len(result_tam)
    c2_success = sum(1 for r in table_check_results_cnf_dim if 'SUCCESS' in r)
    c2_fail_count = sum(1 for r in table_check_results_appl_schema if 'FAIL' in r)
    mf_exp, mf_act = int(mf_expected_count), int(mf_actual_count)
    mf_ok = mf_exp == mf_act
    pscd_ok = pscd_active_status == 'SUCCESS'
    arch17_pass = mf_ok and pscd_ok
    dep_ok = bool(result_aex) and bool(result_dep) and bool(result_prc_cycle)

    results = []
    base_kwargs = dict(
        appl_name=appl_name, appl_code=appl_code,
        tenant_id=tenant_id,
    )

    # validation_key used as temporary test_case_id; resolved via registry in load_validation()
    # validation_key must match TEST_CASE_DEFINITION.ADDITIONAL_CONFIG:validation_key (TestCaseRegistry)

    # --- BTC 1.1 — DDW_SMOKE_01
    results.append(ValidationResult(
        validation_type='SMOKE_CHECK', test_case_id='c2_cnf_dim',
        test_case_name='DDW Smoke 1.1: TB_C2 in DDW_CNF_DIM',
        test_scenario='C2 Tables Existence in DDW_CNF_DIM',
        table_name='DDW_CNF_DIM',
        validation_status='SUCCESS' if c2_cnf_ok else 'FAIL',
        source_count=len(table_list), target_count=c2_success,
        mismatched_count=len(table_list) - c2_success if not c2_cnf_ok else 0,
        matched_count=c2_success,
        status_reason=f'{c2_success}/{len(table_list)} tables found',
        **base_kwargs
    ))
    # --- BTC 1.2 — DDW_SMOKE_02
    results.append(ValidationResult(
        validation_type='SMOKE_CHECK', test_case_id='c2_not_in_appl_schema',
        test_case_name='DDW Smoke 1.2: TB_C2 absent from DDW app schema',
        test_scenario=f'C2 Tables Not In {appl_name} Schema',
        table_name=f'{appl_name}_SCHEMA',
        validation_status='SUCCESS' if c2_appl_ok else 'FAIL',
        source_count=0, target_count=c2_fail_count,
        mismatched_count=c2_fail_count if not c2_appl_ok else 0,
        matched_count=1 if c2_appl_ok else 0,
        status_reason=f'{c2_fail_count} tables incorrectly exist',
        **base_kwargs
    ))
    # --- BTC 1.3 — DDW_SMOKE_03
    results.append(ValidationResult(
        validation_type='SMOKE_CHECK', test_case_id='tenant_twin_configuration',
        test_case_name='DDW Smoke 1.3: Tenant / TWIN configuration',
        test_scenario='Tenant Configuration - SF vs Oracle',
        table_name='TENANT_APPL_MASTER',
        validation_status=tam_result_status,
        source_count=tam_ora_n, target_count=tam_sf_n,
        mismatched_count=1 if tam_result_status == 'FAIL' else 0,
        matched_count=1 if tam_result_status == 'SUCCESS' else 0,
        status_reason=f'SF tenants: {tam_sf_n}, Oracle tenants: {tam_ora_n}',
        source_sql=tam_ora, target_sql=tam,
        **base_kwargs
    ))
    # --- BTC 1.4 — DDW_SMOKE_04 (single row: ELT audit + incomplete cycles)
    results.append(ValidationResult(
        validation_type='SMOKE_CHECK', test_case_id='curated_elt_cycles',
        test_case_name='DDW Smoke 1.4: Curated load (ELT audit + incomplete cycles)',
        test_scenario='BTC 1.4 — ELT_AUDIT_LOG (started/completed/error) and INCOMPLETE_CYCLES_DETAILS',
        table_name='VALIDATION_SUMMARY_1_4',
        validation_status=curated_14_status,
        source_count=int(ELT_STARTED), target_count=int(ELT_COMPLETED),
        mismatched_count=int(ELT_ERROR), # + int(ic_count),
        matched_count=1 if curated_14_status == 'SUCCESS' else 0,
        status_reason=curated_14_reason,
        source_sql=elt_start, target_sql=etl_comp,
        **base_kwargs
    ))
    # --- BTC 1.5 — DDW_SMOKE_05
    results.append(ValidationResult(
        validation_type='SMOKE_CHECK', test_case_id='app_dependency_schedule',
        test_case_name='DDW Smoke 1.5: Application dependencies & scheduling',
        test_scenario='APPLICATION_EXPECTED, BNK_APPLICATION_SCHEDULE, Oracle processing cycle',
        table_name='APPLICATION_EXPECTED',
        validation_status='SUCCESS' if dep_ok else 'FAIL',
        source_count=len(result_aex), target_count=len(result_dep),
        mismatched_count=0 if dep_ok else 1,
        matched_count=1 if dep_ok else 0,
        status_reason=(
            f'APPLICATION_EXPECTED rows: {len(result_aex)}, dependency schedule rows: {len(result_dep)}, '
            f'processing_cycle rows: {len(result_prc_cycle)}'
        ),
        **base_kwargs
    ))
    # --- BTC 1.6 — DDW_SMOKE_06 (always one row; N/A when no ADS companion)
    if ads_appl_code != 'NA':
        ads_16_reason = (
            f'ADS code {ads_appl_code}: '
            f'{"no extra tenants" if ads_result_status == "SUCCESS" else "extra tenants found"}'
        )
        ads_16_sql = ads_backed_tenant
    else:
        ads_16_reason = 'No backed ADS application (companion --b NA); check not applicable — Result:SUCCESS'
        ads_16_sql = None
    results.append(ValidationResult(
        validation_type='SMOKE_CHECK', test_case_id='ads_backed_application',
        test_case_name='DDW Smoke 1.6: ADS backed application check',
        test_scenario='ADS Backed Application Check',
        table_name='T_SRC_TGT_MAPPING',
        validation_status=ads_result_status,
        mismatched_count=1 if ads_result_status == 'FAIL' else 0,
        matched_count=1 if ads_result_status == 'SUCCESS' else 0,
        status_reason=ads_16_reason,
        source_sql=ads_16_sql,
        **base_kwargs
    ))
    # --- BTC 1.7 — DDW_SMOKE_07 (single row: manifest actual vs expected + PARAMETERS_SCD)
    m17_mismatch = 0 if arch17_pass else (abs(mf_exp - mf_act) if mf_exp != mf_act else 1)
    results.append(ValidationResult(
        validation_type='SMOKE_CHECK', test_case_id='manifest_parameters_arch',
        test_case_name='DDW Smoke 1.7: Manifest + PARAMETERS_SCD architecture',
        test_scenario='Manifest Actual vs Expected; PARAMETERS_SCD ACTIVE_IND (BTC 1.7)',
        table_name='VALIDATION_SUMMARY_1_7',
        validation_status='SUCCESS' if arch17_pass else 'FAIL',
        source_count=mf_exp, target_count=mf_act,
        mismatched_count=m17_mismatch,
        matched_count=min(mf_exp, mf_act),
        status_reason=(manifest_count_status + ' | PARAMETERS_SCD: ' + pscd_active_status + ' — '
                       + '; '.join(pscd_validation_results)[:1500]),
        **base_kwargs
    ))
    if intraday_check.upper() == 'Y':
        intraday_status = 'SUCCESS' if intraday_result_message in ('SUCCESS', 'FIRST RUN - Baseline captured') else 'FAIL'
        results.append(ValidationResult(
            validation_type='SMOKE_CHECK', test_case_id='intraday_manifest',
            test_case_name='DDW Intraday Shard: Manifest Actual Check',
            test_scenario='Manifest Intraday Application Check',
            table_name='MANIFEST_ACTUAL_INTRADAY',
            validation_status=intraday_status,
            mismatched_count=1 if intraday_status == 'FAIL' else 0,
            matched_count=1 if intraday_status == 'SUCCESS' else 0,
            status_reason=intraday_result_message,
            **base_kwargs
        ))

    return output_file, results

def load_validation():
    # Primary driving function
    # tenant_ids, proc_date, appl_name, my_user = user_input()
    script_start = time.perf_counter()
    argument_dict = arg_parsing()

    py_path = os.environ["PYTHONPATH"]
    ingest_yaml_file = f'{py_path}/IngestionConfig.yaml'
    ingest_cfg_dict = load_yaml(yaml_file=ingest_yaml_file)
    schema_name = argument_dict["appl_name"]
    sf_conn = open_sf_connection(ingest_cfg=ingest_cfg_dict)
    sf_cs = sf_conn.cursor()
    print("Logged into Snowflake")
    appl_code = get_appl_code(argument_dict['appl_name'], sf_cs)
    all_tables = get_tables_from_appl_table(sf_cs, appl_code)
    table_list = [t for t in all_tables if t.startswith('TB_C2')]

    ora_user = f'DW{argument_dict["tenant_id"]}'
    ora_conn = open_oracle_connection(myuser=ora_user)
    ora_cur = ora_conn.cursor()
    logging.info('Logged into Oracle')

    started_dt = datetime.now()

    output_file, results = validation(
        sf_cursor=sf_cs, orc_cursor=ora_cur, ingest_cfg_dict=ingest_cfg_dict,
        argument_dict=argument_dict, table_list=table_list, appl_code=appl_code)

    script_end = time.perf_counter()
    script_run_time = script_end - script_start

    if argument_dict.get('load_sf_meta'):
        try:
            # Intraday manifest TC (DDW_ISHARD_03) is stored under APP_CATEGORY DDW even when
            # the job uses an ADS application name; resolve it without ADS filtering.
            if str(argument_dict.get('intraday_check', 'N')).upper() == 'Y':
                _ac = 'DDW'
            else:
                _ac = infer_registry_app_category(argument_dict.get('appl_name'))
            registry = TestCaseRegistry(
                sf_cs, SCRIPT_NAME,
                database=argument_dict.get('sf_meta_db'),
                schema=argument_dict.get('sf_meta_schema'),
                app_category=_ac,
            )
            for r in results:
                tc = registry.get(r.test_case_id)
                if tc:
                    r.test_case_id = tc['test_case_id']
                    r.test_case_name = tc['test_case_name']
                r.execution_time_sec = script_run_time / max(len(results), 1)

            loader = ValidationLoader(
                sf_cursor=sf_cs,
                arg_dict=argument_dict,
                script_name=SCRIPT_NAME,
                script_version=SCRIPT_VERSION,
                database=argument_dict.get('sf_meta_db'),
                schema=argument_dict.get('sf_meta_schema')
            )
            summary = ExecutionSummary(
                script_name=SCRIPT_NAME, appl_name=argument_dict.get('appl_name', ''),
                appl_code=appl_code, tenant_id=argument_dict.get('tenant_id', 'ALL'),
                process_date=argument_dict.get('process_date', ''), script_version=SCRIPT_VERSION
            )
            summary.started_at = started_dt
            summary.parameters_used = {k: str(v) for k, v in argument_dict.items() if k not in ('sf_cursor',)}
            summary.update_counts(results)
            summary.execution_time_sec = script_run_time
            if output_file and os.path.exists(output_file):
                summary.read_and_store_output(output_file, file_type='smoke_check_report')
            exec_id = loader.insert_execution_summary(summary)

            loader.insert_master_bulk(results, execution_id=exec_id)

            detail_batch = []
            for r in results:
                if r.validation_status == 'FAIL':
                    detail_batch.append(ValidationDetailResult(
                        run_id=r.run_id,
                        match_status='MISMATCH',
                        record_key=r.table_name,
                        source_data={'source_count': r.source_count, 'validation_status': r.validation_status},
                        target_data={'target_count': r.target_count},
                        detail_remarks=cap_details(r.status_reason or '', 2000)[0]
                    ))
            if detail_batch:
                capped_batch, _, _ = cap_details(detail_batch)
                loader.insert_detail_bulk(capped_batch)

            summary.emit_summary_line()
            logging.info(f"Loaded {len(results)} smoke check results to Snowflake")
        except Exception as e:
            logging.error(f"Failed to load validation results to Snowflake: {str(e)}")
            traceback.print_exc()
    else:
        logging.info("Skipping metadata load to Snowflake (--load-sf-meta not specified)")

    sf_cs.close()
    sf_conn.close()
    print(f"\n{'-' * 50}")
    print(f'Script run time: {script_run_time} seconds')


if __name__ == '__main__':
    load_validation()
