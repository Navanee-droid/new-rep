#!/usr/bin/env python3
# Modified by: Agalya Karikalan
# Modified on: 2025-12-29
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
    cap_details
)

from script_utils import (
    setup_tenant_shard,
    get_tables_from_sharding_driver,
    load_yaml,
    open_sf_connection,
    logging_config,
    get_appl_code,
    parse_args,
)

SCRIPT_NAME = "App_Cur_Count_Validation.py"
SCRIPT_VERSION = "v2.0"


def arg_parsing() -> dict:
    return parse_args(
        required    = ['--a', '--t', '--p'],
        optional    = ['--l', '--o', '--load-sf-meta', '--sf-meta-db', '--sf-meta-schema'],
        description = 'Validates record counts between CUR and Application schemas.',
    )



def sf_query_db(sql, cursor):
    # Execute SQL using the provided Snowflake cursor and return fetched rows
    sf_result = None
    try:
        cursor.execute(sql)
        sf_result = cursor.fetchall()
        # print(sf_result)
    except Exception:
        print(f'The following query has failed.')
        print(sql)
        traceback.print_exc()
    return sf_result


def rec_count_validation(sf_cursor, tenant_id, table_list, process_date, appl_name, ingest_cfg_dict, app_db_name):
    # Validate per-table record counts between CUR_IBS and the application DB and write a summary file
    """Validate that row counts for each table match between CUR_IBS and the application DB for a tenant/date.

    Writes a per-table summary to a validation output file and returns nothing. Uses safe_count() to
    tolerate query failures and avoids raising for individual table mismatches.
    """
    did_not_fail = True
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    file_path = f"{ingest_cfg_dict['snowflake_connection']['validation_path']}/{appl_name}/{script_name}"
    output_file = (f"{file_path}/{appl_name}_{tenant_id}_"
        f'record_counts_{process_date.replace("-", "")}.txt')

    query_file = (f"{file_path}/{appl_name}_{tenant_id}_"
        f'queries_{process_date.replace("-", "")}.txt')

    # ensure output directory exists and remove any stale file
    output_dir = os.path.dirname(output_file)
    os.makedirs(output_dir, exist_ok=True)
    if os.path.exists(output_file):
        os.remove(output_file)
    if os.path.exists(query_file):
        os.remove(query_file)

    def log_query(label, sql):
        with open(query_file, 'a', encoding='utf-8') as qf:
            qf.write(f'[{label}] {sql}\n')

    def safe_count(query: str):
        """Execute a count query and return an integer result.
        Returns None on query error (e.g. table does not exist), 0 on successful empty result."""
        try:
            res = sf_query_db(sql=query, cursor=sf_cursor)
            if res is None:
                return None  # query failed — table not found or other error
            if res and len(res) and len(res[0]):
                return int(res[0][0])
            return 0
        except Exception:
            logging.exception("Count query failed: %s", query)
            return None

    COL_TBL = 62
    COL_CNT = 12
    COL_RES = 20
    SEP = ' | '
    TOTAL_W = COL_TBL + len(SEP) + COL_CNT + len(SEP) + COL_CNT + len(SEP) + COL_RES

    shard_num = app_db_name.split('_')[-1] if app_db_name else '?'

    def fmt_count(val):
        if val is None:
            return 'N/A'.rjust(COL_CNT)
        return f'{val:,}'.rjust(COL_CNT)

    def write_row(out_f, tbl_name, cur_cnt, app_cnt, result_lbl):
        tbl_col = tbl_name.ljust(COL_TBL)[:COL_TBL]
        out_f.write(f'{tbl_col}{SEP}{fmt_count(cur_cnt)}{SEP}{fmt_count(app_cnt)}{SEP}{result_lbl}\n')

    table_outcomes = []
    with open(output_file, mode='w', encoding='utf-8') as out_f:
        # ── Header box ──
        box_inner = TOTAL_W - 2
        out_f.write('╔' + '═' * box_inner + '╗\n')
        out_f.write('║' + ' CUR (CURATED) vs APPLICATION RECORD COUNT VALIDATION REPORT'.ljust(box_inner) + '║\n')
        out_f.write('╠' + '═' * box_inner + '╣\n')
        info = f' Application: {appl_name:<18} | Tenant: {tenant_id:<10} | Shard: {shard_num:<6} | Date: {process_date}'
        out_f.write('║' + info.ljust(box_inner) + '║\n')
        out_f.write('╚' + '═' * box_inner + '╝\n\n')

        # ── Column headers ──
        out_f.write('Table Name'.ljust(COL_TBL) + SEP + 'Curated'.rjust(COL_CNT) + SEP + 'Application'.rjust(COL_CNT) + SEP + 'Result\n')
        out_f.write('-' * COL_TBL + SEP + '-' * COL_CNT + SEP + '-' * COL_CNT + SEP + '-' * COL_RES + '\n')

        for table in table_list:
            print(f'Validating {tenant_id}.{table}')

            date_col = 'eff_dte' if any(
                substring in table for substring in ["SCD", "RCD", "RPD"]) and appl_name.startswith('DDW') else 'prcs_dte'

            if table.startswith('TB_C2') and appl_name.startswith('DDW'):
                query_schema = 'DDW_CNF_DIM'
                src_appl_filter = f" and SRC_APPL_NAME='{appl_name}'"
            else:
                query_schema = appl_name
                src_appl_filter = ''
            app_table = table.replace("VW_", "TB_")
            cur_query = f"select count(*) from CUR_IBS.{query_schema}.{table} where tenant_id='{tenant_id}' and {date_col}=TO_DATE('{process_date}','YYYYMMDD'){src_appl_filter};"
            app_query = f"select count(*) from {app_db_name}.{query_schema}.{app_table} where tenant_id='{tenant_id}' and {date_col}=TO_DATE('{process_date}','YYYYMMDD'){src_appl_filter};"
            log_query('CUR ', cur_query)
            log_query('APP ', app_query)
            cur_table_count = safe_count(cur_query)
            app_table_count = safe_count(app_query)

            if cur_table_count is None or app_table_count is None:
                result_message = 'SKIPPED'
                missing = []
                if cur_table_count is None:
                    missing.append(f'CUR_IBS.{query_schema}.{table}')
                if app_table_count is None:
                    missing.append(f'{app_db_name}.{query_schema}.{app_table}')
                result_label = 'SKIPPED (table not found)'
                reason = f'Table not found: {", ".join(missing)}'
                print(f'{tenant_id}.{app_table} skipped — {reason}')
                did_not_fail = False
            elif cur_table_count != app_table_count:
                result_message = 'FAIL'
                result_label = 'FAIL'
                reason = f'Count mismatch: CUR={cur_table_count}, App={app_table_count}'
                print(f'{tenant_id}.{app_table} record validation has failed.')
                did_not_fail = False
            else:
                result_message = 'SUCCESS'
                result_label = 'SUCCESS'
                reason = 'Count match'

            write_row(out_f, app_table, cur_table_count, app_table_count, result_label)
            table_outcomes.append({'table': app_table, 'status': result_message,
                                   'reason': reason,
                                   'src_count': cur_table_count or 0, 'tgt_count': app_table_count or 0})

            if date_col == 'eff_dte':
                cur_exp_query = f"select count(*) from CUR_IBS.{query_schema}.{table} where tenant_id='{tenant_id}' and sor_exp_dte=TO_DATE('{process_date}','YYYYMMDD'){src_appl_filter};"
                app_exp_query = f"select count(*) from {app_db_name}.{query_schema}.{app_table} where tenant_id='{tenant_id}' and sor_exp_dte=TO_DATE('{process_date}','YYYYMMDD'){src_appl_filter};"
                log_query('CEXP', cur_exp_query)
                log_query('AEXP', app_exp_query)
                cur_table_count_exp = safe_count(cur_exp_query)
                app_table_count_exp = safe_count(app_exp_query)
                if cur_table_count_exp is None or app_table_count_exp is None:
                    result_message_1 = 'SKIPPED'
                    result_label_1 = 'SKIPPED (table not found)'
                    reason_1 = 'Table not found or query error for SOR_EXP_DTE'
                    print(f'{tenant_id}.{app_table} SOR_EXP_DTE count query error.')
                    did_not_fail = False
                elif cur_table_count_exp != app_table_count_exp:
                    result_message_1 = 'FAIL'
                    result_label_1 = 'FAIL'
                    reason_1 = f'SOR_EXP_DTE mismatch: CUR={cur_table_count_exp}, App={app_table_count_exp}'
                    print(f'{tenant_id}.{app_table} record validation has failed for SOR_EXP_DTE.')
                    did_not_fail = False
                else:
                    result_message_1 = 'SUCCESS'
                    result_label_1 = 'SUCCESS'
                    reason_1 = 'SOR_EXP_DTE count match'
                write_row(out_f, f'{app_table} (SOR_EXP_DTE)', cur_table_count_exp, app_table_count_exp, result_label_1)
                table_outcomes.append({'table': f'{app_table}_SOR_EXP_DTE', 'status': result_message_1,
                                       'reason': reason_1,
                                       'src_count': cur_table_count_exp or 0, 'tgt_count': app_table_count_exp or 0})

        # ── Footer ──
        footer_msg = '✓ Result: ALL VALIDATIONS PASSED ✓' if did_not_fail else '✗ Result: SOME VALIDATIONS FAILED ✗'
        out_f.write('\n' + '-' * TOTAL_W + '\n')
        out_f.write('VALIDATION COMPLETE'.center(TOTAL_W) + '\n')
        out_f.write('-' * TOTAL_W + '\n')
        out_f.write(footer_msg.center(TOTAL_W) + '\n')
        out_f.write('-' * TOTAL_W + '\n')

    if did_not_fail:
        print(f'Record count validation is successful for tenant_id: {tenant_id}.')
        print(f'Please see {output_file} for details.')
    else:
        print(f'Record count validation failed for tenant_id {tenant_id}.')
        print(f'Please examine {output_file} for details.')

    return table_outcomes, output_file


def load_validation():
    # Main driver: parse args, open connections and run validations for all tenants
    # tenant_ids, proc_date, appl_name, my_user = user_input()
    script_start = time.perf_counter()
    argument_dict = arg_parsing()

    tenant_id_tuple = tuple(argument_dict["tenant_id"].split(','))
    app_db = None

    py_path = os.environ["PYTHONPATH"]
    env = os.environ["PRJ_ENVIRONMENT"]
    ingest_yaml_file = f'{py_path}/IngestionConfig.yaml'
    ingest_cfg_dict = load_yaml(yaml_file=ingest_yaml_file)

    sf_conn = open_sf_connection(ingest_cfg=ingest_cfg_dict)
    sf_cs = sf_conn.cursor()
    appl_code = get_appl_code(argument_dict['appl_name'], sf_cs)
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    logging_config(argument_dict['logging_directory'], appl_code, script_name, ingest_cfg_dict, argument_dict['log_level'])
    print("Logged into Snowflake")

    tables = get_tables_from_sharding_driver(argument_dict["appl_name"], sf_cs)
    # Test 1: oracle to snowflake record counts
    table_outcomes = []
    output_file = None
    for tenant_id in tenant_id_tuple:
        app_db = setup_tenant_shard(tenant_id, sf_cs, env, tenant_id_tuple)
        if app_db is None:
            continue

        outcomes, out_file = rec_count_validation(sf_cursor=sf_cs, tenant_id=tenant_id, table_list=tables,
                             process_date=argument_dict["process_date"], appl_name=argument_dict["appl_name"],
                             ingest_cfg_dict=ingest_cfg_dict,
                             app_db_name=app_db)
        if outcomes:
            table_outcomes.extend(outcomes)
        if out_file:
            output_file = out_file

    if argument_dict.get('load_sf_meta'):
        try:
            registry = TestCaseRegistry(sf_cs, SCRIPT_NAME,
                                        database=argument_dict.get('sf_meta_db'),
                                        schema=argument_dict.get('sf_meta_schema'))
            val_results = []
            for outcome in table_outcomes:
                src_c = outcome.get('src_count', 0)
                tgt_c = outcome.get('tgt_count', 0)
                val_results.append(registry.create_result(
                    validation_key='app_cur_count_validation',
                    test_scenario=f'CUR vs App count: {outcome["table"]}',
                    appl_name=argument_dict['appl_name'],
                    appl_code=appl_code,
                    tenant_id=argument_dict['tenant_id'],
                    table_name=outcome['table'],
                    validation_status=outcome['status'],
                    status_reason=outcome['reason'],
                    source_count=src_c,
                    target_count=tgt_c,
                    mismatched_count=abs(src_c - tgt_c) if outcome['status'] == 'FAIL' else 0,
                    matched_count=min(src_c, tgt_c)
                ))
            if not val_results:
                val_results.append(registry.create_result(
                    validation_key='app_cur_count_validation',
                    test_scenario='CUR vs App DB record count validation',
                    appl_name=argument_dict['appl_name'],
                    appl_code=appl_code,
                    tenant_id=argument_dict['tenant_id'],
                    table_name='ALL_TABLES',
                    validation_status='SUCCESS',
                    status_reason='No tables processed',
                    mismatched_count=0,
                    matched_count=0
                ))
            loader = ValidationLoader(
                sf_cursor=sf_cs, arg_dict=argument_dict,
                script_name=SCRIPT_NAME, script_version=SCRIPT_VERSION,
                database=argument_dict.get('sf_meta_db'), schema=argument_dict.get('sf_meta_schema')
            )
            summary = ExecutionSummary(
                script_name=SCRIPT_NAME, appl_name=argument_dict.get('appl_name', ''),
                appl_code=appl_code, tenant_id=argument_dict.get('tenant_id', 'ALL'),
                process_date=argument_dict.get('process_date', ''), script_version=SCRIPT_VERSION
            )
            summary.started_at = datetime.fromtimestamp(time.time() - (time.perf_counter() - script_start))
            summary.parameters_used = {k: str(v) for k, v in argument_dict.items() if k not in ('sf_cursor',)}
            summary.update_counts(val_results)
            summary.execution_time_sec = time.perf_counter() - script_start
            if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
                summary.read_and_store_output(output_file, file_type='count_validation_report')
            exec_id = loader.insert_execution_summary(summary)

            run_id_map = {}
            for r in val_results:
                rid = loader.insert_master(r, execution_id=exec_id)
                run_id_map[r.table_name] = rid

            detail_batch = []
            for outcome in table_outcomes:
                if outcome['status'] == 'FAIL':
                    rid = run_id_map.get(outcome['table'], 0)
                    if rid:
                        detail_batch.append(ValidationDetailResult(
                            run_id=rid,
                            match_status='MISMATCH',
                            record_key=outcome['table'],
                            source_data={'count': outcome.get('src_count', 0)},
                            target_data={'count': outcome.get('tgt_count', 0)},
                            detail_remarks=outcome.get('reason', '')
                        ))
            if detail_batch:
                loader.insert_detail_bulk(detail_batch)

            summary.emit_summary_line()
            passed = sum(1 for o in table_outcomes if o['status'] == 'SUCCESS')
            failed = sum(1 for o in table_outcomes if o['status'] == 'FAIL')
            logging.info(f"Loaded {len(val_results)} result(s) — {passed} SUCCESS, {failed} FAIL")
        except Exception as e:
            logging.error(f"Failed to load validation results to Snowflake: {str(e)}")
            traceback.print_exc()
    else:
        logging.info("Skipping metadata load to Snowflake (--load-sf-meta not specified)")

    sf_cs.close()
    sf_conn.close()

    # Script stats output
    script_end = time.perf_counter()
    script_run_time = script_end - script_start
    print(f"\n{'-' * 50}")
    print(f'Script run time: {script_run_time} seconds')


if __name__ == '__main__':
    load_validation()