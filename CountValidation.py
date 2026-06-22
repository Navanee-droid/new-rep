# This Script does count validation between Oracle and SF.
# Input --a ApplicationName --t Tenant --p Process Date --l Log Level --o Log Path
# Created:08/08/2024 - Balaji V
# Modified: 06/22/2025 - Oracle Connection configuration changed to yaml file

import os.path
import os
import subprocess
import sys
import time
import traceback
import oracledb
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
    get_snowflake_dtm_views,
    parse_args,
    get_appl_code,
    open_sf_connection,
    open_oracle_connection,
    get_tables_from_information_schema,
    setup_tenant_shard,
    logging_config,
    load_yaml
)

SCRIPT_NAME = "CountValidation.py"
SCRIPT_VERSION = "v2.0"


def arg_parsing() -> dict:
    return parse_args(
        required=['--a', '--t', '--p'],
        optional=['--load-sf-meta', '--sf-meta-db', '--sf-meta-schema'],
        description='Oracle to Snowflake record count validation.',
    )

def sf_query_db(sql, cursor):
    # Snowflake query function. Returns (result, error_msg).
    try:
        cursor.execute(sql)
        return cursor.fetchall(), None
    except Exception as e:
        print(f'The following Snowflake query has failed.')
        print(sql)
        traceback.print_exc()
        return None, str(e)


def ora_query_db(sql, cursor):
    # Generic oracle query function. Returns (result, error_msg).
    try:
        cursor.execute(sql)
        return cursor.fetchall(), None
    except Exception as e:
        print(f'The following Oracle query has failed.')
        print(sql)
        traceback.print_exc()
        return None, str(e)


def write_query_reference(output_dir, appl_name, tenant_id, process_date, query_list):
    """Write all queries used during validation to a separate reference text file."""
    query_file = os.path.join(output_dir, f"{appl_name}_{tenant_id}_queries_{process_date.replace('-', '')}.txt")
    if os.path.exists(query_file):
        os.remove(query_file)

    W = 120
    with open(query_file, 'w', encoding='utf-8') as f:
        f.write('=' * W + '\n')
        f.write('QUERY REFERENCE DOCUMENT\n')
        f.write('=' * W + '\n')
        f.write(f'Application  : {appl_name}\n')
        f.write(f'Tenant ID    : {tenant_id}\n')
        f.write(f'Process Date : {process_date}\n')
        f.write(f'Generated    : {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n')
        f.write(f'Total Tables : {len(query_list)}\n')
        f.write('=' * W + '\n\n')

        for entry in query_list:
            f.write('=' * W + '\n')
            f.write(f"TABLE: {entry['table_name']}\n")
            f.write('=' * W + '\n')
            f.write('\n  SNOWFLAKE (Curated):\n')
            f.write(f"    {entry['snowflake_query']}\n")
            f.write('\n  ORACLE:\n')
            f.write(f"    {entry['oracle_query']}\n")
            if entry.get('shard_query'):
                f.write('\n  SNOWFLAKE (Shard):\n')
                f.write(f"    {entry['shard_query']}\n")
            f.write('\n' + '-' * W + '\n\n')

    logging.info(f'Query reference file created: {query_file}')
    print(f'Query reference file: {query_file}')
    return query_file


def rec_count_validation(ora_cursor, sf_cursor, tenant_id, table_list, process_date, appl_name, ingest_cfg_dict, app_db_name=None):
    # Validates the load counts for a given process date: Oracle vs SF Curated vs SF Shard (3-layer)
    # Returns (output_file, table_counts) where table_counts is list of dicts
    run_date = process_date
    table_counts = []
    query_list = []

    script_name = os.path.splitext(os.path.basename(__file__))[0]
    full_path = f"{ingest_cfg_dict['snowflake_connection']['validation_path']}/{appl_name}/{script_name}"
    os.makedirs(full_path, exist_ok=True)
    output_file = os.path.join(full_path, f"{appl_name}_{tenant_id}_record_counts_{process_date.replace('-', '')}.txt")
    print(output_file)

    W = 140
    div = '-' * W
    dbl = '=' * W
    TBL_W, CNT_W, STS_W = 55, 14, 12

    shard_label = app_db_name if app_db_name else 'N/A'
    lines = []

    # Header box
    lines.append('+' + '=' * (W - 2) + '+')
    lines.append('|' + ' ORACLE vs SNOWFLAKE 3-LAYER COUNT VALIDATION REPORT'.ljust(W - 2) + '|')
    lines.append('+' + '=' * (W - 2) + '+')
    lines.append('|' + f' Schema : {appl_name:<22} | Tenant : {tenant_id:<22} | Process Date : {run_date}'.ljust(W - 2) + '|')
    lines.append('|' + f' Layer 1: DW{tenant_id} (Oracle)  vs  CUR_IBS.{appl_name} (SF Curated)'.ljust(W - 2) + '|')
    lines.append('|' + f' Layer 2: CUR_IBS.{appl_name} (SF Curated)  vs  {shard_label}.{appl_name} (SF Shard)'.ljust(W - 2) + '|')
    lines.append('+' + '=' * (W - 2) + '+')
    lines.append('')

    # Table grid header
    lines.append(div)
    lines.append(div)
    hdr = (f"{'Table':<{TBL_W}} | {'Oracle Cnt':>{CNT_W}} | "
           f"{'SF Cur Cnt':>{CNT_W}} | {'SF Shrd Cnt':>{CNT_W}} | {'Status':<{STS_W}} | Notes")
    sep = ('-' * TBL_W + '-+-' + '-' * CNT_W + '-+-' +
           '-' * CNT_W + '-+-' + '-' * CNT_W + '-+-' + '-' * STS_W + '-+------')
    lines.append(hdr)
    lines.append(sep)

    pass_count, fail_count, error_count = 0, 0, 0

    for table in table_list:
        print(f'Validating {tenant_id}.{table}')
        date_col = 'eff_dte' if (any(k in table for k in ('SCD', 'RCD', 'RPD')) and appl_name.upper().startswith('DDW')) else 'prcs_dte'
        ora_date_col = date_col.upper()
        shard_count = None
        sf_table_count = None
        ora_table_count = None
        fail_stage = None
        error_msg = ''
        try:
            sf_query = f"select count(*) from CUR_IBS.{appl_name}.{table} where tenant_id='{tenant_id}' and {date_col}='{process_date}';"
            sf_res, sf_err = sf_query_db(sql=sf_query, cursor=sf_cursor)
            sf_table_count = sf_res[0][0] if sf_res is not None else None

            if table.upper().startswith('VW_'):
                table = 'TB_'+table[3:]
            ora_query = f"select count(*) from DW{tenant_id}2.{table} where {ora_date_col}=to_date('{process_date}', 'YYYY-MM-DD')"
            ora_res, ora_err = ora_query_db(sql=ora_query, cursor=ora_cursor)
            if ora_res is None:
                raise RuntimeError(f"Oracle query failed for {table} — column {ora_date_col} may not exist: {ora_err}")
            ora_table_count = ora_res[0][0]

            if sf_table_count is None:
                # Table missing in SF but exists in Oracle — show Oracle count, mark ERROR
                raise RuntimeError(f"Table not found in Snowflake (Oracle count: {ora_table_count:,}): {sf_err}")

            shard_query = None
            if app_db_name:
                shard_query = f"select count(*) from {app_db_name}.{appl_name}.{table} where tenant_id='{tenant_id}' and {date_col}='{process_date}';"
                shard_res, shard_err = sf_query_db(sql=shard_query, cursor=sf_cursor)
                if shard_res is None:
                    raise RuntimeError(f"Shard query failed for {table} (SF curated: {sf_table_count:,}, Oracle: {ora_table_count:,}): {shard_err}")
                shard_count = shard_res[0][0]

            query_list.append({
                'table_name': table,
                'snowflake_query': sf_query.strip(),
                'oracle_query': ora_query.strip(),
                'shard_query': shard_query,
            })

            layer1_ok = (ora_table_count == sf_table_count)
            layer2_ok = (shard_count is None) or (sf_table_count == shard_count)

            if not layer1_ok:
                status = 'FAIL'
                fail_stage = 'ORA_VS_SF'
                notes = 'Layer 1 mismatch - Oracle vs SF Curated'
                fail_count += 1
            elif not layer2_ok:
                status = 'FAIL'
                fail_stage = 'SF_VS_SHARD'
                notes = 'Layer 2 mismatch - SF Curated vs SF Shard'
                fail_count += 1
            else:
                status = 'SUCCESS'
                notes = ''
                pass_count += 1

            table_counts.append({
                'table': table, 'ora_count': ora_table_count,
                'sf_count': sf_table_count, 'sf_shard_count': shard_count,
                'status': status, 'fail_stage': fail_stage,
                'ora_query': ora_query, 'sf_query': sf_query
            })
            if status == 'FAIL':
                pass  # no drilldown section

            ora_fmt   = f"{ora_table_count:,}"
            sf_fmt    = f"{sf_table_count:,}"
            shard_fmt = f"{shard_count:,}" if shard_count is not None else 'N/A'
        except Exception as e:
            print(f'Error getting count for {table}: {e}')
            status = 'ERROR'
            notes = str(e)[:60]
            error_count += 1
            _locals = locals()
            _ora   = _locals.get('ora_table_count')
            _sf    = _locals.get('sf_table_count')
            _shard = _locals.get('shard_count')
            ora_fmt   = f"{_ora:,}"   if _ora   is not None else 'N/A'
            sf_fmt    = f"{_sf:,}"    if _sf    is not None else 'N/A'
            shard_fmt = f"{_shard:,}" if _shard is not None else 'N/A'
            table_counts.append({'table': table,
                                  'ora_count':      _ora,
                                  'sf_count':       _sf,
                                  'sf_shard_count': _shard,
                                  'status': 'ERROR', 'error_msg': str(e)})

        lines.append(
            f"{table:<{TBL_W}} | {ora_fmt:>{CNT_W}} | "
            f"{sf_fmt:>{CNT_W}} | {shard_fmt:>{CNT_W}} | {status:<{STS_W}} | {notes}"
        )

    lines.append('')

    # Summary footer
    total = len(table_counts)
    lines.append(dbl)
    lines.append('VALIDATION SUMMARY'.center(W))
    lines.append(dbl)
    lines.append(f'  Total checks : {total}')
    lines.append(f'  PASS         : {pass_count}')
    lines.append(f'  FAIL         : {fail_count}')
    lines.append(f'  ERROR        : {error_count}')
    lines.append('')
    if fail_count > 0 or error_count > 0:
        lines.append('*** SOME VALIDATIONS FAILED - review details above ***'.center(W))
    else:
        lines.append('*** ALL VALIDATIONS PASSED ***'.center(W))
    lines.append(dbl)

    with open(output_file, 'w', encoding='ascii', errors='replace') as f:
        f.write('\n'.join(lines) + '\n')

    query_ref_file = write_query_reference(full_path, appl_name, tenant_id, process_date, query_list)

    passed = pass_count == total - error_count
    if fail_count == 0 and error_count == 0:
        print(f'Record count validation is successful for tenant_id: {tenant_id}.')
    else:
        print(f'Record count validation failed for tenant_id {tenant_id}.')
    print(f'Please see {output_file} for details.')
    return output_file, table_counts, query_ref_file

def load_validation():

    # tenant_ids, proc_date, appl_name, my_user = user_input()
    script_start = time.perf_counter()
    argument_dict = arg_parsing()

    tenant_id_tuple = tuple(argument_dict["tenant_id"].split(','))

    py_path = os.environ["PYTHONPATH"]
    env = os.environ.get("PRJ_ENVIRONMENT", "")
    ingest_yaml_file = f'{py_path}/IngestionConfig.yaml'
    ingest_cfg_dict = load_yaml(yaml_file=ingest_yaml_file)
    # orc_conn_yaml removed - Oracle connection now reads from orc_connections.toml via SNOWFLAKE_HOME

    sf_conn = open_sf_connection(ingest_cfg=ingest_cfg_dict)
    sf_cs = sf_conn.cursor()
    print("Logged into Snowflake")
    appl_code = get_appl_code(argument_dict["appl_name"], sf_cs)
    tables = get_tables_from_information_schema(argument_dict["appl_name"], sf_cs)
    dtm_tables = get_snowflake_dtm_views(sf_conn, argument_dict["appl_name"])
    tables = tables + dtm_tables
    all_table_counts = []
    all_output_files = []
    all_query_ref_files = []
    for tenant_id in tenant_id_tuple:
        app_db_name = setup_tenant_shard(tenant_id, sf_cs, env, tenant_id_tuple)
        ora_user = f'DW{tenant_id}'
        ora_conn = open_oracle_connection(myuser=ora_user)
        ora_cs = ora_conn.cursor()
        print('Logged into Oracle')
        output_file, table_counts, query_ref_file = rec_count_validation(
            ora_cursor=ora_cs, sf_cursor=sf_cs, tenant_id=tenant_id, table_list=tables,
            process_date=argument_dict["process_date_ts"], appl_name=argument_dict["appl_name"],
            ingest_cfg_dict=ingest_cfg_dict, app_db_name=app_db_name)
        for tc in table_counts:
            tc['tenant_id'] = tenant_id
        all_table_counts.extend(table_counts)
        all_output_files.append(output_file)
        all_query_ref_files.append(query_ref_file)
        ora_cs.close()
        ora_conn.close()

    if argument_dict.get('load_sf_meta'):
        try:
            registry = TestCaseRegistry(sf_cs, SCRIPT_NAME,
                                        database=argument_dict.get('sf_meta_db'),
                                        schema=argument_dict.get('sf_meta_schema'))

            results = []
            script_run_time = time.perf_counter() - script_start
            per_table_time = script_run_time / max(len(all_table_counts), 1)
            for tc in all_table_counts:
                ora_c = tc['ora_count']
                sf_c = tc['sf_count']
                if tc['status'] == 'ERROR':
                    mis_count = 0
                    mat_count = 0
                    reason = tc.get('error_msg', 'Query error')[:200]
                elif tc['status'] == 'FAIL':
                    mis_count = abs(ora_c - sf_c) if ora_c is not None and sf_c is not None else 0
                    mat_count = min(ora_c, sf_c) if ora_c is not None and sf_c is not None else 0
                    reason = 'Count mismatch'
                else:
                    mis_count = 0
                    mat_count = min(ora_c, sf_c) if ora_c is not None and sf_c is not None else 0
                    reason = 'Count match'
                results.append(registry.create_result(
                    validation_key='count_validation',
                    test_scenario='Oracle vs Snowflake record count validation',
                    appl_name=argument_dict['appl_name'],
                    appl_code=argument_dict.get('appl_code', argument_dict['appl_name']),
                    tenant_id=tc['tenant_id'],
                    table_name=tc['table'],
                    validation_status=tc['status'],
                    source_count=ora_c,
                    target_count=sf_c,
                    mismatched_count=mis_count,
                    matched_count=mat_count,
                    status_reason=reason,
                    source_sql=tc.get('ora_query'),
                    target_sql=tc.get('sf_query'),
                    execution_time_sec=per_table_time,
                ))

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
            summary.started_at = datetime.fromtimestamp(time.time() - (time.perf_counter() - script_start))
            summary.parameters_used = {k: str(v) for k, v in argument_dict.items() if k not in ('sf_cursor',)}
            summary.update_counts(results)
            summary.execution_time_sec = script_run_time
            for of in all_output_files:
                if os.path.exists(of):
                    summary.read_and_store_output(of, file_type='count_validation_report')
            for qrf in all_query_ref_files:
                if os.path.exists(qrf) and os.path.getsize(qrf) > 0:
                    summary.read_and_store_output(qrf, file_type='query_reference')
            exec_id = loader.insert_execution_summary(summary)

            loader.insert_master_bulk(results, execution_id=exec_id)
            run_id_map = {(r.table_name, r.tenant_id): r.run_id for r in results}

            detail_batch = []
            for tc in all_table_counts:
                if tc['status'] == 'FAIL':
                    rid = run_id_map.get((tc['table'], tc['tenant_id']), 0)
                    if rid:
                        detail_batch.append(ValidationDetailResult(
                            run_id=rid,
                            match_status='MISMATCH',
                            record_key=tc['table'],
                            source_data={'count': tc['ora_count']},
                            target_data={'count': tc['sf_count']},
                            detail_remarks='Count mismatch'
                        ))
            if detail_batch:
                loader.insert_detail_bulk(detail_batch)

            summary.emit_summary_line()
            logging.info(f"Loaded {len(results)} count validation results to Snowflake")
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
