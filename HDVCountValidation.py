import logging
import os
import sys
import time
import traceback
import csv
import json

from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from validation_utils import (
    ValidationLoader,
    ValidationDetailResult,
    TestCaseRegistry,
    ExecutionSummary,
    cap_details,
)

from script_utils import (
    get_shard_name,
    get_tables_from_appl_table,
    get_tb_c2_filter,
    parse_args,
    get_appl_code,
    open_sf_connection,
    open_oracle_connection,
    logging_config,
    load_yaml
)

SCRIPT_NAME = 'HDVCountValidation.py'
SCRIPT_VERSION = 'v2.0'

def arg_parsing() -> dict:
    return parse_args(
        required=['--a', '--t'],
        optional=['--tb','--l','--o','--filter','--a2','--load-sf-meta', '--sf-meta-db', '--sf-meta-schema'],
        description='Oracle to Snowflake record count validation.',
    )

def sf_query_db(sql, cursor):
    sf_result = None
    try:
        cursor.execute(sql)
        sf_result = cursor.fetchall()
    except Exception:
        logging.error(f'The following Snowflake query has failed.')
        logging.error(sql)
        logging.error(traceback.format_exc())
    return sf_result


def ora_query_db(sql, cursor):
    ora_result = None
    try:
        cursor.execute(sql)
        ora_result = cursor.fetchall()
    except Exception:
        logging.error(f'The following Oracle query has failed.')
        logging.error(sql)
        logging.error(traceback.format_exc())
    return ora_result

def _normalize_date(val):
    s = str(val)
    return s[:10]


def drilldown(appl_name, tenant_id, table, sf_cur_table, sf_cursor, ora_cursor, sf_filter='', orc_filter='', sf_schema=None, appl_name2=None, ora_table=None, query_file=None, sf_filter2=None):
    if sf_schema is None:
        sf_schema = appl_name
    if ora_table is None:
        ora_table = table
    def log_query(label, sql):
        if query_file:
            with open(query_file, 'a', encoding='ascii', errors='replace') as qf:
                qf.write(f'[{label}] {sql}\n')
    date_col = get_date_column(ora_table, appl_name)
    sf_query = f"select count(*), {date_col} from {sf_schema}.{sf_cur_table} where tenant_id='{tenant_id}' {sf_filter} group by {date_col} order by {date_col}"
    log_query('DD_SF  ', sf_query)
    sf_result = sf_query_db(sql=sf_query, cursor=sf_cursor)
    ora_query = f"select count(*), {date_col} from DW{tenant_id}.{ora_table} {orc_filter} group by {date_col} order by {date_col}"
    log_query('DD_ORA ', ora_query)
    ora_result = ora_query_db(sql=ora_query, cursor=ora_cursor)

    sf_by_date = {_normalize_date(row[1]): row[0] for row in sf_result} if sf_result else {}

    if appl_name2:
        sf_schema2 = sf_schema if sf_schema == 'DDW_CNF_DIM' else appl_name2
        sf_query2 = f"select count(*), {date_col} from {sf_schema2}.{sf_cur_table} where tenant_id='{tenant_id}' {sf_filter2} group by {date_col} order by {date_col}"
        log_query('DD_SF2 ', sf_query2)
        sf_result2 = sf_query_db(sql=sf_query2, cursor=sf_cursor)
        sf_by_date2 = {_normalize_date(row[1]): row[0] for row in sf_result2} if sf_result2 else {}
        all_sf_dates = set(sf_by_date) | set(sf_by_date2)
        sf_by_date = {d: sf_by_date.get(d, 0) + sf_by_date2.get(d, 0) for d in all_sf_dates}

    ora_by_date = {_normalize_date(row[1]): row[0] for row in ora_result} if ora_result else {}

    mismatches = []
    for prcs_dte in sorted(set(sf_by_date.keys()) | set(ora_by_date.keys())):
        sf_cnt = sf_by_date.get(prcs_dte, 0)
        ora_cnt = ora_by_date.get(prcs_dte, 0)
        if sf_cnt != ora_cnt:
            mismatches.append({date_col: prcs_dte, 'ora_count': ora_cnt, 'sf_count': sf_cnt})
    return mismatches


def write_drilldown_csv(csv_file, appl_name, tenant_id, fail_tables):
    with open(csv_file, 'w', newline='', encoding='ascii', errors='replace') as f:
        writer = csv.writer(f)
        writer.writerow(['Table', 'Layer', 'Date_Column', 'Date_Value', 'Source_Count', 'Target_Count', 'Difference'])

        for r in fail_tables:
            stage = r.get('fail_stage', 'ORA_VS_SF')
            date_col = get_date_column(r['table'], appl_name)

            if stage in ('ORA_VS_SF', 'BOTH'):
                for m in r.get('mismatch_details', []):
                    ora_cnt = m.get('ora_count', 0)
                    sf_cnt = m.get('sf_count', 0)
                    diff = sf_cnt - ora_cnt
                    writer.writerow([r['table'], 'Layer1_Oracle_vs_SF_Curated', date_col, m[date_col], ora_cnt, sf_cnt, diff])

            if stage in ('SF_VS_SHARD', 'BOTH'):
                for m in r.get('mismatch_details_l2', []):
                    sf_cnt = m.get('sf_count', 0)
                    shard_cnt = m.get('sf_shard_count', 0)
                    diff = shard_cnt - sf_cnt
                    writer.writerow([r['table'], 'Layer2_SF_Curated_vs_Shard', date_col, m[date_col], sf_cnt, shard_cnt, diff])


def write_formatted_report(output_file, appl_name, tenant_id, results, run_date, shard=None, appl_name2=None):
    W = 140
    div = '-' * W
    dbl = '=' * W
    TBL_W, CNT_W, STS_W = 55, 14, 12
    shard_label = shard if shard else 'SF Shard'

    lines = []

    lines.append('+' + '=' * (W - 2) + '+')
    lines.append('|' + ' ORACLE vs SNOWFLAKE 3-LAYER COUNT VALIDATION REPORT'.ljust(W - 2) + '|')
    lines.append('+' + '=' * (W - 2) + '+')
    lines.append('|' + f' Schema : {appl_name:<22} | Tenant : {tenant_id:<22} | Run Date : {run_date}'.ljust(W - 2) + '|')
    lines.append('|' + f' Layer 1: DW{tenant_id} (Oracle)  vs  CUR_IBS.{appl_name} (SF Curated)'.ljust(W - 2) + '|')
    lines.append('|' + f' Layer 2: CUR_IBS.{appl_name} (SF Curated)  vs  {shard_label}.{appl_name} (SF Shard)'.ljust(W - 2) + '|')
    if appl_name2:
        lines.append('|' + f' Note   : SF Curated count = {appl_name} + {appl_name2} (combined)'.ljust(W - 2) + '|')
    lines.append('+' + '=' * (W - 2) + '+')
    lines.append('')

    lines.append(div)
    hdr = (f"{'Table':<{TBL_W}} | {'Oracle Cnt':>{CNT_W}} | "
           f"{'SF Cur Cnt':>{CNT_W}} | {'SF Shrd Cnt':>{CNT_W}} | {'Status':<{STS_W}} | Notes")
    sep = ('-' * TBL_W + '-+-' + '-' * CNT_W + '-+-' +
           '-' * CNT_W + '-+-' + '-' * CNT_W + '-+-' + '-' * STS_W + '-+------')
    lines.append(hdr)
    lines.append(sep)

    fail_results = [r for r in results if r['status'] == 'FAIL']
    error_results = [r for r in results if r['status'] == 'ERROR']
    success_results = [r for r in results if r['status'] == 'SUCCESS']
    sorted_results = fail_results + error_results + success_results

    fail_tables, pass_count, fail_count, error_count = [], 0, 0, 0

    for r in sorted_results:
        if r['status'] == 'ERROR':
            status_str = 'ERROR'
            notes = r.get('error_msg', '')
            error_count += 1
        elif r['status'] == 'FAIL':
            stage = r.get('fail_stage', '')
            status_str = 'FAIL'
            if stage == 'BOTH':
                if r.get('sf_shard_count') is None:
                    notes = 'Layer 1 mismatch & Layer 2 table not found in shard'
                else:
                    notes = 'Layer 1 & 2 mismatch - see drilldown CSV'
            elif stage == 'ORA_VS_SF':
                if r.get('ora_count') is None:
                    notes = 'Layer 1 - table does not exist in Oracle layer'
                elif r.get('sf_count') is None:
                    notes = 'Layer 1 - table does not exist in SF Curated layer'
                else:
                    notes = 'Layer 1 mismatch - see drilldown CSV'
            else:
                if r.get('sf_shard_count') is None:
                    notes = 'Layer 2 - table does not exist in shard layer'
                else:
                    notes = 'Layer 2 mismatch - see drilldown CSV'
            fail_count += 1
            fail_tables.append(r)
        else:
            status_str = 'SUCCESS'
            notes = ''
            pass_count += 1

        ora_fmt   = f"{r['ora_count']:,}"        if r.get('ora_count')        is not None else 'N/A'
        sf_fmt    = f"{r['sf_count']:,}"         if r.get('sf_count')         is not None else 'N/A'
        shard_fmt = f"{r['sf_shard_count']:,}"   if r.get('sf_shard_count')   is not None else 'N/A'

        lines.append(
            f"{r['table']:<{TBL_W}} | {ora_fmt:>{CNT_W}} | "
            f"{sf_fmt:>{CNT_W}} | {shard_fmt:>{CNT_W}} | {status_str:<{STS_W}} | {notes}"
        )

    lines.append('')

    total = len(results)
    lines.append(dbl)
    lines.append('VALIDATION SUMMARY'.center(W))
    lines.append(dbl)
    lines.append(f'  Total checks : {total}')
    lines.append(f'  SUCCESS      : {pass_count}')
    lines.append(f'  FAIL         : {fail_count}')
    lines.append(f'  ERROR        : {error_count}')
    lines.append('')
    if fail_count > 0 or error_count > 0:
        lines.append('*** SOME VALIDATIONS FAILED - review drilldown CSV for details ***'.center(W))
    else:
        lines.append('*** ALL VALIDATIONS PASSED ***'.center(W))
    lines.append(dbl)

    with open(output_file, 'w', encoding='ascii', errors='replace') as f:
        f.write('\n'.join(lines) + '\n')

    return fail_tables


def shard_drilldown(appl_name, tenant_id, table, sf_cur_table, sf_cursor, shard, sf_filter, sf_schema=None, appl_name2=None, query_file=None, sf_filter2=None):
    date_col = get_date_column(table, appl_name)
    if sf_schema is None:
        sf_schema = appl_name
    def log_query(label, sql):
        if query_file:
            with open(query_file, 'a', encoding='ascii', errors='replace') as qf:
                qf.write(f'[{label}] {sql}\n')
    sf_query = f"select count(*), {date_col} from {sf_schema}.{sf_cur_table} where tenant_id='{tenant_id}' {sf_filter} group by {date_col} order by {date_col}"
    log_query('SD_SF  ', sf_query)
    sf_result = sf_query_db(sql=sf_query, cursor=sf_cursor)
    sf_shard_query = f"select count(*), {date_col} from {shard}.{sf_schema}.{table} where tenant_id='{tenant_id}' {sf_filter} group by {date_col} order by {date_col}"
    log_query('SD_SHRD', sf_shard_query)
    sf_shard_result = sf_query_db(sql=sf_shard_query, cursor=sf_cursor)

    sf_by_date = {_normalize_date(row[1]): row[0] for row in sf_result} if sf_result else {}
    sf_shard_by_date = {_normalize_date(row[1]): row[0] for row in sf_shard_result} if sf_shard_result else {}

    if appl_name2:
        is_c2 = table.upper().startswith('TB_C2') and 'DTM' not in table.upper()
        sf_schema2 = 'DDW_CNF_DIM' if is_c2 else appl_name2
        sf_query2 = f"select count(*), {date_col} from {sf_schema2}.{sf_cur_table} where tenant_id='{tenant_id}' {sf_filter2} group by {date_col} order by {date_col}"
        log_query('SD_SF2 ', sf_query2)
        sf_result2 = sf_query_db(sql=sf_query2, cursor=sf_cursor)
        sf_by_date2 = {_normalize_date(row[1]): row[0] for row in sf_result2} if sf_result2 else {}
        sf_shard_query2 = f"select count(*), {date_col} from {shard}.{sf_schema2}.{table} where tenant_id='{tenant_id}' {sf_filter2} group by {date_col} order by {date_col}"
        log_query('SD_SHR2', sf_shard_query2)
        sf_shard_result2 = sf_query_db(sql=sf_shard_query2, cursor=sf_cursor)
        sf_shard_by_date2 = {_normalize_date(row[1]): row[0] for row in sf_shard_result2} if sf_shard_result2 else {}
        all_sf = set(sf_by_date) | set(sf_by_date2)
        sf_by_date = {d: sf_by_date.get(d, 0) + sf_by_date2.get(d, 0) for d in all_sf}
        all_shard = set(sf_shard_by_date) | set(sf_shard_by_date2)
        sf_shard_by_date = {d: sf_shard_by_date.get(d, 0) + sf_shard_by_date2.get(d, 0) for d in all_shard}

    mismatches = []
    for prcs_dte in sorted(set(sf_by_date.keys()) | set(sf_shard_by_date.keys())):
        sf_cnt = sf_by_date.get(prcs_dte, 0)
        sf_shard_cnt = sf_shard_by_date.get(prcs_dte, 0)
        if sf_cnt != sf_shard_cnt:
            mismatches.append({date_col: prcs_dte, 'sf_count': sf_cnt, 'sf_shard_count': sf_shard_cnt})
    return mismatches


def get_date_column(table, appl_name):
    t = table.upper()
    if 'DTM_DIM' in t:
        return 'FULL_DTE'
    if appl_name.upper().startswith('DDW_') and any(k in t for k in ('SCD', 'RCD', 'RPD')):
        return 'EFF_DTE'
    return 'PRCS_DTE'


def count_validation(appl_name, tenant_id, tables, sf_cursor, ora_cursor, output_dir, apply_filter, appl_name2=None):
    apply_filter = (apply_filter or 'N').upper()
    run_date = datetime.now().strftime('%Y-%m-%d')
    ts = datetime.now().strftime('%Y%m%d')
    output_file = os.path.join(output_dir, f"{appl_name}_{tenant_id}_record_counts_{ts}.txt")
    drilldown_csv = os.path.join(output_dir, f"{appl_name}_{tenant_id}_drilldown_{ts}.csv")
    query_file  = os.path.join(output_dir, f"{appl_name}_{tenant_id}_queries_{ts}.txt")
    logging.info(f'Output file  : {output_file}')
    logging.info(f'Drilldown CSV: {drilldown_csv}')
    logging.info(f'Query file   : {query_file}')
    shard = get_shard_name(tenant_id, sf_cursor)
    def log_query(label, sql):
        with open(query_file, 'a', encoding='ascii', errors='replace') as qf:
            qf.write(f'[{label}] {sql}\n')

    results = []
    ddw_app = False
    if appl_name.upper().startswith('DDW_'):
        ddw_app = True
    for table in tables:
        logging.info(f'Validating {tenant_id}.{table}')
        try:
            sf_schema = appl_name
            sf_filter = ""
            orc_filter = ""
            sf_cur_table = table
            ora_table = table
            if any(x in table.upper() for x in ['DTM', 'PRCS_DTE_ARD', 'DAY_TME_ARD', 'PRCS_DTE_DIM', 'BRDG']):
                if table.upper().startswith('VW_'):
                    sf_cur_table = table
                    ora_table = 'TB_' + table[3:]
                else:
                    sf_cur_table = 'VW_' + table[3:]
                    ora_table = table
            if ddw_app and sf_cur_table.startswith('TB_C2') and 'DTM' not in table.upper():
                sf_schema = 'DDW_CNF_DIM'
                if apply_filter == 'Y':
                    sf_filter = f"AND SRC_APPL_NAME = '{appl_name}'"
                    tb_c2_filter = get_tb_c2_filter(table, appl_name, sf_cursor)
                    orc_filter = f"WHERE {tb_c2_filter}" if tb_c2_filter else ""
            sf_query = f"select count(*) from {sf_schema}.{sf_cur_table} where tenant_id='{tenant_id}' {sf_filter};"
            log_query('SF  ', sf_query)
            sf_result = sf_query_db(sql=sf_query, cursor=sf_cursor)
            if sf_result is not None:
                sf_table_count = sf_result[0][0]
            else:
                logging.warning(f'{tenant_id}.{table} does not exist in SF Curated layer ({sf_schema}).')
                sf_table_count = None

            if appl_name2 and sf_table_count is not None:
                sf_schema2 = 'DDW_CNF_DIM' if sf_schema == 'DDW_CNF_DIM' else appl_name2
                sf_filter2 = f"AND SRC_APPL_NAME = '{appl_name2}'" if sf_schema == 'DDW_CNF_DIM' and apply_filter == 'Y' else sf_filter
                sf_query2 = f"select count(*) from {sf_schema2}.{sf_cur_table} where tenant_id='{tenant_id}' {sf_filter2};"
                log_query('SF2 ', sf_query2)
                sf_result2 = sf_query_db(sql=sf_query2, cursor=sf_cursor)
                if sf_result2 is not None:
                    sf_table_count += sf_result2[0][0]
                else:
                    logging.warning(f'{tenant_id}.{table} does not exist in SF Curated layer ({sf_schema2}).')
                    sf_table_count = None

            ora_query = f"select count(*) from DW{tenant_id}.{ora_table} {orc_filter}"
            log_query('ORA ', ora_query)
            ora_result = ora_query_db(sql=ora_query, cursor=ora_cursor)
            if ora_result is not None:
                ora_table_count = ora_result[0][0]
            else:
                logging.warning(f'{tenant_id}.{table} does not exist in Oracle layer (DW{tenant_id}).')
                ora_table_count = None

            sf_shard_table_count = None
            mismatch_details = []
            mismatch_details_l2 = []
            fail_stage = None

            layer1_ok = (sf_table_count is not None and ora_table_count is not None and sf_table_count == ora_table_count)
            if not layer1_ok and sf_table_count is not None and ora_table_count is not None:
                logging.warning(f'{tenant_id}.{table} Layer 1 FAIL (Oracle vs SF Curated). Finding per-date mismatch...')
                mismatch_details = drilldown(appl_name, tenant_id, table, sf_cur_table, sf_cursor, ora_cursor, sf_filter, orc_filter, sf_schema, appl_name2, ora_table, query_file, sf_filter2 if appl_name2 else None)
            elif sf_table_count is None or ora_table_count is None:
                logging.warning(f'{tenant_id}.{table} Layer 1 FAIL - table not found in {"SF Curated" if sf_table_count is None else "Oracle"} layer.')

            shard_schema = sf_schema
            sf_shard_query = f"select count(*) from {shard}.{shard_schema}.{ora_table} where tenant_id='{tenant_id}' {sf_filter};"
            log_query('SHRD', sf_shard_query)
            sf_shard_result = sf_query_db(sql=sf_shard_query, cursor=sf_cursor)
            if sf_shard_result is not None:
                sf_shard_table_count = sf_shard_result[0][0]
            else:
                logging.warning(f'{tenant_id}.{ora_table} does not exist in shard layer ({shard}.{shard_schema}). Skipping Layer 2 comparison.')

            if appl_name2 and sf_shard_table_count is not None:
                shard_schema2 = 'DDW_CNF_DIM' if sf_schema == 'DDW_CNF_DIM' else appl_name2
                sf_shard_query2 = f"select count(*) from {shard}.{shard_schema2}.{ora_table} where tenant_id='{tenant_id}' {sf_filter2};"
                log_query('SHR2', sf_shard_query2)
                sf_shard_result2 = sf_query_db(sql=sf_shard_query2, cursor=sf_cursor)
                if sf_shard_result2 is not None:
                    sf_shard_table_count += sf_shard_result2[0][0]
                else:
                    logging.warning(f'{tenant_id}.{ora_table} does not exist in shard layer ({shard}.{shard_schema2}). Skipping Layer 2 comparison.')
                    sf_shard_table_count = None

            if sf_shard_table_count is not None:
                layer2_ok = (sf_shard_table_count == sf_table_count)
            else:
                layer2_ok = False  # Table not found in shard layer - mark as FAIL
            if not layer2_ok and sf_shard_table_count is not None:
                logging.warning(f'{tenant_id}.{ora_table} Layer 2 FAIL (SF Curated vs Shard). Finding per-date mismatch...')
                mismatch_details_l2 = shard_drilldown(appl_name, tenant_id, ora_table, sf_cur_table, sf_cursor, shard, sf_filter, sf_schema, appl_name2, query_file, sf_filter2 if appl_name2 else None)

            if not layer1_ok and not layer2_ok:
                status = 'FAIL'
                fail_stage = 'BOTH'
            elif not layer1_ok:
                status = 'FAIL'
                fail_stage = 'ORA_VS_SF'
            elif not layer2_ok:
                status = 'FAIL'
                fail_stage = 'SF_VS_SHARD'
            else:
                status = 'SUCCESS'

            results.append({
                'table': table,
                'ora_count': ora_table_count,
                'sf_count': sf_table_count,
                'sf_shard_count': sf_shard_table_count,
                'status': status,
                'fail_stage': fail_stage,
                'mismatch_details': mismatch_details,
                'mismatch_details_l2': mismatch_details_l2,
            })
        except Exception as e:
            logging.error(f'Error getting count for {table}: {e}')
            results.append({
                'table': table, 'ora_count': None, 'sf_count': None,
                'sf_shard_count': None, 'status': 'ERROR',
                'fail_stage': None, 'mismatch_details': [], 'mismatch_details_l2': [], 'error_msg': str(e),
            })

    fail_tables = write_formatted_report(output_file, appl_name, tenant_id, results, run_date, shard, appl_name2)
    if fail_tables:
        write_drilldown_csv(drilldown_csv, appl_name, tenant_id, fail_tables)
        logging.info(f'Drilldown CSV written to: {drilldown_csv}')
    else:
        logging.info('No failures -- drilldown CSV not generated.')
    logging.info(f'Report written to: {output_file}')

    return results, output_file, drilldown_csv

def get_snowflake_dtm_views(sf_conn, database, schema):
    query = f"""
    SELECT TABLE_NAME
    FROM {database}.INFORMATION_SCHEMA.VIEWS
    WHERE TABLE_SCHEMA = '{schema}'
    AND (TABLE_NAME LIKE '%DTM_DIM%'
    OR TABLE_NAME LIKE '%DTM_ARD%'
    OR TABLE_NAME LIKE '%PRCS_DTE_ARD%'
    OR TABLE_NAME LIKE '%DAY_TME_ARD%'
    OR TABLE_NAME LIKE '%PRCS_DTE_DIM%'
    OR TABLE_NAME LIKE '%BRDG%'
    )
    ORDER BY TABLE_NAME
    """
    cursor = sf_conn.cursor()
    cursor.execute(query)
    views = [row[0] for row in cursor.fetchall()]
    cursor.close()
    logging.info(f"Found {len(views)} DTM views in Snowflake schema {schema}")
    return views

def load_validation():
    script_start = time.perf_counter()
    argument_dict = arg_parsing()
    appl_name = argument_dict['appl_name']
    appl_name2 = argument_dict.get('appl_name2')
    tenant_id = argument_dict['tenant_id']
    py_path = os.environ["PYTHONPATH"]
    ingest_yaml_file = f'{py_path}/IngestionConfig.yaml'
    ingest_cfg_dict = load_yaml(yaml_file=ingest_yaml_file)

    sf_conn = open_sf_connection(ingest_cfg=ingest_cfg_dict)
    sf_cs = sf_conn.cursor()
    appl_code = get_appl_code(appl_name, sf_cs)
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    logging_config(argument_dict.get('logging_directory'), appl_code, script_name, ingest_cfg_dict)
    ora_user = f'DW{tenant_id}'
    ora_conn = open_oracle_connection(myuser=ora_user)
    oracle_cs = ora_conn.cursor()
    logging.info('Connections to Snowflake and Oracle established successfully.')
    tables = argument_dict['table_filter']
    if not tables:
        tables = get_tables_from_appl_table(cursor=sf_cs, appl_code=appl_code)
        dtm_tables = get_snowflake_dtm_views(sf_conn=sf_conn, database='CUR_IBS', schema=appl_name)
        tables += dtm_tables
    logging.info(f'Tables to be validated: {tables}')
    output_dir = f"{ingest_cfg_dict['snowflake_connection']['validation_path']}/{argument_dict['appl_name']}/{script_name}"
    os.makedirs(output_dir, exist_ok=True)
    results, output_file, drilldown_csv = count_validation(appl_name, tenant_id, tables, sf_cs, oracle_cs, output_dir, argument_dict.get('apply_filter', 'N') or 'N', appl_name2)

    # --- Framework metadata load ---
    if argument_dict.get('load_sf_meta') and sf_conn:
        try:
            sf_cursor = sf_conn.cursor()
            registry = TestCaseRegistry(sf_cursor, SCRIPT_NAME,
                                        database=argument_dict.get('sf_meta_db'),
                                        schema=argument_dict.get('sf_meta_schema'))

            # Build table_outcomes from results
            table_outcomes = []
            for r in results:
                if r['status'] == 'ERROR':
                    reason = f"Error: {r.get('error_msg', 'Unknown error')}"
                elif r['status'] == 'FAIL':
                    stage = r.get('fail_stage', '')
                    if stage == 'BOTH':
                        l1_diff = (r.get('sf_count') or 0) - (r.get('ora_count') or 0)
                        l2_diff = (r.get('sf_shard_count') or 0) - (r.get('sf_count') or 0)
                        reason = (f"Layer 1 mismatch: Oracle={r.get('ora_count')}, SF Curated={r.get('sf_count')}, Diff={l1_diff}; "
                                  f"Layer 2 mismatch: SF Curated={r.get('sf_count')}, Shard={r.get('sf_shard_count')}, Diff={l2_diff}")
                    elif stage == 'ORA_VS_SF':
                        if r.get('ora_count') is None:
                            reason = 'Layer 1 - table does not exist in Oracle layer'
                        elif r.get('sf_count') is None:
                            reason = 'Layer 1 - table does not exist in SF Curated layer'
                        else:
                            diff = r['sf_count'] - r['ora_count']
                            reason = f"Layer 1 mismatch: Oracle={r['ora_count']}, SF Curated={r['sf_count']}, Diff={diff}"
                    else:
                        if r.get('sf_shard_count') is None:
                            reason = 'Layer 2 - table does not exist in shard layer'
                        else:
                            diff = r['sf_shard_count'] - r['sf_count']
                            reason = f"Layer 2 mismatch: SF Curated={r['sf_count']}, Shard={r['sf_shard_count']}, Diff={diff}"
                else:
                    reason = 'All layer counts match'
                table_outcomes.append({'table': r['table'], 'status': r['status'], 'reason': reason})

            # Create validation results for framework
            val_results = []
            for outcome in table_outcomes:
                mis_c = 1 if outcome['status'] in ('FAIL', 'ERROR') else 0
                mat_c = 1 if outcome['status'] == 'SUCCESS' else 0
                val_results.append(registry.create_result(
                    validation_key='record_count',
                    test_scenario=f"Record count: {outcome['table']}",
                    appl_name=appl_name,
                    appl_code=appl_code,
                    tenant_id=tenant_id,
                    table_name=outcome['table'],
                    validation_status=outcome['status'],
                    status_reason=outcome['reason'],
                    mismatched_count=mis_c,
                    matched_count=mat_c,
                ))
            if not val_results:
                val_results.append(registry.create_result(
                    validation_key='record_count',
                    test_scenario='Record count validation',
                    appl_name=appl_name,
                    appl_code=appl_code,
                    tenant_id=tenant_id,
                    table_name='ALL_TABLES',
                    validation_status='SUCCESS',
                    status_reason='No tables processed',
                ))

            # Create loader and execution summary
            loader = ValidationLoader(
                sf_cursor=sf_cursor, arg_dict=argument_dict,
                script_name=SCRIPT_NAME, script_version=SCRIPT_VERSION,
                database=argument_dict.get('sf_meta_db'),
                schema=argument_dict.get('sf_meta_schema')
            )
            summary = ExecutionSummary(
                script_name=SCRIPT_NAME, appl_name=appl_name,
                appl_code=appl_code,
                tenant_id=tenant_id,
                process_date=argument_dict.get('process_date', ''),
                script_version=SCRIPT_VERSION
            )
            summary.started_at = datetime.fromtimestamp(time.time() - (time.perf_counter() - script_start))
            summary.parameters_used = {k: str(v) for k, v in argument_dict.items() if k not in ('sf_cursor',)}
            summary.update_counts(val_results)
            summary.execution_time_sec = time.perf_counter() - script_start

            # Attach ONLY the summary report to execution summary OUTPUT_CONTENT
            if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
                summary.read_and_store_output(output_file, file_type='count_validation_report')
            # Register drilldown CSV in output_files metadata (path/size) but NOT in output_content
            if os.path.exists(drilldown_csv) and os.path.getsize(drilldown_csv) > 0:
                summary.add_output_file(drilldown_csv, content=None, file_type='drilldown_csv')

            # Insert execution summary (header row)
            exec_id = loader.insert_execution_summary(summary)

            # Insert master rows (one per table)
            run_id_map = {}
            for r in val_results:
                rid = loader.insert_master(r, execution_id=exec_id)
                run_id_map[r.table_name] = rid

            # Insert detail rows for failures — summary + drilldown rows
            detail_batch = []
            for outcome in table_outcomes:
                if outcome['status'] in ('FAIL', 'ERROR'):
                    rid = run_id_map.get(outcome['table'], 0)
                    if not rid:
                        continue
                    # Find corresponding result to get count details
                    orig = next((r for r in results if r['table'] == outcome['table']), {})

                    # Summary-level detail row for this table
                    ora_c = orig.get('ora_count')
                    sf_c = orig.get('sf_count')
                    shard_c = orig.get('sf_shard_count')
                    l1_diff = (sf_c or 0) - (ora_c or 0) if ora_c is not None and sf_c is not None else None
                    l2_diff = (shard_c or 0) - (sf_c or 0) if shard_c is not None and sf_c is not None else None
                    source_data = {
                        'table': outcome['table'],
                        'validation': 'record_count',
                        'ora_count': ora_c,
                        'sf_count': sf_c,
                        'sf_shard_count': shard_c,
                        'fail_stage': orig.get('fail_stage'),
                        'layer1_diff': l1_diff,
                        'layer2_diff': l2_diff,
                    }
                    target_data = {'failure_reasons': outcome.get('reason', '')[:2000]}
                    detail_batch.append(ValidationDetailResult(
                        run_id=rid,
                        match_status='MISMATCH',
                        record_key=outcome['table'],
                        source_data=source_data,
                        target_data=target_data,
                        detail_remarks=(outcome.get('reason', '') or '')[:2000]
                    ))

                    # Drilldown detail rows — one per date-level mismatch
                    fail_stage = orig.get('fail_stage', '')
                    date_col = get_date_column(outcome['table'], appl_name)

                    # Layer 1 drilldown (Oracle vs SF Curated)
                    if fail_stage in ('ORA_VS_SF', 'BOTH'):
                        for m in orig.get('mismatch_details', []):
                            ora_cnt = m.get('ora_count', 0)
                            sf_cnt = m.get('sf_count', 0)
                            diff = sf_cnt - ora_cnt
                            detail_batch.append(ValidationDetailResult(
                                run_id=rid,
                                match_status='MISMATCH',
                                record_key=f"{outcome['table']}|L1|{m.get(date_col, '')}",
                                source_data={
                                    'layer': 'Layer1_Oracle_vs_SF_Curated',
                                    'date_column': date_col,
                                    'date_value': m.get(date_col, ''),
                                    'oracle_count': ora_cnt,
                                    'sf_curated_count': sf_cnt,
                                    'difference': diff,
                                },
                                target_data={
                                    'source_count': ora_cnt,
                                    'target_count': sf_cnt,
                                },
                                detail_remarks=f"Layer 1 mismatch on {date_col}={m.get(date_col, '')}: Oracle={ora_cnt}, SF Curated={sf_cnt}, Diff={diff}"
                            ))

                    # Layer 2 drilldown (SF Curated vs Shard)
                    if fail_stage in ('SF_VS_SHARD', 'BOTH'):
                        for m in orig.get('mismatch_details_l2', []):
                            sf_cnt = m.get('sf_count', 0)
                            shard_cnt = m.get('sf_shard_count', 0)
                            diff = shard_cnt - sf_cnt
                            detail_batch.append(ValidationDetailResult(
                                run_id=rid,
                                match_status='MISMATCH',
                                record_key=f"{outcome['table']}|L2|{m.get(date_col, '')}",
                                source_data={
                                    'layer': 'Layer2_SF_Curated_vs_Shard',
                                    'date_column': date_col,
                                    'date_value': m.get(date_col, ''),
                                    'sf_curated_count': sf_cnt,
                                    'shard_count': shard_cnt,
                                    'difference': diff,
                                },
                                target_data={
                                    'source_count': sf_cnt,
                                    'target_count': shard_cnt,
                                },
                                detail_remarks=f"Layer 2 mismatch on {date_col}={m.get(date_col, '')}: SF Curated={sf_cnt}, Shard={shard_cnt}, Diff={diff}"
                            ))

            if detail_batch:
                capped_batch, _, _ = cap_details(detail_batch)
                loader.insert_detail_bulk(capped_batch)

            summary.emit_summary_line()
            print(f"Loaded {len(val_results)} record count result(s) to VALIDATION_RUN_MASTER")
        except Exception as e:
            print(f"Failed to load validation results to Snowflake: {str(e)}")
            traceback.print_exc()
    else:
        print("Skipping metadata load to Snowflake (--load-sf-meta not specified)")


if __name__ == '__main__':
    load_validation()
