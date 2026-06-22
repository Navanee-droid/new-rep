# ============================================================================
# ANALYTICS VIEW VALIDATION
# Purpose: Validates that all tables in APP_IBS_SHARD_*.<APPL_NAME> have
#          corresponding views in the ANALYTICS schema and compares row counts.
# Converted from: CUR_IBS.APP_OPS.VALIDATE_ANALYTICS_VIEWS_PRC (SQL Procedure)
# ============================================================================

import logging
import os
import sys
import time
import traceback

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
    get_shard_count,
    get_shard_name,
    parse_args,
    get_appl_code,
    open_sf_connection,
    logging_config,
    load_yaml,
    get_tables_from_sharding_driver
)

SCRIPT_NAME = 'Analytics_View_Validation.py'
SCRIPT_VERSION = 'v3.0'


def arg_parsing() -> dict:
    return parse_args(
        required=['--a'],
        optional=['--t', '--p', '--l', '--o', '--tb', '--load-sf-meta', '--sf-meta-db', '--sf-meta-schema'],
        description='Validates ANALYTICS views exist for all tables in APP_IBS_SHARD_*.<APPL_NAME> and compares row counts.',
    )


def sf_query_db(sql, cursor):
    """Execute a Snowflake query and return results, or None on error."""
    try:
        cursor.execute(sql)
        return cursor.fetchall()
    except Exception:
        logging.error(f'The following Snowflake query has failed.')
        logging.error(sql)
        logging.error(traceback.format_exc())
        return None


def discover_shard_databases(cursor):
    """Discover all APP_IBS_SHARD_* databases."""
    cursor.execute("SHOW DATABASES LIKE '%IBS%SHARD%'")
    result = cursor.fetchall()
    # Column index 1 is 'name' in SHOW DATABASES output
    shard_dbs = [row[1] for row in result]
    logging.info(f'Discovered shard databases: {shard_dbs}')
    return shard_dbs


def get_tables_in_schema(shard_db, appl_name, cursor):
    """Get all base tables in <shard_db>.<appl_name> schema."""
    query = (
        f"SELECT TABLE_NAME FROM {shard_db}.INFORMATION_SCHEMA.TABLES "
        f"WHERE TABLE_SCHEMA = '{appl_name}' AND TABLE_TYPE = 'BASE TABLE' "
        f"ORDER BY TABLE_NAME"
    )
    result = sf_query_db(query, cursor)
    if result:
        return [row[0] for row in result]
    return []


def get_all_tables_matching(shard_db, table_names, cursor):
    """Find all occurrences of table_names across ALL schemas in the shard.
    Returns list of (schema, table_name) tuples."""
    if not table_names:
        return []
    # Build IN clause
    in_clause = ','.join([f"'{t}'" for t in table_names])
    query = (
        f"SELECT TABLE_SCHEMA, TABLE_NAME "
        f"FROM {shard_db}.INFORMATION_SCHEMA.TABLES "
        f"WHERE TABLE_TYPE = 'BASE TABLE' "
        f"AND TABLE_NAME IN ({in_clause}) "
        f"ORDER BY TABLE_SCHEMA, TABLE_NAME"
    )
    result = sf_query_db(query, cursor)
    if result:
        return [(row[0], row[1]) for row in result]
    return []


def get_analytics_views(shard_db, cursor):
    """Get all view names in the ANALYTICS schema of this shard."""
    query = (
        f"SELECT TABLE_NAME FROM {shard_db}.INFORMATION_SCHEMA.VIEWS "
        f"WHERE TABLE_SCHEMA = 'ANALYTICS' ORDER BY TABLE_NAME"
    )
    result = sf_query_db(query, cursor)
    if result:
        return set(row[0] for row in result)
    return set()


def get_row_count(shard_db, schema, table_name, cursor, tenant_ids=None, process_date=None, date_column='PRCS_DTE'):
    """Get row count for a specific table/view, optionally filtered by tenant(s) and process date."""
    where_clauses = []
    if tenant_ids:
        if len(tenant_ids) == 1:
            where_clauses.append(f"TENANT_ID = '{tenant_ids[0]}'")
        else:
            ids_str = ', '.join(f"'{t}'" for t in tenant_ids)
            where_clauses.append(f"TENANT_ID IN ({ids_str})")
    if process_date:
        where_clauses.append(f"{date_column} = TO_DATE('{process_date}', 'YYYYMMDD')")

    if where_clauses:
        where_str = ' WHERE ' + ' AND '.join(where_clauses)
    else:
        where_str = ''

    query = f"SELECT COUNT(*) FROM {shard_db}.{schema}.{table_name}{where_str}"
    result = sf_query_db(query, cursor)
    if result is not None and len(result) > 0:
        return result[0][0]
    return None


def aggregate_results(raw_results):
    """Aggregate per-schema results into per-table results.
    
    If a table exists in multiple schemas (applications), sum their counts
    and produce one aggregated entry per (shard_db, table_name).
    Also tracks which tables appear in multiple schemas for the detail section.
    """
    from collections import defaultdict

    # Group by (shard_db, table_name)
    grouped = defaultdict(list)
    for r in raw_results:
        key = (r['shard_db'], r['table_name'])
        grouped[key].append(r)

    aggregated = []
    multi_app_tables = []  # Tables present in multiple schemas

    for (shard_db, table_name), entries in grouped.items():
        # Sum table counts across all schemas
        total_tbl_count = 0
        schema_breakdown = []
        has_error = False

        for entry in entries:
            if entry.get('table_row_count') is not None:
                total_tbl_count += entry['table_row_count']
                schema_breakdown.append({
                    'schema': entry['table_schema'],
                    'count': entry['table_row_count'],
                })
            else:
                has_error = True
                schema_breakdown.append({
                    'schema': entry['table_schema'],
                    'count': None,
                })

        # View count is the same regardless of which schema entry we look at
        # (it's the same ANALYTICS view), take from first entry that has it
        view_count = None
        view_status = 'VIEW_NOT_FOUND'
        for entry in entries:
            if entry.get('view_row_count') is not None:
                view_count = entry['view_row_count']
                break
            if entry['status'] != 'FAIL' or not entry.get('reason', '').startswith('VIEW_NOT_FOUND'):
                view_count = entry.get('view_row_count')
                break

        # Determine status based on aggregated count vs view count
        if has_error:
            status = 'ERROR'
            reason = 'Unable to retrieve counts for some schemas'
        elif any(e.get('reason', '').startswith('VIEW_NOT_FOUND') for e in entries):
            status = 'FAIL'
            reason = 'VIEW_NOT_FOUND'
            view_count = None
        elif view_count is not None:
            if total_tbl_count == view_count:
                status = 'SUCCESS'
                reason = ''
            else:
                diff = abs(total_tbl_count - view_count)
                if total_tbl_count > view_count:
                    reason = f'TABLE_COUNT_GREATER_THAN_VIEW - DIFF: {diff:,}'
                else:
                    reason = f'VIEW_COUNT_GREATER_THAN_TABLE - DIFF: {diff:,}'
                status = 'FAIL'
        else:
            status = 'ERROR'
            reason = 'Unable to retrieve view count'

        # Schemas list (joined for display)
        schemas_list = sorted(set(e['table_schema'] for e in entries))
        schemas_display = ', '.join(schemas_list) if len(schemas_list) <= 3 else f'{schemas_list[0]} +{len(schemas_list)-1} more'

        aggregated.append({
            'shard_db': shard_db,
            'table_name': table_name,
            'schemas': schemas_list,
            'schemas_display': schemas_display,
            'table_row_count': total_tbl_count,
            'view_row_count': view_count,
            'status': status,
            'reason': reason,
            'multi_app': len(schemas_list) > 1,
        })

        # Track multi-application tables for detail section
        if len(schemas_list) > 1:
            multi_app_tables.append({
                'shard_db': shard_db,
                'table_name': table_name,
                'breakdown': schema_breakdown,
                'total_tbl_count': total_tbl_count,
                'view_count': view_count,
                'status': status,
            })

    return aggregated, multi_app_tables


def write_formatted_report(output_file, appl_name, raw_results, run_date, shard_dbs, tenant_ids=None, process_date=None):
    """Write a formatted text report.

    Layout:
      1. Header box
      2. Per-shard sections: results table + multi-app drilldown + shard summary
      3. Overall Validation Summary
    """
    from collections import defaultdict as _dd

    W = 140
    div = '-' * W
    dbl = '=' * W
    TBL_W, CNT_W, STS_W = 55, 14, 12

    # Aggregate results: one row per (shard_db, table_name)
    results, multi_app_tables = aggregate_results(raw_results)

    # Group by shard
    shard_results_map = _dd(list)
    shard_multi_map = _dd(list)
    for r in results:
        shard_results_map[r['shard_db']].append(r)
    for mt in multi_app_tables:
        shard_multi_map[mt['shard_db']].append(mt)

    lines = []

    # ── HEADER BOX ────────────────────────────────────────────────────────────
    if tenant_ids:
        tenant_display = ', '.join(tenant_ids)
        tenant_label = 'Tenant' if len(tenant_ids) == 1 else 'Tenants'
    else:
        tenant_display = 'ALL'
        tenant_label = 'Tenant'

    lines.append('+' + '=' * (W - 2) + '+')
    lines.append('|' + ' ANALYTICS VIEW VALIDATION REPORT'.ljust(W - 2) + '|')
    lines.append('+' + '=' * (W - 2) + '+')
    lines.append('|' + f' Application : {appl_name:<30} | Run Date : {run_date}'.ljust(W - 2) + '|')
    lines.append('|' + f' {tenant_label:<11} : {tenant_display:<30} | Process Date : {process_date or "ALL"}'.ljust(W - 2) + '|')
    lines.append('|' + f' Shards      : {", ".join(shard_dbs)}'.ljust(W - 2) + '|')
    lines.append('|' + ' Check       : Sum of table counts across all schemas == ANALYTICS view count'.ljust(W - 2) + '|')
    if tenant_ids or process_date:
        filter_desc = []
        if tenant_ids:
            if len(tenant_ids) == 1:
                filter_desc.append(f"TENANT_ID = '{tenant_ids[0]}'")
            else:
                ids_str = ', '.join(f"'{t}'" for t in tenant_ids)
                filter_desc.append(f"TENANT_ID IN ({ids_str})")
        if process_date:
            filter_desc.append(f"PRCS_DTE = '{process_date}'")
        lines.append('|' + f' Filter      : {" AND ".join(filter_desc)}'.ljust(W - 2) + '|')
    lines.append('+' + '=' * (W - 2) + '+')
    lines.append('')

    # ── PER-SHARD SECTIONS ────────────────────────────────────────────────────
    hdr = (f"{'Table':<{TBL_W}} | {'Tbl Count (Sum)':>{CNT_W}} | "
           f"{'View Count':>{CNT_W}} | {'Status':<{STS_W}} | Notes")
    sep = ('-' * TBL_W + '-+-' + '-' * CNT_W + '-+-' + '-' * CNT_W + '-+-' + '-' * STS_W + '-+------')

    overall_pass = overall_fail = overall_error = 0

    for shard_db in shard_dbs:
        shard_rs = shard_results_map.get(shard_db, [])
        shard_mt = shard_multi_map.get(shard_db, [])

        # ── Shard section header ──
        lines.append(dbl)
        lines.append(f' SHARD: {shard_db} -- Results'.center(W))
        lines.append(dbl)
        lines.append(hdr)
        lines.append(sep)

        # Sort: FAIL first, then ERROR, then SUCCESS
        fail_rs = [r for r in shard_rs if r['status'] == 'FAIL']
        error_rs = [r for r in shard_rs if r['status'] == 'ERROR']
        success_rs = [r for r in shard_rs if r['status'] == 'SUCCESS']
        sorted_rs = fail_rs + error_rs + success_rs

        pass_c = len(success_rs)
        fail_c = len(fail_rs)
        err_c = len(error_rs)

        for r in sorted_rs:
            tbl_fmt = f"{r['table_row_count']:,}" if r.get('table_row_count') is not None else 'N/A'
            view_fmt = f"{r['view_row_count']:,}" if r.get('view_row_count') is not None else 'N/A'

            if r.get('multi_app'):
                notes = f"Schemas: {r['schemas_display']} - see drilldown"
            elif r['status'] == 'FAIL' and r.get('reason', '').startswith('VIEW_NOT_FOUND'):
                notes = 'View does not exist in ANALYTICS schema'
            elif r['status'] == 'FAIL':
                notes = r.get('reason', '')
            else:
                notes = f"Schema: {r['schemas_display']}"

            lines.append(
                f"{r['table_name']:<{TBL_W}} | {tbl_fmt:>{CNT_W}} | "
                f"{view_fmt:>{CNT_W}} | {r['status']:<{STS_W}} | {notes}"
            )

        lines.append('')

        # ── Multi-application drilldown for this shard ──
        if shard_mt:
            DR_W = 58
            dr_hdr = (f"{'Table (Schema)':<{DR_W}} | {'Tbl Count':>{CNT_W}} | "
                      f"{'View Count':>{CNT_W}} | {'Result':<{STS_W}}")
            dr_sep = ('-' * DR_W + '-+-' + '-' * CNT_W + '-+-' + '-' * CNT_W + '-+-' + '-' * STS_W)

            lines.append(dbl)
            lines.append(f'MULTI-APPLICATION DRILLDOWN -- {shard_db}'.center(W))
            lines.append(dbl)
            lines.append(dr_hdr)
            lines.append(dr_sep)

            _drilldown_order = {'FAIL': 0, 'ERROR': 1, 'SUCCESS': 2}
            for mt in sorted(shard_mt, key=lambda x: (_drilldown_order.get(x.get('status', 'SUCCESS'), 2), x['table_name'])):
                table_name = mt['table_name']
                total_tbl = mt['total_tbl_count']
                view_count = mt['view_count']

                for bd in sorted(mt['breakdown'], key=lambda x: x['schema']):
                    cnt_fmt = f"{bd['count']:,}" if bd['count'] is not None else 'N/A'
                    schema_label = f"  {table_name} ({bd['schema']})"
                    lines.append(
                        f"{schema_label:<{DR_W}} | {cnt_fmt:>{CNT_W}} | "
                        f"{'':>{CNT_W}} | {'':<{STS_W}}"
                    )

                total_fmt = f"{total_tbl:,}"
                view_fmt_d = f"{view_count:,}" if view_count is not None else 'N/A'
                result_d = 'SUCCESS' if (view_count is not None and total_tbl == view_count) else 'FAIL'
                sum_label = f"  {table_name} (SUM)"
                lines.append(
                    f"{sum_label:<{DR_W}} | {total_fmt:>{CNT_W}} | "
                    f"{view_fmt_d:>{CNT_W}} | {result_d:<{STS_W}}"
                )
                lines.append(dr_sep)

            lines.append('')

        # ── Per-shard summary line ──
        lines.append(div)
        lines.append(
            f'  Shard Summary [{shard_db}]  :  '
            f'Total={len(shard_rs)}  |  SUCCESS={pass_c}  |  FAIL={fail_c}  |  ERROR={err_c}'
        )
        lines.append(div)
        lines.append('')

        overall_pass += pass_c
        overall_fail += fail_c
        overall_error += err_c

    # ── OVERALL VALIDATION SUMMARY ────────────────────────────────────────────
    total_tables = overall_pass + overall_fail + overall_error
    all_fail_results = [r for r in results if r['status'] == 'FAIL']
    lines.append(dbl)
    lines.append('OVERALL VALIDATION SUMMARY'.center(W))
    lines.append(dbl)
    lines.append(f'  Total Tables    : {total_tables}')
    lines.append(f'  Multi-App       : {len(multi_app_tables)}')
    lines.append(f'  SUCCESS            : {overall_pass}')
    lines.append(f'  FAIL            : {overall_fail}')
    lines.append(f'  ERROR           : {overall_error}')
    lines.append('')

    if overall_fail > 0 or overall_error > 0:
        view_not_found = sum(1 for r in all_fail_results if r.get('reason', '').startswith('VIEW_NOT_FOUND'))
        count_mismatch = overall_fail - view_not_found
        lines.append(f'  VIEW_NOT_FOUND  : {view_not_found}')
        lines.append(f'  COUNT_MISMATCH  : {count_mismatch}')
        lines.append('')
        lines.append('*** SOME VALIDATIONS FAILED - review drilldown details above ***'.center(W))
    else:
        lines.append('*** ALL VALIDATIONS PASSED ***'.center(W))
    lines.append(dbl)

    with open(output_file, 'w', encoding='ascii', errors='replace') as f:
        f.write('\n'.join(lines) + '\n')

    return results


def _get_date_column(table_name, appl_name):
    """Return the appropriate date filter column for the given table.

    For DDW applications, SCD, RCD, and RPD tables filter on EFF_DTE rather than PRCS_DTE.
    """
    if appl_name.upper().startswith('DDW'):
        tn_upper = table_name.upper()
        if 'SCD' in tn_upper or 'RCD' in tn_upper or 'RPD' in tn_upper or 'USCD' in tn_upper:
            return 'EFF_DTE'
        if 'DTM_DIM' in tn_upper:
            return 'FULL_DTE'
    return 'PRCS_DTE'


def validate_analytics_views(appl_name, sf_cursor, output_dir, table_filter=None, tenant_ids=None, process_date=None):
    """Main validation logic.

    tenant_ids: list of tenant IDs to filter on, or empty list / None for all tenants.
    If process_date is provided, row counts are filtered by the appropriate date column.
    """
    run_date = datetime.now().strftime('%Y-%m-%d')
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = os.path.join(output_dir, f"{appl_name}_analytics_view_validation_{ts}.txt")
    logging.info(f'Output file: {output_file}')

    if tenant_ids:
        logging.info(f'Filtering by Tenant IDs: {", ".join(tenant_ids)}')
    if process_date:
        logging.info(f'Filtering by Process Date: {process_date}')

    # Step 1: Discover shard databases
    # If tenant_ids are provided, resolve each to its shard (duplicates removed).
    # Otherwise, scan all shards.
    if tenant_ids:
        shard_dbs = []
        for tid in tenant_ids:
            try:
                shard_db = get_shard_name(tid, sf_cursor)
                if shard_db not in shard_dbs:
                    shard_dbs.append(shard_db)
                logging.info(f'Resolved tenant {tid} to shard: {shard_db}')
            except ValueError as e:
                logging.error(f'Failed to resolve shard for tenant {tid}: {e}')
        if not shard_dbs:
            logging.error('No shards could be resolved for the provided tenant IDs')
            return [], output_file
    else:
        shard_dbs = discover_shard_databases(sf_cursor)
        if not shard_dbs:
            logging.error('No shard databases found matching pattern %IBS%SHARD%')
            return [], output_file

    results = []

    appl_tables = get_tables_from_sharding_driver(appl_name, sf_cursor)
    tb_c2_tables = [t for t in appl_tables if appl_name.upper().startswith('DDW') and t.upper().startswith('TB_C2') and 'DTM' not in t.upper()]

    if table_filter:
        filter_set = set(t.strip().upper() for t in table_filter.split(','))
        appl_tables = [t for t in appl_tables if t.upper() in filter_set]
        tb_c2_tables = [t for t in tb_c2_tables if t.upper() in filter_set]
        logging.info(f'After filter: {len(appl_tables)} tables')

    for shard_db in shard_dbs:
        logging.info(f'Processing shard: {shard_db}')

        # Step 3: Find same table names across ALL schemas in this shard
        # Exclude TB_C2 tables (DDW) — they are queried from DDW_CNF_DIM schema in a dedicated loop below
        tb_c2_set = set(t.upper() for t in tb_c2_tables)
        main_tables = [t for t in appl_tables if t.upper() not in tb_c2_set]
        all_table_locations = get_all_tables_matching(shard_db, main_tables, sf_cursor)
        logging.info(f'Found {len(all_table_locations)} table instances across all schemas in {shard_db}')

        # Step 4: Get ANALYTICS views for this shard
        analytics_views = get_analytics_views(shard_db, sf_cursor)
        logging.info(f'Found {len(analytics_views)} views in {shard_db}.ANALYTICS')

        # Step 5: For each table instance, get its row count.
        # Also get view count once per unique table_name (not per schema).
        # Aggregation into one row per table happens in write_formatted_report().
        view_count_cache = {}  # cache view counts per table_name in this shard

        for table_schema, table_name in all_table_locations:
            try:
                # Get table row count for this specific schema (filtered if tenant/date provided)
                date_col = _get_date_column(table_name, appl_name)
                table_count = get_row_count(shard_db, table_schema, table_name, sf_cursor,
                                            tenant_ids=tenant_ids, process_date=process_date,
                                            date_column=date_col)

                # Get ANALYTICS view count (cached per table_name, same filter applied)
                if table_name not in view_count_cache:
                    if table_name in analytics_views:
                        view_count_cache[table_name] = get_row_count(
                            shard_db, 'ANALYTICS', table_name, sf_cursor,
                            tenant_ids=tenant_ids, process_date=process_date,
                            date_column=date_col)
                    else:
                        view_count_cache[table_name] = None  # VIEW_NOT_FOUND

                view_count = view_count_cache[table_name]

                if table_name not in analytics_views:
                    status = 'FAIL'
                    reason = 'VIEW_NOT_FOUND'
                else:
                    # Individual status — final comparison happens after aggregation
                    status = 'PENDING_AGG'
                    reason = ''

                results.append({
                    'shard_db': shard_db,
                    'table_schema': table_schema,
                    'table_name': table_name,
                    'table_row_count': table_count,
                    'view_row_count': view_count,
                    'status': status,
                    'reason': reason,
                })

            except Exception as e:
                logging.error(f'Error validating {shard_db}.{table_schema}.{table_name}: {e}')
                results.append({
                    'shard_db': shard_db,
                    'table_schema': table_schema,
                    'table_name': table_name,
                    'table_row_count': None,
                    'view_row_count': None,
                    'status': 'ERROR',
                    'reason': str(e),
                })

        for table_name in tb_c2_tables:
            try:
                # Get table row count for this specific schema (filtered if tenant/date provided)
                date_col = _get_date_column(table_name, appl_name)
                table_count = get_row_count(shard_db, 'DDW_CNF_DIM', table_name, sf_cursor,
                                            tenant_ids=tenant_ids, process_date=process_date,
                                            date_column=date_col)

                # Get ANALYTICS view count (cached per table_name, same filter applied)
                if table_name not in view_count_cache:
                    if table_name in analytics_views:
                        view_count_cache[table_name] = get_row_count(
                            shard_db, 'ANALYTICS', table_name, sf_cursor,
                            tenant_ids=tenant_ids, process_date=process_date,
                            date_column=date_col)
                    else:
                        view_count_cache[table_name] = None  # VIEW_NOT_FOUND

                view_count = view_count_cache[table_name]

                if table_name not in analytics_views:
                    status = 'FAIL'
                    reason = 'VIEW_NOT_FOUND'
                else:
                    # Individual status — final comparison happens after aggregation
                    status = 'PENDING_AGG'
                    reason = ''

                results.append({
                    'shard_db': shard_db,
                    'table_schema': 'DDW_CNF_DIM',
                    'table_name': table_name,
                    'table_row_count': table_count,
                    'view_row_count': view_count,
                    'status': status,
                    'reason': reason,
                })

            except Exception as e:
                logging.error(f'Error validating {shard_db}.DDW_CNF_DIM.{table_name}: {e}')
                results.append({
                    'shard_db': shard_db,
                    'table_schema': 'DDW_CNF_DIM',
                    'table_name': table_name,
                    'table_row_count': None,
                    'view_row_count': None,
                    'status': 'ERROR',
                    'reason': str(e),
                })


    # Write report
    fail_tables = write_formatted_report(output_file, appl_name, results, run_date, shard_dbs,
                                         tenant_ids=tenant_ids, process_date=process_date)
    logging.info(f'Report written to: {output_file}')

    return results, output_file


def load_validation():
    """Main entry point — follows the same pattern as other validation scripts."""
    script_start = time.perf_counter()
    argument_dict = arg_parsing()
    appl_name = argument_dict['appl_name']

    py_path = os.environ["PYTHONPATH"]
    ingest_yaml_file = f'{py_path}/IngestionConfig.yaml'
    ingest_cfg_dict = load_yaml(yaml_file=ingest_yaml_file)

    sf_conn = open_sf_connection(ingest_cfg=ingest_cfg_dict)
    sf_cs = sf_conn.cursor()

    appl_code = get_appl_code(appl_name, sf_cs)
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    logging_config(argument_dict.get('logging_directory'), appl_code, script_name, ingest_cfg_dict)

    logging.info(f'Starting Analytics View Validation for: {appl_name}')
    logging.info(f'Snowflake connection established.')

    # Determine output directory
    output_dir = f"{ingest_cfg_dict['snowflake_connection']['validation_path']}/{appl_name}/{script_name}"
    os.makedirs(output_dir, exist_ok=True)

    # Run validation
    table_filter = argument_dict.get('table_filter') or None
    tenant_id_raw = argument_dict.get('tenant_id') or None
    tenant_ids = [t.strip() for t in tenant_id_raw.split(',')] if tenant_id_raw else []
    process_date = argument_dict.get('process_date') or None
    results, output_file = validate_analytics_views(appl_name, sf_cs, output_dir, table_filter,
                                                    tenant_ids=tenant_ids, process_date=process_date)

    # --- Framework metadata load ---
    if argument_dict.get('load_sf_meta') and sf_conn:
        try:
            sf_cursor = sf_conn.cursor()
            registry = TestCaseRegistry(sf_cursor, SCRIPT_NAME,
                                        database=argument_dict.get('sf_meta_db'),
                                        schema=argument_dict.get('sf_meta_schema'))

            val_results = []
            for r in results:
                mis_c = 1 if r['status'] in ('FAIL', 'ERROR') else 0
                mat_c = 1 if r['status'] == 'SUCCESS' else 0
                val_results.append(registry.create_result(
                    validation_key='analytics_view',
                    test_scenario=f"Analytics view: {r['shard_db']}.{r['table_schema']}.{r['table_name']}",
                    appl_name=appl_name,
                    appl_code=appl_code,
                    tenant_id=', '.join(tenant_ids) if tenant_ids else 'ALL',
                    table_name=r['table_name'],
                    validation_status=r['status'] if r['status'] != 'SUCCESS' else 'SUCCESS',
                    status_reason=r.get('reason', 'View exists and row counts match'),
                    mismatched_count=mis_c,
                    matched_count=mat_c,
                ))

            if not val_results:
                val_results.append(registry.create_result(
                    validation_key='analytics_view',
                    test_scenario='Analytics view validation',
                    appl_name=appl_name,
                    appl_code=appl_code,
                    tenant_id='ALL',
                    table_name='ALL_TABLES',
                    validation_status='SUCCESS',
                    status_reason='No tables processed',
                ))

            loader = ValidationLoader(
                sf_cursor=sf_cursor, arg_dict=argument_dict,
                script_name=SCRIPT_NAME, script_version=SCRIPT_VERSION,
                database=argument_dict.get('sf_meta_db'),
                schema=argument_dict.get('sf_meta_schema')
            )
            summary = ExecutionSummary(
                script_name=SCRIPT_NAME, appl_name=appl_name,
                appl_code=appl_code,
                tenant_id=', '.join(tenant_ids) if tenant_ids else 'ALL',
                process_date=process_date if process_date else datetime.now().strftime('%Y%m%d'),
                script_version=SCRIPT_VERSION
            )
            summary.started_at = datetime.fromtimestamp(time.time() - (time.perf_counter() - script_start))
            summary.parameters_used = {k: str(v) for k, v in argument_dict.items() if k not in ('sf_cursor',)}
            summary.update_counts(val_results)
            summary.execution_time_sec = time.perf_counter() - script_start

            if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
                summary.read_and_store_output(output_file, file_type='analytics_view_report')

            exec_id = loader.insert_execution_summary(summary)

            run_id_map = {}
            for r in val_results:
                rid = loader.insert_master(r, execution_id=exec_id)
                run_id_map[r.table_name] = rid

            # Insert detail rows for failures
            detail_batch = []
            for r in results:
                if r['status'] in ('FAIL', 'ERROR'):
                    rid = run_id_map.get(r['table_name'], 0)
                    if not rid:
                        continue
                    source_data = {
                        'shard_db': r['shard_db'],
                        'table_schema': r['table_schema'],
                        'table_name': r['table_name'],
                        'table_row_count': r.get('table_row_count'),
                        'view_row_count': r.get('view_row_count'),
                    }
                    target_data = {'reason': r.get('reason', '')}
                    detail_batch.append(ValidationDetailResult(
                        run_id=rid,
                        match_status='MISMATCH',
                        record_key=f"{r['shard_db']}.{r['table_schema']}.{r['table_name']}",
                        source_data=source_data,
                        target_data=target_data,
                        detail_remarks=r.get('reason', '')[:2000]
                    ))

            if detail_batch:
                capped_batch, _, _ = cap_details(detail_batch)
                loader.insert_detail_bulk(capped_batch)

            summary.emit_summary_line()
            print(f"Loaded {len(val_results)} analytics view result(s) to VALIDATION_RUN_MASTER")
        except Exception as e:
            print(f"Failed to load validation results to Snowflake: {str(e)}")
            traceback.print_exc()
    else:
        print("Skipping metadata load to Snowflake (--load-sf-meta not specified)")

    sf_conn.close()
    logging.info('Done.')


if __name__ == '__main__':
    load_validation()
