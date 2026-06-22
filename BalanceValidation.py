# This Script compares the balance count between oracle & snowflake for all the tables
# Input Python BalanceValidation --a appl_code --p prcs_dte --t tenant 
# Created: 17/11/2025 Developer: Krishnan Ravisankar

import snowflake.connector
import oracledb
import yaml
import os
import toml
import time
import subprocess
import traceback
import sys
from datetime import datetime
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from validation_utils import (
    ValidationLoader,
    ValidationResult,
    ValidationDetailResult,
    TestCaseRegistry,
    ExecutionSummary,
    cap_details
)

from script_utils import (
    parse_args,
    open_sf_connection,
    open_oracle_connection,
    get_tb_c2_filter,
    get_appl_code,
    load_yaml
)

SCRIPT_NAME = "BalanceValidation.py"
SCRIPT_VERSION = "v2.1"

def arg_parsing() -> dict:
    args = parse_args(
        required=['--a', '--t', '--p'],
        optional=['--load-sf-meta', '--sf-meta-db', '--sf-meta-schema'],
        description='Balance validation between Oracle and Snowflake.',
    )
    args['prcs_dte'] = args['process_date']
    args['tenant'] = args['tenant_id']
    return args


def validate_balance(table_columns_dict, cs, ocs, appl_nme, prcs_dte, tenant):
    """
    Validates balance columns by comparing sum values between Snowflake and Oracle
    """
    print("\n" + "="*80)
    print("Starting Balance Validation...")
    print("="*80)
    
    validation_results = []
    
    for table_name, columns in table_columns_dict.items():
        try:
            date_column = 'EFF_DTE' if 'SCD' in table_name or 'RCD' in table_name else 'PRCS_DTE'
            # Batch all columns into a single SUM query per table
            col_sums = ', '.join(f'SUM({c}) AS {c}' for c in columns)

            if 'TB_C2_' in table_name:
                sf_sql = (f"SELECT {col_sums} FROM CUR_IBS.DDW_CNF_DIM.{table_name} "
                          f"WHERE {date_column} = TO_DATE('{prcs_dte}', 'YYYYMMDD') "
                          f"AND TENANT_ID = '{tenant}' AND SRC_APPL_NAME = '{appl_nme}'")
            else:
                sf_sql = (f"SELECT {col_sums} FROM CUR_IBS.{appl_nme}.{table_name} "
                          f"WHERE {date_column} = TO_DATE('{prcs_dte}', 'YYYYMMDD') "
                          f"AND TENANT_ID = '{tenant}'")
            cs.execute(sf_sql)
            sf_row = cs.fetchone()

            ora_c2_filter = ''
            if 'TB_C2_' in table_name:
                _filter = get_tb_c2_filter(table_name, appl_nme, cs)
                if _filter:
                    ora_c2_filter = f" AND {_filter}"
            ora_sql = (f"SELECT {col_sums} FROM DW{tenant.upper()}.{table_name} "
                       f"WHERE {date_column} = TO_DATE('{prcs_dte}', 'YYYYMMDD'){ora_c2_filter}")
            ocs.execute(ora_sql)
            ora_row = ocs.fetchone()

            for i, column_name in enumerate(columns):
                sf_sum = float(sf_row[i]) if sf_row and sf_row[i] is not None else 0.0
                ora_sum = float(ora_row[i]) if ora_row and ora_row[i] is not None else 0.0
                difference = sf_sum - ora_sum
                match = abs(difference) < 0.01
                validation_results.append({
                    'table': table_name,
                    'column': column_name,
                    'snowflake_sum': sf_sum,
                    'oracle_sum': ora_sum,
                    'difference': difference,
                    'match': match
                })
        except Exception as e:
            for column_name in columns:
                validation_results.append({
                    'table': table_name,
                    'column': column_name,
                    'error': str(e)
                })
    
    return validation_results


def validate_balance_new(table_columns_dict, cs, ocs, appl_nme, prcs_dte, tenant):
    """
    Validates balance columns for TB_C2 tables by category breakdown (DATA_SRC_CDE, LOG_DATA_SRC_CDE, APPL_CDE)
    Uses GROUP BY to fetch all category values and sums in a single query per category column.
    """
    print("\n" + "="*80)
    print("Starting C2 Balance Category Validation...")
    print("="*80)

    validation_results_new = []
    c2_tables = {table: columns for table, columns in table_columns_dict.items() if table.startswith('TB_C2')}

    print(f"Found {len(c2_tables)} C2 tables: {list(c2_tables.keys())}")

    if not c2_tables:
        print("No C2 tables found in table_columns_dict")
        return validation_results_new

    category_columns = ['DATA_SRC_CDE', 'LOG_DATA_SRC_CDE', 'APPL_CDE']

    for table_name, columns in c2_tables.items():
        try:
            # Single query to find which category columns exist in this table
            cat_in = ", ".join(f"'{c}'" for c in category_columns)
            check_col_sql = f"""
            SELECT COLUMN_NAME FROM CUR_IBS.information_schema.columns
            WHERE table_schema='DDW_CNF_DIM' AND table_catalog='CUR_IBS'
            AND table_name='{table_name}' AND COLUMN_NAME IN ({cat_in})
            """
            cs.execute(check_col_sql)
            found = {row[0] for row in cs.fetchall()}
            existing_category_cols = [c for c in category_columns if c in found]

            print(f"Table {table_name}: Found category columns: {existing_category_cols}")

            if not existing_category_cols:
                print(f"Skipping {table_name} - no category columns found")
                continue

            date_column = 'EFF_DTE' if 'SCD' in table_name or 'RCD' in table_name else 'PRCS_DTE'
            col_sums = ', '.join(f'SUM({c}) AS {c}' for c in columns)

            for category_col in existing_category_cols:
                # One grouped query fetches all category values and sums at once
                sf_sql = f"""
                SELECT {category_col}, {col_sums}
                FROM CUR_IBS.DDW_CNF_DIM.{table_name}
                WHERE {date_column} = TO_DATE('{prcs_dte}', 'YYYYMMDD')
                AND TENANT_ID = '{tenant}' AND SRC_APPL_NAME = '{appl_nme}'
                GROUP BY {category_col}
                """
                ora_sql = f"""
                SELECT {category_col}, {col_sums}
                FROM DW{tenant.upper()}1.{table_name}
                WHERE {date_column} = TO_DATE('{prcs_dte}', 'YYYYMMDD')
                GROUP BY {category_col}
                """
                cs.execute(sf_sql)
                sf_rows = cs.fetchall()
                ocs.execute(ora_sql)
                ora_rows = ocs.fetchall()

                # Build lookup: category_value -> tuple of sums
                sf_map = {(str(row[0]) if row[0] is not None else None): row[1:] for row in sf_rows}
                ora_map = {(str(row[0]) if row[0] is not None else None): row[1:] for row in ora_rows}

                print(f"Table {table_name}, Category {category_col}: "
                      f"SF={len(sf_map)} values, Oracle={len(ora_map)} values")

                for category_value in set(sf_map) | set(ora_map):
                    sf_vals = sf_map.get(category_value)
                    ora_vals = ora_map.get(category_value)
                    display_value = category_value if category_value is not None else 'NULL'
                    for i, column_name in enumerate(columns):
                        try:
                            sf_sum = float(sf_vals[i]) if sf_vals and sf_vals[i] is not None else 0.0
                            ora_sum = float(ora_vals[i]) if ora_vals and ora_vals[i] is not None else 0.0
                            difference = sf_sum - ora_sum
                            match = abs(difference) < 0.01
                            validation_results_new.append({
                                'table': table_name,
                                'column': column_name,
                                'category_col': category_col,
                                'category_value': display_value,
                                'snowflake_sum': sf_sum,
                                'oracle_sum': ora_sum,
                                'difference': difference,
                                'match': match
                            })
                        except Exception as e:
                            validation_results_new.append({
                                'table': table_name,
                                'column': column_name,
                                'category_col': category_col,
                                'category_value': display_value,
                                'error': str(e)
                            })
        except Exception as e:
            validation_results_new.append({
                'table': table_name,
                'error': str(e)
            })

    return validation_results_new


def print_validation_summary(validation_results, appl_nme, tenant_id, process_date, validation_results_new=None, output_dir=None):
    """
    Prints a summary of validation results grouped by table, with a final SUCCESS/fail summary.
    """
    from collections import OrderedDict

    output_filename = f"{output_dir}/Summary_BalanceValidation_{appl_nme}_{tenant_id}_{process_date}.txt"
    summary_lines = []
    summary_lines.append("=" * 150)
    summary_lines.append(f"*** BALANCE COUNT VALIDATION REPORT ***")
    summary_lines.append(f"*** APPLICATION: {appl_nme} ***")
    summary_lines.append(f"*** TENANT ID: {tenant_id} ***")
    summary_lines.append(f"*** PROCESS DATE: {process_date} ***")
    summary_lines.append("=" * 150)

    def fmt_num(val):
        return f"{val:,.2f}"

    def build_rows(results, is_c2=False):
        """Convert result dicts to formatted string tuples."""
        rows = []
        for r in results:
            if 'error' in r:
                err = r['error'].replace('\n', ' ').replace('  ', ' ')
                if is_c2:
                    rows.append((r.get('column', 'N/A'), r.get('category_col', 'N/A'),
                                 r.get('category_value', 'N/A'), 'ERROR', 'ERROR', 'ERROR', err))
                else:
                    rows.append((r['column'], 'ERROR', 'ERROR', 'ERROR', err))
            else:
                status = 'SUCCESS' if r['match'] else 'FAIL'
                if is_c2:
                    rows.append((r['column'], r['category_col'], r['category_value'],
                                 fmt_num(r['snowflake_sum']), fmt_num(r['oracle_sum']),
                                 fmt_num(r['difference']), status))
                else:
                    rows.append((r['column'], fmt_num(r['snowflake_sum']),
                                 fmt_num(r['oracle_sum']), fmt_num(r['difference']), status))
        return rows

    def compute_widths(all_rows, headers):
        """Compute global column widths across all rows from all tables."""
        widths = [len(h) for h in headers]
        for row in all_rows:
            for i, cell in enumerate(row):
                widths[i] = max(widths[i], len(str(cell)))
        return [w + 2 for w in widths]

    def render_rows(rows, widths, headers, is_c2=False):
        """Render rows with pre-computed global widths."""
        right_cols = (3, 4, 5) if is_c2 else (1, 2, 3)

        def fmt_row(row):
            parts = []
            for i, cell in enumerate(row):
                if i in right_cols:
                    parts.append(str(cell).rjust(widths[i]))
                else:
                    parts.append(str(cell).ljust(widths[i]))
            return "  " + " ".join(parts).rstrip()

        sep = "  " + "-" * (sum(widths) + len(widths) - 1)
        lines = [sep, fmt_row(headers), sep]
        for row in rows:
            lines.append(fmt_row(row))
        lines.append("")
        return lines

    STD_HEADERS = ('Column Name', 'Snowflake Sum', 'Oracle Sum', 'Difference', 'Status')
    C2_HEADERS  = ('Column Name', 'Category', 'Category Value',
                   'Snowflake Sum', 'Oracle Sum', 'Difference', 'Status')

    # ── Group standard results by table ──────────────────────────────────────
    by_table = OrderedDict()
    for r in validation_results:
        tbl = r['table']
        if tbl not in by_table:
            by_table[tbl] = []
        by_table[tbl].append(r)

    # Build all rows first, compute global widths, then render
    all_std_rows_by_table = {tbl: build_rows(res, is_c2=False) for tbl, res in by_table.items()}
    all_std_rows_flat = [row for rows in all_std_rows_by_table.values() for row in rows]
    std_widths = compute_widths(all_std_rows_flat, STD_HEADERS)

    table_status = {}

    summary_lines.append("")
    summary_lines.append("=" * 150)
    summary_lines.append("*** DETAILED RESULTS BY TABLE ***")
    summary_lines.append("=" * 150)

    for table_name, results in by_table.items():
        has_mismatch = any(r.get('match') == False for r in results)
        has_error    = any('error' in r for r in results)
        tbl_status   = 'ERROR' if has_error else ('FAIL' if has_mismatch else 'SUCCESS')
        table_status[table_name] = tbl_status

        summary_lines.append("")
        summary_lines.append(f"  TABLE: {table_name}  [{tbl_status}]")
        summary_lines.extend(render_rows(all_std_rows_by_table[table_name], std_widths, STD_HEADERS, is_c2=False))

    # ── C2 Category breakdown grouped by table ────────────────────────────────
    if validation_results_new:
        c2_by_table = OrderedDict()
        for r in validation_results_new:
            tbl = r['table']
            if tbl not in c2_by_table:
                c2_by_table[tbl] = []
            c2_by_table[tbl].append(r)

        summary_lines.append("=" * 150)
        summary_lines.append("*** C2 CATEGORY BREAKDOWN BY TABLE ***")
        summary_lines.append("=" * 150)

        # Build all C2 rows first, compute global C2 widths, then render
        all_c2_rows_by_table = {tbl: build_rows(res, is_c2=True) for tbl, res in c2_by_table.items()}
        all_c2_rows_flat = [row for rows in all_c2_rows_by_table.values() for row in rows]
        c2_widths = compute_widths(all_c2_rows_flat, C2_HEADERS)

        for table_name, results in c2_by_table.items():
            has_mismatch = any(r.get('match') == False for r in results)
            has_error    = any('error' in r for r in results)
            tbl_status   = 'ERROR' if has_error else ('FAIL' if has_mismatch else 'SUCCESS')
            prev = table_status.get(table_name, 'SUCCESS')
            if tbl_status == 'ERROR' or prev == 'ERROR':
                table_status[table_name] = 'ERROR'
            elif tbl_status == 'FAIL' or prev == 'FAIL':
                table_status[table_name] = 'FAIL'
            else:
                table_status[table_name] = 'SUCCESS'

            summary_lines.append("")
            summary_lines.append(f"  TABLE: {table_name}  [{tbl_status}]")
            summary_lines.extend(render_rows(all_c2_rows_by_table[table_name], c2_widths, C2_HEADERS, is_c2=True))

    # ── Final table-level summary ─────────────────────────────────────────────
    passed_tables = [t for t, s in table_status.items() if s == 'SUCCESS']
    failed_tables = [t for t, s in table_status.items() if s == 'FAIL']
    error_tables  = [t for t, s in table_status.items() if s == 'ERROR']

    summary_lines.append("=" * 150)
    summary_lines.append("*** TABLE-LEVEL SUMMARY ***")
    summary_lines.append("=" * 150)
    summary_lines.append(f"  Total Tables  : {len(table_status)}")
    summary_lines.append(f"  Passed        : {len(passed_tables)}")
    summary_lines.append(f"  Failed        : {len(failed_tables)}")
    summary_lines.append(f"  Errors        : {len(error_tables)}")
    summary_lines.append("")

    if passed_tables:
        summary_lines.append("  PASSED TABLES:")
        for t in passed_tables:
            summary_lines.append(f"    [SUCCESS] {t}")
        summary_lines.append("")

    if failed_tables:
        summary_lines.append("  FAILED TABLES:")
        summary_lines.append("  NOTE: For column mismatches run field level validation to identify specific records.")
        for t in failed_tables:
            summary_lines.append(f"    [FAIL] {t}")
        summary_lines.append("")

    if error_tables:
        summary_lines.append("  ERROR TABLES:")
        for t in error_tables:
            summary_lines.append(f"    [ERROR] {t}")
        summary_lines.append("")

    summary_lines.append("=" * 150)

    # Write to file
    try:
        with open(output_filename, 'w', encoding='utf-8-sig') as f:
            f.write("\n".join(summary_lines))
        print(f"\nSummary saved to: {output_filename}")
    except Exception as e:
        print(f"\nError saving summary to file: {e}")
    
if __name__ == "__main__":
    script_start = time.perf_counter()
    py_path = os.environ["PYTHONPATH"]
    ingest_yaml_file = f'{py_path}/IngestionConfig.yaml'
    ingest_cfg_dict = load_yaml(yaml_file=ingest_yaml_file)
    argument_dict = arg_parsing()
    appl_nme = argument_dict["appl_name"]
    prcs_dte = argument_dict["prcs_dte"]
    tenant = argument_dict["tenant"]
    print(f"Application Name: {appl_nme}, Process Date: {prcs_dte}, Tenant: {tenant}")
    #Snowflake connection
    conn = open_sf_connection(ingest_cfg=ingest_cfg_dict)
    cs = conn.cursor()
    appl_code = get_appl_code(appl_nme,cs)
    #oracle connection
    ora_user = f'DW{argument_dict["tenant"]}'
    oconn = open_oracle_connection(myuser=ora_user)
    ocs = oconn.cursor()
    sql = f"""
        SELECT tbl_nme FROM RAW_IBS.ARCHITECTURE.T_APPL_TABLE
        WHERE appl_cde LIKE lower('{appl_code}%')
        AND tbl_nme NOT LIKE '%DTM%'
    """
    cs.execute(sql)
    table_name_tuples = cs.fetchall()
    
    # Convert list of tuples to single list of table names
    table_names = [table[0].upper() for table in table_name_tuples]
    print(table_names)
  
    # Dictionary to store table names and their matching columns
    table_columns_dict = {}

    # Batch column discovery: single query for all tables instead of N individual queries
    if table_names:
        in_clause = ", ".join(f"'{t}'" for t in table_names)
        column_sql = f"""
        SELECT table_name, COLUMN_NAME
        FROM CUR_IBS.information_schema.columns
        WHERE (table_schema='{appl_nme}' OR table_schema='DDW_CNF_DIM')
        AND table_catalog='CUR_IBS'
        AND table_name IN ({in_clause})
        AND DATA_TYPE IN ('NUMBER', 'FLOAT', 'DECIMAL', 'INTEGER')
        AND COLUMN_NAME NOT LIKE 'DW%'
        AND (COLUMN_NAME LIKE '%BAL%'
             OR COLUMN_NAME LIKE '%AMT%'
             OR COLUMN_NAME LIKE '%TOT%'
             OR COLUMN_NAME LIKE '%CNT%'
             OR COLUMN_NAME LIKE '%RTE%'
             OR COLUMN_NAME LIKE '%NBR%'
             OR COLUMN_NAME LIKE '%QTY%'
             )
        ORDER BY table_name, COLUMN_NAME
        """
        cs.execute(column_sql)
        for row in cs.fetchall():
            tbl, col = row[0].upper(), row[1]
            if tbl not in table_columns_dict:
                table_columns_dict[tbl] = []
            table_columns_dict[tbl].append(col)
    print(f"Discovered balance columns for {len(table_columns_dict)} tables")
    
    # Validate balances
    validation_results = validate_balance(table_columns_dict, cs, ocs, appl_nme, prcs_dte, tenant)
    
    # Validate C2 balances with category breakdown
    # validation_results_new = validate_balance_new(table_columns_dict, cs, ocs, appl_nme, prcs_dte, tenant)
    
    #print(f"\nC2 validation completed. Found {len(validation_results_new)} results.")
    
    # Print summary
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    output_dir = f"{ingest_cfg_dict['snowflake_connection']['validation_path']}/{appl_nme}/{script_name}"
    os.makedirs(output_dir, exist_ok=True)
    print_validation_summary(validation_results, appl_nme, tenant, prcs_dte, None, output_dir=output_dir)
    
    if argument_dict.get('load_sf_meta') and validation_results:
        try:
            registry = TestCaseRegistry(cs, SCRIPT_NAME,
                                        database=argument_dict.get('sf_meta_db'),
                                        schema=argument_dict.get('sf_meta_schema'))
            vr_list = []
            for r in validation_results:
                if 'error' in r:
                    continue
                status = 'SUCCESS' if r.get('match') else 'FAIL'
                vr_list.append(registry.create_result(
                    validation_key='balance_validation',
                    test_scenario='Balance sum comparison Oracle vs Snowflake',
                    appl_name=appl_nme,
                    appl_code=appl_code,
                    tenant_id=tenant,
                    table_name=r['table'],
                    validation_status=status,
                    source_count=int(r.get('oracle_sum', 0)),
                    target_count=int(r.get('snowflake_sum', 0)),
                    status_reason=f"Column: {r['column']}, Diff: {r.get('difference', 0)}"[:500],
                    source_sql=None,
                    target_sql=None,
                ))

            loader = ValidationLoader(
                sf_cursor=cs,
                arg_dict=argument_dict,
                script_name=SCRIPT_NAME,
                script_version=SCRIPT_VERSION,
                database=argument_dict.get('sf_meta_db'),
                schema=argument_dict.get('sf_meta_schema')
            )
            summary = ExecutionSummary(
                script_name=SCRIPT_NAME, appl_name=appl_nme,
                appl_code=appl_code, tenant_id=tenant,
                process_date=prcs_dte, script_version=SCRIPT_VERSION
            )
            summary.started_at = datetime.fromtimestamp(time.time() - (time.perf_counter() - script_start))
            summary.parameters_used = {k: str(v) for k, v in argument_dict.items()}
            summary.update_counts(vr_list)
            summary.execution_time_sec = time.perf_counter() - script_start
            output_filename = f"{output_dir}/Summary_BalanceValidation_{appl_nme}_{tenant}_{prcs_dte}.txt"
            summary.read_and_store_output(output_filename, file_type='balance_validation_report')
            exec_id = loader.insert_execution_summary(summary)

            if vr_list:
                run_ids = loader.insert_master_bulk(vr_list, execution_id=exec_id)
                for i, vr in enumerate(vr_list):
                    vr.run_id = run_ids[i] if i < len(run_ids) else None

                detail_batch = []
                for vr in vr_list:
                    if vr.validation_status == 'FAIL' and vr.run_id:
                        detail_batch.append(ValidationDetailResult(
                            run_id=vr.run_id,
                            match_status='MISMATCH',
                            record_key=vr.table_name,
                            record_key_columns='TABLE_NAME',
                            source_data={'oracle_sum': vr.source_count},
                            target_data={'snowflake_sum': vr.target_count},
                            detail_remarks=(vr.status_reason or 'Balance mismatch')[:500]
                        ))
                if detail_batch:
                    capped, total, _ = cap_details(detail_batch, 500)
                    loader.insert_detail_bulk(capped)
                    if total > 500:
                        print(f"Capped detail rows to 500 (total: {total})")

            summary.emit_summary_line()
            print(f"Loaded {len(vr_list)} balance validation records to Snowflake")
        except Exception as e:
            print(f"Failed to load validation results to Snowflake: {str(e)}")
            traceback.print_exc()
    else:
        print("Skipping metadata load to Snowflake (--load-sf-meta not specified)")

    ocs.close()
    oconn.close()
    cs.close()
    conn.close()
    
    print("\nConnections closed.")
    # Script stats output
    script_end = time.perf_counter()
    script_run_time = script_end - script_start
    print(f"Script run time: {script_run_time} seconds\n")