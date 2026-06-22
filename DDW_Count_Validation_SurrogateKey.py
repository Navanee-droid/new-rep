# Standard library imports
import gzip
import logging
import os
import os.path
import subprocess
import sys
import time
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

# Third-party library imports
import oracledb
import snowflake
import toml
import yaml
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake.connector import DictCursor

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
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
    parse_args,
    get_appl_code,
    open_sf_connection,
    load_yaml,
    get_tables_from_appl_table,
    open_oracle_connection,
    logging_config,
    get_tb_c2_filter,
    setup_tenant_shard,
    get_snowflake_dtm_views
)

SCRIPT_NAME = "DDW_Count_Validation_SurrogateKey.py"
SCRIPT_VERSION = "v2.0"

# Constants
DEFAULT_HEADER_MATCH_THRESHOLD = 50.0  # Percentage
FILE_BASE_DIR = f'/dwbic_bkup_{os.environ["PRJ_ENVIRONMENT"]}/BIC'
DEFAULT_DATABASE = 'CUR_IBS'
DEFAULT_SCHEMA_PREFIX = 'RAW_IBS.ARCHITECTURE'
TB_C2_FILTER_CACHE = {}


def is_tbc2(table_name: str) -> bool:
    return table_name.upper().startswith('TB_C2_') and 'DTM' not in table_name.upper()

def parse_arguments() -> Dict[str, str]:
    args = parse_args(
        required=['--a', '--t', '--p'],
        optional=['--l', '--o', '--load-sf-meta', '--sf-meta-db', '--sf-meta-schema'],
        description='Compare record counts between Oracle and Snowflake for DDW applications.',
    )
    # prc_dte = raw YYYYMMDD, process_date = YYYY-MM-DD (added by _post_process as process_date_ts)
    args['prc_dte'] = args['process_date']
    args['process_date'] = args.get('process_date_ts', '')
    return args


def has_header_in_file(file_path: str, sf_cursor, table_name: str, schema: str) -> bool:
    """
    Detect if a file has a header by comparing first line with Snowflake table columns.

    Uses column matching to determine if the first line represents a header row.
    A match threshold of 50% or higher indicates the presence of a header.

    Args:
        file_path: Path to the file (supports .gz compressed files)
        sf_cursor: Snowflake database cursor
        table_name: Table name in Snowflake
        schema: Schema name

    Returns:
        bool: True if file has header, False otherwise
    """
    try:
        # Get column names from Snowflake table
        col_query = f"""
            SELECT COLUMN_NAME 
            FROM {DEFAULT_DATABASE}.INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = '{schema}' 
            AND TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION
        """
        sf_cursor.execute(col_query)
        sf_columns = [row[0].upper() for row in sf_cursor.fetchall()]

        if not sf_columns:
            logging.warning(f"No columns found for table {table_name}")
            print(f"    ⚠ No columns found for table {table_name}")
            return False

        # Read first line from file
        first_line = None
        if file_path.endswith('.gz'):
            with gzip.open(file_path, 'rt', encoding='utf-8', errors='ignore') as f:
                first_line = f.readline().strip()
        else:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                first_line = f.readline().strip()

        if not first_line:
            return False

        # Split first line by common delimiters
        delimiters = ['|', ',', '\t', ';']
        file_fields = []

        for delimiter in delimiters:
            if delimiter in first_line:
                file_fields = [field.strip().upper() for field in first_line.split(delimiter)]
                break

        if not file_fields:
            # Try space delimiter as last resort
            file_fields = [field.strip().upper() for field in first_line.split()]

        # Check if first line fields match Snowflake columns
        matching_fields = sum(1 for field in file_fields if field in sf_columns)
        match_percentage = (matching_fields / len(sf_columns)) * 100 if sf_columns else 0

        has_header = match_percentage >= DEFAULT_HEADER_MATCH_THRESHOLD

        logging.info(
            f"Header detection for {table_name}: {match_percentage:.1f}% match "
            f"({matching_fields}/{len(sf_columns)} columns)"
        )
        print(f"    → Header detection: {match_percentage:.1f}% match ({matching_fields}/{len(sf_columns)} columns)")
        print(f"    → First line preview: {first_line[:100]}...")
        print(f"    → Has header: {'YES' if has_header else 'NO'}")

        return has_header

    except Exception as e:
        logging.error(f"Error detecting header in {file_path}: {str(e)}")
        print(f"    ⚠ Error detecting header: {str(e)}")
        return False


def count_file_records(
        file_path: str,
        sf_cursor,
        table_name: str,
        schema: str
) -> Tuple[int, bool, Optional[str]]:
    """
    Count records in a file, automatically detecting and excluding header if present.

    Args:
        file_path: Path to the file (supports .gz compressed files)
        sf_cursor: Snowflake database cursor
        table_name: Table name
        schema: Schema name

    Returns:
        Tuple containing:
            - record_count (int): Number of data records (excluding header)
            - has_header (bool): Whether the file has a header row
            - error_message (Optional[str]): Error message if failed, None if successful
    """
    try:
        # Check if file has header
        has_header = has_header_in_file(file_path, sf_cursor, table_name, schema)

        # Count all non-empty lines
        total_lines = 0
        if file_path.endswith('.gz'):
            with gzip.open(file_path, 'rt', encoding='utf-8', errors='ignore') as f:
                total_lines = sum(1 for line in f if line.strip())
        else:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                total_lines = sum(1 for line in f if line.strip())

        # Subtract header if present
        record_count = total_lines - 1 if has_header else total_lines

        logging.info(
            f"File record count: {file_path} - Total lines: {total_lines}, "
            f"Header: {'Yes' if has_header else 'No'}, Data records: {record_count}"
        )
        print(
            f"    → Total lines: {total_lines:,}, Header: {'Yes' if has_header else 'No'}, "
            f"Data records: {record_count:,}"
        )

        return record_count, has_header, None

    except Exception as e:
        error_msg = f"Error reading file: {str(e)}"
        logging.error(f"Failed to count records in {file_path}: {str(e)}")
        return -1, False, error_msg


def write_query_reference(
        output_dir: str,
        appl_name: str,
        tenant_id: str,
        process_date: str,
        query_list: List[Dict[str, str]]
) -> str:
    """
    Write all queries to a reference file grouped by target table.

    Each target table gets its own section containing its Standard, EXP, CUR_REC_IND,
    FMT, and UPD queries grouped together with aligned labels.

    Args:
        output_dir: Directory to save the file
        appl_name: Application name
        tenant_id: Tenant ID
        process_date: Process date in YYYY-MM-DD format
        query_list: List of dicts with keys: 'table_name', 'query_type',
                    'parent_table', 'snowflake_query', 'oracle_query'

    Returns:
        str: Path to the created query reference file
    """
    query_file = f'{output_dir}/{appl_name}_{tenant_id}_queries_{process_date.replace("-", "")}.txt'

    if os.path.exists(query_file):
        os.remove(query_file)

    # Group queries by parent_table, preserving insertion order.
    # Entries with parent_table=None (unmatched FMT/UPD) go to a trailing section.
    groups: dict = {}
    orphans: list = []
    for q in query_list:
        pt = q.get('parent_table')
        if pt is None:
            orphans.append(q)
        else:
            groups.setdefault(pt, []).append(q)

    # Sort-order for sub-query type labels displayed inside each group
    _TYPE_ORDER = {
        'Standard Query': 0,
        'Expiry Date Query': 1,
        'Current Record Query': 2,
        'Format File Query': 3,
        'Update File Query': 4,
    }

    with open(file=query_file, mode='w', encoding='utf-8') as f:
        f.write("=" * 120 + "\n")
        f.write("QUERY REFERENCE DOCUMENT\n")
        f.write("=" * 120 + "\n")
        f.write(f"Application  : {appl_name}\n")
        f.write(f"Tenant ID    : {tenant_id}\n")
        f.write(f"Process Date : {process_date}\n")
        f.write(f"Generated    : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Target Tables: {len(groups)}\n")
        f.write(f"Total Queries: {len(query_list)}\n")
        f.write("=" * 120 + "\n\n")

        for parent, entries in groups.items():
            # Sort sub-queries by canonical type order
            entries_sorted = sorted(entries, key=lambda e: _TYPE_ORDER.get(e.get('query_type', ''), 99))

            f.write("=" * 120 + "\n")
            f.write(f"TABLE: {parent}\n")
            f.write("=" * 120 + "\n")

            for entry in entries_sorted:
                tname = entry.get('table_name', 'N/A')
                qtype = entry.get('query_type', '')
                sf_q  = entry.get('snowflake_query', 'N/A')
                ora_q = entry.get('oracle_query', 'N/A')

                # Use the query type as the sub-section label; for the base query use the
                # table name directly, for sub-queries indent with a tag
                if qtype == 'Standard Query':
                    label = f"[{qtype}]"
                else:
                    label = f"  [{qtype}]  {tname}"

                f.write(f"\n{label}\n")
                f.write("-" * 80 + "\n")
                if qtype == 'Shard Query':
                    f.write("  SHARD:\n")
                    f.write(f"    {sf_q}\n")
                else:
                    shard_q = entry.get('shard_query', '')
                    f.write("  ORACLE:\n")
                    f.write(f"    {ora_q}\n")
                    f.write("  SNOWFLAKE:\n")
                    f.write(f"    {sf_q}\n")
                    if shard_q:
                        f.write("  SHARD:\n")
                        f.write(f"    {shard_q}\n")

            f.write("\n" + "-" * 120 + "\n\n")

        # Unmatched FMT/UPD entries that had no parent resolved
        if orphans:
            f.write("=" * 120 + "\n")
            f.write("OTHER QUERIES (unmatched FMT/UPD)\n")
            f.write("=" * 120 + "\n")
            for entry in orphans:
                tname = entry.get('table_name', 'N/A')
                qtype = entry.get('query_type', '')
                sf_q  = entry.get('snowflake_query', 'N/A')
                ora_q = entry.get('oracle_query', 'N/A')
                f.write(f"\n[{qtype}]  {tname}\n")
                f.write("-" * 80 + "\n")
                f.write("  SNOWFLAKE:\n")
                f.write(f"    {sf_q}\n\n")
                f.write("  ORACLE:\n")
                f.write(f"    {ora_q}\n")
            f.write("\n" + "-" * 120 + "\n\n")

    logging.info(f"Query reference file created: {query_file}")
    print(f"\n Query reference file created: {query_file}")
    return query_file

def get_fmt_tables(appl_name: str, sf_cursor) -> List[str]:
    """
    Retrieve list of FMT (format) tables from Snowflake.

    Args:
        appl_name: Application name
        sf_cursor: Snowflake database cursor

    Returns:
        List[str]: List of FMT table names
    """
    table_fmt_list = []
    query = f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema IN ('{appl_name}_TSM', '{appl_name}_STG_TSM') 
        AND table_name LIKE 'DW%_FMT' 
        AND table_name NOT LIKE 'TB_APPLICATION_%' 
        AND table_name NOT LIKE '%DAY_TME%' 
        AND table_name NOT LIKE '%BRDG%' 
        AND table_name NOT LIKE '%TEMP%' 
        ORDER BY table_name
    """
    fallback_query = f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema IN ('{appl_name}', '{appl_name}_STG') 
        AND table_name LIKE 'DW%_FMT' 
        AND table_name NOT LIKE 'TB_APPLICATION_%' 
        AND table_name NOT LIKE '%DAY_TME%' 
        AND table_name NOT LIKE '%BRDG%' 
        AND table_name NOT LIKE '%TEMP%' 
        ORDER BY table_name
    """

    try:
        sf_cursor.execute(query)
        result = sf_cursor.fetchall()
        if result:
            table_fmt_list = [res[0] for res in result]

        if not table_fmt_list:
            logging.info(f"No FMT tables found in _TSM schemas for {appl_name}, trying without _TSM")
            sf_cursor.execute(fallback_query)
            result = sf_cursor.fetchall()
            if result:
                table_fmt_list = [res[0] for res in result]

        logging.info(f"Retrieved {len(table_fmt_list)} FMT tables for {appl_name}")
        print(f"FMT tables: {table_fmt_list}")
    except Exception as e:
        logging.error(f"Failed to retrieve FMT tables for {appl_name}: {str(e)}")
        print(f"✗ Error retrieving FMT tables: {str(e)}")

    return table_fmt_list


def get_upd_tables(appl_name: str, sf_cursor) -> List[str]:
    """
    Retrieve list of UPD (update) tables from Snowflake.

    Args:
        appl_name: Application name
        sf_cursor: Snowflake database cursor

    Returns:
        List[str]: List of UPD table names
    """
    table_upd_list = []
    query = f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema IN ('{appl_name}_TSM', '{appl_name}_STG_TSM') 
        AND table_name LIKE 'DW%_UPD' 
        AND table_name NOT LIKE 'TB_APPLICATION_%' 
        AND table_name NOT LIKE '%DAY_TME%' 
        AND table_name NOT LIKE '%BRDG%' 
        AND table_name NOT LIKE '%TEMP%' 
        ORDER BY table_name
    """
    fallback_query = f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema IN ('{appl_name}', '{appl_name}_STG') 
        AND table_name LIKE 'DW%_UPD' 
        AND table_name NOT LIKE 'TB_APPLICATION_%' 
        AND table_name NOT LIKE '%DAY_TME%' 
        AND table_name NOT LIKE '%BRDG%' 
        AND table_name NOT LIKE '%TEMP%' 
        ORDER BY table_name
    """

    try:
        sf_cursor.execute(query)
        result = sf_cursor.fetchall()
        if result:
            table_upd_list = [res[0] for res in result]

        if not table_upd_list:
            logging.info(f"No UPD tables found in _TSM schemas for {appl_name}, trying without _TSM")
            sf_cursor.execute(fallback_query)
            result = sf_cursor.fetchall()
            if result:
                table_upd_list = [res[0] for res in result]

        logging.info(f"Retrieved {len(table_upd_list)} UPD tables for {appl_name}")
        print(f"UPD tables: {table_upd_list}")
    except Exception as e:
        logging.error(f"Failed to retrieve UPD tables for {appl_name}: {str(e)}")
        print(f"✗ Error retrieving UPD tables: {str(e)}")

    return table_upd_list


def execute_snowflake_query(sql: str, cursor) -> Tuple:
    """
    Execute a Snowflake query and return results.

    Args:
        sql: SQL query string to execute
        cursor: Snowflake database cursor

    Returns:
        Tuple: (result, None) on success, (None, error_message) on failure
    """
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
        return result, None
    except Exception as e:
        err_str = str(e)
        # Extract the readable part after the SQL error code (e.g. "Object does not exist...")
        parts = err_str.split(':', 2)
        clean_msg = parts[-1].strip() if len(parts) >= 2 else err_str
        logging.warning(f'Snowflake query error: {clean_msg}')
        print(f'  [WARNING] Snowflake query error: {clean_msg}')
        return None, clean_msg


def execute_oracle_query(sql: str, cursor) -> Tuple:
    """
    Execute an Oracle query and return results.

    Args:
        sql: SQL query string to execute
        cursor: Oracle database cursor

    Returns:
        Tuple: (result, None) on success, (None, error_message) on failure
    """
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
        return result, None
    except Exception as e:
        logging.error(f'Oracle query failed: {str(e)}\nQuery: {sql}')
        print(f'✗ The following Oracle query has failed:')
        print(sql)
        traceback.print_exc()
        return None, str(e)


def check_data_src_cde_in_table(cursor, table: str, appl_name: str) -> List[str]:
    """
    Retrieve column names from a table, excluding DW_ prefixed columns.

    Args:
        cursor: Snowflake database cursor
        table: Table name
        appl_name: Application name (schema)

    Returns:
        List[str]: List of column names
    """
    column_query = f"""
        SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE 
        FROM information_schema.columns 
        WHERE table_name = '{table}' 
        AND table_catalog = '{DEFAULT_DATABASE}' 
        AND TABLE_SCHEMA = '{appl_name}' 
        AND COLUMN_NAME NOT LIKE 'DW_%'
    """

    column_result, _ = execute_snowflake_query(sql=column_query, cursor=cursor)
    column_names = [row[0] for row in column_result] if column_result is not None else []
    return column_names


def _derive_fmt_upd_name(table_name: str, suffix: str) -> str:
    """Derive FMT/UPD table name from a target table name.
    e.g. TB_C2_DA0_... -> DWC2FDA0111__FMT, TB_CC_DZ0_... -> DWCCFDZ0111__UPD
    """
    parts = table_name.split('_')
    code = parts[1]      # e.g. C2, CC, CI
    table_id = parts[2]  # e.g. DA0, DZ0, SZX
    return f"DW{code}F{table_id}111__{suffix}"


def rec_count_validation(ora_cursor, sf_cursor, tenant_id, table_list, process_date, appl_name, appl_code,
                         table_fmt_list, table_upd_list, proc_date, file_path, ingest_cfg_dict=None, app_db_name=None):
    did_not_fail = True

    # Initialize query collection list
    query_list = []
    table_outcomes = []

    # Calculate date range for directory search
    date_str = f'{process_date}'
    dt = datetime.strptime(date_str, "%Y-%m-%d")

    curr_date = dt.strftime("%Y%m%d")
    prev_date = (dt - timedelta(days=1)).strftime("%Y%m%d")
    next_date = (dt + timedelta(days=1)).strftime("%Y%m%d")
    nxt1_date = (dt + timedelta(days=2)).strftime("%Y%m%d")

    date_dirs = [prev_date, curr_date, next_date, nxt1_date]

    # Get application code for directory path
    app_code_lower = appl_code.lower() if appl_code else 'd6'

    output_file = f'{file_path}/{appl_name}_{tenant_id}_record_counts_{process_date.replace("-", "")}.txt'

    if os.path.exists(output_file):
        os.remove(output_file)

    W = 140
    div = '-' * W
    dbl = '=' * W
    TBL_W, CNT_W, STS_W = 55, 14, 12
    shard_label = app_db_name if app_db_name else 'N/A'
    _combined = f'{appl_name} + {app_db_name}' if app_db_name else appl_name
    lines = []

    # Header box
    lines.append('+' + '=' * (W - 2) + '+')
    lines.append('|' + ' ORACLE vs SNOWFLAKE 3-LAYER COUNT VALIDATION REPORT'.ljust(W - 2) + '|')
    lines.append('+' + '=' * (W - 2) + '+')
    lines.append('|' + f' Schema : {appl_name:<22} | Tenant : {tenant_id:<22} | Process Date : {process_date}'.ljust(W - 2) + '|')
    lines.append('|' + f' Layer 1: DW{tenant_id} (Oracle)  vs  CUR_IBS.{appl_name} (SF Curated)'.ljust(W - 2) + '|')
    lines.append('|' + f' Layer 2: CUR_IBS.{appl_name} (SF Curated)  vs  {shard_label}.{appl_name} (SF Shard)'.ljust(W - 2) + '|')
    lines.append('+' + '=' * (W - 2) + '+')
    lines.append('')

    # Table grid header
    hdr = (f"{'Table':<{TBL_W}} | {'Oracle Cnt':>{CNT_W}} | "
           f"{'SF Cur Cnt':>{CNT_W}} | {'SF Shrd Cnt':>{CNT_W}} | {'Status':<{STS_W}} | Notes")
    sep = ('-' * TBL_W + '-+-' + '-' * CNT_W + '-+-' +
           '-' * CNT_W + '-+-' + '-' * CNT_W + '-+-' + '-' * STS_W + '-+------')
    lines.append(div)
    lines.append(hdr)
    lines.append(sep)

    results_summary = []
    pass_count, fail_count = 0, 0
    env = os.environ.get('PRJ_ENVIRONMENT', 'opin')

    # Application Tables Validation
    print(f"\n{dbl}")
    print(f"APPLICATION TABLE VALIDATION")
    print(dbl)

    failed_tables = []  # collect Layer 1 failures for drilldown section

    for table in table_list:
        sf_query, ora_query, sf_src_cde, sf_data_src_cde = '', '', '', ''
        orc_tb_c2_filter = ''
        sf_table_count = None
        ora_table_count = None
        shard_count = None

        print(f'\nValidating {tenant_id}.{table}')

        if is_tbc2(table):
            if table in TB_C2_FILTER_CACHE:
                orc_tb_c2_filter = TB_C2_FILTER_CACHE[table]
            else:
                orc_tb_c2_filter = get_tb_c2_filter(table, appl_name, sf_cursor, fetch = True)
                if orc_tb_c2_filter:
                    orc_tb_c2_filter = f" and {orc_tb_c2_filter}"
                TB_C2_FILTER_CACHE[table] = orc_tb_c2_filter

        # Build queries based on table type
        if any(k in table for k in ("SCD", "RCD", "RPD", "USCD")) and "TB_C2" in table:
            sf_query = f"""select count(*) from DDW_CNF_DIM.{table} where tenant_id = '{tenant_id}' and eff_dte 
            in ('{process_date}') and SRC_APPL_NAME = '{appl_name}';"""
            ora_query = f"""select count(*) from DW{tenant_id}.{table} where 
            eff_dte = to_date('{process_date}','YYYY-MM-DD'){orc_tb_c2_filter}"""
        elif "TB_C2" in table and 'DTM' not in table:
            sf_query = (f"select count(*) from DDW_CNF_DIM.{table} where tenant_id='{tenant_id}' and prcs_dte="
                        f"to_date('{process_date}', 'YYYY-MM-DD') and SRC_APPL_NAME = '{appl_name}';")
            ora_query = (f"select count(*) from DW{tenant_id}.{table} where prcs_dte=to_date('{process_date}', "
                         f"'YYYY-MM-DD'){orc_tb_c2_filter}")
        elif any(k in table for k in ("SCD", "RCD", "RPD", "USCD")):
            sf_query = f"""select count(*) from {appl_name}.{table} where tenant_id = '{tenant_id}' and eff_dte 
            in ('{process_date}'){sf_src_cde}{sf_data_src_cde};"""
            ora_query = f"""select count(*) from DW{tenant_id}.{table} where  
            eff_dte = to_date('{process_date}','YYYY-MM-DD')"""

        elif table.upper().startswith('VW_'):
            # DTM/BRDG views: Snowflake uses VW_ view name, Oracle uses TB_ table name
            ora_tb_name = 'TB_' + table[3:]
            date_col = 'full_dte' if 'DTM' in table.upper() else 'prcs_dte'
            sf_query = (f"select count(*) from {appl_name}.{table} where tenant_id='{tenant_id}' and {date_col}="
                        f"to_date('{process_date}', 'YYYY-MM-DD');")
            ora_query = (f"select count(*) from DW{tenant_id}.{ora_tb_name} where {date_col}=to_date('{process_date}', "
                         f"'YYYY-MM-DD')")
        else:
            sf_query = (f"select count(*) from {appl_name}.{table} where tenant_id='{tenant_id}' and prcs_dte="
                        f"to_date('{process_date}', 'YYYY-MM-DD'){sf_data_src_cde}{sf_src_cde};")
            ora_query = (f"select count(*) from DW{tenant_id}.{table} where prcs_dte=to_date('{process_date}', "
                         f"'YYYY-MM-DD')")

        # Build CUR_REC_IND = 'Y' queries for SCD/RCD/RPD tables that have the column
        # (col metadata is looked up later, only for failing tables)

        try:
            sf_result, sf_err = execute_snowflake_query(sql=sf_query, cursor=sf_cursor)
            sf_table_count = int(sf_result[0][0]) if sf_result else 0

            ora_result, ora_err = execute_oracle_query(sql=ora_query, cursor=ora_cursor)
            if ora_result is None:
                raise RuntimeError(f"__ORA__: {ora_err}")
            ora_table_count = int(ora_result[0][0])

            if sf_result is None:
                raise RuntimeError(f"__SF__: Table not found in Snowflake (Oracle count: {ora_table_count:,}): {sf_err}")

            notes = ''
            if app_db_name:
                sf_schema_for_shard = 'DDW_CNF_DIM' if 'TB_C2' in table and 'DTM' not in table else appl_name
                date_col_for_shard = 'eff_dte' if any(k in table for k in ('SCD', 'RCD', 'RPD')) else ('full_dte' if (table.upper().startswith('VW_') and 'DTM' in table.upper()) else 'prcs_dte')
                src_filter_shard = f" and SRC_APPL_NAME='{appl_name}'" if 'TB_C2' in table and 'DTM' not in table else ''
                shard_table_name = ('TB_' + table[3:]) if table.upper().startswith('VW_') else table
                shard_query = (f"select count(*) from {app_db_name}.{sf_schema_for_shard}.{shard_table_name} "
                               f"where tenant_id='{tenant_id}' and {date_col_for_shard}='{process_date}'{src_filter_shard};")
                query_list.append({
                    'table_name': table,
                    'query_type': 'Standard Query',
                    'parent_table': table,
                    'snowflake_query': sf_query.strip(),
                    'oracle_query': ora_query.strip(),
                    'shard_query': shard_query.strip()
                })
                shard_res, shard_err = execute_snowflake_query(sql=shard_query, cursor=sf_cursor)
                if shard_res is None:
                    raise RuntimeError(f"__SF__: Shard query failed for {table} (SF curated: {sf_table_count:,}, Oracle: {ora_table_count:,}): {shard_err}")
                shard_count = int(shard_res[0][0]) if shard_res else 0
            else:
                query_list.append({
                    'table_name': table,
                    'query_type': 'Standard Query',
                    'parent_table': table,
                    'snowflake_query': sf_query.strip(),
                    'oracle_query': ora_query.strip()
                })

            layer1_ok = (ora_table_count == sf_table_count)
            layer2_ok = (shard_count is None) or (sf_table_count == shard_count)
            if not layer1_ok or not layer2_ok:
                result_message = 'FAIL'
                did_not_fail = False
                notes = 'Layer 1 mismatch - see drilldown' if not layer1_ok else 'Layer 2 mismatch'
                if not layer1_ok:
                    failed_tables.append({
                        'table': table,
                        'orc_tb_c2_filter': orc_tb_c2_filter,
                        'ora_count': ora_table_count,
                        'sf_count': sf_table_count,
                        'shard_count': shard_count,
                    })
            else:
                result_message = 'SUCCESS'

            if result_message == 'SUCCESS':
                pass_count += 1
            else:
                fail_count += 1

            ora_fmt   = f"{ora_table_count:,}"
            sf_fmt    = f"{sf_table_count:,}"
            shard_fmt = f"{shard_count:,}" if shard_count is not None else 'N/A'

            main_row = (f"{table:<{TBL_W}} | {ora_fmt:>{CNT_W}} | "
                        f"{sf_fmt:>{CNT_W}} | {shard_fmt:>{CNT_W}} | {result_message:<{STS_W}} | {notes}")
            lines.append(main_row)
            print(main_row)

            results_summary.append({
                'table': table, 'oracle_count': ora_table_count,
                'snowflake_count': sf_table_count, 'result': result_message, 'row_type': 'target'
            })
            table_outcomes.append({
                'table': table,
                'status': result_message,
                'reason': 'Count match' if result_message == 'SUCCESS' else (
                    f'Count mismatch: ORA={ora_table_count}, SF={sf_table_count}'
                ),
                'src_count': ora_table_count,
                'tgt_count': sf_table_count
            })
        except Exception as e:
            did_not_fail = False
            fail_count += 1
            err_str = str(e)
            _locals = locals()
            _ora = _locals.get('ora_table_count')
            _sf  = _locals.get('sf_table_count')
            _shard = _locals.get('shard_count')
            if err_str.startswith('__SF__:'):
                ora_fmt = f"{_ora:,}" if _ora is not None else 'N/A'
                sf_fmt = 'ERROR'
                notes = ('SF query failed: ' + err_str[7:].strip())[:80]
            elif err_str.startswith('__ORA__:'):
                ora_fmt, sf_fmt = 'ERROR', 'N/A'
                notes = ('ORA query failed: ' + err_str[8:].strip())[:80]
            else:
                ora_fmt = sf_fmt = 'ERROR'
                notes = err_str[:80]
            shard_fmt = f"{_shard:,}" if _shard is not None else 'N/A'
            sf_fmt_display = sf_fmt if sf_fmt == 'ERROR' else (f"{_sf:,}" if _sf is not None else 'N/A')
            main_row = (f"{table:<{TBL_W}} | {ora_fmt:>{CNT_W}} | "
                        f"{sf_fmt_display:>{CNT_W}} | {shard_fmt:>{CNT_W}} | {'ERROR':<{STS_W}} | {notes}")
            lines.append(main_row)
            print(main_row)
            table_outcomes.append({
                'table': table, 'status': 'FAIL',
                'reason': f'Query error: {str(e)[:100]}',
                'src_count': _ora if _ora is not None else 0,
                'tgt_count': _sf  if _sf  is not None else 0
            })

    lines.append('')

    # --- Drilldown Section: flat table, only for Layer 1 failures ---
    if failed_tables:
        DR_W = 58  # wider col to fit qualifiers like (CUR_REC_IND='Y')
        dr_hdr = (f"{'Table':<{DR_W}} | {'Oracle_Count':>{CNT_W}} | "
                  f"{'Snowflake_Count':>{CNT_W}} | {'Result':<{STS_W}}")
        dr_sep = ('-' * DR_W + '-+-' + '-' * CNT_W + '-+-' +
                  '-' * CNT_W + '-+-' + '-' * STS_W)

        lines.append(dbl)
        lines.append('FAILURE DRILLDOWN'.center(W))
        lines.append(dbl)
        lines.append(dr_hdr)
        lines.append(dr_sep)

        for fd in failed_tables:
            table = fd['table']
            orc_tb_c2_filter = fd['orc_tb_c2_filter']

            # Main count row
            ora_str = f"{fd['ora_count']:,}" if fd['ora_count'] is not None else 'N/A'
            sf_str  = f"{fd['sf_count']:,}"  if fd['sf_count']  is not None else 'N/A'
            lines.append(f"{table:<{DR_W}} | {ora_str:>{CNT_W}} | {sf_str:>{CNT_W}} | {'FAIL':<{STS_W}}")

            # Column metadata - only fetched for failing tables
            if 'TB_C2' in table:
                col_names = check_data_src_cde_in_table(cursor=sf_cursor, table=table, appl_name='DDW_CNF_DIM')
            else:
                col_names = check_data_src_cde_in_table(cursor=sf_cursor, table=table, appl_name=appl_name)
            col_names_upper = [c.upper() for c in col_names]
            exp_date_col = 'prcs_exp_dte' if 'PRCS_EXP_DTE' in col_names_upper else 'sor_exp_dte'
            has_cur_rec_ind = 'CUR_REC_IND' in col_names_upper

            is_scd = any(k in table for k in ('SCD', 'RCD', 'RPD'))
            sf_query_exp, ora_query_exp = '', ''
            sf_query_cur, ora_query_cur = '', ''

            if is_scd and 'TB_C2' in table:
                sf_query_exp = (f"select count(*) from DDW_CNF_DIM.{table} where tenant_id = '{tenant_id}' and "
                                f"{exp_date_col} in ('{process_date}') and SRC_APPL_NAME = '{appl_name}';")
                ora_query_exp = (f"select count(*) from DW{tenant_id}.{table} where {exp_date_col} = "
                                 f"to_date('{process_date}','YYYY-MM-DD'){orc_tb_c2_filter}")
            elif is_scd:
                sf_query_exp = (f"select count(*) from {appl_name}.{table} where tenant_id = '{tenant_id}' and "
                                f"{exp_date_col} in ('{process_date}');")
                ora_query_exp = (f"select count(*) from DW{tenant_id}.{table} where {exp_date_col} = "
                                 f"to_date('{process_date}','YYYY-MM-DD')")

            if has_cur_rec_ind and is_scd:
                if 'TB_C2' in table:
                    sf_query_cur = (f"select count(*) from DDW_CNF_DIM.{table} where tenant_id = '{tenant_id}' and "
                                    f"SRC_APPL_NAME = '{appl_name}' and CUR_REC_IND='Y';")
                    ora_query_cur = (f"select count(*) from DW{tenant_id}.{table} where "
                                     f"1=1{orc_tb_c2_filter} and CUR_REC_IND='Y'")
                else:
                    sf_query_cur = (f"select count(*) from {appl_name}.{table} where tenant_id = '{tenant_id}' and "
                                    f"CUR_REC_IND='Y';")
                    ora_query_cur = f"select count(*) from DW{tenant_id}.{table} where CUR_REC_IND='Y'"

            if sf_query_exp and ora_query_exp:
                query_list.append({
                    'table_name': f"{table} ({exp_date_col.upper()})",
                    'query_type': 'Expiry Date Query', 'parent_table': table,
                    'snowflake_query': sf_query_exp.strip(), 'oracle_query': ora_query_exp.strip()
                })
                sf_res_exp, _ = execute_snowflake_query(sql=sf_query_exp, cursor=sf_cursor)
                ora_res_exp, _ = execute_oracle_query(sql=ora_query_exp, cursor=ora_cursor)
                ora_cnt_exp = int(ora_res_exp[0][0]) if ora_res_exp is not None else 0
                if sf_res_exp is None:
                    sf_cnt_exp, msg_exp = 0, 'N/A'
                else:
                    sf_cnt_exp = int(sf_res_exp[0][0]) if sf_res_exp else 0
                    msg_exp = 'FAIL' if sf_cnt_exp != ora_cnt_exp else 'SUCCESS'
                    if msg_exp == 'FAIL':
                        did_not_fail = False
                sf_exp_disp = 'N/A' if sf_res_exp is None else f"{sf_cnt_exp:,}"
                exp_label = f"{table} ({exp_date_col.upper()})"
                lines.append(f"{exp_label:<{DR_W}} | {ora_cnt_exp:>{CNT_W},} | {sf_exp_disp:>{CNT_W}} | {msg_exp:<{STS_W}}")
                table_outcomes.append({
                    'table': exp_label, 'status': msg_exp,
                    'reason': f'EXP mismatch: ORA={ora_cnt_exp}, SF={sf_cnt_exp}' if msg_exp == 'FAIL' else 'EXP match',
                    'src_count': ora_cnt_exp, 'tgt_count': sf_cnt_exp
                })

            if sf_query_cur and ora_query_cur:
                query_list.append({
                    'table_name': f"{table} (CUR_REC_IND='Y')",
                    'query_type': 'Current Record Query', 'parent_table': table,
                    'snowflake_query': sf_query_cur.strip(), 'oracle_query': ora_query_cur.strip()
                })
                sf_res_cur, _ = execute_snowflake_query(sql=sf_query_cur, cursor=sf_cursor)
                ora_res_cur, _ = execute_oracle_query(sql=ora_query_cur, cursor=ora_cursor)
                ora_cnt_cur = int(ora_res_cur[0][0]) if ora_res_cur is not None else 0
                if sf_res_cur is None:
                    sf_cnt_cur, msg_cur = 0, 'N/A'
                else:
                    sf_cnt_cur = int(sf_res_cur[0][0]) if sf_res_cur else 0
                    msg_cur = 'FAIL' if sf_cnt_cur != ora_cnt_cur else 'SUCCESS'
                    if msg_cur == 'FAIL':
                        did_not_fail = False
                sf_cur_disp = 'N/A' if sf_res_cur is None else f"{sf_cnt_cur:,}"
                cur_label = f"{table} (CUR_REC_IND='Y')"
                lines.append(f"{cur_label:<{DR_W}} | {ora_cnt_cur:>{CNT_W},} | {sf_cur_disp:>{CNT_W}} | {msg_cur:<{STS_W}}")
                table_outcomes.append({
                    'table': cur_label, 'status': msg_cur,
                    'reason': f'CUR_REC_IND mismatch: ORA={ora_cnt_cur}, SF={sf_cnt_cur}' if msg_cur == 'FAIL' else 'CUR_REC_IND match',
                    'src_count': ora_cnt_cur, 'tgt_count': sf_cnt_cur
                })

            # FMT rows - processed on demand for this failing table only
            _parts = table.split('_')
            _fmt_prefix = f"DW{_parts[1]}F{_parts[2]}".upper()
            _tnt_lwr = tenant_id.lower()
            for _fmt_tbl in sorted(t for t in table_fmt_list if t.upper().startswith(_fmt_prefix)):
                _qs = (f"SELECT table_schema FROM information_schema.tables WHERE table_name = '{_fmt_tbl}'"
                       f" AND table_schema IN ('{appl_name}_TSM','{appl_name}_STG_TSM','{appl_name}','{appl_name}_STG')")
                _sf_res, _ = execute_snowflake_query(sql=_qs, cursor=sf_cursor)
                if not (_sf_res and len(_sf_res) > 0):
                    _qs = f"SELECT table_schema FROM information_schema.tables WHERE table_name = '{_fmt_tbl}'"
                    _sf_res, _ = execute_snowflake_query(sql=_qs, cursor=sf_cursor)
                if not (_sf_res and len(_sf_res) > 0):
                    continue
                _schema = _sf_res[0][0]
                _fmt_q = (f"SELECT COUNT(*) FROM {DEFAULT_DATABASE}.{_schema}.{_fmt_tbl} "
                          f"WHERE prcs_dte = '{process_date}' AND tenant_id = '{tenant_id}'")
                query_list.append({
                    'table_name': f"{_fmt_tbl} (FMT)", 'query_type': 'Format File Query',
                    'parent_table': table,
                    'snowflake_query': _fmt_q.strip(),
                    'oracle_query': f'File-based count from: {FILE_BASE_DIR}/{{date}}/mdw/{env}/files8/format/{app_code_lower}/'
                })
                _sf_fmt_res, _ = execute_snowflake_query(sql=_fmt_q, cursor=sf_cursor)
                _sf_fmt_cnt = int(_sf_fmt_res[0][0]) if _sf_fmt_res else 0
                _tbl_pfx = _fmt_tbl.split("__")[0].lower()
                _matched = []
                for _ddir in date_dirs:
                    _fdir = f'{FILE_BASE_DIR}/{_ddir}/mdw/{env}/files8/format/{app_code_lower}/'
                    if os.path.exists(_fdir):
                        for _fn in os.listdir(_fdir):
                            _fnl = _fn.lower()
                            if (_fnl.startswith(_tbl_pfx) and _tnt_lwr in _fnl
                                    and proc_date in _fnl and '.fmt' in _fnl):
                                _matched.append((os.path.join(_fdir, _fn), _fn, _ddir))
                        if _matched:
                            break
                _fcounts = []
                if _matched:
                    for _mp, _fn, _dd in _matched:
                        _rc, _hh, _err = count_file_records(_mp, sf_cursor, _fmt_tbl, _schema)
                        _fcounts.append((_fn, _rc if _err is None else -1, _hh))
                    _ora_cnt = sum(c for _, c, _ in _fcounts if c >= 0)
                    if any(c == -1 for _, c, _ in _fcounts):
                        _fmt_result = 'Error'
                    elif _sf_fmt_cnt != _ora_cnt:
                        _fmt_result = 'FAIL'
                        did_not_fail = False
                    else:
                        _fmt_result = 'SUCCESS'
                else:
                    _ora_cnt = 0
                    _fmt_result = 'N/A'
                lines.append(f"  [FMT] {_fmt_tbl}".ljust(DR_W) +
                              f" | {_ora_cnt:>{CNT_W},} | {_sf_fmt_cnt:>{CNT_W},} | {_fmt_result:<{STS_W}}")
                for fname, fcount, has_hdr in _fcounts:
                    if fcount >= 0:
                        lines.append(f"         - {fname}: {fcount:,} records"
                                     f"{' (with header)' if has_hdr else ' (no header)'}")
                _fmt_status = 'SUCCESS' if _fmt_result == 'SUCCESS' else ('FAIL' if _fmt_result in ('FAIL', 'Error') else 'SUCCESS')
                table_outcomes.append({
                    'table': _fmt_tbl, 'status': _fmt_status,
                    'reason': f'FMT count: File={_ora_cnt}, SF={_sf_fmt_cnt}, Result={_fmt_result}',
                    'src_count': _ora_cnt, 'tgt_count': _sf_fmt_cnt
                })

            # UPD rows - processed on demand for this failing table only
            for _upd_tbl in sorted(t for t in table_upd_list if t.upper().startswith(_fmt_prefix)):
                _qs = (f"SELECT table_schema FROM information_schema.tables WHERE table_name = '{_upd_tbl}'"
                       f" AND table_schema IN ('{appl_name}_TSM','{appl_name}_STG_TSM','{appl_name}','{appl_name}_STG')")
                _sf_res, _ = execute_snowflake_query(sql=_qs, cursor=sf_cursor)
                if not (_sf_res and len(_sf_res) > 0):
                    _qs = f"SELECT table_schema FROM information_schema.tables WHERE table_name = '{_upd_tbl}'"
                    _sf_res, _ = execute_snowflake_query(sql=_qs, cursor=sf_cursor)
                if not (_sf_res and len(_sf_res) > 0):
                    continue
                _schema = _sf_res[0][0]
                query_list.append({
                    'table_name': f"{_upd_tbl} (UPD)", 'query_type': 'Update File Query',
                    'parent_table': table,
                    'snowflake_query': 'N/A - File-based validation only',
                    'oracle_query': f'File-based count from: {FILE_BASE_DIR}/{{date}}/mdw/{env}/files8/extract/{app_code_lower}/'
                })
                _tbl_pfx = _upd_tbl.split("__")[0].lower()
                _matched = []
                for _ddir in date_dirs:
                    _udir = f'{FILE_BASE_DIR}/{_ddir}/mdw/{env}/files8/extract/{app_code_lower}/'
                    if os.path.exists(_udir):
                        for _fn in os.listdir(_udir):
                            _fnl = _fn.lower()
                            if (_fnl.startswith(_tbl_pfx) and _tnt_lwr in _fnl
                                    and proc_date in _fnl and '.upd' in _fnl):
                                _matched.append((os.path.join(_udir, _fn), _fn, _ddir))
                        if _matched:
                            break
                _fcounts = []
                if _matched:
                    for _mp, _fn, _dd in _matched:
                        _rc, _hh, _err = count_file_records(_mp, sf_cursor, _upd_tbl, _schema)
                        _fcounts.append((_fn, _rc if _err is None else -1, _hh))
                    _ora_cnt = sum(c for _, c, _ in _fcounts if c >= 0)
                    _upd_result = 'Error' if any(c == -1 for _, c, _ in _fcounts) else 'N/A'
                else:
                    _ora_cnt = 0
                    _upd_result = 'N/A'
                lines.append(f"  [UPD] {_upd_tbl}".ljust(DR_W) +
                              f" | {_ora_cnt:>{CNT_W},} | {'':>{CNT_W}} | {_upd_result:<{STS_W}}")
                for fname, fcount, has_hdr in _fcounts:
                    if fcount >= 0:
                        lines.append(f"         - {fname}: {fcount:,} records"
                                     f"{' (with header)' if has_hdr else ' (no header)'}")
                _upd_status = 'SUCCESS' if _upd_result in ('SUCCESS', 'N/A') else 'FAIL'
                table_outcomes.append({
                    'table': _upd_tbl, 'status': _upd_status,
                    'reason': f'UPD file count={_ora_cnt}, Result={_upd_result}',
                    'src_count': _ora_cnt, 'tgt_count': 0
                })

            lines.append(div)
            lines.append('')

    # Summary footer
    total_tables = pass_count + fail_count
    lines.append(dbl)
    lines.append('VALIDATION SUMMARY'.center(W))
    lines.append(dbl)
    lines.append(f'  Total Tables : {total_tables}')
    lines.append(f'  PASS         : {pass_count}')
    lines.append(f'  FAIL         : {fail_count}')
    lines.append('')
    if fail_count > 0:
        lines.append('*** SOME VALIDATIONS FAILED - review drilldown details above ***'.center(W))
    else:
        lines.append('*** ALL VALIDATIONS PASSED ***'.center(W))
    lines.append(dbl)

    with open(output_file, 'w', encoding='ascii', errors='replace') as f:
        f.write('\n'.join(lines) + '\n')

    query_ref_file = write_query_reference(file_path, appl_name, tenant_id, process_date, query_list)

    # Console summary
    print(f'\n{dbl}')
    print('VALIDATION SUMMARY'.center(W))
    print(dbl)
    print(f'Total Tables:           {total_tables}')
    print(f'Passed:                 {pass_count}')
    print(f'Failed:                 {fail_count}')
    print(dbl)

    if did_not_fail:
        print(f'\n✓ Record count validation is SUCCESSFUL for tenant_id: {tenant_id}.')
    else:
        print(f'\n✗ Record count validation FAILED for tenant_id {tenant_id}.')

    print(f'\nOutput Files:')
    print(f'  Validation Report: {output_file}')
    print(f'  Query Reference:   {query_ref_file}')

    return table_outcomes, output_file, query_ref_file

def load_validation():
    """Main function to execute the validation process."""
    script_start = time.perf_counter()
    argument_dict = parse_arguments()

    tenant_id_tuple = tuple(argument_dict["tenant_id"].split(','))
    py_path = os.environ["PYTHONPATH"]
    ingest_yaml_file = f'{py_path}/IngestionConfig.yaml'
    ingest_cfg_dict = load_yaml(yaml_file=ingest_yaml_file)
    orc_conn_yaml = f'{py_path}/orc_connections.yaml'
    # orc_conn_dict removed - Oracle connection now reads from orc_connections.toml via SNOWFLAKE_HOME

    sf_conn = open_sf_connection(ingest_cfg=ingest_cfg_dict)
    sf_cs = sf_conn.cursor()
    appl_code = get_appl_code(argument_dict["appl_name"], sf_cs)
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    logging_config(argument_dict.get('logging_directory'), appl_code, script_name, ingest_cfg_dict)
    print("✓ Logged into Snowflake")

    file_path = f"{ingest_cfg_dict['snowflake_connection']['validation_path']}/{argument_dict['appl_name']}/{script_name}"
    os.makedirs(file_path, exist_ok=True)
    

    tables = get_tables_from_appl_table(sf_cs, appl_code)
    dtm_tables = get_snowflake_dtm_views(sf_conn, argument_dict["appl_name"])
    tables = tables + dtm_tables
    fmt_tables = get_fmt_tables(argument_dict["appl_name"], sf_cs)
    upd_tables = get_upd_tables(argument_dict["appl_name"], sf_cs)

    table_outcomes = []
    output_file = ''
    query_ref_files = []
    env = os.environ.get('PRJ_ENVIRONMENT', '')
    for tenant_id in tenant_id_tuple:
        app_db_name = setup_tenant_shard(tenant_id, sf_cs, env, tenant_id_tuple)
        # setup_tenant_shard issues 'USE DATABASE <shard_db>' on the cursor;
        # reset back to the curated DB so information_schema queries and
        # unqualified table references resolve correctly inside rec_count_validation.
        sf_cs.execute(f'USE DATABASE {DEFAULT_DATABASE};')
        ora_user = f'DW{tenant_id}'
        ora_conn = open_oracle_connection(myuser=ora_user)
        ora_cs = ora_conn.cursor()
        print(f'✓ Logged into Oracle as {ora_user}')

        outcomes, output_file, query_ref_file = rec_count_validation(ora_cursor=ora_cs, sf_cursor=sf_cs, tenant_id=tenant_id, table_list=tables,
                             process_date=argument_dict["process_date"], appl_name=argument_dict["appl_name"],
                             appl_code=appl_code, table_fmt_list=fmt_tables,
                             table_upd_list=upd_tables, proc_date=argument_dict["prc_dte"],
                             file_path=file_path, app_db_name=app_db_name)
        table_outcomes.extend(outcomes)
        query_ref_files.append(query_ref_file)
        ora_cs.close()
        ora_conn.close()
        print(f'✓ Oracle connection closed for {ora_user}')

    sf_cs.close()
    sf_conn.close()
    print('✓ Snowflake connection closed')

    script_end = time.perf_counter()
    script_run_time = script_end - script_start

    if argument_dict.get('load_sf_meta'):
        try:
            sf_conn_meta = open_sf_connection(ingest_cfg=ingest_cfg_dict)
            meta_cur = sf_conn_meta.cursor()
            registry = TestCaseRegistry(meta_cur, SCRIPT_NAME,
                                        database=argument_dict.get('sf_meta_db'),
                                        schema=argument_dict.get('sf_meta_schema'))
            val_results = []
            for outcome in table_outcomes:
                src_c = outcome.get('src_count', 0)
                tgt_c = outcome.get('tgt_count', 0)
                val_results.append(registry.create_result(
                    validation_key='record_count',
                    test_scenario=f'Count validation: {outcome["table"]}',
                    appl_name=argument_dict['appl_name'],
                    appl_code=appl_code,
                    tenant_id=argument_dict['tenant_id'],
                    table_name=outcome['table'],
                    validation_status=outcome['status'],
                    status_reason=outcome['reason'],
                    source_count=src_c,
                    target_count=tgt_c,
                    mismatched_count=abs(src_c - tgt_c) if outcome['status'] == 'FAIL' else 0,
                    matched_count=min(src_c, tgt_c),
                    execution_time_sec=script_run_time / max(len(table_outcomes), 1)
                ))
            if not val_results:
                val_results.append(registry.create_result(
                    validation_key='record_count',
                    test_scenario='Compare record counts between Oracle and Snowflake',
                    appl_name=argument_dict['appl_name'],
                    appl_code=appl_code,
                    tenant_id=argument_dict['tenant_id'],
                    table_name='ALL_TABLES', validation_status='PASS',
                    status_reason='No tables processed',
                    mismatched_count=0,
                    matched_count=0
                ))
            loader = ValidationLoader(
                sf_cursor=meta_cur, arg_dict=argument_dict,
                script_name=SCRIPT_NAME, script_version=SCRIPT_VERSION,
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
            summary.update_counts(val_results)
            summary.execution_time_sec = script_run_time
            if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
                summary.read_and_store_output(output_file, file_type='count_surr_key_report')
            for qrf in query_ref_files:
                if os.path.exists(qrf) and os.path.getsize(qrf) > 0:
                    summary.read_and_store_output(qrf, file_type='query_reference')
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
                capped_batch, _, _ = cap_details(detail_batch)
                loader.insert_detail_bulk(capped_batch)

            summary.emit_summary_line()
            passed = sum(1 for o in table_outcomes if o['status'] == 'SUCCESS')
            failed = sum(1 for o in table_outcomes if o['status'] == 'FAIL')
            logging.info(f"Loaded {len(val_results)} result(s) — {passed} PASS, {failed} FAIL")
            sf_conn_meta.close()
        except Exception as e:
            logging.error(f"Failed to load validation results to Snowflake: {str(e)}")
            traceback.print_exc()
    else:
        logging.info("Skipping metadata load to Snowflake (--load-sf-meta not specified)")

    logging.info(f'Total script runtime: {script_run_time:.2f} seconds ({script_run_time / 60:.2f} minutes)')
    print(f"\n{'-' * 50}")
    print(f'Total Script Runtime: {script_run_time:.2f} seconds ({script_run_time / 60:.2f} minutes)')
    print(f"{'-' * 50}")


def main() -> None:
    """Main entry point for the script."""
    try:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(funcName)s - %(message)s',
            handlers=[logging.StreamHandler()]
        )
        logging.info('=' * 80)
        logging.info('DDW Count Validation Script Started')
        logging.info('=' * 80)
        load_validation()
        logging.info('DDW Count Validation Script Completed Successfully')
    except Exception as e:
        logging.error(f'Script failed with error: {str(e)}')
        logging.error(traceback.format_exc())
        print(f'\n✗ Script failed: {str(e)}')
        raise


if __name__ == '__main__':
    main()