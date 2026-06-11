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





#!/usr/bin/env python3
# ============================================================================
# Hash Comparison Validation Script v1.0 - FIS Edition
# ============================================================================
# Purpose:
#   Row-level and table-level hash comparison between Oracle (single-tenant)
#   and Snowflake (multi-tenant) for FIS DDW applications.
#
# Architecture:
#   Oracle:    DW{tenant_id}.{table_name}  (no TENANT_ID filter)
#   Snowflake: CUR_IBS.{appl_name}.{table_name} WHERE TENANT_ID = '{tenant_id}'
#
# Strategy:
#   1. Auto-discover tables from Snowflake INFORMATION_SCHEMA
#   2. For each table:
#      a. Get column metadata (types, precision, scale)
#      b. Build canonicalized hash expressions (Oracle + Snowflake)
#      c. Compare table-level aggregate hashes (fast path)
#      d. If mismatch: drill down to row-level hash comparison
#   3. Write results (TXT summary, CSV detail, SQL debug, Snowflake dashboard)
#
# Usage:
#   python -m FieldValidationDDW --a DDW_LN --c 16 --t 6A --p 20251210 --l INFO --o /log/path
#
# Input:
#   --a: Application Name (required) - Snowflake schema name
#   --c: Application Code (required) - Oracle DATA_SRC_CDE value
#   --t: Tenant ID (optional) - if not provided, runs for ALL tenants
#   --p: Process Date YYYYMMDD
#   --l: Log Level (default: INFO)
#   --o: Log/Output Directory (required)
#   --tables: Comma-separated table names (optional, auto-discover if omitted)
#   --types: Table types to validate: SCD,FACT,DIM,OTHER (default: all)
#   --mode: Comparison mode: table_first, row_level, count_only (default: table_first)
#   --drill-down: Enable row-level drill-down on table hash mismatch (default: True)
#   --max-mismatches: Max row mismatches to collect (default: 100)
#   --load-to-sf: Load results to Snowflake dashboard table (default: False)
#   --biz-keys: Path to business keys CSV config (optional)
#   --ora-schema: Custom Oracle schema (overrides DW{tenant_id} pattern)
#   --sf-database: Snowflake database name (default: CUR_IBS)
#   --no-ddw: Disable DDW mode (allow all table names, not just TB_*)
#   --include-c2: Also discover/validate TB_C2 tables from DDW_CNF_DIM schema
#   --c2-only: Validate ONLY TB_C2 tables (skip regular app-schema tables)
#
# Created: February 2026
# Version: 1.0
# ============================================================================

import csv
import logging
import os
import sys
import time
import gc
import traceback
from datetime import datetime, timedelta
from itertools import combinations

# Add the run_hash_rowlevel_v2.1 directory to path for module imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'run_hash_rowlevel_v2.1'))

from connections import (
    load_yaml,
    open_sf_connection,
    open_oracle_connection,
    get_oracle_credentials,
)
from table_discovery import (
    get_all_tables,
    get_c2_tables,
    get_application_codes,
    get_table_metadata,
    prefetch_all_table_metadata,
    prefetch_oracle_columns,
    clear_caches,
    get_tenants_for_app,
    resolve_oracle_table,
    get_oracle_columns,
    filter_columns_to_oracle,
    get_tables_from_appl_table,
    get_tb_c2_filter,
    prefetch_tb_c2_filters,
)
from hash_comparator import (
    compare_table_hashes,
    compare_table_hashes_date_range,
    compare_row_hashes,
    compare_row_hashes_ranged,
    compare_row_hashes_keyless,
    compare_counts_only,
    TableComparisonResult,
    RANGE_DRILL_THRESHOLD,
)
from result_writer import (
    write_summary_report,
    write_csv_results,
    write_debug_queries,
    load_results_to_snowflake,
    ensure_output_dir,
    write_fv_summary,
    write_fv_diff_files,
)
from validation_loader import build_validation_results

# sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# from validation_utils import (
#     ValidationLoader,
#     ValidationResult,
#     ValidationDetailResult,
#     TestCaseRegistry,
#     ExecutionSummary,
#     cap_details,
#     read_diff_file,
# )
from script_utils import get_tenant_number, logging_config

SCRIPT_NAME = "FieldValidationDDW.py"
SCRIPT_VERSION = "v2.0"


def _resolve_appl_code(sf_cursor, database, schema, appl_name, oracle_code):
    """Resolve APPLICATION_CONFIG.APPL_CODE from APPL_NAME.
    
    The hash script's --c arg is the Oracle DATA_SRC_CDE (e.g. '16'),
    but the dashboard expects the short APPL_CODE (e.g. 'LN') from
    APPLICATION_CONFIG. This function looks up the mapping.
    Falls back to the Oracle code if no match is found.
    """
    try:
        sf_cursor.execute(
            f"SELECT APPL_CODE FROM {database}.{schema}.APPLICATION_CONFIG "
            f"WHERE APPL_NAME = %s AND IS_ACTIVE = 'Y'",
            (appl_name,)
        )
        row = sf_cursor.fetchone()
        if row:
            logging.info(f"Resolved APPL_CODE: {appl_name} -> {row[0]} (from APPLICATION_CONFIG)")
            return row[0]
    except Exception as e:
        logging.warning(f"Could not resolve APPL_CODE from APPLICATION_CONFIG: {e}")
    logging.info(f"Using Oracle DATA_SRC_CDE as APPL_CODE: {oracle_code}")
    return oracle_code


def resolve_dw_trim_length(tenant_number_arg: str, tenant_id: str, sf_cursor) -> int:
    """Resolve how many characters to trim from DW_ID columns.

    Args:
        tenant_number_arg: 'N' (exclude DW_ID), 'Y' (fetch from DB), or a literal value
        tenant_id: Current tenant ID (used when fetching from DB)
        sf_cursor: Snowflake cursor (used when fetching from DB)

    Returns:
        -1 if DW_ID should be excluded entirely,
        0 if entire DW_ID value should be used (no trim),
        >0 = number of leading characters to remove from DW_ID values.
    """
    if tenant_number_arg.upper() == 'N':
        return -1  # exclude DW_ID columns entirely

    if tenant_number_arg.upper() == 'Y':
        tenant_number = get_tenant_number(tenant_id, sf_cursor)
    else:
        tenant_number = tenant_number_arg  # literal value

    if not tenant_number:
        logging.info("Tenant number is empty/None — using full DW_ID value (trim length=0)")
        return 0

    trim_len = len(str(tenant_number))
    logging.info(f"Tenant number='{tenant_number}' (length={trim_len}) — will trim first {trim_len} chars from DW_ID values")
    return trim_len


def arg_parsing() -> dict:
    """Parse command-line arguments using script_utils central registry."""
    from script_utils import parse_args, get_appl_code

    args = parse_args(
        required=['--a'],
        optional=['--t', '--p', '--l', '--o', '--tb', '--t_nbr',
                  '--start_date', '--end_date', '--types', '--biz-keys',
                  '--load-sf-meta', '--sf-meta-db', '--sf-meta-schema'],
        description='Hash Comparison Validation between Oracle and Snowflake (DDW).',
    )

    # Validate date arguments
    pdate = args.get('process_date')
    start_date = args.get('start_date')
    end_date = args.get('end_date')

    if not pdate and not start_date:
        raise ValueError("Either --p (single date) or --start_date/--end_date (date range) is required")
    if start_date and not end_date:
        raise ValueError("--end_date is required when using --start_date")
    if end_date and not start_date:
        raise ValueError("--start_date is required when using --end_date")
    if pdate and start_date:
        raise ValueError("Cannot use both --p and --start_date/--end_date")

    # Build date list
    if start_date and end_date:
        start_ts = args.get('start_date_ts', start_date)
        end_ts = args.get('end_date_ts', end_date)
        date_list = _generate_date_range(start_date, end_date)
        if not date_list:
            raise ValueError(f"Invalid date range: {start_date} to {end_date}")
        pdate = date_list[0]
    else:
        date_list = [pdate]

    process_date_ts = f'{pdate[0:4]}-{pdate[4:6]}-{pdate[6:8]}'

    args_dict = {
        'appl_name': args['appl_name'],
        'appl_code': None,  # Resolved after SF connection via get_appl_code()
        'tenant_id': args.get('tenant_id'),
        'process_date': pdate,
        'process_date_ts': process_date_ts,
        'proc_yr_mth': pdate[0:6],
        'log_level': args.get('log_level', 'INFO'),
        'output_directory': args.get('logging_directory', ''),
        'specific_tables': args['table_filter'].split(',') if args.get('table_filter') else None,
        'table_types': args['table_types'].split(',') if args.get('table_types') else None,
        'comparison_mode': 'table_first',
        'drill_down': True,
        'max_mismatches': 100,
        'load_to_sf': False,
        'biz_keys_path': args.get('biz_keys'),
        'ora_schema': None,
        'sf_database': 'CUR_IBS',
        'ddw_mode': True,
        'table_pattern': None,
        'exclude_surrogates': [],
        'load_sf_meta': args.get('load_sf_meta', False),
        'sf_meta_db': args.get('sf_meta_db'),
        'sf_meta_schema': args.get('sf_meta_schema', 'VALIDATION_DASHBOARD'),
        'sf_detail_mode': 'sample',
        'sf_samples_per_category': 5,
        'include_c2': False,
        'c2_yaml': None,
        'platform_detail': False,
        'c2_only': False,
        'date_list': date_list,
        'test_case_id': 'DDW_D1_07',
        'test_case_name': 'DDW Day1: Row-Level Hash Comparison',
        'tenant_number_arg': args.get('tenant_number', 'N'),
    }

    return args_dict


def _generate_date_range(start_str: str, end_str: str) -> list:
    """Generate list of YYYYMMDD date strings from start to end (inclusive)."""
    try:
        start = datetime.strptime(start_str, '%Y%m%d')
        end = datetime.strptime(end_str, '%Y%m%d')
    except ValueError:
        return []
    if end < start:
        return []
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime('%Y%m%d'))
        current += timedelta(days=1)
    return dates


def setup_logging(arg_dict: dict) -> None:
    """Configure logging using script_utils.logging_config()."""
    log_dir = arg_dict['output_directory']

    py_path = os.environ.get("PYTHONPATH", "")
    ingest_yaml_file = os.path.join(py_path, 'IngestionConfig.yaml')
    if os.path.exists(ingest_yaml_file):
        ingest_cfg = load_yaml(yaml_file=ingest_yaml_file)
    else:
        ingest_cfg = {}

    script_name = os.path.splitext(os.path.basename(__file__))[0]
    appl_code = arg_dict.get('appl_code') or arg_dict['appl_name']

    logging_config(log_dir, appl_code, script_name, ingest_cfg, arg_dict.get('log_level', 'INFO'))


def load_business_keys(biz_keys_path: str, appl_code: str) -> tuple:
    """
    Load business keys and exclude columns configuration from CSV file.

    CSV format:
        table_name,business_key,exclude_columns
        TB_C2_DA0_ACCT_SCD,ACCT_NBR,COL_TO_IGNORE1|COL_TO_IGNORE2
        TB_C2_DA0_CUST_SCD,CUST_NBR,
        TB_C2_DA0_TXN_FACT,TXN_ID,AUDIT_COL|TEMP_COL

    The exclude_columns field is optional and pipe-separated (|).
    Columns listed here will be excluded from the hash comparison for that table.

    Args:
        biz_keys_path: Path to CSV file
        appl_code: Application code (for default file naming)

    Returns:
        Tuple of (biz_keys_config, exclude_columns_config):
          - biz_keys_config: Dict mapping table_name -> list of key column names
          - exclude_columns_config: Dict mapping table_name -> set of column names to exclude
    """
    biz_config = {}
    exclude_config = {}

    if not biz_keys_path:
        # Try default location
        script_dir = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(script_dir, f'config/business_keys_{appl_code}.csv')
        if not os.path.exists(csv_path):
            logging.info(f"No business keys config found at {csv_path}")
            return biz_config, exclude_config
        biz_keys_path = csv_path

    if not os.path.exists(biz_keys_path):
        logging.warning(f"Business keys file not found: {biz_keys_path}")
        return biz_config, exclude_config

    logging.info(f"Loading business keys from: {biz_keys_path}")

    try:
        with open(biz_keys_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                table_name = row.get('table_name', '').strip().upper()
                biz_key = row.get('business_key', '').strip()
                exclude_cols = row.get('exclude_columns', '').strip()
                if table_name and biz_key:
                    biz_config[table_name] = [k.strip() for k in biz_key.split(',')]
                if table_name and exclude_cols:
                    exclude_config[table_name] = {
                        c.strip().upper() for c in exclude_cols.split('|') if c.strip()
                    }
        logging.info(
            f"Loaded business keys for {len(biz_config)} tables, "
            f"exclude columns for {len(exclude_config)} tables"
        )
    except Exception as e:
        logging.error(f"Error loading business keys: {str(e)}")

    return biz_config, exclude_config


def probe_oracle_app_codes(
    ora_cursor, table_name: str, tenant_id: str, table_meta, c2_appl_codes: list
) -> list:
    """
    Probe Oracle table to find which application codes actually have data
    in LOG_DATA_SRC_CDE or DATA_SRC_CDE columns.

    Instead of blindly using all lookup codes in an IN clause, this tests each
    code against the actual Oracle table. Returns only codes that have data.

    Args:
        ora_cursor: Oracle cursor
        table_name: Table name
        tenant_id: Tenant ID for Oracle schema
        table_meta: TableMetadata with column flags
        c2_appl_codes: List of app codes to probe

    Returns:
        Filtered list of codes that actually have data, or original list on error
    """
    if not c2_appl_codes or not table_meta.is_c2:
        return c2_appl_codes

    ora_table = f"DW{tenant_id}.{table_name}"

    column = None
    if table_meta.has_log_data_src_cde:
        column = 'LOG_DATA_SRC_CDE'
    elif table_meta.has_data_src_cde:
        column = 'DATA_SRC_CDE'

    if not column:
        return c2_appl_codes

    # Single query to find all valid codes instead of one query per code
    codes_str = ", ".join(f"'{c}'" for c in c2_appl_codes)
    valid_codes = []
    try:
        ora_cursor.execute(
            f"SELECT DISTINCT {column} FROM {ora_table} "
            f"WHERE {column} IN ({codes_str}) AND ROWNUM <= {len(c2_appl_codes)}"
        )
        found = {row[0] for row in ora_cursor.fetchall()}
        valid_codes = [c for c in c2_appl_codes if c in found]
    except Exception:
        valid_codes = list(c2_appl_codes)

    if valid_codes:
        if set(valid_codes) != set(c2_appl_codes):
            logging.info(
                f"  [AppCodeProbe] {table_name}: {column} has data for "
                f"{valid_codes} (out of {c2_appl_codes})"
            )
        return valid_codes

    logging.warning(
        f"  [AppCodeProbe] {table_name}: No data found for any code in {column}. "
        f"Using all codes as fallback."
    )
    return c2_appl_codes


def infer_business_key(
    sf_cursor, appl_name: str, table_name: str, sf_database: str = 'CUR_IBS',
    tenant_id: str = None, ora_cursor=None,
    table_meta=None, process_date_ts: str = None,
    end_date_ts: str = None,
    dw_trim_length: int = -1,
) -> list:
    """
    Infer business key by analyzing actual data in the table.

    Multi-layered approach:
      Layer 0: Check Oracle PK constraints (ALL_CONSTRAINTS + ALL_CONS_COLUMNS)
               Oracle PKs often contain real business keys unlike Snowflake surrogates
      Layer 1: Check Snowflake PRIMARY KEY / UNIQUE constraints (free metadata)
      Layer 2: Profile candidate columns against real data -- compute uniqueness
               ratio, null rate, and cardinality to find true key columns
      Layer 3: Try 2- and 3-column composite keys if no single column is unique
      Layer 4: Fall back to column-name heuristic (_NBR > _CD > _KEY > _ID)

    A column qualifies as a business key if:
      - Uniqueness ratio >= 0.98 (distinct values / total rows)
      - Null ratio < 0.01
      - Not a system/metadata/surrogate column

    For TB_C2 tables, the schema is automatically set to DDW_CNF_DIM.

    Args:
        sf_cursor: Snowflake cursor
        appl_name: Application name (Snowflake schema)
        table_name: Table name
        sf_database: Snowflake database name (default: CUR_IBS)
        tenant_id: Optional tenant filter for multi-tenant tables
        ora_cursor: Optional Oracle cursor for PK constraint lookup

    Returns:
        List of inferred key column names (may be empty)
    """
    from table_discovery import C2_SCHEMA
    is_dtm = 'DTM' in table_name.upper() or 'DAY_TME' in table_name.upper()
    if is_dtm:
        schema = appl_name
        sf_name = ('VW_' + table_name.upper()[3:]) if table_name.upper().startswith('TB_') else table_name
    elif table_name.upper().startswith('TB_C2_'):
        schema = C2_SCHEMA
        sf_name = table_name
    else:
        schema = appl_name
        sf_name = table_name
    fqtn = f"{sf_database}.{schema}.{sf_name}"

    # ------------------------------------------------------------------
    # Layer 0: Check Oracle PK constraints (more reliable for business keys)
    # ------------------------------------------------------------------
    if ora_cursor and tenant_id:
        oracle_pk = _infer_from_oracle_constraints(ora_cursor, table_name, tenant_id, dw_trim_length)
        if oracle_pk:
            logging.info(f"  [KeyInfer] Oracle PK constraint key: {oracle_pk}")
            return oracle_pk

    # ------------------------------------------------------------------
    # Layer 1: Check Snowflake constraints (PK / UNIQUE)
    # ------------------------------------------------------------------
    key_from_constraint = _infer_from_constraints(sf_cursor, fqtn, table_name, dw_trim_length)
    if key_from_constraint:
        logging.info(f"  [KeyInfer] Found constraint-based key: {key_from_constraint}")
        return key_from_constraint

    # ------------------------------------------------------------------
    # Layer 2: Profile real data for single-column keys
    # ------------------------------------------------------------------
    # Build filter that matches the actual comparison WHERE clause.
    # Without SCD filters, profiling sees historical rows where business keys
    # repeat across SOR_EXP_DTE values, making keys appear non-unique.
    # For date-range runs, use BETWEEN so profiling sees enough rows.
    filter_parts = []
    if tenant_id:
        filter_parts.append(f"TENANT_ID = '{tenant_id}'")
    if table_meta:
        if table_meta.table_type == 'SCD':
            if table_meta.has_sor_exp_dte:
                filter_parts.append("SOR_EXP_DTE = '4444-12-31'")
            if table_meta.has_cur_rec_ind:
                filter_parts.append("CUR_REC_IND = 'Y'")
            if table_meta.has_eff_dte and process_date_ts:
                if end_date_ts and end_date_ts != process_date_ts:
                    filter_parts.append(
                        f"EFF_DTE BETWEEN '{process_date_ts}' AND '{end_date_ts}'"
                    )
                else:
                    filter_parts.append(f"EFF_DTE = '{process_date_ts}'")
        else:
            if table_meta.has_prcs_dte and process_date_ts:
                if end_date_ts and end_date_ts != process_date_ts:
                    filter_parts.append(
                        f"PRCS_DTE BETWEEN TO_DATE('{process_date_ts}', 'YYYY-MM-DD') "
                        f"AND TO_DATE('{end_date_ts}', 'YYYY-MM-DD')"
                    )
                else:
                    filter_parts.append(
                        f"PRCS_DTE = TO_DATE('{process_date_ts}', 'YYYY-MM-DD')"
                    )
    tenant_filter = "WHERE " + " AND ".join(filter_parts) if filter_parts else ""

    candidates = _get_candidate_columns(sf_cursor, sf_database, schema, sf_name)
    if not candidates:
        logging.info(f"  [KeyInfer] No candidate columns found, falling back to name heuristic")
        return _infer_from_column_names(
            sf_cursor, sf_database, schema, table_name,
            tenant_id=tenant_id, data_filter=tenant_filter
        )

    profile = _profile_columns(sf_cursor, fqtn, candidates, tenant_filter)
    if not profile:
        logging.info(f"  [KeyInfer] Profiling returned no data, falling back to name heuristic")
        return _infer_from_column_names(
            sf_cursor, sf_database, schema, table_name,
            tenant_id=tenant_id, data_filter=tenant_filter
        )

    total_rows = profile['total_rows']
    if total_rows == 0:
        logging.info(
            f"  [KeyInfer] Profiling returned 0 rows for {table_name}, "
            f"falling back to name heuristic"
        )
        return _infer_from_column_names(
            sf_cursor, sf_database, schema, table_name,
            tenant_id=tenant_id, data_filter=tenant_filter
        )

    # Find single columns with uniqueness >= 98%
    single_winners = []
    for col_name, stats in profile['columns'].items():
        uniq_ratio = stats['distinct'] / total_rows if total_rows > 0 else 0
        null_ratio = stats['nulls'] / total_rows if total_rows > 0 else 1
        if uniq_ratio >= 0.98 and null_ratio < 0.01:
            single_winners.append((col_name, uniq_ratio, stats['name_score']))

    if single_winners:
        single_winners.sort(key=lambda x: (-x[1], x[2]))
        chosen = single_winners[0][0]
        ratio_pct = single_winners[0][1] * 100
        logging.info(
            f"  [KeyInfer] Data-driven key: [{chosen}] "
            f"(uniqueness={ratio_pct:.1f}%, rows={total_rows})"
        )
        return [chosen]

    # ------------------------------------------------------------------
    # Layer 3: Try 2- and 3-column composite keys
    # ------------------------------------------------------------------
    top_candidates = sorted(
        profile['columns'].items(),
        key=lambda x: (-x[1]['distinct'], x[1]['name_score'])
    )[:6]

    composite_key = _try_composite_keys(
        sf_cursor, fqtn, top_candidates, total_rows, tenant_filter
    )
    if composite_key:
        logging.info(
            f"  [KeyInfer] Data-driven composite key: {composite_key} (rows={total_rows})"
        )
        return composite_key

    # ------------------------------------------------------------------
    # Layer 4: Fall back to name-based heuristic
    # ------------------------------------------------------------------
    logging.info(f"  [KeyInfer] No unique key found in data, falling back to name heuristic")
    return _infer_from_column_names(
        sf_cursor, sf_database, schema, table_name,
        tenant_id=tenant_id, data_filter=tenant_filter
    )


CONSTRAINT_EXCLUDE_COLUMNS = {
    'TENANT_ID', 'SOR_EXP_DTE', 'PRCS_EXP_DTE', 'EFF_DTE',
    'PRCS_DTE', 'FULL_DTE', 'CUR_REC_IND', 'DATA_SRC_CDE', 'LOG_DATA_SRC_CDE',
    'SOURCE_FILE', 'LOAD_TS', 'PRCS_YR_MTH_NBR',
    'RUN_ID', 'BATCH_ID', 'HASH_KEY_TXT', 'HASH_DATA_VAL_TXT',
}

CONSTRAINT_EXCLUDE_PREFIXES = ('DW_',)


def _get_key_exclude_prefixes(dw_trim_length: int = -1) -> tuple:
    """Return prefix exclusion tuple for business key inference.

    When dw_trim_length >= 0 (DW_* columns are included in hash),
    DW_* columns should also be eligible as business keys.
    """
    if dw_trim_length >= 0:
        return ()  # Don't exclude DW_* from key inference
    return CONSTRAINT_EXCLUDE_PREFIXES


def _infer_from_constraints(sf_cursor, fqtn: str, table_name: str, dw_trim_length: int = -1) -> list:
    """Check Snowflake SHOW PRIMARY KEYS / SHOW UNIQUE KEYS.

    Filters out system/SCD columns that are part of DDW composite PKs
    (e.g. DW_xxx_KEY, TENANT_ID, SOR_EXP_DTE) since those are not
    business keys. When dw_trim_length >= 0, DW_* columns are kept.
    """
    exclude_upper = {c.upper() for c in CONSTRAINT_EXCLUDE_COLUMNS}
    prefix_upper = tuple(p.upper() for p in _get_key_exclude_prefixes(dw_trim_length))

    for cmd in [f"SHOW PRIMARY KEYS IN TABLE {fqtn}",
                f"SHOW UNIQUE KEYS IN TABLE {fqtn}"]:
        try:
            sf_cursor.execute(cmd)
            rows = sf_cursor.fetchall()
            if rows:
                desc = [d[0].upper() for d in sf_cursor.description]
                col_idx = desc.index('COLUMN_NAME') if 'COLUMN_NAME' in desc else 4
                key_cols = [r[col_idx] for r in rows]
                key_cols = [
                    c for c in key_cols
                    if c.upper() not in exclude_upper
                    and not any(c.upper().startswith(p) for p in prefix_upper)
                ]
                if key_cols:
                    return key_cols
        except Exception:
            continue
    return []


_oracle_pk_cache: dict = {}


def prefetch_oracle_pk_constraints(ora_cursor, table_names: list, tenant_id: str, dw_trim_length: int = -1) -> None:
    """
    Bulk-fetch Oracle PK constraints for all tables at once.

    Instead of 2 queries per table (one per owner suffix), runs a single
    query per owner suffix covering ALL tables. Cached results are used
    by _infer_from_oracle_constraints.
    """
    global _oracle_pk_cache
    exclude_upper = {c.upper() for c in CONSTRAINT_EXCLUDE_COLUMNS}
    prefix_upper = tuple(p.upper() for p in _get_key_exclude_prefixes(dw_trim_length))

    table_list = ", ".join(f"'{t.upper()}'" for t in table_names)

    for suffix in ('1', ''):
        owner = f'DW{tenant_id}{suffix}'
        pk_query = (
            f"SELECT b.TABLE_NAME, b.COLUMN_NAME "
            f"FROM ALL_CONSTRAINTS a "
            f"JOIN ALL_CONS_COLUMNS b "
            f"  ON a.OWNER = b.OWNER "
            f"  AND a.TABLE_NAME = b.TABLE_NAME "
            f"  AND a.CONSTRAINT_NAME = b.CONSTRAINT_NAME "
            f"WHERE a.OWNER = '{owner}' "
            f"  AND a.TABLE_NAME IN ({table_list}) "
            f"  AND a.CONSTRAINT_TYPE = 'P' "
            f"ORDER BY b.TABLE_NAME, b.POSITION"
        )
        try:
            ora_cursor.execute(pk_query)
            rows = ora_cursor.fetchall()
            by_table = {}
            for table, col in rows:
                by_table.setdefault(table.upper(), []).append(col)
            surr_only_tables = []
            for tname, cols in by_table.items():
                if tname in _oracle_pk_cache:
                    continue
                filtered = [
                    c for c in cols
                    if c.upper() not in exclude_upper
                    and not any(c.upper().startswith(p) for p in prefix_upper)
                ]
                if filtered:
                    _oracle_pk_cache[tname] = filtered
                else:
                    surr_only_tables.append(tname)
            if by_table:
                logging.info(
                    f"Prefetched Oracle PK constraints for {len(by_table)} tables "
                    f"from {owner}"
                )
            if surr_only_tables:
                logging.info(
                    f"  {len(surr_only_tables)} tables have surrogate-only PKs "
                    f"(will use data profiling for keys): "
                    f"{surr_only_tables}"
                )
        except Exception as e:
            logging.debug(
                f"Bulk Oracle PK prefetch failed for owner={owner}: {e}"
            )
            continue


def _infer_from_oracle_constraints(ora_cursor, table_name: str, tenant_id: str, dw_trim_length: int = -1) -> list:
    """
    Check Oracle ALL_CONSTRAINTS + ALL_CONS_COLUMNS for PRIMARY KEY columns.
    Uses the bulk-prefetched cache when available.

    If the PK is entirely surrogate/SCD columns, falls back to the DW_*_KEY
    column(s) which are usable for row-level joins in DDW tables.
    When dw_trim_length >= 0, DW_* columns are kept as valid business keys.
    """
    cached = _oracle_pk_cache.get(table_name.upper())
    if cached is not None:
        logging.info(f"  [KeyInfer] Oracle PK from cache: {cached}")
        return cached

    exclude_upper = {c.upper() for c in CONSTRAINT_EXCLUDE_COLUMNS}
    prefix_upper = tuple(p.upper() for p in _get_key_exclude_prefixes(dw_trim_length))

    for suffix in ('1', ''):
        owner = f'DW{tenant_id}{suffix}'
        pk_query = (
            f"SELECT b.COLUMN_NAME "
            f"FROM ALL_CONSTRAINTS a "
            f"JOIN ALL_CONS_COLUMNS b "
            f"  ON a.OWNER = b.OWNER "
            f"  AND a.TABLE_NAME = b.TABLE_NAME "
            f"  AND a.CONSTRAINT_NAME = b.CONSTRAINT_NAME "
            f"WHERE a.OWNER = '{owner}' "
            f"  AND a.TABLE_NAME = '{table_name}' "
            f"  AND a.CONSTRAINT_TYPE = 'P' "
            f"ORDER BY b.POSITION"
        )
        try:
            ora_cursor.execute(pk_query)
            rows = ora_cursor.fetchall()
            if rows:
                all_cols = [r[0] for r in rows]
                biz_cols = [
                    c for c in all_cols
                    if c.upper() not in exclude_upper
                    and not any(c.upper().startswith(p) for p in prefix_upper)
                ]
                if biz_cols:
                    logging.info(
                        f"  [KeyInfer] Oracle PK from {owner}: {biz_cols}"
                    )
                    return biz_cols
                logging.info(
                    f"  [KeyInfer] Oracle PK for {table_name} in {owner} "
                    f"is all surrogate/SCD columns: {all_cols} "
                    f"-- will fall through to data profiling"
                )
        except Exception as e:
            logging.debug(f"  [KeyInfer] Oracle PK query failed for owner={owner}: {e}")
            continue
    return []


def _get_candidate_columns(
    sf_cursor, sf_database: str, appl_name: str, table_name: str
) -> list:
    """Get columns that could plausibly be business keys (by type and name)."""
    query = f"""
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, ORDINAL_POSITION
        FROM {sf_database}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{appl_name}'
          AND TABLE_NAME = '{table_name}'
          AND DATA_TYPE IN ('TEXT', 'VARCHAR', 'NUMBER', 'FIXED', 'INTEGER', 'FLOAT')
          AND COLUMN_NAME NOT LIKE 'DW_%'
          AND COLUMN_NAME != 'TENANT_ID'
          AND COLUMN_NAME NOT LIKE '%HASH%'
          AND COLUMN_NAME NOT LIKE '%CHK%'
          AND COLUMN_NAME NOT LIKE '%LOAD%'
          AND COLUMN_NAME NOT LIKE '%BATCH%'
          AND COLUMN_NAME NOT LIKE '%RUN_ID%'
          AND COLUMN_NAME NOT LIKE '%SOURCE_FILE%'
          AND COLUMN_NAME NOT LIKE '%PRCS_YR%'
          AND COLUMN_NAME NOT LIKE '%_TS'
          AND COLUMN_NAME NOT LIKE '%_AMT'
          AND COLUMN_NAME NOT LIKE '%_BAL'
          AND COLUMN_NAME NOT LIKE '%_CNT'
          AND COLUMN_NAME NOT LIKE '%_PCT'
          AND COLUMN_NAME NOT LIKE '%_RATE'
          AND COLUMN_NAME NOT LIKE '%_DESC'
          AND COLUMN_NAME NOT LIKE '%_TXT'
          AND COLUMN_NAME NOT LIKE '%_IND'
          AND COLUMN_NAME NOT LIKE '%_FLAG'
        ORDER BY ORDINAL_POSITION
    """
    try:
        sf_cursor.execute(query)
        return sf_cursor.fetchall()
    except Exception:
        return []


def _name_priority_score(col_name: str) -> int:
    """Lower is better -- used as tiebreaker when uniqueness is equal."""
    upper = col_name.upper()
    if upper.endswith('_NBR'):
        return 1
    if upper.endswith('_CD') or upper.endswith('_CODE'):
        return 2
    if upper.endswith('_KEY'):
        return 3
    if upper.endswith('_ID'):
        return 4
    return 5


def _profile_columns(
    sf_cursor, fqtn: str, candidates: list, tenant_filter: str
) -> dict:
    """
    Run a single query that profiles all candidate columns at once:
      COUNT(*), and per column: COUNT(DISTINCT col), COUNT(*) - COUNT(col) as nulls.
    """
    if not candidates:
        return {}

    col_names = [row[0] for row in candidates]
    if len(col_names) > 20:
        scored = sorted(col_names, key=_name_priority_score)
        col_names = scored[:20]

    agg_parts = []
    for c in col_names:
        agg_parts.append(f'COUNT(DISTINCT "{c}") AS "dist_{c}"')
        agg_parts.append(f'SUM(CASE WHEN "{c}" IS NULL THEN 1 ELSE 0 END) AS "null_{c}"')

    agg_sql = ",\n        ".join(agg_parts)
    query = f"""
        SELECT COUNT(*) AS total_rows,
        {agg_sql}
        FROM {fqtn}
        {tenant_filter}
    """

    try:
        sf_cursor.execute(query)
        row = sf_cursor.fetchone()
    except Exception as e:
        logging.warning(f"  [KeyInfer] Profiling query failed: {e}")
        return {}

    if not row:
        return {}

    total_rows = row[0]
    columns = {}
    idx = 1
    for c in col_names:
        columns[c] = {
            'distinct': row[idx],
            'nulls': row[idx + 1],
            'name_score': _name_priority_score(c),
        }
        idx += 2

    return {'total_rows': total_rows, 'columns': columns}


def _try_composite_keys(
    sf_cursor, fqtn: str, top_candidates: list, total_rows: int, tenant_filter: str
) -> list:
    """Try 2-column and 3-column composites from top candidates.

    Uniqueness threshold is 95% (DDW tables often have composite keys
    that only reach high uniqueness at 3 columns).
    """
    COMPOSITE_THRESHOLD = 0.95
    col_names = [c[0] for c in top_candidates]

    if len(col_names) < 2:
        return []

    # Phase 1: 2-column combos
    pairs = list(combinations(col_names, 2))[:10]
    for col_a, col_b in pairs:
        query = f"""
            SELECT COUNT(DISTINCT CONCAT("{col_a}", '||', "{col_b}"))
            FROM {fqtn}
            {tenant_filter}
        """
        try:
            sf_cursor.execute(query)
            result = sf_cursor.fetchone()
            if result and result[0] >= total_rows * COMPOSITE_THRESHOLD:
                logging.debug(
                    f"  [KeyInfer] Composite ({col_a}, {col_b}): "
                    f"{result[0]}/{total_rows} = {result[0]/total_rows*100:.1f}%"
                )
                return [col_a, col_b]
        except Exception:
            continue

    # Phase 2: 3-column combos (DDW tables often need 3-part keys)
    if len(col_names) >= 3:
        triples = list(combinations(col_names, 3))[:15]
        for col_a, col_b, col_c in triples:
            query = f"""
                SELECT COUNT(DISTINCT CONCAT("{col_a}", '||', "{col_b}", '||', "{col_c}"))
                FROM {fqtn}
                {tenant_filter}
            """
            try:
                sf_cursor.execute(query)
                result = sf_cursor.fetchone()
                if result and result[0] >= total_rows * COMPOSITE_THRESHOLD:
                    logging.debug(
                        f"  [KeyInfer] Composite ({col_a}, {col_b}, {col_c}): "
                        f"{result[0]}/{total_rows} = {result[0]/total_rows*100:.1f}%"
                    )
                    return [col_a, col_b, col_c]
            except Exception:
                continue

    return []


def _infer_from_column_names(
    sf_cursor, sf_database: str, appl_name: str, table_name: str,
    tenant_id: str = None, data_filter: str = None,
) -> list:
    """Name-based heuristic returning a composite key (up to 3 columns).

    DDW tables typically have composite keys like (BNK_NBR, ACCT_NBR, NOTE_NBR).
    We return up to 3 best candidate columns sorted by naming convention
    priority, then verify the composite's uniqueness against actual data
    using the same date/tenant filter as the comparison queries.
    """
    from table_discovery import C2_SCHEMA
    is_dtm = 'DTM' in table_name.upper() or 'DAY_TME' in table_name.upper()
    if is_dtm:
        schema = appl_name
        sf_name = ('VW_' + table_name.upper()[3:]) if table_name.upper().startswith('TB_') else table_name
    elif table_name.upper().startswith('TB_C2_'):
        schema = C2_SCHEMA
        sf_name = table_name
    else:
        schema = appl_name
        sf_name = table_name
    fqtn = f"{sf_database}.{schema}.{sf_name}"

    query = f"""
        SELECT COLUMN_NAME
        FROM {sf_database}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}'
          AND TABLE_NAME = '{sf_name}'
          AND (
              COLUMN_NAME LIKE '%!_NBR' ESCAPE '!'
              OR COLUMN_NAME LIKE '%!_CDE' ESCAPE '!'
              OR COLUMN_NAME LIKE '%!_CD' ESCAPE '!'
              OR COLUMN_NAME LIKE '%!_CODE' ESCAPE '!'
              OR COLUMN_NAME LIKE '%!_KEY' ESCAPE '!'
              OR (COLUMN_NAME LIKE '%!_ID' ESCAPE '!' AND COLUMN_NAME NOT LIKE 'DW!_%' ESCAPE '!')
          )
          AND COLUMN_NAME NOT LIKE 'DW!_%' ESCAPE '!'
          AND COLUMN_NAME != 'TENANT_ID'
          AND COLUMN_NAME NOT IN ('PRCS_DTE', 'PRCS_EXP_DTE', 'PRCS_YR_MTH_NBR')
          AND COLUMN_NAME NOT LIKE '%SOR!_%' ESCAPE '!'
          AND COLUMN_NAME NOT LIKE '%EFF!_DTE%' ESCAPE '!'
          AND COLUMN_NAME NOT LIKE '%EXP!_DTE%' ESCAPE '!'
          AND COLUMN_NAME NOT LIKE 'CUR!_REC%' ESCAPE '!'
          AND COLUMN_NAME NOT LIKE '%HASH%'
          AND COLUMN_NAME NOT LIKE '%CHK%'
          AND COLUMN_NAME NOT LIKE '%SRC!_APPL%' ESCAPE '!'
          AND COLUMN_NAME NOT LIKE '%DATA!_SRC%' ESCAPE '!'
          AND COLUMN_NAME NOT LIKE '%LOG!_DATA%' ESCAPE '!'
        ORDER BY
            CASE
                WHEN COLUMN_NAME LIKE '%!_NBR' ESCAPE '!' THEN 1
                WHEN COLUMN_NAME LIKE '%!_CDE' ESCAPE '!' THEN 2
                WHEN COLUMN_NAME LIKE '%!_CD' ESCAPE '!' THEN 2
                WHEN COLUMN_NAME LIKE '%!_CODE' ESCAPE '!' THEN 2
                WHEN COLUMN_NAME LIKE '%!_KEY' ESCAPE '!' THEN 3
                ELSE 4
            END,
            ORDINAL_POSITION
        LIMIT 6
    """
    try:
        sf_cursor.execute(query)
        rows = sf_cursor.fetchall()
        if not rows:
            logging.info(f"  [KeyInfer] Name-heuristic: no candidate columns for {table_name}")
            return []

        candidates = [r[0] for r in rows]
        logging.info(f"  [KeyInfer] Name-heuristic candidates for {table_name}: {candidates}")

        if len(candidates) == 1:
            return candidates

        # Use the same date-aware filter as profiling so uniqueness
        # checks are against the same data slice being compared.
        verify_filter = data_filter if data_filter else (
            f"WHERE TENANT_ID = '{tenant_id}'" if tenant_id else ""
        )

        for size in range(len(candidates), 0, -1):
            subset = candidates[:size]
            concat_expr = " || '||' || ".join(f'COALESCE("{c}", \'\')' for c in subset)
            verify_sql = f"""
                SELECT COUNT(*) AS total, COUNT(DISTINCT ({concat_expr})) AS dist
                FROM {fqtn}
                {verify_filter}
            """
            try:
                sf_cursor.execute(verify_sql)
                vrow = sf_cursor.fetchone()
                if vrow and vrow[0] > 0:
                    total, dist = vrow[0], vrow[1]
                    ratio = dist / total
                    if ratio >= 0.95:
                        logging.info(
                            f"  [KeyInfer] Name-heuristic key {subset}: "
                            f"uniqueness={ratio*100:.1f}% ({dist}/{total})"
                        )
                        return subset
                    else:
                        logging.debug(
                            f"  [KeyInfer] Name-heuristic {subset}: "
                            f"uniqueness={ratio*100:.1f}% -- too low"
                        )
            except Exception:
                continue

        # None verified; return the top 2 _NBR columns as best-effort
        nbr_cols = [c for c in candidates if c.endswith('_NBR')]
        if len(nbr_cols) >= 2:
            logging.info(
                f"  [KeyInfer] Name-heuristic: no verified key, "
                f"using best-effort _NBR columns: {nbr_cols[:2]}"
            )
            return nbr_cols[:2]
        logging.info(
            f"  [KeyInfer] Name-heuristic: no verified key, "
            f"using best single candidate: {candidates[:1]}"
        )
        return candidates[:1]
    except Exception as e:
        logging.warning(f"  [KeyInfer] Name-heuristic failed for {table_name}: {e}")
    return []


def run_validation_for_tenant(
    sf_cursor,
    ora_cursor,
    arg_dict: dict,
    ingest_cfg_dict: dict,
    tenant_id: str
) -> list:
    """
    Run hash comparison validation for a single tenant.

    Args:
        sf_cursor: Snowflake cursor
        ora_cursor: Oracle cursor
        arg_dict: Arguments dictionary
        ingest_cfg_dict: Ingestion config
        tenant_id: Tenant ID to validate

    Returns:
        List of TableComparisonResult
    """
    appl_name = arg_dict['appl_name']
    appl_code = arg_dict['appl_code']
    process_date_ts = arg_dict['process_date_ts']
    comparison_mode = arg_dict['comparison_mode']
    ora_schema = arg_dict.get('ora_schema')
    sf_database = arg_dict.get('sf_database', 'CUR_IBS')
    ddw_mode = arg_dict.get('ddw_mode', True)

    # Resolve DW_ID trim length: -1=exclude, 0=full value, >0=trim N chars
    tenant_number_arg = arg_dict.get('tenant_number_arg', 'N')
    dw_trim_length = resolve_dw_trim_length(tenant_number_arg, tenant_id, sf_cursor)

    include_c2 = arg_dict.get('include_c2', False)
    c2_only = arg_dict.get('c2_only', False)

    # Reset caches for this tenant (important when running multiple tenants)
    clear_caches()
    global _oracle_pk_cache
    _oracle_pk_cache = {}

    logging.info("\n" + "=" * 80)
    logging.info(f"HASH COMPARISON VALIDATION - TENANT: {tenant_id}")
    logging.info(f"  Oracle schema: {ora_schema or f'DW{tenant_id} (default)'}")
    logging.info(f"  SF database:   {sf_database}")
    logging.info(f"  DDW mode:      {ddw_mode}")
    logging.info(f"  Include C2:    {include_c2}")
    logging.info(f"  C2 Only:       {c2_only}")
    logging.info("=" * 80)

    # ---- Application code lookup (needed for Oracle LOG_DATA_SRC_CDE filtering) ----
    # Oracle stores source codes like 'l5','l6' (from APPLICATION_CODE_NAME),
    # NOT the numeric appl_code '16'.  We always look these up.
    c2_appl_codes = get_application_codes(sf_cursor, appl_name)
    if c2_appl_codes:
        logging.info(f"Oracle application source codes for {appl_name}: {c2_appl_codes}")
    else:
        logging.warning(
            f"No application source codes found for {appl_name}. "
            f"Oracle LOG_DATA_SRC_CDE / DATA_SRC_CDE filtering will fall back to "
            f"appl_code='{appl_code}'."
        )

    # ---- Auto-discover C2 YAML config if not explicitly provided ----
    c2_yaml = arg_dict.get('c2_yaml')
    if not c2_yaml and (include_c2 or c2_only):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        search_paths = [
            os.path.join(script_dir, 'configs', f'surr_keys_{appl_name}_C2.yaml'),
            os.path.join(script_dir, '..', 'configs', f'surr_keys_{appl_name}_C2.yaml'),
            os.path.join(script_dir, '..', f'surr_keys_{appl_name}_C2.yaml'),
        ]
        for candidate in search_paths:
            candidate = os.path.normpath(candidate)
            if os.path.exists(candidate):
                c2_yaml = candidate
                logging.info(f"Auto-discovered C2 YAML config: {c2_yaml}")
                break
        if not c2_yaml:
            logging.info(
                f"No C2 YAML config found (searched: "
                f"{', '.join(os.path.normpath(p) for p in search_paths)}); "
                f"will use SHARDING_DRIVER for C2 table discovery"
            )

    # ---- Determine tables to validate ----
    if arg_dict['specific_tables']:
        tables = arg_dict['specific_tables']
        logging.info(f"Using specified tables: {tables}")
    else:
        tables = get_tables_from_appl_table(
            sf_cursor, appl_code, table_types=arg_dict['table_types']
        )
        logging.info(f"Tables from T_APPL_TABLE: {tables}")

    if not tables:
        logging.warning(f"No tables found for {appl_name}")
        return []

    logging.info(f"Validating {len(tables)} tables")

    # Prefetch Snowflake column metadata for ALL tables in one query
    # (replaces 2 * len(tables) individual INFORMATION_SCHEMA queries)
    prefetch_all_table_metadata(sf_cursor, appl_name, tables, sf_database=sf_database)

    # Prefetch Oracle column metadata + PK constraints for all tables
    # (replaces ~312 individual Oracle queries with 2-3 bulk queries)
    ora_owner = arg_dict.get('ora_schema') or f"DW{tenant_id}"
    prefetch_oracle_columns(ora_cursor, tables, ora_owner)
    prefetch_oracle_pk_constraints(ora_cursor, tables, tenant_id, dw_trim_length)

    # Bulk-fetch Oracle TB_C2 filter conditions from TB_C2_FILTER_FIELDS
    # (mirrors DDW_Count_Validation's get_tb_c2_filter approach)
    prefetch_tb_c2_filters(sf_cursor, tables, appl_name)

    # Load business keys and exclude columns config
    biz_keys_config, exclude_columns_config = load_business_keys(arg_dict['biz_keys_path'], appl_code)

    # Setup output directory
    validation_path = ingest_cfg_dict.get('snowflake_connection', {}).get(
        'validation_path', arg_dict['output_directory']
    )
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    output_dir = os.path.join(
        validation_path, appl_name, script_name
    )
    ensure_output_dir(output_dir)

    # Process each table
    all_results = []
    phase_times = {'metadata': 0.0, 'key_inference': 0.0, 'comparison': 0.0, 'drill_down': 0.0}
    for table_idx, table_name in enumerate(tables, 1):
        logging.info(f"\n--- Processing ({table_idx}/{len(tables)}): {table_name} ---")

        try:
            # Get table metadata
            t_meta_start = time.perf_counter()
            table_meta = get_table_metadata(
                sf_cursor, appl_name, table_name, sf_database=sf_database,
                dw_trim_length=dw_trim_length
            )

            # Populate Oracle TB_C2 filter from TB_C2_FILTER_FIELDS cache
            if table_meta.is_c2:
                table_meta.c2_oracle_filter = get_tb_c2_filter(sf_cursor, table_name, appl_name)
                if table_meta.c2_oracle_filter:
                    logging.info(f"  TB_C2 Oracle filter: {table_meta.c2_oracle_filter}")

            if not table_meta.data_columns:
                logging.warning(f"No data columns found for {table_name}, skipping")
                result = TableComparisonResult(
                    table_name=table_name,
                    table_type=table_meta.table_type,
                    status='SKIP',
                    error_message='No data columns found'
                )
                all_results.append(result)
                continue

            ora_owner = ora_schema or f"DW{tenant_id}"
            resolved_table = resolve_oracle_table(
                ora_cursor, table_name, ora_owner
            )
            if resolved_table != table_name:
                logging.info(f"  Oracle synonym resolved: {table_name} -> {resolved_table}")

            ora_cols = get_oracle_columns(ora_cursor, resolved_table, ora_owner)
            if not ora_cols:
                logging.warning(
                    f"  Table {table_name} not found in Oracle "
                    f"(no columns under any owner variant of {ora_owner}). "
                    f"Skipping -- this table may not exist in Oracle."
                )
                result = TableComparisonResult(
                    table_name=table_name,
                    table_type=table_meta.table_type,
                    status='SKIP',
                    error_message=f'Table not found in Oracle ({ora_owner})'
                )
                all_results.append(result)
                continue
            else:
                original_count = len(table_meta.data_columns)
                table_meta.data_columns = filter_columns_to_oracle(
                    table_meta.data_columns, ora_cols
                )
                # Exclude columns specified in config CSV
                excl_cols = exclude_columns_config.get(table_name.upper(), set())
                if excl_cols:
                    before_count = len(table_meta.data_columns)
                    table_meta.data_columns = [
                        c for c in table_meta.data_columns if c.name.upper() not in excl_cols
                    ]
                    if len(table_meta.data_columns) < before_count:
                        logging.info(
                            f"  Excluded {before_count - len(table_meta.data_columns)} columns "
                            f"per config: {sorted(excl_cols)}"
                        )
                if not table_meta.data_columns:
                    result = TableComparisonResult(
                        table_name=table_name,
                        table_type=table_meta.table_type,
                        status='SKIP',
                        error_message='No common columns between Oracle and Snowflake'
                    )
                    all_results.append(result)
                    continue
            phase_times['metadata'] += time.perf_counter() - t_meta_start

            # For C2 tables, probe Oracle to find which app codes actually have data
            codes = c2_appl_codes if c2_appl_codes else None
            if codes and table_meta.is_c2:
                codes = probe_oracle_app_codes(
                    ora_cursor, table_name, tenant_id, table_meta, codes
                )

            # Helper to infer key (deferred in table_first mode for speed)
            def _get_key_columns():
                t_key_start = time.perf_counter()
                kc = biz_keys_config.get(table_name.upper(), [])
                if kc:
                    logging.info(f"  Business key (from config): {kc}")
                else:
                    kc = infer_business_key(
                        sf_cursor, appl_name, table_name,
                        sf_database=sf_database, tenant_id=tenant_id,
                        ora_cursor=ora_cursor,
                        table_meta=table_meta,
                        process_date_ts=process_date_ts,
                        dw_trim_length=dw_trim_length,
                    )
                    if kc:
                        logging.info(f"  Inferred business key: {kc}")
                    else:
                        logging.info(f"  No business key found (will use table-level hash only)")

                # Validate inferred keys exist in Oracle columns to prevent
                # ORA-00904 when Snowflake-only columns are used as keys
                if kc and ora_cols:
                    valid_kc = [k for k in kc if k.upper() in ora_cols]
                    removed_kc = [k for k in kc if k.upper() not in ora_cols]
                    if removed_kc:
                        logging.warning(
                            f"  Business key columns not in Oracle, dropped: {removed_kc}"
                        )
                    if not valid_kc:
                        logging.warning(
                            f"  All key columns removed after Oracle filter; "
                            f"falling back to table-level hash"
                        )
                    kc = valid_kc

                phase_times['key_inference'] += time.perf_counter() - t_key_start
                return kc

            # Execute comparison based on mode
            t_comp_start = time.perf_counter()
            key_columns = []

            if comparison_mode == 'count_only':
                result = compare_counts_only(
                    ora_cursor, sf_cursor, table_name, table_meta,
                    tenant_id, process_date_ts, appl_name, appl_code,
                    ora_schema=ora_schema, sf_database=sf_database,
                    c2_appl_codes=codes,
                )

            elif comparison_mode == 'row_level':
                key_columns = _get_key_columns()
                if key_columns:
                    result = compare_row_hashes(
                        ora_cursor, sf_cursor, table_name, table_meta,
                        tenant_id, process_date_ts, appl_name, appl_code,
                        key_columns, arg_dict['max_mismatches'],
                        ora_schema=ora_schema, sf_database=sf_database,
                        c2_appl_codes=codes,
                    )
                else:
                    logging.info(
                        f"  No business key found -- using keyless hash "
                        f"comparison for {table_name}")
                    result = compare_row_hashes_keyless(
                        ora_cursor, sf_cursor, table_name, table_meta,
                        tenant_id, process_date_ts, appl_name, appl_code,
                        arg_dict['max_mismatches'],
                        ora_schema=ora_schema, sf_database=sf_database,
                        c2_appl_codes=codes,
                    )

            else:  # table_first (default)
                result = compare_table_hashes(
                    ora_cursor, sf_cursor, table_name, table_meta,
                    tenant_id, process_date_ts, appl_name, appl_code,
                    ora_schema=ora_schema, sf_database=sf_database,
                    c2_appl_codes=codes,
                )
                phase_times['comparison'] += time.perf_counter() - t_comp_start

                # Drill down if table hash mismatches and drill-down enabled
                # In table_first mode, defer key inference until we know we need it
                if result.status == 'FAIL' and arg_dict['drill_down'] and not key_columns:
                    key_columns = _get_key_columns()

                if result.status == 'FAIL' and arg_dict['drill_down']:
                    t_drill_start = time.perf_counter()
                    table_ora_sql = result.oracle_sql
                    table_sf_sql = result.snowflake_sql

                    if result.oracle_row_count == 0 or result.snowflake_row_count == 0:
                        if result.oracle_row_count == 0:
                            result.missing_in_oracle = result.snowflake_row_count
                            result.comments.append(
                                f"Oracle returned 0 rows; all {result.snowflake_row_count:,} "
                                f"Snowflake rows missing from Oracle (no drill-down needed)"
                            )
                        else:
                            result.missing_in_snowflake = result.oracle_row_count
                            result.comments.append(
                                f"Snowflake returned 0 rows; all {result.oracle_row_count:,} "
                                f"Oracle rows missing from Snowflake (no drill-down needed)"
                            )
                        result.drill_down_performed = False
                        logging.info(
                            f"  One side empty (ora={result.oracle_row_count}, "
                            f"sf={result.snowflake_row_count}), skipping drill-down"
                        )
                    elif key_columns:
                        total_rows = result.oracle_row_count + result.snowflake_row_count
                        if total_rows >= RANGE_DRILL_THRESHOLD:
                            logging.info(
                                f"  L3: Bucket hash comparison ({total_rows:,} rows, "
                                f"256 buckets → mismatched buckets only → row hash → column diff)")
                            result = compare_row_hashes_ranged(
                                ora_cursor, sf_cursor, table_name, table_meta,
                                tenant_id, process_date_ts, appl_name, appl_code,
                                key_columns, arg_dict['max_mismatches'],
                                ora_schema=ora_schema, sf_database=sf_database,
                                c2_appl_codes=codes,
                            )
                        else:
                            logging.info(
                                f"  L3: Direct row hash comparison "
                                f"({total_rows:,} rows, small enough for in-memory)")
                            result = compare_row_hashes(
                                ora_cursor, sf_cursor, table_name, table_meta,
                                tenant_id, process_date_ts, appl_name, appl_code,
                                key_columns, arg_dict['max_mismatches'],
                                ora_schema=ora_schema, sf_database=sf_database,
                                c2_appl_codes=codes,
                            )

                        result.oracle_row_sql = result.oracle_row_sql or result.oracle_sql
                        result.snowflake_row_sql = result.snowflake_row_sql or result.snowflake_sql
                        result.oracle_sql = table_ora_sql
                        result.snowflake_sql = table_sf_sql
                    else:
                        logging.info(
                            f"  No business key -- using keyless hash comparison "
                            f"for {table_name}..."
                        )
                        result = compare_row_hashes_keyless(
                            ora_cursor, sf_cursor, table_name, table_meta,
                            tenant_id, process_date_ts, appl_name, appl_code,
                            arg_dict['max_mismatches'],
                            ora_schema=ora_schema, sf_database=sf_database,
                            c2_appl_codes=codes,
                        )
                        result.oracle_row_sql = result.oracle_row_sql or result.oracle_sql
                        result.snowflake_row_sql = result.snowflake_row_sql or result.snowflake_sql
                        result.oracle_sql = table_ora_sql
                        result.snowflake_sql = table_sf_sql

                    phase_times['drill_down'] += time.perf_counter() - t_drill_start

            if comparison_mode in ('count_only', 'row_level'):
                phase_times['comparison'] += time.perf_counter() - t_comp_start

            all_results.append(result)

        except Exception as e:
            logging.error(f"Error processing {table_name}: {str(e)}")
            logging.debug(traceback.format_exc())
            result = TableComparisonResult(
                table_name=table_name,
                table_type='UNKNOWN',
                status='ERROR',
                error_message=str(e)
            )
            all_results.append(result)
        finally:
            gc.collect()

    # Write results
    tenant_arg_dict = {**arg_dict, 'tenant_id': tenant_id}

    try:
        write_summary_report(all_results, tenant_arg_dict, output_dir)
    except Exception as e:
        logging.error(f"Failed to write summary: {str(e)}")

    try:
        write_csv_results(all_results, tenant_arg_dict, output_dir)
    except Exception as e:
        logging.error(f"Failed to write CSV: {str(e)}")

    try:
        write_debug_queries(all_results, tenant_arg_dict, output_dir)
    except Exception as e:
        logging.error(f"Failed to write debug queries: {str(e)}")

    # FieldValidation-style output (Summary, per-table DIFF files, QueriesUsed)
    try:
        write_fv_summary(all_results, tenant_arg_dict, output_dir)
    except Exception as e:
        logging.error(f"Failed to write FV-style summary: {str(e)}")

    try:
        write_fv_diff_files(all_results, tenant_arg_dict, output_dir)
    except Exception as e:
        logging.error(f"Failed to write FV-style diff files: {str(e)}")

    # Load to Snowflake dashboard (legacy table)
    if arg_dict['load_to_sf']:
        try:
            load_results_to_snowflake(sf_cursor, all_results, tenant_arg_dict)
        except Exception as e:
            logging.error(f"Failed to load to Snowflake: {str(e)}")

    # Load to Snowflake VALIDATION_RUN_MASTER/DETAIL (new format)
    if arg_dict.get('load_sf_meta'):
        t_upload_start = time.perf_counter()
        try:
            meta_db = arg_dict.get('sf_meta_db') or sf_database
            meta_schema = arg_dict.get('sf_meta_schema', 'VALIDATION_DASHBOARD')

            loader = ValidationLoader(
                sf_cursor=sf_cursor,
                arg_dict=arg_dict,
                database=meta_db,
                schema=meta_schema,
                script_name=SCRIPT_NAME,
                script_version=SCRIPT_VERSION,
            )
            summary = ExecutionSummary(
                script_name=SCRIPT_NAME,
                appl_name=appl_name,
                appl_code=appl_code,
                tenant_id=tenant_id,
                process_date=process_date_ts,
                script_version=SCRIPT_VERSION,
            )
            summary.parameters_used = {k: str(v) for k, v in arg_dict.items() if k not in ('sf_cursor',)}

            registry = TestCaseRegistry(sf_cursor, SCRIPT_NAME,
                                        database=meta_db, schema=meta_schema)
            tc = registry.get('hash_row_level')
            resolved_tc_id = tc['test_case_id'] if tc else arg_dict.get('test_case_id', 'DDW_D1_07')
            resolved_tc_name = tc['test_case_name'] if tc else arg_dict.get('test_case_name', 'DDW Day1: Row-Level Hash Comparison')
            logging.info(f"Resolved test_case_id={resolved_tc_id} from {'registry' if tc else 'CLI defaults'}")

            resolved_appl_code = _resolve_appl_code(sf_cursor, meta_db, meta_schema, appl_name, appl_code)

            config = {
                'appl_name': appl_name,
                'appl_code': resolved_appl_code,
                'tenant_id': tenant_id,
                'test_case_id': resolved_tc_id,
                'test_case_name': resolved_tc_name,
            }

            all_masters = []
            all_details_by_master = []
            for r in all_results:
                result_dict = {
                    'table': r.table_name,
                    'type': r.table_type,
                    'scenario': '',
                    'description': '',
                    'status': r.status,
                    'src_count': r.oracle_row_count,
                    'tgt_count': r.snowflake_row_count,
                    'matched': r.matched_rows,
                    'data_diffs': r.data_diffs,
                    'platform_diffs': r.platform_only_diffs,
                    'columns_compared': r.columns_compared,
                    'key_cols_used': r.key_columns_used,
                    'exclude_from_hash': r.exclude_from_hash,
                    'src_table_hash': r.oracle_table_hash,
                    'tgt_table_hash': r.snowflake_table_hash,
                    'src_hash_sql': r.oracle_sql,
                    'tgt_hash_sql': r.snowflake_sql,
                    'src_row_hash_sql': r.oracle_row_sql,
                    'tgt_row_hash_sql': r.snowflake_row_sql,
                    'comments': r.comments,
                    'time': r.execution_time_sec,
                    'row_details': r.row_details,
                    'missing_in_tgt_keys': getattr(r, 'missing_in_sf_keys', []),
                    'missing_in_src_keys': getattr(r, 'missing_in_ora_keys', []),
                }

                master, details = build_validation_results(
                    result_dict, config,
                    detail_mode=arg_dict.get('sf_detail_mode', 'sample'),
                    samples_per_category=arg_dict.get('sf_samples_per_category', 5),
                )
                all_masters.append(master)
                all_details_by_master.append(details)

            summary.update_counts(all_masters)
            exec_id = loader.insert_execution_summary(summary)

            run_ids = loader.insert_master_bulk(all_masters, execution_id=exec_id)

            all_details_flat = []
            for run_id, details in zip(run_ids, all_details_by_master):
                for d in details:
                    d.run_id = run_id
                all_details_flat.extend(details)

            if all_details_flat:
                loader.insert_detail_bulk(all_details_flat)

            summary.emit_summary_line()
            upload_sec = time.perf_counter() - t_upload_start
            logging.info(
                f"Loaded {len(run_ids)} master + {len(all_details_flat)} detail rows "
                f"to {meta_db}.{meta_schema} in {upload_sec:.1f}s"
            )
        except Exception as e:
            logging.error(f"Failed to load to Snowflake metadata: {str(e)}")

    # Log summary with phase timings
    passed = sum(1 for r in all_results if r.status == 'SUCCESS')
    passed_clean = sum(
        1 for r in all_results
        if r.status == 'SUCCESS' and r.platform_only_diffs == 0
    )
    passed_plat = passed - passed_clean
    failed = sum(1 for r in all_results if r.status == 'FAIL')
    errors = sum(1 for r in all_results if r.status == 'ERROR')
    skipped = sum(1 for r in all_results if r.status == 'SKIP')

    summary_parts = [
        f"{len(all_results)} tables, "
        f"{passed} passed ({passed_clean} clean"
        f"{f', {passed_plat} with platform diffs' if passed_plat else ''}), "
        f"{failed} failed, {errors} errors"
    ]
    if skipped:
        summary_parts.append(f", {skipped} skipped (not in Oracle)")
    logging.info(f"\nTenant {tenant_id} Summary: {''.join(summary_parts)}")
    logging.info(f"  Time Breakdown:")
    logging.info(f"    Metadata discovery:  {phase_times['metadata']:.1f}s")
    logging.info(f"    Key inference:       {phase_times['key_inference']:.1f}s")
    logging.info(f"    Hash comparison:     {phase_times['comparison']:.1f}s")
    logging.info(f"    Drill-down:          {phase_times['drill_down']:.1f}s")
    total_processing = sum(phase_times.values())
    logging.info(f"    Total processing:    {total_processing:.1f}s")

    return all_results


def run_validation_for_tenant_date_range(
    sf_cursor,
    ora_cursor,
    arg_dict: dict,
    ingest_cfg_dict: dict,
    tenant_id: str,
    date_list: list,
) -> list:
    """Run hash comparison for a tenant across a date range in a single pass.

    Discovers tables once, runs one aggregate hash per table covering the
    entire range.  Produces ONE set of results (one TableComparisonResult
    per table), ONE summary report, ONE CSV, ONE query file, and ONE set
    of Snowflake metadata rows.

    Returns:
        list of TableComparisonResult (one per table, not per date)
    """
    appl_name = arg_dict['appl_name']
    appl_code = arg_dict['appl_code']
    comparison_mode = arg_dict['comparison_mode']
    ora_schema = arg_dict.get('ora_schema')
    sf_database = arg_dict.get('sf_database', 'CUR_IBS')
    ddw_mode = arg_dict.get('ddw_mode', True)
    include_c2 = arg_dict.get('include_c2', False)
    c2_only = arg_dict.get('c2_only', False)

    # Resolve DW_ID trim length: -1=exclude, 0=full value, >0=trim N chars
    tenant_number_arg = arg_dict.get('tenant_number_arg', 'N')
    dw_trim_length = resolve_dw_trim_length(tenant_number_arg, tenant_id, sf_cursor)

    start_date_ts = f'{date_list[0][0:4]}-{date_list[0][4:6]}-{date_list[0][6:8]}'
    end_date_ts = f'{date_list[-1][0:4]}-{date_list[-1][4:6]}-{date_list[-1][6:8]}'

    clear_caches()
    global _oracle_pk_cache
    _oracle_pk_cache = {}

    logging.info("\n" + "=" * 80)
    logging.info(f"HASH COMPARISON VALIDATION - TENANT: {tenant_id} (DATE RANGE MODE)")
    logging.info(f"  Dates:         {start_date_ts} to {end_date_ts} ({len(date_list)} days)")
    logging.info(f"  Oracle schema: {ora_schema or f'DW{tenant_id} (default)'}")
    logging.info(f"  SF database:   {sf_database}")
    logging.info(f"  DDW mode:      {ddw_mode}")
    logging.info(f"  Include C2:    {include_c2}")
    logging.info(f"  C2 Only:       {c2_only}")
    logging.info("=" * 80)

    c2_appl_codes = get_application_codes(sf_cursor, appl_name)
    if c2_appl_codes:
        logging.info(f"Oracle application source codes for {appl_name}: {c2_appl_codes}")

    c2_yaml = arg_dict.get('c2_yaml')
    if not c2_yaml and (include_c2 or c2_only):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        for candidate in [
            os.path.join(script_dir, 'configs', f'surr_keys_{appl_name}_C2.yaml'),
            os.path.join(script_dir, '..', 'configs', f'surr_keys_{appl_name}_C2.yaml'),
            os.path.join(script_dir, '..', f'surr_keys_{appl_name}_C2.yaml'),
        ]:
            candidate = os.path.normpath(candidate)
            if os.path.exists(candidate):
                c2_yaml = candidate
                break

    # ---- Discover tables (once) ----
    if arg_dict['specific_tables']:
        tables = arg_dict['specific_tables']
    else:
        tables = get_tables_from_appl_table(
            sf_cursor, appl_code, table_types=arg_dict['table_types']
        )
        logging.info(f"Tables from T_APPL_TABLE: {tables}")

    if not tables:
        logging.warning(f"No tables found for {appl_name}")
        return []

    logging.info(f"Validating {len(tables)} tables across {len(date_list)} dates")

    # ---- Prefetch metadata (once) ----
    prefetch_all_table_metadata(sf_cursor, appl_name, tables, sf_database=sf_database)
    ora_owner = arg_dict.get('ora_schema') or f"DW{tenant_id}"
    prefetch_oracle_columns(ora_cursor, tables, ora_owner)
    prefetch_oracle_pk_constraints(ora_cursor, tables, tenant_id, dw_trim_length)
    prefetch_tb_c2_filters(sf_cursor, tables, appl_name)
    biz_keys_config, exclude_columns_config = load_business_keys(arg_dict['biz_keys_path'], appl_code)

    validation_path = ingest_cfg_dict.get('snowflake_connection', {}).get(
        'validation_path', arg_dict['output_directory'])
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    output_dir = os.path.join(validation_path, appl_name, script_name, tenant_id)
    ensure_output_dir(output_dir)

    # ---- Process each table ----
    all_results = []
    phase_times = {'metadata': 0.0, 'key_inference': 0.0, 'comparison': 0.0, 'drill_down': 0.0}

    for table_idx, table_name in enumerate(tables, 1):
        logging.info(f"\n--- Processing ({table_idx}/{len(tables)}): {table_name} ---")

        try:
            t_meta_start = time.perf_counter()
            table_meta = get_table_metadata(
                sf_cursor, appl_name, table_name, sf_database=sf_database,
                dw_trim_length=dw_trim_length)

            # Populate Oracle TB_C2 filter from TB_C2_FILTER_FIELDS cache
            if table_meta.is_c2:
                table_meta.c2_oracle_filter = get_tb_c2_filter(sf_cursor, table_name, appl_name)
                if table_meta.c2_oracle_filter:
                    logging.info(f"  TB_C2 Oracle filter: {table_meta.c2_oracle_filter}")

            if not table_meta.data_columns:
                logging.warning(f"No data columns found for {table_name}, skipping")
                all_results.append(TableComparisonResult(
                    table_name=table_name, table_type=table_meta.table_type,
                    status='SKIP', error_message='No data columns found'))
                continue

            resolved_table = resolve_oracle_table(ora_cursor, table_name, ora_owner)
            ora_cols = get_oracle_columns(ora_cursor, resolved_table, ora_owner)
            if not ora_cols:
                logging.warning(
                    f"  Table {table_name} not found in Oracle "
                    f"(no columns under any owner variant of {ora_owner}). Skipping.")
                all_results.append(TableComparisonResult(
                    table_name=table_name, table_type=table_meta.table_type,
                    status='SKIP',
                    error_message=f'Table not found in Oracle ({ora_owner})'))
                continue

            table_meta.data_columns = filter_columns_to_oracle(
                table_meta.data_columns, ora_cols)
            # Exclude columns specified in config CSV
            excl_cols = exclude_columns_config.get(table_name.upper(), set())
            if excl_cols:
                before_count = len(table_meta.data_columns)
                table_meta.data_columns = [
                    c for c in table_meta.data_columns if c.name.upper() not in excl_cols
                ]
                if len(table_meta.data_columns) < before_count:
                    logging.info(
                        f"  Excluded {before_count - len(table_meta.data_columns)} columns "
                        f"per config: {sorted(excl_cols)}"
                    )
            if not table_meta.data_columns:
                all_results.append(TableComparisonResult(
                    table_name=table_name, table_type=table_meta.table_type,
                    status='SKIP',
                    error_message='No common columns between Oracle and Snowflake'))
                continue
            phase_times['metadata'] += time.perf_counter() - t_meta_start

            codes = c2_appl_codes if c2_appl_codes else None
            if codes and table_meta.is_c2:
                codes = probe_oracle_app_codes(
                    ora_cursor, table_name, tenant_id, table_meta, codes)

            # ---- Range hash comparison ----
            t_comp_start = time.perf_counter()
            date_results = compare_table_hashes_date_range(
                ora_cursor, sf_cursor, table_name, table_meta,
                tenant_id, date_list, appl_name, appl_code,
                ora_schema=ora_schema, sf_database=sf_database,
                c2_appl_codes=codes)
            phase_times['comparison'] += time.perf_counter() - t_comp_start

            # Check if any dates failed and need drill-down
            failed_dates = [
                dts for dts, r in date_results.items()
                if r.status == 'FAIL' and arg_dict['drill_down']
            ]

            if failed_dates and comparison_mode != 'count_only':
                t_key_start = time.perf_counter()
                kc = biz_keys_config.get(table_name.upper(), [])
                if not kc and failed_dates:
                    kc = infer_business_key(
                        sf_cursor, appl_name, table_name,
                        sf_database=sf_database, tenant_id=tenant_id,
                        ora_cursor=ora_cursor, table_meta=table_meta,
                        process_date_ts=start_date_ts,
                        end_date_ts=end_date_ts,
                        dw_trim_length=dw_trim_length)
                inferred_kc = list(kc) if kc else []
                if kc and ora_cols:
                    valid_kc = [k for k in kc if k.upper() in ora_cols]
                    if len(valid_kc) < len(kc):
                        removed = [k for k in kc if k.upper() not in ora_cols]
                        logging.warning(
                            f"  {table_name}: keys {removed} not found in "
                            f"Oracle columns, removed from key list"
                        )
                    kc = valid_kc
                phase_times['key_inference'] += time.perf_counter() - t_key_start

                if kc:
                    logging.info(f"  Business keys for {table_name}: {kc}")
                    for dts in failed_dates:
                        t_drill_start = time.perf_counter()
                        r = date_results[dts]
                        saved_ora_sql = r.oracle_sql
                        saved_sf_sql = r.snowflake_sql

                        if r.oracle_row_count == 0 or r.snowflake_row_count == 0:
                            if r.oracle_row_count == 0:
                                r.missing_in_oracle = r.snowflake_row_count
                                r.comments.append(
                                    f"[{dts}] Oracle 0 rows; {r.snowflake_row_count:,} "
                                    f"Snowflake rows missing from Oracle")
                                r.row_details.append({
                                    'key': {'PROCESS_DATE': dts},
                                    'row_status': 'MISSING_IN_ORACLE',
                                    'has_data_diff': True,
                                    'diffs': [{'column': 'ROW_COUNT',
                                               'source_value': 0,
                                               'target_value': r.snowflake_row_count,
                                               'diff_type': 'COUNT_MISMATCH',
                                               'explanation': f'Oracle has 0 rows for {dts}; '
                                                              f'{r.snowflake_row_count} rows in Snowflake'}],
                                })
                            else:
                                r.missing_in_snowflake = r.oracle_row_count
                                r.comments.append(
                                    f"[{dts}] Snowflake 0 rows; {r.oracle_row_count:,} "
                                    f"Oracle rows missing from Snowflake")
                                r.row_details.append({
                                    'key': {'PROCESS_DATE': dts},
                                    'row_status': 'MISSING_IN_SNOWFLAKE',
                                    'has_data_diff': True,
                                    'diffs': [{'column': 'ROW_COUNT',
                                               'source_value': r.oracle_row_count,
                                               'target_value': 0,
                                               'diff_type': 'COUNT_MISMATCH',
                                               'explanation': f'Snowflake has 0 rows for {dts}; '
                                                              f'{r.oracle_row_count} rows in Oracle'}],
                                })
                            r.key_columns_used = kc or ['PROCESS_DATE']
                            r.drill_down_performed = False
                        else:
                            total_rows = r.oracle_row_count + r.snowflake_row_count
                            if total_rows >= RANGE_DRILL_THRESHOLD:
                                logging.info(
                                    f"    [{dts}] L3: Bucket hash ({total_rows:,} rows) "
                                    f"→ mismatched buckets → row hash → column diff")
                                drill_r = compare_row_hashes_ranged(
                                    ora_cursor, sf_cursor, table_name, table_meta,
                                    tenant_id, dts, appl_name, appl_code,
                                    kc, arg_dict['max_mismatches'],
                                    ora_schema=ora_schema, sf_database=sf_database,
                                    c2_appl_codes=codes)
                            else:
                                logging.info(
                                    f"    [{dts}] L3: Direct row hash "
                                    f"({total_rows:,} rows, small) → column diff")
                                drill_r = compare_row_hashes(
                                    ora_cursor, sf_cursor, table_name, table_meta,
                                    tenant_id, dts, appl_name, appl_code,
                                    kc, arg_dict['max_mismatches'],
                                    ora_schema=ora_schema, sf_database=sf_database,
                                    c2_appl_codes=codes)
                            drill_r.oracle_row_sql = drill_r.oracle_row_sql or drill_r.oracle_sql
                            drill_r.snowflake_row_sql = drill_r.snowflake_row_sql or drill_r.snowflake_sql
                            drill_r.oracle_sql = saved_ora_sql
                            drill_r.snowflake_sql = saved_sf_sql
                            date_results[dts] = drill_r

                        phase_times['drill_down'] += time.perf_counter() - t_drill_start
                else:
                    if inferred_kc:
                        logging.warning(
                            f"  {table_name}: inferred keys {inferred_kc} "
                            f"not found in Oracle columns -- using keyless "
                            f"hash comparison instead")
                    else:
                        logging.warning(
                            f"  {table_name}: no business keys found -- "
                            f"using keyless hash comparison (sort & compare)")

                    for dts in failed_dates:
                        t_drill_start = time.perf_counter()
                        r = date_results[dts]
                        saved_ora_sql = r.oracle_sql
                        saved_sf_sql = r.snowflake_sql

                        if r.oracle_row_count == 0 or r.snowflake_row_count == 0:
                            r.drill_down_performed = False
                            r.key_columns_used = ['PROCESS_DATE']
                            if r.oracle_row_count == 0:
                                r.missing_in_oracle = r.snowflake_row_count
                                r.comments.append(
                                    f"[{dts}] Oracle 0 rows; "
                                    f"{r.snowflake_row_count:,} rows in "
                                    f"Snowflake")
                                r.row_details.append({
                                    'key': {'PROCESS_DATE': dts},
                                    'row_status': 'MISSING_IN_ORACLE',
                                    'has_data_diff': True,
                                    'diffs': [{'column': 'ROW_COUNT',
                                               'source_value': 0,
                                               'target_value': r.snowflake_row_count,
                                               'diff_type': 'COUNT_MISMATCH',
                                               'explanation': f'Oracle has 0 rows for {dts}; '
                                                              f'{r.snowflake_row_count} rows in Snowflake'}],
                                })
                            else:
                                r.missing_in_snowflake = r.oracle_row_count
                                r.comments.append(
                                    f"[{dts}] Snowflake 0 rows; "
                                    f"{r.oracle_row_count:,} rows in Oracle")
                                r.row_details.append({
                                    'key': {'PROCESS_DATE': dts},
                                    'row_status': 'MISSING_IN_SNOWFLAKE',
                                    'has_data_diff': True,
                                    'diffs': [{'column': 'ROW_COUNT',
                                               'source_value': r.oracle_row_count,
                                               'target_value': 0,
                                               'diff_type': 'COUNT_MISMATCH',
                                               'explanation': f'Snowflake has 0 rows for {dts}; '
                                                              f'{r.oracle_row_count} rows in Oracle'}],
                                })
                        else:
                            drill_r = compare_row_hashes_keyless(
                                ora_cursor, sf_cursor, table_name,
                                table_meta, tenant_id, dts,
                                appl_name, appl_code,
                                arg_dict['max_mismatches'],
                                ora_schema=ora_schema,
                                sf_database=sf_database,
                                c2_appl_codes=codes)
                            drill_r.oracle_row_sql = (
                                drill_r.oracle_row_sql or drill_r.oracle_sql)
                            drill_r.snowflake_row_sql = (
                                drill_r.snowflake_row_sql
                                or drill_r.snowflake_sql)
                            drill_r.oracle_sql = saved_ora_sql
                            drill_r.snowflake_sql = saved_sf_sql
                            date_results[dts] = drill_r

                        phase_times['drill_down'] += (
                            time.perf_counter() - t_drill_start)

            # Build ONE result per table from the date_results dict.
            # If all dates passed (L1 hit), take the first date's result.
            # If some dates failed, merge: status=FAIL, aggregate counts.
            first_dts = list(date_results.keys())[0]
            first_r = date_results[first_dts]
            any_fail = any(r.status == 'FAIL' for r in date_results.values())
            any_error = any(r.status == 'ERROR' for r in date_results.values())

            if any_error:
                err_r = next(r for r in date_results.values() if r.status == 'ERROR')
                all_results.append(err_r)
            elif any_fail:
                merged = TableComparisonResult(
                    table_name=table_name,
                    table_type=table_meta.table_type,
                    columns_compared=len(table_meta.data_columns),
                    status='FAIL',
                )
                # L1 range SQL / hashes (stored on first entry by comparator)
                merged.oracle_sql = getattr(first_r, '_range_ora_sql', first_r.oracle_sql)
                merged.snowflake_sql = getattr(first_r, '_range_sf_sql', first_r.snowflake_sql)
                merged.oracle_table_hash = getattr(first_r, '_range_ora_hash', first_r.oracle_table_hash)
                merged.snowflake_table_hash = getattr(first_r, '_range_sf_hash', first_r.snowflake_table_hash)
                # L2 GROUP BY SQL for per-date drill-down
                merged.oracle_range_sql = first_r.oracle_sql
                merged.snowflake_range_sql = first_r.snowflake_sql

                total_ora = sum(r.oracle_row_count for r in date_results.values())
                total_sf = sum(r.snowflake_row_count for r in date_results.values())
                merged.oracle_row_count = total_ora
                merged.snowflake_row_count = total_sf
                merged.row_count_match = (total_ora == total_sf)
                merged.table_hash_match = False

                fail_dates = [d for d, r in date_results.items() if r.status == 'FAIL']
                pass_dates = [d for d, r in date_results.items() if r.status == 'SUCCESS']
                merged.comments.append(
                    f"Date range {start_date_ts} to {end_date_ts}: "
                    f"{len(pass_dates)} dates passed, "
                    f"{len(fail_dates)} dates failed: {fail_dates}")
                for dts in fail_dates:
                    fr = date_results[dts]
                    merged.mismatched_rows += fr.mismatched_rows
                    merged.data_diffs += fr.data_diffs
                    merged.platform_only_diffs += fr.platform_only_diffs
                    merged.missing_in_oracle += fr.missing_in_oracle
                    merged.missing_in_snowflake += fr.missing_in_snowflake
                    merged.comments.extend(fr.comments)
                    merged.row_details.extend(fr.row_details)
                    merged.missing_in_sf_keys.extend(
                        getattr(fr, 'missing_in_sf_keys', []))
                    merged.missing_in_ora_keys.extend(
                        getattr(fr, 'missing_in_ora_keys', []))
                    if fr.key_columns_used and not merged.key_columns_used:
                        merged.key_columns_used = fr.key_columns_used
                merged.matched_rows = sum(r.matched_rows for r in date_results.values())
                merged.execution_time_sec = sum(r.execution_time_sec for r in date_results.values())
                all_results.append(merged)
            else:
                # All dates passed -- use first_r with aggregated counts
                first_r.oracle_row_count = sum(r.oracle_row_count for r in date_results.values())
                first_r.snowflake_row_count = sum(r.snowflake_row_count for r in date_results.values())
                first_r.matched_rows = first_r.oracle_row_count
                first_r.execution_time_sec = sum(r.execution_time_sec for r in date_results.values())
                all_results.append(first_r)

        except Exception as e:
            logging.error(f"Error processing {table_name}: {str(e)}")
            logging.debug(traceback.format_exc())
            all_results.append(TableComparisonResult(
                table_name=table_name, table_type='UNKNOWN',
                status='ERROR', error_message=str(e)))
        finally:
            gc.collect()

    # ---- Write ONE summary, CSV, query file for the range ----
    range_tag = f"{date_list[0]}_to_{date_list[-1]}"
    range_arg_dict = {
        **arg_dict,
        'tenant_id': tenant_id,
        'process_date': range_tag,
        'process_date_ts': f"{start_date_ts} to {end_date_ts}",
    }

    try:
        write_summary_report(all_results, range_arg_dict, output_dir)
    except Exception as e:
        logging.error(f"Failed to write summary: {str(e)}")
    try:
        write_csv_results(all_results, range_arg_dict, output_dir)
    except Exception as e:
        logging.error(f"Failed to write CSV: {str(e)}")
    try:
        write_debug_queries(all_results, range_arg_dict, output_dir)
    except Exception as e:
        logging.error(f"Failed to write queries: {str(e)}")

    # ---- Load ONE set of master rows (exclude SKIP) ----
    if arg_dict.get('load_sf_meta'):
        loadable = [r for r in all_results if r.status != 'SKIP']
        if loadable:
            try:
                meta_db = arg_dict.get('sf_meta_db') or sf_database
                meta_schema = arg_dict.get('sf_meta_schema', 'VALIDATION_DASHBOARD')
                loader = ValidationLoader(
                    sf_cursor=sf_cursor, arg_dict=arg_dict,
                    database=meta_db, schema=meta_schema,
                    script_name=SCRIPT_NAME, script_version=SCRIPT_VERSION)
                summary = ExecutionSummary(
                    script_name=SCRIPT_NAME,
                    appl_name=appl_name,
                    appl_code=appl_code,
                    tenant_id=tenant_id,
                    process_date=start_date_ts,
                    script_version=SCRIPT_VERSION,
                )
                summary.parameters_used = {k: str(v) for k, v in arg_dict.items() if k not in ('sf_cursor',)}

                registry = TestCaseRegistry(sf_cursor, SCRIPT_NAME,
                                            database=meta_db, schema=meta_schema)
                tc = registry.get('hash_row_level')
                resolved_tc_id = tc['test_case_id'] if tc else arg_dict.get('test_case_id', 'DDW_D1_07')
                resolved_tc_name = tc['test_case_name'] if tc else arg_dict.get('test_case_name', 'DDW Day1: Row-Level Hash Comparison')
                logging.info(f"Resolved test_case_id={resolved_tc_id} from {'registry' if tc else 'CLI defaults'}")

                resolved_appl_code = _resolve_appl_code(sf_cursor, meta_db, meta_schema, appl_name, appl_code)

                range_desc = (f"Date range: {start_date_ts} to {end_date_ts} "
                              f"({len(date_list)} days)")
                config = {
                    'appl_name': appl_name,
                    'appl_code': resolved_appl_code,
                    'tenant_id': tenant_id,
                    'test_case_id': resolved_tc_id,
                    'test_case_name': resolved_tc_name,
                    'date_range': {
                        'start_date': start_date_ts,
                        'end_date': end_date_ts,
                        'num_days': len(date_list),
                    },
                }
                all_masters = []
                all_details_by_master = []
                for r in loadable:
                    result_dict = {
                        'table': r.table_name, 'type': r.table_type,
                        'scenario': '', 'description': range_desc,
                        'status': r.status,
                        'src_count': r.oracle_row_count,
                        'tgt_count': r.snowflake_row_count,
                        'matched': r.matched_rows,
                        'data_diffs': r.data_diffs,
                        'platform_diffs': r.platform_only_diffs,
                        'columns_compared': r.columns_compared,
                        'key_cols_used': r.key_columns_used,
                        'exclude_from_hash': r.exclude_from_hash,
                        'src_table_hash': r.oracle_table_hash,
                        'tgt_table_hash': r.snowflake_table_hash,
                        'src_hash_sql': r.oracle_sql,
                        'tgt_hash_sql': r.snowflake_sql,
                        'src_row_hash_sql': r.oracle_row_sql,
                        'tgt_row_hash_sql': r.snowflake_row_sql,
                        'comments': r.comments,
                        'time': r.execution_time_sec,
                        'row_details': r.row_details,
                        'missing_in_tgt_keys': getattr(r, 'missing_in_sf_keys', []),
                        'missing_in_src_keys': getattr(r, 'missing_in_ora_keys', []),
                    }
                    master, details = build_validation_results(
                        result_dict, config,
                        detail_mode=arg_dict.get('sf_detail_mode', 'sample'),
                        samples_per_category=arg_dict.get('sf_samples_per_category', 5))
                    all_masters.append(master)
                    all_details_by_master.append(details)

                summary.update_counts(all_masters)
                exec_id = loader.insert_execution_summary(summary)

                run_ids = loader.insert_master_bulk(
                    all_masters, execution_id=exec_id)
                all_details_flat = []
                for run_id, details in zip(run_ids, all_details_by_master):
                    for d in details:
                        d.run_id = run_id
                    all_details_flat.extend(details)
                if all_details_flat:
                    loader.insert_detail_bulk(all_details_flat)

                summary.emit_summary_line()
                logging.info(
                    f"Loaded {len(run_ids)} master + {len(all_details_flat)} detail rows to "
                    f"{meta_db}.{meta_schema} (SKIP tables excluded)")
            except Exception as e:
                logging.error(f"Failed to load to Snowflake metadata: {str(e)}")

    # ---- Log summary ----
    passed = sum(1 for r in all_results if r.status == 'SUCCESS')
    failed = sum(1 for r in all_results if r.status == 'FAIL')
    errors = sum(1 for r in all_results if r.status == 'ERROR')
    skipped = sum(1 for r in all_results if r.status == 'SKIP')

    summary_parts = [
        f"{len(all_results)} tables, "
        f"{passed} passed, {failed} failed, {errors} errors"
    ]
    if skipped:
        summary_parts.append(f", {skipped} skipped (not in Oracle)")
    logging.info(f"\nTenant {tenant_id} Summary ({start_date_ts} to {end_date_ts}): "
                 f"{''.join(summary_parts)}")
    logging.info(f"  Time Breakdown:")
    logging.info(f"    Metadata discovery:  {phase_times['metadata']:.1f}s")
    logging.info(f"    Key inference:       {phase_times['key_inference']:.1f}s")
    logging.info(f"    Hash comparison:     {phase_times['comparison']:.1f}s")
    logging.info(f"    Drill-down:          {phase_times['drill_down']:.1f}s")
    total_processing = sum(phase_times.values())
    logging.info(f"    Total processing:    {total_processing:.1f}s")

    return all_results


def _run_single_date(arg_dict, ingest_cfg_dict, orc_conn_dict):
    """Run validation for a single process date.  Returns (tenants, results)."""
    sf_conn = open_sf_connection(
        ingest_cfg=ingest_cfg_dict,
        schema=arg_dict['appl_name']
    )
    sf_cursor = sf_conn.cursor()
    logging.info("Connected to Snowflake")

    # Resolve appl_code if not already set
    if not arg_dict.get('appl_code'):
        from script_utils import get_appl_code
        arg_dict['appl_code'] = get_appl_code(arg_dict['appl_name'], sf_cursor)
        logging.info(f"Resolved appl_code: {arg_dict['appl_code']}")

    tenants = []
    all_tenant_results = []

    try:
        if arg_dict['tenant_id']:
            tenants = [arg_dict['tenant_id']]
        else:
            tenants = get_tenants_for_app(sf_cursor, arg_dict['appl_name'])
            if not tenants:
                logging.error("No tenants found. Exiting.")
                return tenants, all_tenant_results

        for tenant_id in tenants:
            ora_conn = None
            ora_cursor = None
            try:
                ora_user, ora_pass = get_oracle_credentials(tenant_id)
                ora_conn = open_oracle_connection(
                    myuser=ora_user,
                    mypassword=ora_pass,
                    orc_conn_dict=orc_conn_dict
                )
                ora_cursor = ora_conn.cursor()
                logging.info(f"Connected to Oracle as {ora_user}")

                results = run_validation_for_tenant(
                    sf_cursor, ora_cursor, arg_dict, ingest_cfg_dict, tenant_id
                )
                all_tenant_results.extend(results)

            except Exception as e:
                logging.error(f"Failed for tenant {tenant_id}: {str(e)}")
                logging.error(traceback.format_exc())
                continue
            finally:
                if ora_cursor is not None:
                    try:
                        ora_cursor.close()
                    except Exception:
                        pass
                if ora_conn is not None:
                    try:
                        ora_conn.close()
                    except Exception:
                        pass

    finally:
        try:
            sf_cursor.close()
        except Exception:
            pass
        try:
            sf_conn.close()
        except Exception:
            pass
        logging.info("All database connections closed.")

    if all_tenant_results:
        try:
            combined_arg = {**arg_dict, 'tenant_id': arg_dict.get('tenant_id') or 'ALL'}
            write_debug_queries(all_tenant_results, combined_arg, arg_dict['output_directory'])
        except Exception as e:
            logging.error(f"Failed to write combined query log: {str(e)}")

    return tenants, all_tenant_results


def _run_date_range(arg_dict, ingest_cfg_dict, orc_conn_dict, date_list):
    """Run range-based validation: one connection, one table discovery,
    range hash queries.  Returns (tenants, flat results list)."""
    sf_conn = open_sf_connection(
        ingest_cfg=ingest_cfg_dict,
        schema=arg_dict['appl_name']
    )
    sf_cursor = sf_conn.cursor()
    logging.info("Connected to Snowflake")

    # Resolve appl_code if not already set
    if not arg_dict.get('appl_code'):
        from script_utils import get_appl_code
        arg_dict['appl_code'] = get_appl_code(arg_dict['appl_name'], sf_cursor)
        logging.info(f"Resolved appl_code: {arg_dict['appl_code']}")

    tenants = []
    combined_results = []

    try:
        if arg_dict['tenant_id']:
            tenants = [arg_dict['tenant_id']]
        else:
            tenants = get_tenants_for_app(sf_cursor, arg_dict['appl_name'])
            if not tenants:
                logging.error("No tenants found. Exiting.")
                return tenants, combined_results

        for tenant_id in tenants:
            ora_conn = None
            ora_cursor = None
            try:
                ora_user, ora_pass = get_oracle_credentials(tenant_id)
                ora_conn = open_oracle_connection(
                    myuser=ora_user,
                    mypassword=ora_pass,
                    orc_conn_dict=orc_conn_dict
                )
                ora_cursor = ora_conn.cursor()
                logging.info(f"Connected to Oracle as {ora_user}")

                tenant_results = run_validation_for_tenant_date_range(
                    sf_cursor, ora_cursor, arg_dict, ingest_cfg_dict,
                    tenant_id, date_list,
                )
                combined_results.extend(tenant_results)

            except Exception as e:
                logging.error(f"Failed for tenant {tenant_id}: {str(e)}")
                logging.error(traceback.format_exc())
                continue
            finally:
                if ora_cursor is not None:
                    try:
                        ora_cursor.close()
                    except Exception:
                        pass
                if ora_conn is not None:
                    try:
                        ora_conn.close()
                    except Exception:
                        pass

    finally:
        try:
            sf_cursor.close()
        except Exception:
            pass
        try:
            sf_conn.close()
        except Exception:
            pass
        logging.info("All database connections closed.")

    return tenants, combined_results


def main():
    """Main entry point for hash comparison validation."""
    script_start = time.perf_counter()

    arg_dict = arg_parsing()
    date_list = arg_dict.get('date_list', [arg_dict['process_date']])
    is_multi_date = len(date_list) > 1

    setup_logging(arg_dict)

    logging.info("=" * 80)
    logging.info(f"HASH COMPARISON VALIDATION {SCRIPT_VERSION} STARTED")
    logging.info("=" * 80)
    logging.info(f"Application:      {arg_dict['appl_name']}")
    logging.info(f"Application Code: {arg_dict['appl_code'] or '(auto-resolve from Snowflake)'}")
    logging.info(f"Tenant ID:        {arg_dict['tenant_id'] or 'ALL (auto-discover)'}")
    if is_multi_date:
        logging.info(f"Date Range:       {date_list[0]} to {date_list[-1]} ({len(date_list)} days)")
    else:
        logging.info(f"Process Date:     {arg_dict['process_date_ts']}")
    logging.info(f"Comparison Mode:  {arg_dict['comparison_mode']}")
    logging.info(f"Drill Down:       {arg_dict['drill_down']}")
    logging.info(f"Oracle Schema:    {arg_dict.get('ora_schema') or 'DW{{tenant_id}} (FIS default)'}")
    logging.info(f"SF Database:      {arg_dict.get('sf_database', 'CUR_IBS')}")
    logging.info(f"DDW Mode:         {arg_dict.get('ddw_mode', True)}")
    logging.info(f"Include C2:       {arg_dict.get('include_c2', False)}")
    logging.info(f"C2 Only:          {arg_dict.get('c2_only', False)}")
    logging.info(f"SF Metadata:      {'ON' if arg_dict.get('load_sf_meta') else 'OFF'}")
    logging.info(f"Exclude Surrogates: {arg_dict.get('exclude_surrogates', [])}")
    logging.info("=" * 80)

    # Load configuration
    py_path = os.environ.get("PYTHONPATH", "")
    ingest_yaml_file = os.path.join(py_path, 'IngestionConfig.yaml')

    if os.path.exists(ingest_yaml_file):
        ingest_cfg_dict = load_yaml(yaml_file=ingest_yaml_file)
    else:
        logging.warning(f"IngestionConfig.yaml not found at {ingest_yaml_file}, using defaults")
        ingest_cfg_dict = {
            'snowflake_connection': {
                'validation_path': arg_dict['output_directory'],
                'http_proxy': '',
                'https_proxy': '',
                'private_key_loc': ''
            }
        }

    orc_conn_yaml = os.path.join(py_path, 'orc_connections.yaml')
    orc_conn_dict = None
    if os.path.exists(orc_conn_yaml):
        orc_conn_dict = load_yaml(yaml_file=orc_conn_yaml)

    grand_total_results = 0
    grand_total_passed = 0
    grand_total_failed = 0
    grand_total_errors = 0
    grand_total_skipped = 0
    all_tenants = set()

    if is_multi_date:
        # ---- Range mode: single connection, single table discovery,
        #      range hash queries -- returns flat list (one per table) ----
        tenants, results = _run_date_range(
            arg_dict, ingest_cfg_dict, orc_conn_dict, date_list
        )
        all_tenants.update(tenants)

        grand_total_results = len(results)
        grand_total_passed = sum(1 for r in results if r.status == 'SUCCESS')
        grand_total_failed = sum(1 for r in results if r.status == 'FAIL')
        grand_total_errors = sum(1 for r in results if r.status == 'ERROR')
        grand_total_skipped = sum(1 for r in results if r.status == 'SKIP')
    else:
        # ---- Single-date mode ----
        tenants, results = _run_single_date(arg_dict, ingest_cfg_dict, orc_conn_dict)
        all_tenants.update(tenants)

        grand_total_results = len(results)
        grand_total_passed = sum(1 for r in results if r.status == 'SUCCESS')
        grand_total_failed = sum(1 for r in results if r.status == 'FAIL')
        grand_total_errors = sum(1 for r in results if r.status == 'ERROR')
        grand_total_skipped = sum(1 for r in results if r.status == 'SKIP')

    # Final summary
    script_end = time.perf_counter()
    script_run_time = script_end - script_start
    all_ok = grand_total_failed == 0 and grand_total_errors == 0

    logging.info("\n" + "=" * 80)
    logging.info("HASH COMPARISON VALIDATION COMPLETE")
    logging.info("=" * 80)
    all_tenants_list = sorted(all_tenants)
    logging.info(f"Tenants Validated:  {len(all_tenants_list)} - {all_tenants_list}")
    if is_multi_date:
        logging.info(f"Date Range:         {date_list[0]} to {date_list[-1]} ({len(date_list)} days)")
    else:
        logging.info(f"Process Date:       {arg_dict['process_date_ts']}")
    logging.info("-" * 80)
    logging.info(f"Total Tables:       {grand_total_results}")
    logging.info(f"Total Passed:       {grand_total_passed}")
    logging.info(f"Total Failed:       {grand_total_failed}")
    logging.info(f"Total Errors:       {grand_total_errors}")
    if grand_total_skipped:
        logging.info(f"Total Skipped:      {grand_total_skipped} (not in Oracle)")
    logging.info(f"Script Run Time:    {script_run_time:.2f} seconds")
    logging.info(f"Overall Status:     {'SUCCESS' if all_ok else 'FAIL'}")
    logging.info("=" * 80)


if __name__ == '__main__':
    main()





    
FROM all_tab_columns
WHERE owner = 'CUR_IBS'
  AND table_name = 'VW_RC_OZ7_EVT_DAY_TME_ARD'
ORDER BY column_id;
