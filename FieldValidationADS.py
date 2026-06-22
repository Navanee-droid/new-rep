# ============================================================================
# Hash Comparison Validation Script - ADS Edition
# ============================================================================
# Purpose:
#   Row-level and table-level hash comparison between Oracle (single-tenant)
#   and Snowflake (multi-tenant) for ADS applications.
#
# Architecture:
#   Oracle:    DW{tenant_id}2.{table_name} (fallback: DW{tenant_id}.{table_name})
#   Snowflake: CUR_IBS.{appl_name}.{table_name} WHERE TENANT_ID = '{tenant_id}'
#
# Strategy:
#   1. Discover tables from Snowflake INFORMATION_SCHEMA (+ DTM views)
#   2. For each table:
#      a. Get column metadata (types, precision, scale)
#      b. Build canonicalized hash expressions (Oracle + Snowflake)
#      c. Compare table-level aggregate hashes (fast path)
#      d. If mismatch: drill down to row-level hash comparison
#   3. Write results (TXT summary, CSV detail, SQL debug, DIFF files)
#
# Usage:
#   python FieldValidation.py --a ADS_APP --t 6A --p 20251210 --l INFO --o /log/path
#
# Input:
#   --a: Application Name (required) - Snowflake schema name
#   --t: Tenant ID (required)
#   --p: Process Date YYYYMMDD (or --start_date/--end_date for range)
#   --l: Log Level (default: INFO)
#   --o: Log/Output Directory (required)
#   --tb: Comma-separated table names (optional, auto-discover if omitted)
#   --start_date/--end_date: Date range mode (YYYY-MM-DD format)
#   --mode: Comparison mode: table_first, row_level, count_only (default: table_first)
#   --drill-down: Enable row-level drill-down on mismatch (default: True)
#   --max-mismatches: Max row mismatches to collect (default: 100)
#   --load-sf-meta: Load results to Snowflake dashboard tables
#   --sf-meta-db: Snowflake database for metadata tables
#   --sf-meta-schema: Snowflake schema for metadata tables
#
# Created: 08/08/2024 Author: Agalya Karikalan
# Modified: 05/21/2026 - Replaced CSV-export approach with hash-based comparison
# Version: 3.0
# ============================================================================

import gc
import logging
import os
import sys
import time
import glob as _glob
import traceback
from datetime import datetime, timedelta

# Add hash engine modules to path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'run_hash_rowlevel_v2.1'))

from table_discovery import (
    get_all_tables,
    get_table_metadata,
    prefetch_all_table_metadata,
    prefetch_oracle_columns,
    clear_caches,
    resolve_oracle_table,
    get_oracle_columns,
    filter_columns_to_oracle,
)
from hash_comparator import (
    compare_table_hashes,
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
    write_fv_summary,
    write_fv_diff_files,
    ensure_output_dir,
)

from script_utils import (
    parse_args,
    get_appl_code,
    load_yaml,
    logging_config,
    open_sf_connection,
    open_oracle_connection,
    get_tables_from_information_schema,
    get_snowflake_dtm_views
)

SCRIPT_NAME = "FieldValidationADS.py"
SCRIPT_VERSION = "v3.0"


def arg_parsing() -> dict:
    """Parse command-line arguments following ADS CLI conventions."""
    args = parse_args(
        required=['--a', '--t'],
        optional=['--p', '--l', '--o', '--tb', '--start_date', '--end_date',
                  '--load-sf-meta', '--sf-meta-db', '--sf-meta-schema',
                  '--biz-keys'],
        description='Hash-based field-level validation between Oracle and Snowflake (ADS).',
    )
    if not args.get('process_date') and not args.get('start_date'):
        raise ValueError("Either --p (process date) or --start_date/--end_date (date range) must be provided.")
    return args


def get_date_range(start_date_ts, end_date_ts):
    """Generate list of date strings from start to end (inclusive)."""
    start = datetime.strptime(start_date_ts, '%Y-%m-%d')
    end = datetime.strptime(end_date_ts, '%Y-%m-%d')
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime('%Y-%m-%d'))
        current += timedelta(days=1)
    return dates


def load_business_keys(biz_keys_path: str, appl_code: str) -> tuple:
    """
    Load business keys and exclude columns configuration from CSV file.

    CSV format:
        table_name,business_key,exclude_columns
        MY_TABLE,ACCT_NBR,COL_TO_IGNORE1|COL_TO_IGNORE2
        MY_TABLE2,CUST_NBR,
        MY_TABLE3,TXN_ID,AUDIT_COL|TEMP_COL

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
    import csv as csv_mod

    biz_config = {}
    exclude_config = {}

    if not biz_keys_path:
        # Try default location
        script_dir = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(script_dir, f'configs/business_keys_{appl_code}.csv')
        if not os.path.exists(csv_path):
            return biz_config, exclude_config
        biz_keys_path = csv_path

    if not os.path.exists(biz_keys_path):
        logging.warning(f"Business keys file not found: {biz_keys_path}")
        return biz_config, exclude_config

    logging.info(f"Loading business keys from: {biz_keys_path}")

    try:
        with open(biz_keys_path, 'r') as f:
            reader = csv_mod.DictReader(f)
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


def resolve_oracle_schema(ora_cursor, table_name, tenant_id):
    """Resolve Oracle schema for ADS tables.

    ADS convention: DW{tenant_id}2 first, fallback to DW{tenant_id}.
    Returns (schema, resolved_table_name) or (None, None) if not found.
    """
    # Try DW{tenant_id}2 first (ADS primary convention)
    for suffix in ('2', ''):
        schema = f'DW{tenant_id}{suffix}'
        try:
            ora_cursor.execute(
                f"SELECT COUNT(*) FROM ALL_TAB_COLUMNS WHERE OWNER = '{schema}' "
                f"AND TABLE_NAME = '{table_name.upper()}'"
            )
            row = ora_cursor.fetchone()
            if row and row[0] > 0:
                return schema, table_name
        except Exception:
            continue

    # Try synonym resolution
    for suffix in ('2', ''):
        schema = f'DW{tenant_id}{suffix}'
        resolved = resolve_oracle_table(ora_cursor, table_name, schema)
        if resolved != table_name:
            return schema, resolved

    return None, None


def get_oracle_cols_for_table(ora_cursor, table_name, tenant_id):
    """Get Oracle columns for an ADS table, trying DW{tid}2 then DW{tid}."""
    for suffix in ('2', ''):
        schema = f'DW{tenant_id}{suffix}'
        cols = get_oracle_columns(ora_cursor, table_name, schema)
        if cols:
            return cols, schema
    return None, None


def infer_business_key_from_oracle(ora_cursor, table_name, tenant_id):
    """Infer business key from Oracle PK constraint (ADS pattern)."""
    for suffix in ('2', ''):
        owner = f'DW{tenant_id}{suffix}'
        pk_query = (
            f"SELECT b.COLUMN_NAME "
            f"FROM ALL_CONSTRAINTS a "
            f"JOIN ALL_CONS_COLUMNS b "
            f"  ON a.OWNER = b.OWNER "
            f"  AND a.TABLE_NAME = b.TABLE_NAME "
            f"  AND a.CONSTRAINT_NAME = b.CONSTRAINT_NAME "
            f"WHERE a.OWNER = '{owner}' "
            f"  AND a.TABLE_NAME = '{table_name.upper()}' "
            f"  AND a.CONSTRAINT_TYPE = 'P' "
            f"ORDER BY b.POSITION"
        )
        try:
            ora_cursor.execute(pk_query)
            rows = ora_cursor.fetchall()
            if rows:
                # Filter out system/surrogate/date columns that don't work as join keys
                exclude = {'TENANT_ID', 'PRCS_DTE', 'FULL_DTE', 'LOAD_TS', 'SOURCE_FILE',
                           'PRCS_YR_MTH_NBR', 'SOR_EXP_DTE', 'EFF_DTE',
                           'CUR_REC_IND', 'PRCS_EXP_DTE'}
                pk_cols = [r[0] for r in rows
                           if r[0].upper() not in exclude
                           and not r[0].upper().startswith('DW_')]
                if pk_cols:
                    return pk_cols
        except Exception:
            continue
    return []


def run_validation_for_table(
    ora_cursor,
    sf_cursor,
    table_name: str,
    arg_dict: dict,
    ora_schema: str,
    biz_keys_config: dict = None,
    exclude_columns_config: dict = None,
) -> TableComparisonResult:
    """
    Run hash comparison for a single table.

    Args:
        ora_cursor: Oracle cursor
        sf_cursor: Snowflake cursor
        table_name: Table name
        arg_dict: Arguments dictionary
        ora_schema: Resolved Oracle schema (DW{tid}2 or DW{tid})
        biz_keys_config: Optional business keys dict {table -> [key_cols]}
        exclude_columns_config: Optional dict {table -> set of columns to exclude from hash}

    Returns:
        TableComparisonResult
    """
    appl_name = arg_dict['appl_name']
    appl_code = arg_dict['appl_code']
    tenant_id = arg_dict['tenant_id']
    process_date_ts = arg_dict['process_date_ts']
    comparison_mode = arg_dict.get('comparison_mode', 'table_first')
    sf_database = 'CUR_IBS'
    max_mismatches = arg_dict.get('max_mismatches', 100)
    drill_down = arg_dict.get('drill_down', True)

    # Get table metadata from Snowflake
    dw_trim_length = arg_dict.get('dw_trim_length', -1)
    table_meta = get_table_metadata(
        sf_cursor, appl_name, table_name, sf_database=sf_database,
        dw_trim_length=dw_trim_length
    )

    if not table_meta.data_columns:
        return TableComparisonResult(
            table_name=table_name,
            table_type=table_meta.table_type,
            status='SKIP',
            error_message='No data columns found'
        )

    # Get Oracle columns and intersect
    ora_cols = get_oracle_columns(ora_cursor, table_name, ora_schema)
    if not ora_cols:
        return TableComparisonResult(
            table_name=table_name,
            table_type=table_meta.table_type,
            status='SKIP',
            error_message=f'Table not found in Oracle ({ora_schema})'
        )

    table_meta.data_columns = filter_columns_to_oracle(
        table_meta.data_columns, ora_cols
    )

    # Exclude columns specified in business keys config file
    if exclude_columns_config:
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
        return TableComparisonResult(
            table_name=table_name,
            table_type=table_meta.table_type,
            status='SKIP',
            error_message='No common columns between Oracle and Snowflake'
        )

    # Key inference helper (deferred for table_first mode)
    def _get_key_columns():
        if biz_keys_config:
            tname = table_name.upper()
            # Try exact match first, then with/without TB_ prefix
            if tname in biz_keys_config:
                kc = biz_keys_config[tname]
                logging.info(f"  Business key (from config): {kc}")
                return kc
            elif tname.startswith('TB_') and tname[3:] in biz_keys_config:
                kc = biz_keys_config[tname[3:]]
                logging.info(f"  Business key (from config, matched without TB_ prefix): {kc}")
                return kc
            elif f'TB_{tname}' in biz_keys_config:
                kc = biz_keys_config[f'TB_{tname}']
                logging.info(f"  Business key (from config, matched with TB_ prefix): {kc}")
                return kc
            else:
                logging.debug(
                    f"  Table '{tname}' not found in biz_keys_config. "
                    f"Available keys: {list(biz_keys_config.keys())}"
                )
        kc = infer_business_key_from_oracle(ora_cursor, table_name, tenant_id)
        if kc:
            # Validate keys exist in Oracle columns
            valid_kc = [k for k in kc if k.upper() in ora_cols]
            if valid_kc:
                logging.info(f"  Inferred business key (Oracle PK): {valid_kc}")
                return valid_kc
        logging.info(f"  No business key found (will use table-level hash only)")
        return []

    # Execute comparison based on mode
    key_columns = []

    if comparison_mode == 'count_only':
        result = compare_counts_only(
            ora_cursor, sf_cursor, table_name, table_meta,
            tenant_id, process_date_ts, appl_name, appl_code,
            ora_schema=ora_schema, sf_database=sf_database,
        )

    elif comparison_mode == 'row_level':
        key_columns = _get_key_columns()
        if key_columns:
            result = compare_row_hashes(
                ora_cursor, sf_cursor, table_name, table_meta,
                tenant_id, process_date_ts, appl_name, appl_code,
                key_columns, max_mismatches,
                ora_schema=ora_schema, sf_database=sf_database,
            )
        else:
            result = compare_row_hashes_keyless(
                ora_cursor, sf_cursor, table_name, table_meta,
                tenant_id, process_date_ts, appl_name, appl_code,
                max_mismatches,
                ora_schema=ora_schema, sf_database=sf_database,
            )

    else:  # table_first (default)
        result = compare_table_hashes(
            ora_cursor, sf_cursor, table_name, table_meta,
            tenant_id, process_date_ts, appl_name, appl_code,
            ora_schema=ora_schema, sf_database=sf_database,
        )

        # Drill down if table hash mismatches
        if result.status == 'FAIL' and drill_down:
            key_columns = _get_key_columns()
            table_ora_sql = result.oracle_sql
            table_sf_sql = result.snowflake_sql

            if result.oracle_row_count == 0 or result.snowflake_row_count == 0:
                if result.oracle_row_count == 0:
                    result.missing_in_oracle = result.snowflake_row_count
                    result.comments.append(
                        f"Oracle returned 0 rows; all {result.snowflake_row_count:,} "
                        f"Snowflake rows missing from Oracle"
                    )
                else:
                    result.missing_in_snowflake = result.oracle_row_count
                    result.comments.append(
                        f"Snowflake returned 0 rows; all {result.oracle_row_count:,} "
                        f"Oracle rows missing from Snowflake"
                    )
                result.drill_down_performed = False
            elif key_columns:
                total_rows = result.oracle_row_count + result.snowflake_row_count
                if total_rows >= RANGE_DRILL_THRESHOLD:
                    result = compare_row_hashes_ranged(
                        ora_cursor, sf_cursor, table_name, table_meta,
                        tenant_id, process_date_ts, appl_name, appl_code,
                        key_columns, max_mismatches,
                        ora_schema=ora_schema, sf_database=sf_database,
                    )
                else:
                    result = compare_row_hashes(
                        ora_cursor, sf_cursor, table_name, table_meta,
                        tenant_id, process_date_ts, appl_name, appl_code,
                        key_columns, max_mismatches,
                        ora_schema=ora_schema, sf_database=sf_database,
                    )
                result.oracle_row_sql = result.oracle_row_sql or result.oracle_sql
                result.snowflake_row_sql = result.snowflake_row_sql or result.snowflake_sql
                result.oracle_sql = table_ora_sql
                result.snowflake_sql = table_sf_sql
            else:
                result = compare_row_hashes_keyless(
                    ora_cursor, sf_cursor, table_name, table_meta,
                    tenant_id, process_date_ts, appl_name, appl_code,
                    max_mismatches,
                    ora_schema=ora_schema, sf_database=sf_database,
                )
                result.oracle_row_sql = result.oracle_row_sql or result.oracle_sql
                result.snowflake_row_sql = result.snowflake_row_sql or result.snowflake_sql
                result.oracle_sql = table_ora_sql
                result.snowflake_sql = table_sf_sql

    return result


def validate_for_date(ora_cursor, sf_cursor, arg_dict, tables, dtm_views, process_date_ts,
                      biz_keys_config=None, exclude_columns_config=None):
    """
    Run hash-based validation for all tables on a given date.

    Returns:
        List of TableComparisonResult
    """
    tenant_id = arg_dict['tenant_id']
    appl_name = arg_dict['appl_name']

    # Reset caches for fresh metadata
    clear_caches()

    # Determine Oracle schema (DW{tid}2 first, then DW{tid})
    # Probe once using first table to determine the active schema
    all_tables = list(tables)
    if dtm_views:
        # For DTM views, Oracle uses TB_ prefix
        dtm_oracle_names = []
        for v in dtm_views:
            ora_name = v.replace('VW_', 'TB_') if v.startswith('VW_') else v
            dtm_oracle_names.append(ora_name)
        all_tables.extend(dtm_oracle_names)

    # Resolve Oracle schema -- try DW{tid}2 first
    ora_schema = None
    for suffix in ('2', ''):
        candidate = f'DW{tenant_id}{suffix}'
        try:
            ora_cursor.execute(
                f"SELECT 1 FROM ALL_USERS WHERE USERNAME = '{candidate}'"
            )
            if ora_cursor.fetchone():
                ora_schema = candidate
                break
        except Exception:
            continue
    if not ora_schema:
        ora_schema = f'DW{tenant_id}'
    logging.info(f"Using Oracle schema: {ora_schema}")

    # Prefetch Snowflake metadata for all tables
    # For DTM views: pass the TB_ name so prefetch does TB_->VW_ mapping and caches correctly
    dtm_oracle_names = []
    if dtm_views:
        dtm_oracle_names = [
            v.replace('VW_', 'TB_') if v.startswith('VW_') else v
            for v in dtm_views
        ]
    sf_table_names = list(tables) + dtm_oracle_names
    prefetch_all_table_metadata(sf_cursor, appl_name, sf_table_names, sf_database='CUR_IBS')

    # Prefetch Oracle columns
    prefetch_oracle_columns(ora_cursor, all_tables, ora_schema)

    # Update arg_dict with current date
    current_arg_dict = {**arg_dict, 'process_date_ts': process_date_ts}

    all_results = []

    # Process regular tables
    for table_idx, table_name in enumerate(tables, 1):
        logging.info(f"\n--- Processing ({table_idx}/{len(tables)}): {table_name} ---")
        try:
            result = run_validation_for_table(
                ora_cursor, sf_cursor, table_name,
                current_arg_dict, ora_schema,
                biz_keys_config=biz_keys_config,
                exclude_columns_config=exclude_columns_config,
            )
            all_results.append(result)
        except Exception as e:
            logging.error(f"Error processing {table_name}: {str(e)}")
            logging.debug(traceback.format_exc())
            all_results.append(TableComparisonResult(
                table_name=table_name, table_type='OTHER',
                status='ERROR', error_message=str(e)
            ))
        finally:
            gc.collect()

    # Process DTM views
    if dtm_views:
        logging.info(f"\n{'='*60}\nDTM VIEW VALIDATION ({len(dtm_views)} views)\n{'='*60}")
        for view_idx, view_name in enumerate(dtm_views, 1):
            logging.info(f"\n--- DTM ({view_idx}/{len(dtm_views)}): {view_name} ---")
            # Oracle uses TB_ prefix for DTM tables
            ora_table = view_name.replace('VW_', 'TB_') if view_name.startswith('VW_') else view_name
            try:
                # For DTM views, we pass the view name (Snowflake resolves VW_)
                # but use the TB_ name for Oracle via ora_schema
                result = run_validation_for_table(
                    ora_cursor, sf_cursor, ora_table,
                    current_arg_dict, ora_schema,
                    biz_keys_config=biz_keys_config,
                    exclude_columns_config=exclude_columns_config,
                )
                all_results.append(result)
            except Exception as e:
                logging.error(f"Error processing DTM view {view_name}: {str(e)}")
                logging.debug(traceback.format_exc())
                all_results.append(TableComparisonResult(
                    table_name=view_name, table_type='OTHER',
                    status='ERROR', error_message=str(e)
                ))
            finally:
                gc.collect()

    return all_results


def load_validation():
    """Main entry point for ADS hash-based field validation."""
    script_start = time.perf_counter()
    argument_dict = arg_parsing()

    py_path = os.environ.get("PYTHONPATH", "")
    ingest_yaml_file = f'{py_path}/IngestionConfig.yaml'
    if os.path.exists(ingest_yaml_file):
        ingest_cfg_dict = load_yaml(yaml_file=ingest_yaml_file)
    else:
        logging.warning(f"IngestionConfig.yaml not found at {ingest_yaml_file}, using defaults")
        ingest_cfg_dict = {
            'snowflake_connection': {
                'validation_path': argument_dict.get('logging_directory', '/tmp'),
                'http_proxy': '',
                'https_proxy': '',
                'private_key_loc': ''
            }
        }

    sf_conn = open_sf_connection(ingest_cfg=ingest_cfg_dict)
    sf_cursor = sf_conn.cursor()
    appl_code = get_appl_code(argument_dict["appl_name"], sf_cursor)
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    logging_config(argument_dict["logging_directory"], appl_code, script_name, ingest_cfg_dict)

    logging.info("=" * 80)
    logging.info(f"HASH COMPARISON VALIDATION {SCRIPT_VERSION} (ADS) STARTED")
    logging.info("=" * 80)
    logging.info(f"Application:      {argument_dict['appl_name']}")
    logging.info(f"Tenant ID:        {argument_dict['tenant_id']}")
    logging.info(f"Comparison Mode:  {argument_dict.get('comparison_mode', 'table_first')}")
    logging.info("Connected to Snowflake")

    # Build argument dict for hash comparison
    # ADS: DW_* columns always included with full value (dw_trim_length=0)
    # Users can exclude specific columns via --biz-keys CSV file
    arg_dict = {
        'appl_name': argument_dict['appl_name'],
        'appl_code': appl_code,
        'tenant_id': argument_dict['tenant_id'],
        'process_date': argument_dict.get('process_date', ''),
        'process_date_ts': argument_dict.get('process_date_ts', ''),
        'comparison_mode': argument_dict.get('comparison_mode', 'table_first'),
        'drill_down': argument_dict.get('drill_down', True),
        'max_mismatches': int(argument_dict.get('max_mismatches', 100)),
        'output_directory': argument_dict.get('logging_directory', '/tmp'),
        'platform_detail': argument_dict.get('platform_detail', False),
        'load_sf_meta': argument_dict.get('load_sf_meta', False),
        'sf_meta_db': argument_dict.get('sf_meta_db'),
        'sf_meta_schema': argument_dict.get('sf_meta_schema', 'VALIDATION_DASHBOARD'),
        'dw_trim_length': 0,
    }

    # Discover tables
    table_filter = argument_dict.get('table_filter', '').strip().upper()
    if table_filter:
        tables = [t.strip() for t in table_filter.split(',') if t.strip()]
        logging.info(f"Table filter mode: validating {len(tables)} table(s): {tables}")
        # When specific tables are requested, don't auto-discover DTM views
        dtm_views = []
    else:
        tables = get_tables_from_information_schema(argument_dict["appl_name"], sf_cursor)
        # Discover DTM views only when auto-discovering tables
        dtm_views = get_snowflake_dtm_views(sf_conn, argument_dict["appl_name"])
    logging.info(f"Regular tables to validate: {len(tables)}")
    logging.info(f"DTM views to validate: {dtm_views}")

    # Deduplicate: remove view-based tables from regular list (they'll be handled as DTM views)
    # Compare without prefix since regular tables use TB_ and DTM views use VW_
    def _strip_prefix(name):
        upper = name.upper()
        if upper.startswith('TB_') or upper.startswith('VW_'):
            return upper[3:]
        return upper

    dtm_views_stripped = {_strip_prefix(v) for v in dtm_views}
    tables = [t for t in tables if _strip_prefix(t) not in dtm_views_stripped]

    # Connect to Oracle
    ora_user = f'DW{argument_dict["tenant_id"]}'
    ora_conn = open_oracle_connection(myuser=ora_user)
    ora_cursor = ora_conn.cursor()
    logging.info(f'Connected to Oracle as {ora_user}')

    # Setup output directory
    file_path = f"{ingest_cfg_dict['snowflake_connection']['validation_path']}/{argument_dict['appl_name']}/{script_name}"
    ensure_output_dir(file_path)

    # Determine date list
    if argument_dict.get('start_date') and argument_dict.get('end_date'):
        date_list = get_date_range(argument_dict['start_date_ts'], argument_dict['end_date_ts'])
        logging.info(f"Date range mode: {argument_dict['start_date_ts']} to {argument_dict['end_date_ts']} ({len(date_list)} days)")
    else:
        date_list = [argument_dict['process_date_ts']]

    if len(date_list) > 1:
        logging.info(f"Date Range:       {date_list[0]} to {date_list[-1]} ({len(date_list)} days)")
    else:
        logging.info(f"Process Date:     {date_list[0]}")
    logging.info("=" * 80)

    # Load business keys and exclude columns config
    biz_keys_path = argument_dict.get('biz_keys')
    biz_keys_config, exclude_columns_config = load_business_keys(biz_keys_path, appl_code)

    # Run validation for each date
    all_results = []

    # Pre-initialize metadata loading objects if needed (once, outside loop)
    _sf_meta_initialized = False
    _loader = None
    _registry = None
    _config = None
    _meta_db = None
    _meta_schema = None

    if argument_dict.get('load_sf_meta'):
        try:
            sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
            from validation_utils import (
                ValidationLoader,
                ValidationResult,
                TestCaseRegistry,
                ExecutionSummary,
                cap_details,
                read_diff_file,
                infer_registry_app_category,
            )
            from validation_loader import build_validation_results

            _meta_db = argument_dict.get('sf_meta_db') or 'CUR_IBS'
            _meta_schema = argument_dict.get('sf_meta_schema', 'VALIDATION_DASHBOARD')

            _loader = ValidationLoader(
                sf_cursor=sf_cursor,
                arg_dict=arg_dict,
                database=_meta_db,
                schema=_meta_schema,
                script_name=SCRIPT_NAME,
                script_version=SCRIPT_VERSION,
            )

            _ac = infer_registry_app_category(argument_dict['appl_name'])
            _registry = TestCaseRegistry(sf_cursor, SCRIPT_NAME,
                                        database=_meta_db, schema=_meta_schema,
                                        app_category=_ac)
            tc = _registry.get('field_validation')
            resolved_tc_id = tc['test_case_id'] if tc else 'ADS_D1_07'
            resolved_tc_name = tc['test_case_name'] if tc else 'ADS Day1: Row-Level Hash Comparison'

            _config = {
                'appl_name': argument_dict['appl_name'],
                'appl_code': appl_code,
                'tenant_id': argument_dict['tenant_id'],
                'test_case_id': resolved_tc_id,
                'test_case_name': resolved_tc_name,
            }
            _sf_meta_initialized = True
        except Exception as e:
            logging.error(f"Failed to initialize metadata loading: {str(e)}")
            traceback.print_exc()

    for process_date_ts in date_list:
        process_date = process_date_ts.replace('-', '')
        logging.info(f"\n{'='*60}\nProcessing date: {process_date_ts}\n{'='*60}")

        arg_dict['process_date_ts'] = process_date_ts
        arg_dict['process_date'] = process_date

        date_results = validate_for_date(
            ora_cursor, sf_cursor, arg_dict, tables, dtm_views, process_date_ts,
            biz_keys_config=biz_keys_config,
            exclude_columns_config=exclude_columns_config,
        )
        all_results.extend(date_results)

        # Write per-date output
        try:
            write_summary_report(date_results, arg_dict, file_path)
        except Exception as e:
            logging.error(f"Failed to write summary for {process_date_ts}: {e}")

        try:
            write_csv_results(date_results, arg_dict, file_path)
        except Exception as e:
            logging.error(f"Failed to write CSV for {process_date_ts}: {e}")

        try:
            write_debug_queries(date_results, arg_dict, file_path)
        except Exception as e:
            logging.error(f"Failed to write debug queries for {process_date_ts}: {e}")

        try:
            write_fv_summary(date_results, arg_dict, file_path)
        except Exception as e:
            logging.error(f"Failed to write FV summary for {process_date_ts}: {e}")

        try:
            write_fv_diff_files(date_results, arg_dict, file_path)
        except Exception as e:
            logging.error(f"Failed to write DIFF files for {process_date_ts}: {e}")

        # Load per-date results to Snowflake metadata tables
        if _sf_meta_initialized:
            try:
                t_date_load_start = time.perf_counter()

                summary = ExecutionSummary(
                    script_name=SCRIPT_NAME,
                    appl_name=argument_dict['appl_name'],
                    appl_code=appl_code,
                    tenant_id=argument_dict['tenant_id'],
                    process_date=process_date,
                    script_version=SCRIPT_VERSION,
                )
                summary.started_at = datetime.fromtimestamp(
                    time.time() - (time.perf_counter() - script_start))
                summary.parameters_used = {k: str(v) for k, v in arg_dict.items() if k not in ('sf_cursor',)}

                all_masters = []
                all_details_by_master = []
                for r in date_results:
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
                        result_dict, _config,
                        detail_mode='sample',
                        samples_per_category=5,
                    )

                    # Read diff file content and merge into additional_info
                    diff_path = os.path.join(
                        file_path, f"DIFF_{r.table_name}_{argument_dict['tenant_id']}_{process_date}"
                    )
                    diff_ai = read_diff_file(diff_path)
                    if diff_ai:
                        if master.additional_info and isinstance(master.additional_info, dict):
                            master.additional_info.update(diff_ai)
                        else:
                            master.additional_info = diff_ai

                    all_masters.append(master)
                    all_details_by_master.append(details)

                summary.update_counts(all_masters)
                summary.execution_time_sec = time.perf_counter() - t_date_load_start

                # Attach output files to summary
                _run_suffix = f"_{argument_dict['tenant_id']}_{process_date}"
                _all_output_files = (
                    _glob.glob(os.path.join(file_path, '*.txt')) +
                    _glob.glob(os.path.join(file_path, '*.csv'))
                )
                for _fv in _all_output_files:
                    _base = os.path.basename(_fv)
                    if _run_suffix not in _base and _run_suffix not in os.path.splitext(_base)[0]:
                        continue
                    if os.path.getsize(_fv) > 0:
                        summary.read_and_store_output(_fv, file_type='hash_validation_report')

                exec_id = _loader.insert_execution_summary(summary)

                run_ids = _loader.insert_master_bulk(all_masters, execution_id=exec_id)

                all_details_flat = []
                for run_id, details in zip(run_ids, all_details_by_master):
                    for d in details:
                        d.run_id = run_id
                    all_details_flat.extend(details)

                if all_details_flat:
                    _loader.insert_detail_bulk(all_details_flat)

                summary.emit_summary_line()
                logging.info(
                    f"Loaded {len(run_ids)} master + {len(all_details_flat)} detail rows "
                    f"to {_meta_db}.{_meta_schema} for {process_date_ts}"
                )
            except Exception as e:
                logging.error(f"Failed to load metadata for {process_date_ts}: {str(e)}")
                traceback.print_exc()

    # Cleanup connections
    ora_cursor.close()
    ora_conn.close()
    sf_cursor.close()
    sf_conn.close()

    # Final summary
    script_end = time.perf_counter()
    script_run_time = script_end - script_start

    passed = sum(1 for r in all_results if r.status == 'SUCCESS')
    passed_clean = sum(1 for r in all_results if r.status == 'SUCCESS' and r.platform_only_diffs == 0)
    passed_plat = passed - passed_clean
    failed = sum(1 for r in all_results if r.status == 'FAIL')
    errors = sum(1 for r in all_results if r.status == 'ERROR')
    skipped = sum(1 for r in all_results if r.status == 'SKIP')

    logging.info("\n" + "=" * 80)
    logging.info("HASH COMPARISON VALIDATION COMPLETE")
    logging.info("=" * 80)
    logging.info(f"Total Tables:       {len(all_results)}")
    logging.info(f"Passed:             {passed} ({passed_clean} clean{f', {passed_plat} platform diffs' if passed_plat else ''})")
    logging.info(f"Failed:             {failed}")
    logging.info(f"Errors:             {errors}")
    if skipped:
        logging.info(f"Skipped:            {skipped}")
    logging.info(f"Script Run Time:    {script_run_time:.2f} seconds")
    logging.info(f"Overall Status:     {'SUCCESS' if failed == 0 and errors == 0 else 'FAIL'}")
    logging.info("=" * 80)


if __name__ == '__main__':
    load_validation()
