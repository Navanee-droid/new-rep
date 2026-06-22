#!/usr/bin/env python3
import os
import os.path
import sys
import time
import traceback
from collections import defaultdict
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import datetime
from validation_utils import (
    ValidationLoader,
    ValidationDetailResult,
    TestCaseRegistry,
    ExecutionSummary,
    cap_details
)

from script_utils import (
    parse_args,
    load_yaml,
    open_sf_connection,
    logging_config,
    get_tenants,
    setup_tenant_shard,
    get_appl_code,
    get_tables_from_appl_table,
)

SCRIPT_NAME = "Curated_Shard_Validation.py"
SCRIPT_VERSION = "v2.1"

EXCLUDE_FROM_COMPARISON = {'LOAD_TS', 'SOURCE_FILE', 'SHARDING_TS'}

# ─────────────────────────────────────────────────────────────────────────────
# Argument Parsing
# ─────────────────────────────────────────────────────────────────────────────

def arg_parsing() -> dict:
    args = parse_args(
        required=['--a'],
        optional=['--p', '--t', '--l', '--o', '--start_date', '--end_date',
                  '--skip-hash', '--filter',
                  '--load-sf-meta', '--sf-meta-db', '--sf-meta-schema'],
        description='Validates data parity between CUR_IBS (curated) and APP_IBS_SHARD_N (shard).',
    )
    # Resolve process_date: prefer --start_date if given, otherwise fall back to --p
    if args.get('start_date_ts'):
        args['process_date'] = args['start_date_ts']
        args['process_date_end'] = args.get('end_date_ts')
    else:
        args['process_date'] = args.get('process_date_ts')
        args['process_date_end'] = None
    if not args.get('tenant_id'):
        args['tenant_id'] = 'ALL'
    # --filter defaults to Y (apply SRC_APPL_NAME filter on TB_C2 tables)
    if args.get('apply_filter') is None:
        args['apply_filter'] = 'Y'
    return args


# ─────────────────────────────────────────────────────────────────────────────
# Query Helpers
# ─────────────────────────────────────────────────────────────────────────────

def safe_query(sql: str, cursor) -> list | None:
    try:
        cursor.execute(sql)
        return cursor.fetchall()
    except Exception as exc:
        logging.warning(f'Query failed: {exc}\nSQL: {sql}')
        return None


def safe_scalar(sql: str, cursor, default=None):
    result = safe_query(sql, cursor)
    if result and result[0] and result[0][0] is not None:
        return result[0][0]
    return default



# ─────────────────────────────────────────────────────────────────────────────
# Metadata: Table Discovery
# ─────────────────────────────────────────────────────────────────────────────

def get_tables_in_schema(db: str, schema: str, sf_cursor) -> set:
    """Return set of table names (BASE TABLE) in db.schema via INFORMATION_SCHEMA."""
    query = (f"SELECT TABLE_NAME FROM {db}.INFORMATION_SCHEMA.TABLES "
             f"WHERE TABLE_SCHEMA = '{schema.upper()}' AND TABLE_TYPE = 'BASE TABLE' "
             f"ORDER BY TABLE_NAME;")
    rows = safe_query(query, sf_cursor)
    return {row[0] for row in rows} if rows else set()


def get_comparable_tables(schema: str, shard_db: str, sf_cursor, appl_code: str) -> list:
    """
    Returns sorted list of table names to validate between CUR_IBS and shard.
    Uses get_tables_from_appl_table (T_APPL_TABLE) as the authoritative source
    so that TB_C2 (X2) tables are included.
    Tables present in T_APPL_TABLE but missing from either side are logged as warnings.
    """
    # Get authoritative table list from T_APPL_TABLE (includes TB_C2)
    appl_tables = set(get_tables_from_appl_table(sf_cursor, appl_code))

    # Filter to only tables that actually exist in both databases
    cur_tables   = get_tables_in_schema('CUR_IBS', schema, sf_cursor)
    shard_tables = get_tables_in_schema(shard_db, schema, sf_cursor)

    # TB_C2 tables live in DDW_CNF_DIM, not the app schema
    cur_c2_tables   = get_tables_in_schema('CUR_IBS', 'DDW_CNF_DIM', sf_cursor)
    shard_c2_tables = get_tables_in_schema(shard_db, 'DDW_CNF_DIM', sf_cursor)

    # Combine: for regular tables check app schema; for TB_C2 check DDW_CNF_DIM
    valid_tables = []
    for tbl in sorted(appl_tables):
        if tbl.startswith('TB_C2'):
            if tbl in cur_c2_tables and tbl in shard_c2_tables:
                valid_tables.append(tbl)
            elif tbl not in cur_c2_tables:
                logging.warning(f'{tbl} from T_APPL_TABLE not found in CUR_IBS.DDW_CNF_DIM')
            elif tbl not in shard_c2_tables:
                logging.warning(f'{tbl} from T_APPL_TABLE not found in {shard_db}.DDW_CNF_DIM')
        else:
            if tbl in cur_tables and tbl in shard_tables:
                valid_tables.append(tbl)
            elif tbl not in cur_tables:
                logging.warning(f'{tbl} from T_APPL_TABLE not found in CUR_IBS.{schema}')
            elif tbl not in shard_tables:
                logging.warning(f'{tbl} from T_APPL_TABLE not found in {shard_db}.{schema}')

    tb_c2_count = sum(1 for t in valid_tables if t.startswith('TB_C2'))
    logging.info(f'Comparable tables for {shard_db}: {len(valid_tables)} total ({tb_c2_count} TB_C2)')
    return valid_tables


def get_all_columns_for_schema(db: str, schema: str, sf_cursor) -> dict[str, list[str]]:
    """
    Single INFORMATION_SCHEMA query to fetch columns for ALL tables in a schema.
    Returns {TABLE_NAME: [col1, col2, ...]} in ordinal order.
    """
    sql = (
        f"SELECT TABLE_NAME, COLUMN_NAME FROM {db}.INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA = '{schema.upper()}' "
        f"ORDER BY TABLE_NAME, ORDINAL_POSITION;"
    )
    rows = safe_query(sql, sf_cursor) or []
    result: dict[str, list[str]] = {}
    for tbl, col in rows:
        result.setdefault(tbl, []).append(col)
    return result


def resolve_common_columns(tables: list, schema: str, shard_db: str,
                           sf_cursor) -> tuple[dict[str, list], dict[str, list]]:
    """
    Fetches column metadata for ALL tables in the schema in queries total
    (for CUR_IBS and shard_db), then computes common/shard-only columns per table.
    TB_C2 tables are fetched from DDW_CNF_DIM instead of the app schema.

    Returns (col_cache, shard_only_cache) dicts keyed by table name.
    """
    # Separate TB_C2 tables from regular tables
    regular_tables = [t for t in tables if not t.startswith('TB_C2')]
    c2_tables      = [t for t in tables if t.startswith('TB_C2')]

    # Fetch columns for regular tables from the app schema
    cur_schema_cols   = get_all_columns_for_schema('CUR_IBS', schema, sf_cursor) if regular_tables else {}
    shard_schema_cols = get_all_columns_for_schema(shard_db,  schema, sf_cursor) if regular_tables else {}

    # Fetch columns for TB_C2 tables from DDW_CNF_DIM
    cur_c2_cols   = get_all_columns_for_schema('CUR_IBS', 'DDW_CNF_DIM', sf_cursor) if c2_tables else {}
    shard_c2_cols = get_all_columns_for_schema(shard_db,  'DDW_CNF_DIM', sf_cursor) if c2_tables else {}

    col_cache:        dict[str, list] = {}
    shard_only_cache: dict[str, list] = {}

    for table in tables:
        tbl_upper = table.upper()
        if table.startswith('TB_C2'):
            cur_cols   = cur_c2_cols.get(tbl_upper, [])
            shard_cols = shard_c2_cols.get(tbl_upper, [])
        else:
            cur_cols   = cur_schema_cols.get(tbl_upper, [])
            shard_cols = shard_schema_cols.get(tbl_upper, [])

        cur_set   = set(cur_cols)
        shard_set = set(shard_cols)

        common     = [c for c in cur_cols if c in shard_set and c not in EXCLUDE_FROM_COMPARISON]
        shard_only = sorted(shard_set - cur_set)
        excluded   = sorted(((cur_set & shard_set) & EXCLUDE_FROM_COMPARISON) | (shard_set - cur_set))
        cur_only   = sorted(cur_set   - shard_set)

        if excluded:
            logging.info(f"{table}: columns excluded from hash: {excluded}")
        if cur_only:
            logging.warning(f"{table}: CUR-only columns (not in shard): {cur_only}")

        col_cache[table]        = common
        shard_only_cache[table] = excluded

    return col_cache, shard_only_cache


# ─────────────────────────────────────────────────────────────────────────────
# WHERE Clause Builder
# ─────────────────────────────────────────────────────────────────────────────

def _date_col(table_name: str) -> str:
    return 'EFF_DTE' if any(s in table_name for s in ('SCD', 'RCD', 'RPD')) else 'PRCS_DTE'


def _date_filter(table_name: str, process_date: str | None, process_date_end: str | None) -> str | None:
    if not process_date:
        return None
    col = _date_col(table_name)
    if process_date_end and process_date_end != process_date:
        return f"{col} BETWEEN '{process_date}' AND '{process_date_end}'"
    return f"{col} = '{process_date}'"


def build_where_clause(tenant_id: str, table_name: str,
                       process_date: str | None, process_date_end: str | None = None,
                       src_appl_filter: str | None = None) -> str:
    parts = [f"TENANT_ID = '{tenant_id}'"]
    df = _date_filter(table_name, process_date, process_date_end)
    if df:
        parts.append(df)
    if src_appl_filter:
        parts.append(f"SRC_APPL_NAME = '{src_appl_filter}'")
    return ' AND '.join(parts)


def build_combined_where_clause(tenant_ids: list, table_name: str,
                                process_date: str | None, process_date_end: str | None = None,
                                src_appl_filter: str | None = None) -> str:
    id_list = ', '.join(f"'{t}'" for t in tenant_ids)
    parts   = [f"TENANT_ID IN ({id_list})"]
    df = _date_filter(table_name, process_date, process_date_end)
    if df:
        parts.append(df)
    if src_appl_filter:
        parts.append(f"SRC_APPL_NAME = '{src_appl_filter}'")
    return ' AND '.join(parts)


# ─────────────────────────────────────────────────────────────────────────────
# Phase 1: Grouped Count (all tenants in one query)
# ─────────────────────────────────────────────────────────────────────────────

def get_tenant_counts_grouped(db: str, schema: str, table: str,
                              combined_where: str, sf_cursor) -> dict:
    """
    Returns {tenant_id: count} for ALL tenants in a single GROUP BY query.
    One round-trip per side regardless of tenant count.
    Missing tenants (zero rows for that tenant/date) will be absent from the dict → treat as 0.
    """
    sql  = (f"SELECT TENANT_ID, COUNT(*) FROM {db}.{schema}.{table} "
            f"WHERE {combined_where} GROUP BY TENANT_ID;")
    rows = safe_query(sql, sf_cursor)
    return {row[0]: int(row[1]) for row in rows} if rows else {}


# ─────────────────────────────────────────────────────────────────────────────
# Phase 2: Hash Comparison (Snowflake HASH_AGG on common columns)
# ─────────────────────────────────────────────────────────────────────────────

def compare_hashes(schema: str, table: str, shard_db: str,
                   where_clause: str, sf_cursor,
                   common_cols: list | None = None) -> tuple[int | None, int | None, str]:
    """
    Returns (cur_hash, shard_hash, status).

    When common_cols is provided (recommended), both sides hash ONLY those columns via a
    subquery — this correctly handles structural differences such as SHARDING_TS existing
    in the shard but not in CUR_IBS.  Column names are double-quoted to handle reserved
    words and case-sensitive identifiers.

    When common_cols is an empty list (no overlapping columns found), returns ERROR rather
    than generating invalid SQL (SELECT  FROM ...).

    When common_cols is None, falls back to HASH_AGG(*) — only safe when schemas are identical.

    HASH_AGG returns NULL for an empty result set; both-NULL is treated as SUCCESS because
    two empty tables are in parity by definition.

    status: 'SUCCESS' | 'FAIL' | 'ERROR'
    """
    if common_cols is not None and len(common_cols) == 0:
        logging.error(f"{table}: no common columns found -- cannot hash. Returning ERROR.")
        return (None, None, 'ERROR')

    if common_cols:
        col_list  = ', '.join(f'"{c}"' for c in common_cols)
        cur_sql   = (f"SELECT HASH_AGG(*) FROM "
                     f"(SELECT {col_list} FROM CUR_IBS.{schema}.{table} WHERE {where_clause});")
        shard_sql = (f"SELECT HASH_AGG(*) FROM "
                     f"(SELECT {col_list} FROM {shard_db}.{schema}.{table} WHERE {where_clause});")
    else:
        cur_sql   = f"SELECT HASH_AGG(*) FROM CUR_IBS.{schema}.{table} WHERE {where_clause};"
        shard_sql = f"SELECT HASH_AGG(*) FROM {shard_db}.{schema}.{table} WHERE {where_clause};"

    cur_hash   = safe_scalar(cur_sql, sf_cursor)
    shard_hash = safe_scalar(shard_sql, sf_cursor)

    if cur_hash is None and shard_hash is None:
        return (None, None, 'SUCCESS')

    status = 'SUCCESS' if cur_hash == shard_hash else 'FAIL'
    return (cur_hash, shard_hash, status)


def get_diff_sample(schema: str, table: str, shard_db: str,
                    where_clause: str, sf_cursor,
                    common_cols: list, max_rows: int = 5) -> dict:
    col_list = ', '.join(f'"{c}"' for c in common_cols)

    cur_total_sql = f"SELECT COUNT(*) FROM CUR_IBS.{schema}.{table} WHERE {where_clause};"
    shard_total_sql = f"SELECT COUNT(*) FROM {shard_db}.{schema}.{table} WHERE {where_clause};"

    result = {
        'cur_total': 0,
        'shard_total': 0,
        'cur_only_count': 0,
        'shard_only_count': 0,
        'matched_count': 0,
        'differing_columns': [],
        'column_diff_counts': {},
        'sample_diffs': []
    }

    result['cur_total'] = safe_scalar(cur_total_sql, sf_cursor, default=0)
    result['shard_total'] = safe_scalar(shard_total_sql, sf_cursor, default=0)

    key_cols = _identify_key_columns(common_cols)

    if key_cols and result['cur_total'] > 0 and result['shard_total'] > 0:
        key_join = ' AND '.join(f'c."{k}" = s."{k}"' for k in key_cols)
        non_key_cols = [c for c in common_cols if c not in key_cols]

        joined_count_sql = (
            f"SELECT COUNT(*) "
            f"FROM (SELECT {col_list} FROM CUR_IBS.{schema}.{table} WHERE {where_clause}) c "
            f"JOIN (SELECT {col_list} FROM {shard_db}.{schema}.{table} WHERE {where_clause}) s "
            f"ON {key_join};"
        )
        joined_count = safe_scalar(joined_count_sql, sf_cursor, default=0)
        result['matched_count'] = joined_count
        result['cur_only_count'] = result['cur_total'] - joined_count
        result['shard_only_count'] = result['shard_total'] - joined_count

        if non_key_cols and joined_count > 0:
            col_diffs = []
            for col in non_key_cols:
                col_diffs.append(
                    f'SUM(CASE WHEN NOT EQUAL_NULL(c."{col}", s."{col}") THEN 1 ELSE 0 END) AS "{col}"'
                )
            diff_count_sql = (
                f"SELECT {', '.join(col_diffs)} "
                f"FROM (SELECT {col_list} FROM CUR_IBS.{schema}.{table} WHERE {where_clause}) c "
                f"JOIN (SELECT {col_list} FROM {shard_db}.{schema}.{table} WHERE {where_clause}) s "
                f"ON {key_join};"
            )
            rows = safe_query(diff_count_sql, sf_cursor)
            if rows and rows[0]:
                for i, col in enumerate(non_key_cols):
                    cnt = rows[0][i] or 0
                    if cnt > 0:
                        result['column_diff_counts'][col] = cnt
                result['differing_columns'] = sorted(result['column_diff_counts'].keys())

            if result['differing_columns']:
                diff_col_list = result['differing_columns'][:10]
                select_parts = []
                for k in key_cols:
                    select_parts.append(f'c."{k}" AS "KEY_{k}"')
                for col in diff_col_list:
                    select_parts.append(f'c."{col}" AS "CUR_{col}"')
                    select_parts.append(f's."{col}" AS "SHARD_{col}"')

                where_diff = ' OR '.join(
                    f'NOT EQUAL_NULL(c."{col}", s."{col}")' for col in diff_col_list
                )
                sample_sql = (
                    f"SELECT {', '.join(select_parts)} "
                    f"FROM (SELECT {col_list} FROM CUR_IBS.{schema}.{table} WHERE {where_clause}) c "
                    f"JOIN (SELECT {col_list} FROM {shard_db}.{schema}.{table} WHERE {where_clause}) s "
                    f"ON {key_join} "
                    f"WHERE {where_diff} "
                    f"LIMIT {max_rows};"
                )
                sample_rows = safe_query(sample_sql, sf_cursor)
                if sample_rows:
                    col_names = []
                    for k in key_cols:
                        col_names.append(f"KEY_{k}")
                    for col in diff_col_list:
                        col_names.append(f"CUR_{col}")
                        col_names.append(f"SHARD_{col}")

                    for row in sample_rows:
                        row_dict = dict(zip(col_names, row))
                        entry = {'keys': {}, 'diffs': {}}
                        for k in key_cols:
                            entry['keys'][k] = row_dict.get(f"KEY_{k}")
                        for col in diff_col_list:
                            cur_val = row_dict.get(f"CUR_{col}")
                            shard_val = row_dict.get(f"SHARD_{col}")
                            if cur_val != shard_val:
                                entry['diffs'][col] = {'cur': cur_val, 'shard': shard_val}
                        if entry['diffs']:
                            result['sample_diffs'].append(entry)
    else:
        cur_except_sql = (
            f"SELECT {col_list} FROM CUR_IBS.{schema}.{table} WHERE {where_clause} "
            f"EXCEPT "
            f"SELECT {col_list} FROM {shard_db}.{schema}.{table} WHERE {where_clause}"
        )
        shard_except_sql = (
            f"SELECT {col_list} FROM {shard_db}.{schema}.{table} WHERE {where_clause} "
            f"EXCEPT "
            f"SELECT {col_list} FROM CUR_IBS.{schema}.{table} WHERE {where_clause}"
        )
        result['cur_only_count'] = safe_scalar(f"SELECT COUNT(*) FROM ({cur_except_sql});", sf_cursor, default=0)
        result['shard_only_count'] = safe_scalar(f"SELECT COUNT(*) FROM ({shard_except_sql});", sf_cursor, default=0)
        result['matched_count'] = result['cur_total'] - result['cur_only_count']

    return result


def _identify_key_columns(common_cols: list) -> list:
    dw_keys = []
    for col in common_cols:
        upper = col.upper()
        if upper.startswith('DW_') and upper.endswith('_ID'):
            dw_keys.append(col)

    if dw_keys:
        keys = dw_keys[:1]
        if 'TENANT_ID' in common_cols:
            keys.append('TENANT_ID')
        return keys

    for col in common_cols:
        upper = col.upper()
        if upper == 'TENANT_ID':
            if 'PRCS_DTE' in common_cols:
                return ['TENANT_ID', 'PRCS_DTE'] + [
                    c for c in common_cols
                    if c.upper().endswith('_SEQ_NBR') or c.upper().endswith('_KEY')
                ][:1]
            return ['TENANT_ID']

    return common_cols[:2]

def validate_shard(shard_db: str, tenants: list, schema: str, tables: list,
                   process_date: str | None, process_date_end: str | None,
                   skip_hash: bool, output_file: str, sf_cursor,
                   apply_filter: str = 'Y', appl_name: str = '') -> list:
    """
    Two-level optimized validation.

    Level 1 — per table, all tenants batched into ONE query each:
      - Grouped COUNT:  SELECT TENANT_ID, COUNT(*) ... GROUP BY TENANT_ID
                        → 1 query per side (vs N_tenants queries before)
      - Combined HASH:  HASH_AGG across ALL tenants' rows at once
                        → if SUCCESS, every tenant verified with just 1 query per side

    Level 2 — drill-down, only when combined hash FAILS:
      - Per-tenant HASH_AGG to isolate exactly which tenant(s) differ

    Query savings (11 tenants, 100 tables, all SUCCESS):
      Before:  11 × 100 × 4 = 4,400 queries
      After:   100 × 4       =   400 queries  (~10× fewer)
    """
    outcomes = []

    # Pre-compute common columns for ALL tables in 2 queries total (not 2×N)
    col_cache: dict[str, list]      = {}
    shard_only_cache: dict[str, list] = {}
    if not skip_hash:
        print(f'  Resolving column mappings for {len(tables)} table(s) in {shard_db}.{schema} (2 queries)...')
        col_cache, shard_only_cache = resolve_common_columns(tables, schema, shard_db, sf_cursor)

    with open(output_file, mode='a', encoding='utf-8') as out_f:
        out_f.write(f'\n{"-"*130}\n')
        out_f.write(f'  Shard: {shard_db}   Tenants: {", ".join(tenants)}\n')
        out_f.write(f'{"-"*130}\n')
        out_f.write(
            f'{"Table":<55} | {"Tenant":<6} | {"CUR":>10} | {"Shard":>10} '
            f'| {"Count":^8} | {"Data":^8} | Notes\n'
        )
        out_f.write(f'{"-"*55}-+-{"-"*6}-+-{"-"*10}-+-{"-"*10}-+-{"-"*8}-+-{"-"*8}-+-{"-"*30}\n')

        for table in tables:
            common_cols   = col_cache.get(table)        if not skip_hash else None
            excluded_cols = shard_only_cache.get(table, []) if not skip_hash else []
            excl_note     = f'excl:{",".join(excluded_cols)}' if excluded_cols else ''

            # TB_C2 tables reside in DDW_CNF_DIM, not the app schema
            is_c2 = table.startswith('TB_C2')
            tbl_schema = 'DDW_CNF_DIM' if is_c2 else schema

            # Apply SRC_APPL_NAME filter for TB_C2 tables when --filter=Y
            src_appl_filter = appl_name if (is_c2 and apply_filter.upper() == 'Y') else None

            combined_where = build_combined_where_clause(
                tenants, table, process_date, process_date_end,
                src_appl_filter=src_appl_filter
            )

            # ── Level 1a: Grouped COUNT — 1 query per side for ALL tenants ──────
            cur_counts   = get_tenant_counts_grouped('CUR_IBS', tbl_schema, table, combined_where, sf_cursor)
            shard_counts = get_tenant_counts_grouped(shard_db,  tbl_schema, table, combined_where, sf_cursor)

            all_counts_SUCCESS = all(
                cur_counts.get(t, 0) == shard_counts.get(t, 0) for t in tenants
            )

            # ── Level 1b: Combined HASH — 1 query per side for ALL tenants ──────
            combined_hash_status     = 'SKIP'
            per_tenant_hash: dict[str, str] = {}
            per_tenant_diff: dict[str, dict] = {}

            if not skip_hash and all_counts_SUCCESS:
                _, _, combined_hash_status = compare_hashes(
                    tbl_schema, table, shard_db, combined_where, sf_cursor,
                    common_cols=common_cols
                )
                if combined_hash_status == 'FAIL':
                    # ── Level 2: Drill-down — per-tenant hash to isolate which differs
                    print(f'    >> Combined hash FAILED for {table} -- drilling down per tenant...')
                    for t in tenants:
                        if cur_counts.get(t, 0) == 0 and shard_counts.get(t, 0) == 0:
                            per_tenant_hash[t] = 'SKIP'
                        else:
                            t_where = build_where_clause(
                                t, table, process_date, process_date_end,
                                src_appl_filter=src_appl_filter
                            )
                            _, _, per_tenant_hash[t] = compare_hashes(
                                tbl_schema, table, shard_db, t_where, sf_cursor,
                                common_cols=common_cols
                            )
                            if per_tenant_hash[t] == 'FAIL' and common_cols:
                                diff = get_diff_sample(tbl_schema, table, shard_db, t_where,
                                                       sf_cursor, common_cols)
                                per_tenant_diff[f"{t}|{table}"] = diff

            # ── Resolve per-tenant results and write report rows ─────────────────
            first_row = True
            for tenant_id in tenants:
                cur_cnt   = cur_counts.get(tenant_id, 0)
                shard_cnt = shard_counts.get(tenant_id, 0)
                count_status = 'SUCCESS' if cur_cnt == shard_cnt else 'FAIL'

                if skip_hash or count_status != 'SUCCESS':
                    hash_status = 'SKIP'
                    hash_note   = ''
                elif combined_hash_status == 'SUCCESS':
                    hash_status = 'SUCCESS'
                    hash_note   = 'combined'
                elif combined_hash_status == 'SKIP':
                    hash_status = 'SKIP'
                    hash_note   = ''
                else:
                    hash_status = per_tenant_hash.get(tenant_id, 'ERROR')
                    hash_note   = 'drill-down'

                overall_status = (
                    'FAIL'  if count_status == 'FAIL' or hash_status == 'FAIL'
                    else 'ERROR' if count_status == 'ERROR' or hash_status == 'ERROR'
                    else 'SUCCESS'
                )

                reason_parts = []
                if count_status != 'SUCCESS':
                    reason_parts.append(f'Count mismatch: CUR={cur_cnt} SHARD={shard_cnt}')
                if hash_status not in ('SUCCESS', 'SKIP'):
                    reason_parts.append('Hash mismatch (counts match, data differs)')
                reason = '; '.join(reason_parts) if reason_parts else 'All checks SUCCESSed'

                note      = ' '.join(filter(None, [hash_note, excl_note if first_row else '']))
                table_col = table if first_row else ''

                out_f.write(
                    f'{table_col:<55} | {tenant_id:<6} | {cur_cnt:>10,} | {shard_cnt:>10,} '
                    f'| {count_status:^8} | {hash_status:^8} | {note}\n'
                )

                status_icon = '[SUCCESS]' if overall_status == 'SUCCESS' else '[FAIL]'
                print(f'  {status_icon} [{shard_db}] {tenant_id}.{table:<45} '
                      f'CUR:{cur_cnt:>10,} SHARD:{shard_cnt:>10,} '
                      f'Count:{count_status} Data:{hash_status}')

                outcomes.append({
                    'tenant_id':    tenant_id,
                    'shard_db':     shard_db,
                    'table':        table,
                    'cur_count':    cur_cnt,
                    'shard_count':  shard_cnt,
                    'count_status': count_status,
                    'hash_status':  hash_status,
                    'status':       overall_status,
                    'reason':       reason,
                    'diff_sample':  per_tenant_diff.get(f"{tenant_id}|{table}")
                })
                first_row = False

    return outcomes


# ─────────────────────────────────────────────────────────────────────────────
# Report Header / Footer Helpers
# ─────────────────────────────────────────────────────────────────────────────

def write_report_header(output_file: str, schema: str, tenants_label: str,
                        process_date: str | None, process_date_end: str | None, skip_hash: bool):
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    if os.path.exists(output_file):
        os.remove(output_file)
    date_label = 'ALL'
    if process_date:
        date_label = f'{process_date} to {process_date_end}' if process_date_end and process_date_end != process_date else process_date
    with open(output_file, mode='w', encoding='utf-8') as f:
        f.write(f'+{"="*120}+\n')
        f.write(f'| CURATED vs SHARD DATA VALIDATION REPORT{" "*80}|\n')
        f.write(f'+{"="*120}+\n')
        f.write(f'| Schema  : {schema:<25} | Tenants : {tenants_label:<35} | Process Date : {date_label:<13} |\n')
        f.write(f'| Mode    : {"Count + Data" if not skip_hash else "Count Only":<25} | Source: CUR_IBS vs APP_IBS_SHARD_N{" "*45}|\n')
        f.write(f'+{"="*120}+\n')


def write_report_footer(output_file: str, outcomes: list):
    total  = len(outcomes)
    SUCCESSed = sum(1 for o in outcomes if o['status'] == 'SUCCESS')
    failed = sum(1 for o in outcomes if o['status'] == 'FAIL')
    errors = sum(1 for o in outcomes if o['status'] == 'ERROR')

    with open(output_file, mode='a', encoding='utf-8') as f:
        diff_outcomes = [o for o in outcomes if o.get('diff_sample')]
        if diff_outcomes:
            f.write(f'\n\n{"="*122}\n')
            f.write(f'{"DATA DIFFERENCE SAMPLES":^122}\n')
            f.write(f'{"="*122}\n')
            for o in diff_outcomes:
                diff = o['diff_sample']
                f.write(f'\n{"-"*122}\n')
                f.write(f'  Table: {o["table"]}  |  Tenant: {o["tenant_id"]}  |  Shard: {o["shard_db"]}\n')
                f.write(f'  Total rows -- CUR: {diff["cur_total"]:,}  |  Shard: {diff["shard_total"]:,}\n')
                f.write(f'  Matched: {diff["matched_count"]:,}  |  Only in CUR: {diff["cur_only_count"]:,}  |  Only in Shard: {diff["shard_only_count"]:,}\n')
                if diff.get('differing_columns'):
                    f.write(f'  Columns with differences:\n')
                    col_counts = diff.get('column_diff_counts', {})
                    for col in diff['differing_columns']:
                        cnt = col_counts.get(col)
                        if cnt:
                            f.write(f'    - {col} ({cnt:,} rows differ)\n')
                        else:
                            f.write(f'    - {col}\n')
                if diff.get('sample_diffs'):
                    f.write(f'\n  Sample differences (up to 5 rows):\n')
                    for i, entry in enumerate(diff['sample_diffs'], 1):
                        key_str = ', '.join(f'{k}={v}' for k, v in entry['keys'].items() if v is not None)
                        f.write(f'    [{i}] Row key: {key_str}\n')
                        for col, vals in entry['diffs'].items():
                            f.write(f'        {col}:  CUR={vals["cur"]}  |  Shard={vals["shard"]}\n')
            f.write(f'\n{"="*122}\n')

        f.write(f'\n{"="*122}\n')
        f.write(f'{"VALIDATION SUMMARY":^122}\n')
        f.write(f'{"="*122}\n')
        f.write(f'  Total checks : {total}\n')
        f.write(f'  SUCCESS      : {SUCCESSed}\n')
        f.write(f'  FAIL         : {failed}\n')
        f.write(f'  ERROR        : {errors}\n')
        if failed == 0 and errors == 0:
            f.write(f'\n{"[SUCCESS] ALL VALIDATIONS SUCCESSED":^122}\n')
        else:
            f.write(f'\n{"[FAIL] SOME VALIDATIONS FAILED -- review details above":^122}\n')
        f.write(f'{"="*122}\n')



# ─────────────────────────────────────────────────────────────────────────────
# Main Driver
# ─────────────────────────────────────────────────────────────────────────────

def load_validation():
    script_start  = time.perf_counter()
    argument_dict = arg_parsing()

    py_path         = os.environ["PYTHONPATH"]
    ingest_cfg_dict = load_yaml(f'{py_path}/IngestionConfig.yaml')

    script_name_base = os.path.splitext(os.path.basename(__file__))[0]
    

    sf_conn = open_sf_connection(ingest_cfg=ingest_cfg_dict)
    sf_cs   = sf_conn.cursor()
    logging.info('Snowflake connection opened (database: CUR_IBS)')

    schema       = argument_dict['appl_name']
    process_date     = argument_dict['process_date']
    process_date_end = argument_dict.get('process_date_end')
    skip_hash    = argument_dict['skip_hash']
    apply_filter = argument_dict['apply_filter']

    # Resolve appl_code from T_APPL_TABLE lookup
    appl_code = get_appl_code(schema, sf_cs)

    logging_config(
        log_dir=argument_dict.get('logging_directory', ''),
        appl_code=appl_code,
        script_name=script_name_base,
        ingestion_file=ingest_cfg_dict,
        log_level=argument_dict.get('log_level', 'INFO')
    )
    logging.info(f'Starting {SCRIPT_NAME} {SCRIPT_VERSION}')

    # ── Step 1: Resolve tenants and build shard mapping ───────────────────────
    if argument_dict['tenant_id'] != 'ALL':
        tenant_list = [t.strip() for t in argument_dict['tenant_id'].split(',')]
    else:
        tenant_list = get_tenants(schema, sf_cs)

    if not tenant_list:
        logging.error('No tenants found. Exiting.')
        sf_cs.close()
        sf_conn.close()
        return

    env = os.environ.get("PRJ_ENVIRONMENT", "")
    tenant_id_tuple = tuple(tenant_list)

    # Build shard_map {shard_db: [tenant_ids]} using setup_tenant_shard
    shard_map = defaultdict(list)
    for tenant_id in tenant_list:
        app_db = setup_tenant_shard(tenant_id, sf_cs, env, tenant_id_tuple)
        if app_db is None:
            logging.warning(f'Tenant {tenant_id} skipped (setup_tenant_shard returned None).')
            continue
        shard_map[app_db].append(tenant_id)

    if not shard_map:
        logging.error('No valid tenant-shard mappings resolved. Exiting.')
        sf_cs.close()
        sf_conn.close()
        return

    logging.info(f"Tenant-shard mapping resolved: {dict({k: len(v) for k, v in shard_map.items()})}")

    all_tenants_label = ','.join(
        t for tenants in shard_map.values() for t in tenants
    )

    # ── Step 2: Prepare output file ──────────────────────────────────────────
    # Build filename with tenant and process date for easy identification
    tenant_label = argument_dict['tenant_id'].replace(',', '_') if argument_dict['tenant_id'] != 'ALL' else 'ALL'
    if process_date and process_date_end and process_date_end != process_date:
        date_label = f"{process_date.replace('-', '')}_{process_date_end.replace('-', '')}"
    else:
        date_label = process_date.replace('-', '') if process_date else 'NODATE'
    file_path   = f"{ingest_cfg_dict['snowflake_connection']['validation_path']}/{schema}/{script_name_base}"
    output_file = f"{file_path}/{schema}_Curated_Shard_Validation_{tenant_label}_{date_label}.txt"

    write_report_header(output_file, schema, all_tenants_label, process_date, process_date_end, skip_hash)
    print(f'\n{"="*70}')
    print(f'Output: {output_file}')
    print(f'{"="*70}\n')

    # ── Step 3: For each shard, discover tables and run validations ──────────
    all_outcomes = []
    for shard_db, tenants in shard_map.items():
        print(f'\n>>> Processing shard: {shard_db}  ({len(tenants)} tenant(s): {tenants})')
        tables = get_comparable_tables(schema, shard_db, sf_cs, appl_code)
        if not tables:
            logging.warning(f'No comparable tables found for {shard_db}.{schema} -- skipping.')
            continue
        tb_c2_count = sum(1 for t in tables if t.startswith('TB_C2'))
        print(f'    Tables to validate: {len(tables)} ({tb_c2_count} TB_C2)')

        shard_outcomes = validate_shard(
            shard_db=shard_db, tenants=tenants, schema=schema,
            tables=tables, process_date=process_date,
            process_date_end=process_date_end,
            skip_hash=skip_hash, output_file=output_file, sf_cursor=sf_cs,
            apply_filter=apply_filter, appl_name=schema
        )
        all_outcomes.extend(shard_outcomes)

    # ── Step 4: Write report footer ──────────────────────────────────────────
    write_report_footer(output_file, all_outcomes)
    SUCCESSed = sum(1 for o in all_outcomes if o['status'] == 'SUCCESS')
    failed = sum(1 for o in all_outcomes if o['status'] == 'FAIL')
    errors = sum(1 for o in all_outcomes if o['status'] == 'ERROR')
    print(f'\nTotal: {len(all_outcomes)} | SUCCESS: {SUCCESSed} | FAIL: {failed} | ERROR: {errors}')
    if failed == 0 and errors == 0:
        print(f'[SUCCESS] Curated-Shard validation SUCCESSED for schema: {schema}')
    else:
        print(f'[FAIL] Curated-Shard validation has FAILURES -- see {output_file}')

    script_run_time = time.perf_counter() - script_start

    # ── Step 5: Load metadata to Snowflake ───────────────────────────────────
    if argument_dict.get('load_sf_meta'):
        try:
            registry = TestCaseRegistry(
                sf_cs, SCRIPT_NAME,
                database=argument_dict.get('sf_meta_db'),
                schema=argument_dict.get('sf_meta_schema')
            )
            # NOTE: 'curated_shard_count_hash_validation' must exist in TEST_CASE_DEFINITION
            # table before running with --load-sf-meta, otherwise TestCaseRegistry will fall
            # back with a warning and dashboard filters keyed on this value may not match.
            val_results = []
            table_groups: dict[str, list] = defaultdict(list)
            for outcome in all_outcomes:
                key = f"{outcome['shard_db']}|{outcome['table']}"
                table_groups[key].append(outcome)

            for key, group in table_groups.items():
                shard_db_g, table_g = key.split('|', 1)
                all_SUCCESS = all(o['status'] == 'SUCCESS' for o in group)

                if all_SUCCESS:
                    total_cur   = sum(o.get('cur_count', 0) for o in group)
                    total_shard = sum(o.get('shard_count', 0) for o in group)
                    tenant_list = ','.join(o['tenant_id'] for o in group)
                    val_results.append(registry.create_result(
                        validation_key='curated_shard_count_hash_validation',
                        test_scenario=(
                            f"[{shard_db_g}] CUR vs Shard: "
                            f"{table_g} ({len(group)} tenants) Count:SUCCESS Hash:SUCCESS"
                        ),
                        appl_name=argument_dict['appl_name'],
                        appl_code=appl_code,
                        tenant_id=tenant_list,
                        table_name=table_g,
                        validation_status='SUCCESS',
                        status_reason='All checks SUCCESSed',
                        source_count=total_cur,
                        target_count=total_shard,
                        mismatched_count=0,
                        matched_count=min(total_cur, total_shard)
                    ))
                else:
                    for outcome in group:
                        src_c = outcome.get('cur_count', 0)
                        tgt_c = outcome.get('shard_count', 0)
                        val_results.append(registry.create_result(
                            validation_key='curated_shard_count_hash_validation',
                            test_scenario=(
                                f"[{outcome['shard_db']}] CUR vs Shard: "
                                f"{outcome['tenant_id']}.{outcome['table']} "
                                f"Count:{outcome['count_status']} Hash:{outcome['hash_status']}"
                            ),
                            appl_name=argument_dict['appl_name'],
                            appl_code=appl_code,
                            tenant_id=outcome['tenant_id'],
                            table_name=outcome['table'],
                            validation_status=outcome['status'],
                            status_reason=outcome['reason'],
                            source_count=src_c,
                            target_count=tgt_c,
                            mismatched_count=(
                                abs(src_c - tgt_c) if outcome['count_status'] == 'FAIL'
                                else (1 if outcome['hash_status'] == 'FAIL' else 0)
                            ),
                            matched_count=min(src_c, tgt_c)
                        ))

            if not val_results:
                val_results.append(registry.create_result(
                    validation_key='curated_shard_count_hash_validation',
                    test_scenario='Curated vs Shard validation',
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
                database=argument_dict.get('sf_meta_db'),
                schema=argument_dict.get('sf_meta_schema')
            )
            summary = ExecutionSummary(
                script_name=SCRIPT_NAME,
                appl_name=argument_dict.get('appl_name', ''),
                appl_code=appl_code,
                tenant_id=argument_dict.get('tenant_id', 'ALL'),
                process_date=argument_dict.get('process_date', ''),
                script_version=SCRIPT_VERSION
            )
            summary.started_at       = datetime.fromtimestamp(time.time() - (time.perf_counter() - script_start))
            summary.parameters_used  = {k: str(v) for k, v in argument_dict.items() if k != 'sf_cursor'}
            summary.update_counts(val_results)
            summary.execution_time_sec = script_run_time

            if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
                summary.read_and_store_output(output_file, file_type='curated_shard_validation_report')

            exec_id    = loader.insert_execution_summary(summary)
            run_ids   = loader.insert_master_bulk(val_results, execution_id=exec_id)
            run_id_map = {
                f"{r.tenant_id}_{r.table_name}": rid
                for r, rid in zip(val_results, run_ids)
            }

            detail_batch = []
            for outcome in all_outcomes:
                if outcome['status'] in ('FAIL', 'ERROR'):
                    rid = run_id_map.get(f"{outcome['tenant_id']}_{outcome['table']}", 0)
                    if rid:
                        source_data = {
                            'cur_count':    outcome.get('cur_count', 0),
                            'count_status': outcome['count_status'],
                            'hash_status':  outcome['hash_status']
                        }
                        if outcome.get('diff_sample'):
                            ds = outcome['diff_sample']
                            source_data['diff_summary'] = {
                                'cur_only_count': ds['cur_only_count'],
                                'shard_only_count': ds['shard_only_count'],
                                'differing_columns': ds.get('differing_columns', [])
                            }
                        detail_batch.append(ValidationDetailResult(
                            run_id=rid,
                            match_status='MISMATCH',
                            record_key=f"{outcome['shard_db']}.{outcome['tenant_id']}.{outcome['table']}",
                            source_data=source_data,
                            target_data={
                                'shard_count':  outcome.get('shard_count', 0),
                                'shard_db':     outcome['shard_db']
                            },
                            detail_remarks=outcome.get('reason', '')
                        ))

            capped, total_detail, was_capped = cap_details(detail_batch)
            if capped:
                loader.insert_detail_bulk(capped)
            if was_capped:
                logging.warning(f'Detail rows capped at {len(capped)} of {total_detail}')

            summary.emit_summary_line()
            logging.info(f'Loaded {len(val_results)} result(s) -- {SUCCESSed} SUCCESS, {failed} FAIL, {errors} ERROR')

        except Exception as exc:
            logging.error(f'Failed to load validation results to Snowflake: {exc}')
            traceback.print_exc()
    else:
        logging.info('Skipping metadata load to Snowflake (--load-sf-meta not specified)')

    sf_cs.close()
    sf_conn.close()
    print(f"\n{'-'*50}")
    print(f'Script run time: {script_run_time:.2f} seconds')


if __name__ == '__main__':
    load_validation()
