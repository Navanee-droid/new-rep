# [03-Apr-2026] - Barath Lakshman - Changes done with respect to SCD expiry check

import subprocess
import sys
import traceback
import logging
import oracledb
import snowflake.connector
import pandas as pd
import csv
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import os
import toml
import yaml
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
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
    cap_details
)
from script_utils import (
    parse_args,
    open_sf_connection,
    open_oracle_connection,
    load_yaml,
    logging_config,
    get_appl_code,
)

from DDWJoinValidation_MetadataGenerator import join_validation_metadata_generator

SCRIPT_NAME = "DDWJoinValidation.py"
SCRIPT_VERSION = "v2.1"
METADATA = "_metadata.csv"
SCD_EXP_COL_CACHE = {}  # Cache: join_table -> expiry column that worked


def arg_parsing() -> dict:
    args = parse_args(
        required=['--a', '--t', '--p', '--m_date'],
        optional=['--input_csv', '--l', '--o','--filter', '--load-sf-meta', '--sf-meta-db', '--sf-meta-schema'],
        description='Join validation between fact and mfact tables.',
    )
    args['fact_date'] = args['process_date']
    args['mfact_date'] = args['m_process_date']
    args['input_file'] = args['input_csv']
    args['filter'] = args.get('apply_filter') or 'Y'
    return args

def safe_strip(val):
    return "" if pd.isna(val) else str(val).strip()

def get_fact_date(fact_table,arg_dict):
    t = fact_table.lower()
    return arg_dict["mfact_date"] if any(x in t for x in ["m_fact", "mfact", "mthly_fact", "monthly_fact"]) else arg_dict['fact_date']

def get_snowflake_schema(table_name, default_schema):
    """Return DDW_CNF_DIM if table starts with TB_C2, else use default schema"""
    if table_name.startswith("TB_C2"):
        return "DDW_CNF_DIM"
    return default_schema

def build_src_join_appl_filter(table_name, join_schema):
    join_schema_quoted = ", ".join(f"'{a.strip()}'" for a in join_schema.split(","))
    if table_name.startswith("TB_C2") and join_schema and join_schema.upper() != "DDW_CNF_DIM":
        if table_name not in ('TB_C2_DZ4_DAY_TME_DIM'):
            return f"AND d.SRC_APPL_NAME IN ({join_schema_quoted})"
    return ""

def build_src_appl_filter(table_name, join_schema):
    if table_name.startswith("TB_C2") and join_schema:
        if table_name not in ('TB_C2_DZ4_DAY_TME_DIM'):
            return f"AND f.SRC_APPL_NAME = '{join_schema}'"
    return ""

def build_dim_join_appl_filter(table, date_val):
    # return a PRCS_DTE filter for DIM tables unless the table name contains known SCD/RCD/RPD/FACT markers
    t = table.upper() if table else ''
    exclude_markers = ("SCD", "RCD", "RPD","USCD", "FACT")
    if t.endswith("DIM") and not any(marker in t for marker in exclude_markers) and table not in ('TB_C2_DZ4_DAY_TME_DIM'):
        return f"AND d.PRCS_DTE = TO_DATE('{date_val}','YYYYMMDD')"
    return ""

def apply_alias_to_filter(filter_expr, alias):
    
    if not filter_expr:
        return ""
    
    # SQL keywords and operators that should NOT be prefixed with alias
    sql_keywords = {
        'AND', 'OR', 'NOT', 'IN', 'LIKE', 'IS', 'NULL', 'BETWEEN', 
        'EXISTS', 'ANY', 'ALL', 'SOME', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
        'TRUE', 'FALSE', 'ASC', 'DESC', 'DISTINCT', 'AS'
    }
    
    # First, protect string literals by replacing them with placeholders
    string_literals = []
    def save_literal(match):
        string_literals.append(match.group(0))
        return f"__STRING_LITERAL_{len(string_literals) - 1}__"
    
    # Match single-quoted strings
    protected_expr = re.sub(r"'[^']*'", save_literal, filter_expr)
    
    # Pattern to match column names (word boundaries, not followed by opening parenthesis)
    pattern = r'\b([A-Za-z_][A-Za-z0-9_]*)\b(?!\s*\()'
    
    def replace_column(match):
        col = match.group(1)
        # Skip placeholders
        if col.startswith('__STRING_LITERAL_'):
            return col
        # Don't add alias to SQL keywords
        if col.upper() in sql_keywords:
            return col
        # Check if this column already has an alias (look behind for a dot)
        start_pos = match.start()
        if start_pos > 0 and protected_expr[start_pos - 1] == '.':
            return col
        return f"{alias}.{col}"
    
    # Apply replacement on protected expression
    aliased_filter = re.sub(pattern, replace_column, protected_expr)
    
    # Restore string literals
    for i, literal in enumerate(string_literals):
        aliased_filter = aliased_filter.replace(f"__STRING_LITERAL_{i}__", literal)
    
    return aliased_filter

def run_query(conn, query, label, silent=False):
    try:
        cur = conn.cursor()
        cur.execute(query)
        res = cur.fetchone()
        count = res[0] if res and res[0] is not None else 0
        cur.close()
        return count
    except Exception as e:
        if not silent:
            logging.error(f"{label} failed: {e}")
        return f"ERROR: {e}"


def build_filter(alias, col, val):
    return f"AND {alias}.{col} = '{val}'" if col and val else ""

def parse_filter(expr):
    if "=" in expr:
        parts = [x.strip() for x in expr.split("=")]
        if len(parts) == 2:
            return parts[0], parts[1].strip("'").strip('"')
    return "", ""

#[03-Apr-2026]
def is_scd_like(table):  
    return any(x in table.upper() for x in ["SCD", "RCD", "RPD", "USCD", "DATA_SRC_TBL_CUR_DIM"])

#[03-Apr-2026]
def run_scd_expiry_check(  sf_conn, sf_db, sf_schema, join_sf_schema, table, join_table,
                         column, join_column, date_val, tenant_id,
                         src_appl_filter, src_join_appl_filter):
    """Snowflake-only check: rows joined to expired SCD/RCD/RPD/USCD records must be 0.
    Tries PRCS_EXP_DTE first, falls back to SOR_EXP_DTE. Caches result per join_table."""
    if join_table in SCD_EXP_COL_CACHE:
        exp_col = SCD_EXP_COL_CACHE[join_table]
        query = f"""SELECT COUNT(*)
            FROM {sf_db}.{sf_schema}.{table} f
            JOIN {sf_db}.{join_sf_schema}.{join_table} d ON f.{column} = d.{join_column}
            WHERE f.PRCS_DTE = TO_DATE('{date_val}', 'YYYYMMDD')
            AND f.{column} <> 0
            AND f.TENANT_ID = '{tenant_id}'
            AND d.{exp_col} < TO_DATE('{date_val}', 'YYYYMMDD')
            {src_appl_filter} {src_join_appl_filter}"""
        result = run_query(sf_conn, query, f"SF SCD Expiry [{exp_col}] {table}-{join_table}")
        return (result, exp_col, query) if not isinstance(result, str) else ("SKIPPED", None, "")
    for exp_col in ['PRCS_EXP_DTE', 'SOR_EXP_DTE']:
        query = f"""SELECT COUNT(*)
            FROM {sf_db}.{sf_schema}.{table} f
            JOIN {sf_db}.{join_sf_schema}.{join_table} d ON f.{column} = d.{join_column}
            WHERE f.PRCS_DTE = TO_DATE('{date_val}', 'YYYYMMDD')
            AND f.{column} <> 0
            AND f.TENANT_ID = '{tenant_id}'
            AND d.{exp_col} < TO_DATE('{date_val}', 'YYYYMMDD')
            {src_appl_filter} {src_join_appl_filter}"""
        result = run_query(sf_conn, query, f"SF SCD Expiry [{exp_col}] {table}-{join_table}", silent=(exp_col == 'PRCS_EXP_DTE'))
        if not isinstance(result, str):  # successful, not an error string
            SCD_EXP_COL_CACHE[join_table] = exp_col
            if exp_col == 'SOR_EXP_DTE':
                print(f"  [{join_table}] PRCS_EXP_DTE not found, using SOR_EXP_DTE for expiry check.")
            return result, exp_col, query
    return "SKIPPED", None, ""  #[06-Apr-2026]

def get_filter(table, appl_name, sf_conn):
    appl_names_quoted = ", ".join(f"'{a.strip()}'" for a in appl_name.split(","))
    get_filter_query = f"""
        SELECT FILTER_FIELDS
        FROM RAW_IBS.VALIDATION_DASHBOARD.TB_C2_FILTER_FIELDS
        WHERE table_name = '{table}' AND appl_name IN ({appl_names_quoted})
    """
    cur = sf_conn.cursor()
    try:
        cur.execute(get_filter_query)
        results = cur.fetchall()
        if not results:
            return ""
        seen = set()
        filters = []
        for row in results:
            val = row[0]
            if val and val not in seen:
                seen.add(val)
                filters.append(val)
        return "(" + " OR ".join(filters) + ")" if filters else ""
    except Exception as e:
        logging.error(f"Error retrieving filter for table {table} and application {appl_name}: {e}")
        return ""
    finally:
        cur.close()

def validate_relationship(rel,sf_conn,oracle_conn,arg_dict,ora_user):
    ORACLE_SCHEMA = ora_user
    TENANT_ID = arg_dict["tenant_id"]
    SF_DB = 'CUR_IBS'
    schema = safe_strip(rel["SCHEMA"])
    table = safe_strip(rel["TABLE"])
    filter_expr = safe_strip(rel["FILTER"])
    column = safe_strip(rel["COLUMN"])
    join_schema = safe_strip(rel["JOIN_SCHEMA"])
    join_table = safe_strip(rel["JOIN_TABLE"])
    orc_filter = safe_strip(rel["ORC_FILTER"])
    sf_filter = safe_strip(rel["SF_FILTER"])
    join_column = safe_strip(rel["JOIN_COLUMN"])

    date_val = get_fact_date(table,arg_dict)

    logging.info(f"Executing: {table}.{column} -> {join_table}.{join_column}")

    # Determine actual Snowflake schemas
    actual_sf_schema = get_snowflake_schema(table, schema)
    actual_join_sf_schema = get_snowflake_schema(join_table, join_schema if join_schema else schema)
    
    join_appl_info = ""
    appl_info = ""
    if actual_sf_schema == "DDW_CNF_DIM" and schema and schema.upper() != "DDW_CNF_DIM":
        appl_info = "-"+schema.strip()
    if actual_join_sf_schema == "DDW_CNF_DIM" and join_schema and join_schema.upper() != "DDW_CNF_DIM":
        join_appl_info = "-"+join_schema.strip()
    # Build SRC_APPL_NME filter for TB_C2 tables
    src_appl_filter = build_src_appl_filter(table, schema)
    src_join_appl_filter = build_src_join_appl_filter(join_table, join_schema)
    dim_join_filter = build_dim_join_appl_filter(join_table,date_val)
    sf_join_filter = f"AND {sf_filter}" if sf_filter else ""

    
 
    # Apply alias to filter expressions
    if not orc_filter and join_table.startswith("TB_C2") and join_schema:
        orc_filter = get_filter(join_table, join_schema, sf_conn)
    oracle_filter = f"AND {apply_alias_to_filter(filter_expr, 'f')}" if filter_expr and arg_dict['filter'].upper() == 'Y' else ""
    oracle_join_filter = f"AND {apply_alias_to_filter(orc_filter, 'd')}" if orc_filter and arg_dict['filter'].upper() == 'Y' else ""

    # Validation queries
    ora_fact_sql = f"""SELECT COUNT(*) FROM {ORACLE_SCHEMA}.{table} f WHERE f.PRCS_DTE = TO_DATE('{date_val}','YYYYMMDD') AND f.{column} <> 0 {oracle_filter}"""
    ora_join_sql = f"""SELECT COUNT(*) FROM {ORACLE_SCHEMA}.{table} f JOIN {ORACLE_SCHEMA}.{join_table} d ON f.{column} = d.{join_column} WHERE f.PRCS_DTE = TO_DATE('{date_val}','YYYYMMDD') AND f.{column} <> 0 {oracle_filter} {oracle_join_filter}"""
    ora_zero_sql = f"""SELECT COUNT(*) FROM {ORACLE_SCHEMA}.{table} f WHERE f.PRCS_DTE = TO_DATE('{date_val}','YYYYMMDD') AND f.{column} = 0 {oracle_filter}"""

    sf_fact_sql = f"""SELECT COUNT(*) FROM {SF_DB}.{actual_sf_schema}.{table} f WHERE f.PRCS_DTE = TO_DATE('{date_val}', 'YYYYMMDD') AND f.{column} <> 0 AND f.TENANT_ID = '{TENANT_ID}' {src_appl_filter}"""
    sf_join_sql = f"""SELECT COUNT(*) FROM {SF_DB}.{actual_sf_schema}.{table} f JOIN {SF_DB}.{actual_join_sf_schema}.{join_table} d ON f.{column} = d.{join_column} WHERE f.PRCS_DTE = TO_DATE('{date_val}', 'YYYYMMDD') AND f.{column} <> 0 AND f.TENANT_ID = '{TENANT_ID}' {dim_join_filter} {src_appl_filter} {src_join_appl_filter} {sf_join_filter}"""
    sf_zero_sql = f"""SELECT COUNT(*) FROM {SF_DB}.{actual_sf_schema}.{table} f WHERE f.PRCS_DTE = TO_DATE('{date_val}', 'YYYYMMDD') AND f.{column} = 0 AND f.TENANT_ID = '{TENANT_ID}' {src_appl_filter}"""

    ora_fact_count = run_query(oracle_conn, ora_fact_sql, f"Oracle Fact {table}")
    ora_join_count = run_query(oracle_conn, ora_join_sql, f"Oracle Join {table}-{join_table}")
    ora_zero_count = run_query(oracle_conn, ora_zero_sql, f"Oracle Zero {table}")

    sf_fact_count = run_query(sf_conn, sf_fact_sql, f"Snowflake Fact {table}")
    sf_join_count = run_query(sf_conn, sf_join_sql, f"Snowflake Join {table}-{join_table}")
    sf_zero_count = run_query(sf_conn, sf_zero_sql, f"Snowflake Zero {table}")

    # SCD/RCD/RPD/USCD expiry check (Snowflake only) — count of rows joined to expired records must be 0  #[03-Apr-2026]
    sf_scd_expiry_count = "N/A"  #[03-Apr-2026]
    sf_scd_expiry_status = "N/A"  #[03-Apr-2026]
    sf_scd_expiry_sql = "N/A"  #[06-Apr-2026]
    if is_scd_like(join_table):  #[03-Apr-2026]
        sf_scd_expiry_count, _used_exp_col, sf_scd_expiry_sql = run_scd_expiry_check(  #[06-Apr-2026]
            sf_conn, SF_DB, actual_sf_schema, actual_join_sf_schema,
            table, join_table, column, join_column,
            date_val, TENANT_ID, src_appl_filter, src_join_appl_filter
        )
        if sf_scd_expiry_count == "SKIPPED":
            sf_scd_expiry_status = "SKIPPED"
        elif isinstance(sf_scd_expiry_count, str):  # ERROR string
            sf_scd_expiry_status = "ERROR"
        else:
            sf_scd_expiry_status = "SUCCESS" if sf_scd_expiry_count == 0 else "FAIL"

    ora_status = "ERROR" if isinstance(ora_fact_count, str) or isinstance(ora_join_count, str) else "NO DATA" if ora_fact_count == 0 and ora_zero_count == 0 else "SUCCESS" if ora_fact_count == ora_join_count else "FAIL"
    sf_status = "ERROR" if isinstance(sf_fact_count, str) or isinstance(sf_join_count, str) else "NO DATA" if sf_fact_count == 0 and sf_zero_count == 0 else "SUCCESS" if sf_fact_count == sf_join_count else "FAIL"
    # cross_status = "ERROR" if isinstance(ora_join_count, str) or isinstance(sf_join_count, str) else "SUCCESS" if ora_join_count == sf_join_count else "FAIL"
    cross_status = "ERROR" if isinstance(ora_join_count, str) or isinstance(sf_join_count, str) else ora_status if ora_status == sf_status else "FAIL"
    zero_status = "ERROR" if isinstance(ora_zero_count, str) or isinstance(sf_zero_count, str) else "SUCCESS" if ora_zero_count == sf_zero_count else "FAIL"

    oracle_query_format = f"""
SELECT '{TENANT_ID}', '{table}' AS TABLE_NAME, 'FACT' AS RELATION_WITH, '{actual_sf_schema}' AS APPLICATION, COUNT(*), f.PRCS_DTE, '{column}' AS DW_ID
FROM {ORACLE_SCHEMA}.{table} f
WHERE f.PRCS_DTE = TO_DATE('{date_val}','YYYYMMDD') AND f.{column} <> 0 {oracle_filter}
GROUP BY f.PRCS_DTE
UNION ALL
SELECT '{TENANT_ID}', '{table}' AS TABLE_NAME, '{join_table}' AS RELATION_WITH, '{actual_join_sf_schema}' AS APPLICATION, COUNT(*), f.PRCS_DTE, '{join_column}' AS DW_ID
FROM {ORACLE_SCHEMA}.{table} f, {ORACLE_SCHEMA}.{join_table} d
WHERE f.{column} = d.{join_column} AND f.{column} <> 0 {oracle_filter} {oracle_join_filter} 
AND f.PRCS_DTE = TO_DATE('{date_val}','YYYYMMDD')
GROUP BY f.PRCS_DTE
""".strip()

    snowflake_query_format = f"""
SELECT '{TENANT_ID}', '{table}' AS TABLE_NAME, 'FACT' AS RELATION_WITH, '{actual_sf_schema}' AS APPLICATION, COUNT(*), f.PRCS_DTE, '{column}' AS DW_ID
FROM {SF_DB}.{actual_sf_schema}.{table} f
WHERE f.PRCS_DTE = TO_DATE('{date_val}', 'YYYYMMDD') AND f.TENANT_ID = '{TENANT_ID}' AND f.{column} <> 0 {src_appl_filter}
GROUP BY f.PRCS_DTE
UNION ALL
SELECT '{TENANT_ID}', '{table}' AS TABLE_NAME, '{join_table}' AS RELATION_WITH, '{actual_join_sf_schema}' AS APPLICATION, COUNT(*), f.PRCS_DTE, '{join_column}' AS DW_ID
FROM {SF_DB}.{actual_sf_schema}.{table} f, {SF_DB}.{actual_join_sf_schema}.{join_table} d
WHERE f.{column} = d.{join_column} AND f.{column} <> 0 {src_appl_filter} {src_join_appl_filter} {dim_join_filter} {sf_join_filter}
AND f.PRCS_DTE = TO_DATE('{date_val}', 'YYYYMMDD') AND f.TENANT_ID = '{TENANT_ID}'
GROUP BY f.PRCS_DTE
""".strip()
    
    return {
        "SCHEMA": actual_sf_schema + appl_info,
        "TABLE": table,
        "COLUMN": column,
        "JOIN_SCHEMA": actual_join_sf_schema + join_appl_info,
        "JOIN_TABLE": join_table,
        "JOIN_COLUMN": join_column,
        "ORACLE_FACT_COUNT": ora_fact_count,
        "ORACLE_JOIN_COUNT": ora_join_count,
        "ORACLE_STATUS": ora_status,
        "SNOWFLAKE_FACT_COUNT": sf_fact_count,
        "SNOWFLAKE_JOIN_COUNT": sf_join_count,
        "SNOWFLAKE_STATUS": sf_status,
        "ORACLE_SF_JOIN_STATUS": cross_status,
        "ORACLE_QUERY_FORMAT": oracle_query_format,
        "SNOWFLAKE_QUERY_FORMAT": snowflake_query_format,
        "ORACLE_ZERO_COUNT": ora_zero_count,
        "SNOWFLAKE_ZERO_COUNT": sf_zero_count,
        "ZERO_COUNT_STATUS": zero_status,
        "ORACLE_ZERO_SQL": ora_zero_sql,
        "SNOWFLAKE_ZERO_SQL": sf_zero_sql,
        "SF_SCD_EXPIRY_COUNT": sf_scd_expiry_count,  #[03-Apr-2026]
        "SF_SCD_EXPIRY_STATUS": sf_scd_expiry_status,  #[03-Apr-2026]
        "SF_SCD_EXPIRY_SQL": sf_scd_expiry_sql  #[06-Apr-2026]
    }

def insert_result_into_snowflake(sf_conn, results, arg_dict, csv_file_path, filename):
    try:
        cur = sf_conn.cursor()

        # Delete existing records for this PRCS_DTE, TENANT_ID, and APPL_NAME
        tenant_id = arg_dict["tenant_id"]
        appl_name = arg_dict["appl_name"]
        fact_date = arg_dict["fact_date"]
        
        delete_sql = f"""
        DELETE FROM RAW_IBS.VALIDATION_DASHBOARD.JOIN_VALIDATION_RESULTS
        WHERE PRCS_DTE = TO_DATE('{fact_date}', 'YYYYMMDD')
        AND TENANT_ID = '{tenant_id}'
        AND APPL_NAME = '{appl_name}'
        """
        cur.execute(delete_sql)
        deleted_rows = cur.rowcount

        put_sql = f"PUT file://{csv_file_path} @~/RAW_IBS.VALIDATION_DASHBOARD.FIS_EXT_STG/"
        cur.execute(put_sql)
        
        copy_sql = f"""
        COPY INTO RAW_IBS.VALIDATION_DASHBOARD.JOIN_VALIDATION_RESULTS (
            UNIQUE_ID,
            APPL_NAME,
            APPL_TABLE,
            APPL_COL,
            JOIN_APPL_NAME,
            JOIN_APPL_TABLE,
            JOIN_APPL_COL,
            ORC_TABLE_COUNT,
            ORC_JOIN_COUNT,
            ORC_STATUS,
            SF_TABLE_COUNT,
            SF_JOIN_COUNT,
            SF_STATUS,
            ORC_VS_SF_STATUS,
            ORC_QUERY,
            SF_QUERY,
            ORC_ZERO_COUNT,
            SF_ZERO_COUNT,
            ORC_VS_SF_ZERO_COUNT_STATUS,
            ORC_ZERO_SQL,
            SF_ZERO_SQL,
            PRCS_DTE,
            TENANT_ID,
            SF_SCD_EXPIRY_COUNT,
            SF_SCD_EXPIRY_STATUS,
            SF_SCD_EXPIRY_SQL
        )
        FROM (
            SELECT 
                RAW_IBS.VALIDATION_DASHBOARD.JOIN_VALIDATION_SEQ.NEXTVAL,
                $23 AS APPL_NAME,
                $2 AS APPL_TABLE,
                $3 AS APPL_COL,
                $4 AS JOIN_APPL_NAME,
                $5 AS JOIN_APPL_TABLE,
                $6 AS JOIN_APPL_COL,
                $7 AS ORC_TABLE_COUNT,
                $8 AS ORC_JOIN_COUNT,
                $9 AS ORC_STATUS,
                $10 AS SF_TABLE_COUNT,
                $11 AS SF_JOIN_COUNT,
                $12 AS SF_STATUS,
                $13 AS ORC_VS_SF_STATUS,
                $14 AS ORC_QUERY,
                $15 AS SF_QUERY,
                $16 AS ORC_ZERO_COUNT,
                $17 AS SF_ZERO_COUNT,
                $18 AS ORC_VS_SF_ZERO_COUNT_STATUS,
                $19 AS ORC_ZERO_SQL,
                $20 AS SF_ZERO_SQL,
                TO_DATE($21, 'YYYYMMDD') AS PRCS_DTE,
                $22 AS TENANT_ID,
                $24 AS SF_SCD_EXPIRY_COUNT,
                $25 AS SF_SCD_EXPIRY_STATUS,
                $26 AS SF_SCD_EXPIRY_SQL
            FROM @~/RAW_IBS.VALIDATION_DASHBOARD.FIS_EXT_STG/{filename}.gz
        )
        FILE_FORMAT = (
            TYPE = 'CSV',
            SKIP_HEADER = 1,
            FIELD_OPTIONALLY_ENCLOSED_BY = '"',
            COMPRESSION = 'AUTO'
        )
        FORCE = TRUE
        PURGE = TRUE
        """
        cur.execute(copy_sql)
        cur.fetchall()
        cur.execute("COMMIT")
        
        cur.close()
        return True
    except Exception as e:
        logging.error(f"Failed to load results using COPY INTO (JOIN_VALIDATION_RESULTS table may not exist): {e}")
        return False

def join_validation():
    script_start = time.perf_counter()
    argument_dict = arg_parsing()
        
    py_path = os.environ["PYTHONPATH"]
    ingest_yaml_file = f'{py_path}/IngestionConfig.yaml'
    ingest_cfg_dict = load_yaml(yaml_file=ingest_yaml_file)
    
    sf_conn = open_sf_connection(ingest_cfg_dict)

    ora_user = f'DW{argument_dict["tenant_id"]}'
    ora_conn = open_oracle_connection(myuser=ora_user)
    
    appl_code = get_appl_code(argument_dict['appl_name'], sf_conn.cursor())
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    logging_config(argument_dict['logging_directory'], argument_dict['appl_name'], script_name, ingest_cfg_dict)
    validation_path = ingest_cfg_dict['snowflake_connection']['validation_path']
    output_dir = os.path.join(validation_path, argument_dict['appl_name'], 'Join_Validation')
    os.makedirs(output_dir, exist_ok=True)

    metadata_file = argument_dict['input_file']
    if metadata_file and metadata_file.upper() == "NEW":
        metadata_file = f"{argument_dict['appl_name']}{METADATA}"
        logging.info(f"Over-writing metadata file: {metadata_file}.") 
        logging.info("Generating metadata using join_validation_metadata_generator.")
        join_validation_metadata_generator(argument_dict['appl_name'], appl_code, sf_conn)
        if not os.path.isfile(os.path.join(output_dir, metadata_file)):
            raise FileNotFoundError(f"Metadata generation failed: {metadata_file} was not created in {output_dir}.")
    elif not metadata_file:
        metadata_file = f"{argument_dict['appl_name']}{METADATA}"
        if not os.path.isfile(os.path.join(output_dir, metadata_file)):
            logging.info(f"Metadata file not found: {metadata_file} in {output_dir}.") 
            logging.info("Generating metadata using join_validation_metadata_generator.")
            join_validation_metadata_generator(argument_dict['appl_name'], appl_code, sf_conn)
            if not os.path.isfile(os.path.join(output_dir, metadata_file)):
                raise FileNotFoundError(f"Metadata generation failed: {metadata_file} was not created in {output_dir}.")
    # read input csv file
    logging.info(f"Reading input metadata from: {metadata_file}")
    input_csv = os.path.join(output_dir, metadata_file)
    if not os.path.isfile(input_csv):
        logging.error(f"ERROR: Input file '{metadata_file}' not found in '{output_dir}'.")
        logging.error("Please place the file in the above directory or remove the --input_csv argument to auto-generate metadata and rerun the script.")
        return
    df = pd.read_csv(input_csv) if input_csv.lower().endswith(".csv") else pd.read_excel(input_csv)
    df.columns = [c.strip().upper() for c in df.columns]
    required_cols = ["SCHEMA", "TABLE", "FILTER", "COLUMN", "JOIN_SCHEMA", "JOIN_TABLE", "ORC_FILTER", "SF_FILTER", "JOIN_COLUMN"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in input file: {missing}")
    rows = df.to_dict("records")

    # Parallel execution
    results = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(validate_relationship, rel,sf_conn,ora_conn,argument_dict,ora_user) for rel in rows]
        for future in as_completed(futures):
            results.append(future.result())
    
    fieldnames = [
        "SCHEMA", "TABLE", "COLUMN",
        "JOIN_SCHEMA", "JOIN_TABLE", "JOIN_COLUMN",
        "ORACLE_FACT_COUNT", "ORACLE_JOIN_COUNT", "ORACLE_STATUS",
        "SNOWFLAKE_FACT_COUNT", "SNOWFLAKE_JOIN_COUNT", "SNOWFLAKE_STATUS",
        "ORACLE_SF_JOIN_STATUS",
        "ORACLE_QUERY_FORMAT", "SNOWFLAKE_QUERY_FORMAT",
        "ORACLE_ZERO_COUNT", "SNOWFLAKE_ZERO_COUNT", "ZERO_COUNT_STATUS",
        "ORACLE_ZERO_SQL", "SNOWFLAKE_ZERO_SQL",
        "PRCS_DTE", "TENANT_ID", "APPL_NAME",
        "SF_SCD_EXPIRY_COUNT", "SF_SCD_EXPIRY_STATUS", "SF_SCD_EXPIRY_SQL"  #[03-Apr-2026] #[06-Apr-2026]
    ]
    file_name = f"{argument_dict['appl_name']}_JoinResults_{argument_dict['tenant_id']}_{argument_dict['fact_date']}.csv"
    OUTPUT_FILE = os.path.join(output_dir, file_name)
    
    # Enrich results with PRCS_DTE, TENANT_ID, APPL_NAME before writing CSV
    enriched_results = []
    for result in results:
        fact_date = get_fact_date(result["TABLE"], argument_dict)
        result_copy = result.copy()
        result_copy["PRCS_DTE"] = fact_date
        result_copy["TENANT_ID"] = argument_dict["tenant_id"]
        result_copy["APPL_NAME"] = argument_dict["appl_name"]
        enriched_results.append(result_copy)
    
    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(enriched_results)
    
    print("\nLoading results into JOIN_VALIDATION_RESULTS using COPY INTO")
    load_success = insert_result_into_snowflake(sf_conn, results, argument_dict, OUTPUT_FILE, file_name)
    if load_success:
        print("Load completed successfully.")
    else:
        print("Load skipped — JOIN_VALIDATION_RESULTS table not available.")

    logging.info(f"\nValidation complete! {len(results)} joins validated.")
    logging.info(f"Results saved to: {OUTPUT_FILE}")

    script_end = time.perf_counter()
    script_run_time = script_end - script_start

    if argument_dict.get('load_sf_meta') and enriched_results:
        sf_conn_meta = open_sf_connection(ingest_cfg=ingest_cfg_dict)
        registry = TestCaseRegistry(sf_conn_meta.cursor(), SCRIPT_NAME,
                                    database=argument_dict.get('sf_meta_db'),
                                    schema=argument_dict.get('sf_meta_schema'))
        validation_results = []
        for result in enriched_results:
            oracle_status = result.get('ORACLE_STATUS', 'UNKNOWN')
            sf_status = result.get('SNOWFLAKE_STATUS', 'UNKNOWN')
            overall_status = 'SUCCESS' if oracle_status == 'SUCCESS' and sf_status == 'SUCCESS' else 'FAIL'
            
            def _safe_int(val):
                try:
                    return int(val)
                except (TypeError, ValueError):
                    return 0

            ora_cnt = result.get('ORACLE_FACT_COUNT') or 0
            sf_cnt = result.get('SNOWFLAKE_FACT_COUNT') or 0
            mis_count = abs(_safe_int(ora_cnt) - _safe_int(sf_cnt)) if overall_status == 'FAIL' else 0
            mat_count = min(_safe_int(ora_cnt), _safe_int(sf_cnt)) if overall_status == 'SUCCESS' else 0
            validation_results.append(registry.create_result(
                validation_key='join_data_flow',
                test_scenario='Validate join counts between Oracle and Snowflake',
                appl_name=argument_dict['appl_name'],
                appl_code=appl_code,
                tenant_id=argument_dict.get('tenant_id', 'ALL'),
                table_name=result.get('TABLE', 'UNKNOWN'),
                join_table_name=result.get('JOIN_TABLE', ''),
                source_count=result.get('ORACLE_FACT_COUNT'),
                target_count=result.get('SNOWFLAKE_FACT_COUNT'),
                mismatched_count=mis_count,
                matched_count=mat_count,
                validation_status=overall_status,
                status_reason=f"Oracle: {oracle_status}, Snowflake: {sf_status}",
                execution_time_sec=script_run_time / max(len(enriched_results), 1),
                additional_info={
                    'column': result.get('COLUMN'),
                    'join_column': result.get('JOIN_COLUMN'),
                    'oracle_join_count': result.get('ORACLE_JOIN_COUNT'),
                    'snowflake_join_count': result.get('SNOWFLAKE_JOIN_COUNT')
                }
            ))
        
        try:
            loader = ValidationLoader(
                sf_cursor=sf_conn_meta.cursor(),
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
            summary.update_counts(validation_results)
            summary.execution_time_sec = script_run_time
            metadata_csv_path = os.path.join(output_dir, f"{argument_dict['appl_name']}_metadata.csv")
            summary_txt_path = os.path.join(output_dir, f"{argument_dict['appl_name']}_summary.txt")
            for fpath, ftype in [
                (OUTPUT_FILE, 'join_validation_csv'),
                (metadata_csv_path, 'join_metadata_csv'),
                (summary_txt_path, 'join_metadata_summary'),
            ]:
                if os.path.exists(fpath) and os.path.getsize(fpath) > 0:
                    summary.read_and_store_output(fpath, file_type=ftype)
            exec_id = loader.insert_execution_summary(summary)

            run_ids = loader.insert_master_bulk(validation_results, execution_id=exec_id)
            run_id_map = {i: r.run_id for i, r in enumerate(validation_results)}

            detail_batch = []
            for i, result in enumerate(enriched_results):
                oracle_status = result.get('ORACLE_STATUS', 'UNKNOWN')
                sf_status = result.get('SNOWFLAKE_STATUS', 'UNKNOWN')
                if oracle_status != 'SUCCESS' or sf_status != 'SUCCESS':
                    rid = run_id_map.get(i, 0)
                    if rid:
                        detail_batch.append(ValidationDetailResult(
                            run_id=rid,
                            match_status='MISMATCH',
                            record_key=result.get('TABLE', 'UNKNOWN'),
                            record_key_columns=result.get('COLUMN', ''),
                            source_data={'oracle_join_count': result.get('ORACLE_JOIN_COUNT'), 'oracle_fact_count': result.get('ORACLE_FACT_COUNT')},
                            target_data={'snowflake_join_count': result.get('SNOWFLAKE_JOIN_COUNT'), 'snowflake_fact_count': result.get('SNOWFLAKE_FACT_COUNT')},
                            detail_remarks=cap_details(f"Oracle: {oracle_status}, Snowflake: {sf_status}", 2000)[0]
                        ))
            if detail_batch:
                capped_batch, _, _ = cap_details(detail_batch)
                loader.insert_detail_bulk(capped_batch)

            summary.emit_summary_line()
            logging.info(f"Loaded {len(run_ids)} join validation records to VALIDATION_RUN_MASTER")
            sf_conn_meta.close()
        except Exception as e:
            logging.error(f"Failed to load validation results to Snowflake: {str(e)}")
            traceback.print_exc()
    elif not argument_dict.get('load_sf_meta'):
        logging.info("Skipping metadata load to Snowflake (--load-sf-meta not specified)")

    ora_conn.close()
    sf_conn.close()
    logging.info("Connections closed.")
    logging.info(f"\n{'-' * 50}")
    logging.info(f'Script run time: {script_run_time} seconds')

if __name__ == "__main__":
    join_validation()
