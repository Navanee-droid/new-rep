# Script is modified for C2 validation and validate additional audit fields (SRC_APPL_NAME) [Use 'C2 Field Adds' label to identify changes]
# ================================================================================
# MODIFICATION HISTORY:
# ================================================================================
# Date: 2026-02-10
# Author: [Developer Name]
# Ticket/CR: [Add ticket number here]
# Description: [CHANGE-2026-02-10] Unit Testing Round 2 Feedback Implementation
#   1. Fixed TB_C2 tables not appearing in results - SQL IN clause was malformed
#      - Old: IN ('table1,table2') - single string with comma
#      - New: IN ('table1','table2') - properly quoted individual values
#   2. Added TABLE_TYPE = 'BASE TABLE' filter to exclude views from validation
#      - Views (like DTM views) were being fetched and validated incorrectly
#   3. Look for comments with [CHANGE-2026-02-10] to identify all modifications
# --------------------------------------------------------------------------------
# Date: 2026-02-09
# Author: [Developer Name]
# Ticket/CR: [Add ticket number here]
# Description: [CHANGE-2026-02-09] Unit Testing Feedback Implementation
#   1. Added "DTM" to EXCLUDED_PATTERNS to exclude DTM tables from validation
#      (e.g., TB_BK_DAZ_DTM_DIM will now be excluded)
#   2. Enhanced output report to include TABLE VALIDATION SUMMARY section
#      - Lists each table considered for validation with its status (SUCCESS/FAIL)
#      - Shows table type (SCD/RCD/RPD, Fact/Dimension, or C2)
#      - Includes summary counts (Total, Success, Failed)
#   3. Detailed failure report section now appears after the summary
#   4. Look for comments with [CHANGE-2026-02-09] to identify all modifications
# --------------------------------------------------------------------------------
# Date: 2026-02-05
# Author: [Developer Name]
# Ticket/CR: [Add ticket number here]
# Description: 
#   1. Fixed SRC_APPL_NAME validation logic - now only validates for TB_C2 tables
#   2. Previously SRC_APPL_NAME was being added to ALL tables when C2_FLAG=True
#   3. Added separate handling for C2 table audit field validation
#   4. Look for comments with [CHANGE-2026-02-05] to identify all modifications
# ================================================================================

import os
import sys
import time
import logging
import traceback
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import yaml
import toml
from datetime import datetime

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
  logging_config,
  get_tables_from_appl_table,
  open_sf_connection,
  load_yaml,
)

SCRIPT_NAME = "AuditFields_DDL_CUR_Validation.py"
SCRIPT_VERSION = "v2.0"

def arg_parsing() -> dict:
    args = parse_args(
        required=['--a'],
        optional=['--l', '--o','--t', '--day0', '--load-sf-meta', '--sf-meta-db', '--sf-meta-schema'],
        description='Validates audit fields between DDL and CUR layers.',
    )
    args['schema'] = args['appl_name']
    args['log_dir'] = args['logging_directory']
    return args

# ----------------- Config -----------------
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
args = arg_parsing()

LOG_DIR = args["log_dir"] # <--  logput directory
SCHEMA_NAME = args["schema"] # <-- schema name
DAY0_SOURCE_VAL = 'HISTORY'


# [CHANGE-2026-02-09] Added "DTM" to exclude DTM tables (e.g., TB_BK_DAZ_DTM_DIM) per unit testing feedback
EXCLUDED_PATTERNS = [
  "TB_APPLICATION_TENANTS",
  "DLV",
  "OAZ_DAY",
  "OAY_DAY",
  "OAX_DAY",
  "OYM_DAY",
  "DTM"  # [CHANGE-2026-02-09] Added - DTM tables to be excluded from validation
  ]

# TB_C2 tables are now retrieved from RAW_IBS.ARCHITECTURE.T_APPL_TABLE at runtime.

#--------------------Validate if Schema Exists---------
def validate_schema_exists(cur, schema_name: str):
  query = f"""
    SELECT COUNT(*)
    FROM CUR_IBS.INFORMATION_SCHEMA.SCHEMATA
    WHERE SCHEMA_NAME = '{schema_name}'
    """
  cur.execute(query)
  result = cur.fetchone()[0]

  if result == 0:
    logging.error(f"Schema '{schema_name}' does not exist in CUR_IBS database. "
    f"Please enter a valid schema name.")
    raise ValueError(
    f"Schema '{schema_name}' does not exist in CUR_IBS. "
    f"Please enter a valid schema name."
    )

#-------------------Validate if directory Exists-------------
def validate_directory_exists(path: str, param_name: str):
  if not os.path.isdir(path):
    logging.error(f"{param_name} '{path}' does not exist. Please enter a valid directory.")
    raise ValueError(f"{param_name} '{path}' does not exist. Please enter a valid directory.")


# ----------------- Get All Tables Starting with TB_ -----------------
def get_all_tb_tables(cur, c2_tables=None):
  # [CHANGE-2026-02-10] Added TABLE_TYPE = 'BASE TABLE' to exclude views per unit testing feedback
  query = f"""
  SELECT TABLE_SCHEMA, TABLE_NAME
  FROM CUR_IBS.INFORMATION_SCHEMA.TABLES
  WHERE TABLE_NAME LIKE 'TB_%'
  AND TABLE_SCHEMA = '{SCHEMA_NAME}'
  AND TABLE_TYPE = 'BASE TABLE'
  """
  # Include TB_C2 tables from T_APPL_TABLE as part of application/schema tables
  if c2_tables:
    # [CHANGE-2026-02-10] Fixed: Properly format IN clause with quotes around each table name
    c2_tables_formatted = "','".join(c2_tables)
    query = query + f"""
    UNION ALL
    SELECT TABLE_SCHEMA, TABLE_NAME
    FROM CUR_IBS.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'DDW_CNF_DIM'
    AND TABLE_NAME IN ('{c2_tables_formatted}')
    AND TABLE_TYPE = 'BASE TABLE'
    """
  cur.execute(query)
  result = cur.fetchall()

  tables_by_schema = {}
  excluded_tables = []

  for schema, table in result:
    if any(x in table for x in EXCLUDED_PATTERNS):
      excluded_tables.append(f"{schema}.{table}")
      continue
    tables_by_schema.setdefault(schema, []).append(table)
  return tables_by_schema, excluded_tables

# ----------------- Get ALL COLUMNS for ALL TB_ tables in ONE QUERY -----------------
def get_all_tb_columns(cur, c2_tables=None):
  query = f"""
  SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE,
  CASE WHEN DATA_TYPE='NUMBER' THEN NUMERIC_PRECISION || ',' ||NUMERIC_SCALE
  ELSE TO_CHAR(CHARACTER_MAXIMUM_LENGTH) END AS DATA_LENGTH
  FROM CUR_IBS.INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_NAME LIKE 'TB_%'
  AND TABLE_SCHEMA = '{SCHEMA_NAME}' 
  """
  # Include TB_C2 table columns from T_APPL_TABLE as part of application/schema columns
  if c2_tables:
    # [CHANGE-2026-02-10] Fixed: Properly format IN clause with quotes around each table name
    c2_tables_formatted = "','".join(c2_tables)
    query = query + f"""
    UNION ALL
    SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE,
    CASE WHEN DATA_TYPE='NUMBER' THEN NUMERIC_PRECISION || ',' ||NUMERIC_SCALE
    ELSE TO_CHAR(CHARACTER_MAXIMUM_LENGTH) END AS DATA_LENGTH
    FROM CUR_IBS.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'DDW_CNF_DIM'
    AND TABLE_NAME IN ('{c2_tables_formatted}') 
    ORDER BY TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME 
    """
  else:
    query = query + f"""
    ORDER BY TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME 
    """

  cur.execute(query)
  result = cur.fetchall()

  column_map = {}
  for schema, table, col, dtype, length in result:
    key = (schema, table)
    if key not in column_map:
      column_map[key] = {}

    column_map[key][col] = {"DATA_TYPE": dtype, "DATA_LENGTH": length,}

  return column_map

# ----------------- Default Field Check -----------------

def default_field_check(schema: str, table: str, buffer: list, all_columns: dict):
  """
  Validates audit fields for a table and returns validation result.
  
  Returns:
    dict: Contains 'status' (SUCCESS/FAIL), 'table_type', and 'issues' list
  """
  # [CHANGE-2026-02-05] Added: Determine if this is a C2 table for proper validation
  is_c2_table = table.startswith('TB_C2')
  
  # Determine table type for reporting
  if "SCD" in table or "RCD" in table or "RPD" in table:
    table_type = "SCD/RCD/RPD"
    default_fields = {
      "TENANT_ID": {"DATA_TYPE": "VARCHAR", "DATA_LENGTH": "10"},
      "PRCS_YR_MTH_NBR": {"DATA_TYPE": "VARCHAR", "DATA_LENGTH": "6"},
      "LOAD_TS": {"DATA_TYPE": "TIMESTAMP_NTZ", "DATA_LENGTH": "3"},
      "SOURCE_FILE": {"DATA_TYPE": "VARCHAR", "DATA_LENGTH": "4000"},
    }
  else:
    table_type = "Fact/Dimension"
    default_fields = {
    "TENANT_ID": {"DATA_TYPE": "VARCHAR", "DATA_LENGTH": "10"},
    "PRCS_DTE": {"DATA_TYPE": "DATE", "DATA_LENGTH": None},
    "PRCS_YR_MTH_NBR": {"DATA_TYPE": "VARCHAR", "DATA_LENGTH": "6"},
    "LOAD_TS": {"DATA_TYPE": "TIMESTAMP_NTZ", "DATA_LENGTH": "3"},
    "SOURCE_FILE": {"DATA_TYPE": "VARCHAR", "DATA_LENGTH": "4000"},
    }
  
  if is_c2_table:
    table_type = f"C2 ({table_type})"

  # C2 Field Adds: Define C2 default fields and combine with primary defaults if C2 Validation is required
  c2_defaults = {
    "SRC_APPL_NAME": {"DATA_TYPE": "VARCHAR", "DATA_LENGTH": "100"},
  }
  # [CHANGE-2026-02-05] Commented: Old logic added SRC_APPL_NAME to ALL tables when C2_FLAG=True
  # if C2_FLAG:
  #   default_fields.update(c2_defaults)
  
  # [CHANGE-2026-02-05] Added: Only add SRC_APPL_NAME to default_fields for TB_C2 tables
  # This ensures SRC_APPL_NAME is only validated for C2 tables, not regular application tables
  if is_c2_table:
    default_fields.update(c2_defaults)

  sf_columns = all_columns.get((schema, table), {})

  missing_fields = []
  mismatched_fields = []
  extra_fields = []

  for col, props in default_fields.items():
    if col not in sf_columns:
      # [CHANGE-2026-02-05] Commented: Old complex logic for C2 field missing check
      # if col in c2_defaults:
      #   if table.startswith('TB_C2'):
      #     missing_fields.append(col)
      # else:
      #   missing_fields.append(col)
      # [CHANGE-2026-02-05] Added: Simplified logic - if column is in default_fields and missing, report it
      # SRC_APPL_NAME is only in default_fields for C2 tables now (due to fix above)
      missing_fields.append(col)
      continue
    # [CHANGE-2026-02-05] Commented: Old logic to check for extra SRC_APPL_NAME in non-C2 tables
    # if not table.startswith('TB_C2') and col in c2_defaults and col in sf_columns:
    #   extra_fields.append(col)
    #   continue
    # [CHANGE-2026-02-05] Added: Check for SRC_APPL_NAME in non-C2 tables (it shouldn't exist there)
    if not is_c2_table and col in c2_defaults and col in sf_columns:
      extra_fields.append(col)
      continue
    if "SCD" in table or "RCD" in table or "RPD" in table:
      if col == "PRCS_DTE" and col in sf_columns:
        extra_fields.append(col)
        continue
    sf_type = sf_columns[col]["DATA_TYPE"].upper()
    sf_len = sf_columns[col]["DATA_LENGTH"]
    expected_type = props["DATA_TYPE"]
    expected_len = props["DATA_LENGTH"]

    if sf_type =="TEXT":
      display_actual_type = "VARCHAR"
    else:
      display_actual_type = sf_type

    if expected_type == "VARCHAR" and sf_type in ("TEXT", "VARCHAR"):
      type_mismatch = False
    else:
      type_mismatch = sf_type != expected_type

    if expected_type.startswith("TIMESTAMP"):
      length_mismatch = False
    else:
      length_mismatch = (expected_len is not None and sf_len is not None and str(sf_len) != str(expected_len))

    if type_mismatch or length_mismatch:
      mismatched_fields.append({
      "COLUMN_NAME": col,
      "EXPECTED_TYPE": expected_type,
      "EXPECTED_LENGTH": expected_len,
      "ACTUAL_TYPE": display_actual_type,
      "ACTUAL_LENGTH": sf_len
      })

  # Determine validation status
  has_issues = bool(missing_fields or mismatched_fields or extra_fields)
  validation_status = "FAIL" if has_issues else "SUCCESS"
  
  # Build result dictionary for summary tracking
  result = {
    "schema": schema,
    "table": table,
    "table_type": table_type,
    "status": validation_status,
    "missing_fields": missing_fields,
    "mismatched_fields": mismatched_fields,
    "extra_fields": extra_fields
  }

  # Only add failure details to buffer (for detailed failure section)
  if has_issues:
    buffer.append(f"CUR_IBS.{schema}.{table}\n")
    # [CHANGE-2026-02-05] Added: Log table type for better debugging
    if is_c2_table:
      buffer.append(f"[Table Type: C2 Table - SRC_APPL_NAME validation applied]\n")

    for m in mismatched_fields:
      if m["EXPECTED_TYPE"] == "VARCHAR":
        buffer.append(f"{m['COLUMN_NAME']} length mismatch: expected {m['EXPECTED_TYPE']}({m['EXPECTED_LENGTH']}), "
        f"found {m['ACTUAL_TYPE']}({m['ACTUAL_LENGTH']})\n"
        )
      else:
        buffer.append(f"{m['COLUMN_NAME']} mismatch: expected {m['EXPECTED_TYPE']}, found {m['ACTUAL_TYPE']}\n"
        )

    for miss in missing_fields:
      buffer.append(f"Missing column: {miss}\n")

    for extra in extra_fields:
      buffer.append(f"Extra Fields, must only be in C2 tables: {extra}\n")

    buffer.append("\n")
  
  return result
  

def source_file_check(schema: str, table: str, all_columns: dict, cur, tenant_id: str = None):
  """Returns a failure message string if SOURCE_FILE value is not 'History',
  '__NO_DATA__' if the table has no rows, else None."""
  query = f"""SELECT DISTINCT SOURCE_FILE FROM CUR_IBS.{schema}.{table}"""
  if tenant_id:
    query += f" WHERE TENANT_ID = '{tenant_id}'"
  cur.execute(query)
  result = cur.fetchall()
  if not result:
    return '__NO_DATA__'
  for row in result:
    source_file_val = row[0]
    if source_file_val is not None and source_file_val != DAY0_SOURCE_VAL:
      return f"SOURCE_FILE value '{source_file_val}' does not match expected '{DAY0_SOURCE_VAL}' for Day 0 load."
  return None


# ----------------- Main Script -----------------
def validate_default_fields_all_schemas():
  start_time = time.perf_counter()

  py_path = os.environ["PYTHONPATH"]
  ingest_yaml_file = f"{py_path}/IngestionConfig.yaml"
  ingest_cfg = load_yaml(ingest_yaml_file)

  script_name = os.path.splitext(os.path.basename(__file__))[0]

  file_path = f"{ingest_cfg['snowflake_connection']['validation_path']}/{SCHEMA_NAME}/{script_name}"
  os.makedirs(file_path, exist_ok=True)
  validate_directory_exists(file_path, "Output directory")

  sf_conn = open_sf_connection(ingest_cfg)
  sf_cur = sf_conn.cursor()
  appl_code = get_appl_code(SCHEMA_NAME, sf_cur)
  logging_config(LOG_DIR,appl_code,script_name,ingest_cfg)
  tenant_id=args.get('tenant_id')
  validate_schema_exists(sf_cur, SCHEMA_NAME)

  all_tables = get_tables_from_appl_table(sf_cur, appl_code)
  c2_tables = [t for t in all_tables if t.startswith('TB_C2')]

  logging.info("Fetching all tables starting with TB_ across all schemas ...")
  tables_by_schema, excluded_tables = get_all_tb_tables(sf_cur, c2_tables=c2_tables)

# Load all column metadata at once
  all_columns = get_all_tb_columns(sf_cur, c2_tables=c2_tables)

  outfname = f"{file_path}/AUDIT_FIELDS_DDL_CUR_IBS_VALIDATION_{timestamp}.txt"

  buffer = [] # RAM buffer for fast processing

  buffer.append("Snowflake Audit Field Validation Report\n")
  buffer.append("Database : CUR_IBS\n")
  buffer.append(f"Schema : {SCHEMA_NAME}\n")
  buffer.append("Excluded Tables:\n")
  for t in excluded_tables:
    buffer.append(f"- {t}\n")
  buffer.append("\n")
  buffer.append("Required Audit Fields to be checked :\n")
  buffer.append("For SCD or RCD or RPD Tables\n")
  buffer.append("- TENANT_ID : VARCHAR(10)\n")
  buffer.append("- PRCS_YR_MTH_NBR : VARCHAR(6)\n")
  buffer.append("- LOAD_TS : TIMESTAMP_NTZ(3)\n")
  buffer.append("- SOURCE_FILE : VARCHAR(4000)\n")
  # [CHANGE-2026-02-05] Updated: Changed label from "Other Tables" to "Fact and Dimension Tables" for clarity
  buffer.append("For Fact and Dimension Tables\n")
  buffer.append("- TENANT_ID : VARCHAR(10)\n")
  buffer.append("- PRCS_DTE : DATE\n")
  buffer.append("- PRCS_YR_MTH_NBR : VARCHAR(6)\n")
  buffer.append("- LOAD_TS : TIMESTAMP_NTZ(3)\n")
  buffer.append("- SOURCE_FILE : VARCHAR(4000)\n")
  # Adjust buffer with C2 table fields if any TB_C2 tables were found in T_APPL_TABLE
  if c2_tables:
    # [CHANGE-2026-02-05] Updated: Added clarity that SRC_APPL_NAME is ADDITIONAL field for C2 tables
    buffer.append("For C2 Tables (TB_C2_* tables in DDW_CNF_DIM schema) - Additional Field:\n")
    buffer.append("- SRC_APPL_NAME : VARCHAR(100)\n")
    buffer.append("Note: C2 tables also require all fields from their respective table type (SCD/RCD/RPD or Fact/Dim)\n\n")
  else:
    buffer.append("No TB_C2 tables found for this application in T_APPL_TABLE. Skipping C2 Tables Validation.\n\n")
    logging.info("No TB_C2 tables found for this application in T_APPL_TABLE. Skipping C2 Tables Validation.")
  buffer.append("\n")
  buffer.append("SOURCE_FILE Value Validation:\n")
  if args["day0"] == "Y":
    if tenant_id:
      buffer.append(f"- Day 0 load mode: SOURCE_FILE must equal '{DAY0_SOURCE_VAL}' for all rows for tenant_id - '{tenant_id}'\n")
    else:
      buffer.append(f"- Day 0 load mode: SOURCE_FILE must equal '{DAY0_SOURCE_VAL}' for all rows for all tenants\n")
  else:
    buffer.append("- Non-Day 0 mode: SOURCE_FILE value check skipped\n")
  buffer.append("\n")
  buffer.append("=" * 100 + "\n\n")

# [CHANGE-2026-02-09] Enhanced output: Validate tables and collect results for summary
  # Previously only failure details were tracked. Now we track all tables with their validation status.
  validation_results = []  # [CHANGE-2026-02-09] Added - stores validation result for each table
  failure_buffer = []  # Separate buffer for failure details
  
  for schema, tables in tables_by_schema.items():
    for table in tables:
      table_buf = []  # per-table temp buffer to group all issues together
      result = default_field_check(schema, table, table_buf, all_columns)
      if args["day0"] == "Y" and 'SOURCE_FILE' not in result.get('missing_fields', []):
        sf_msg = source_file_check(schema, table, all_columns, sf_cur, tenant_id=args.get('tenant_id'))  # [CHANGE-2026-02-05] Added - Day 0 only: check SOURCE_FILE value
        if sf_msg == '__NO_DATA__':
          result['status'] = 'SUCCESS - NO DATA'
        elif sf_msg:
          result['status'] = 'FAIL'
          if not table_buf:  # no audit field issues — start the table block
            table_buf.append(f"CUR_IBS.{schema}.{table}\n")
          elif table_buf[-1] == "\n":  # remove trailing blank line; will re-add after sf message
            table_buf.pop()
          table_buf.append(f"SOURCE_FILE check: {sf_msg}\n")
      if table_buf:
        if table_buf[-1] != "\n":
          table_buf.append("\n")  # ensure blank line separator between table blocks
        failure_buffer.extend(table_buf)
      validation_results.append(result)  # [CHANGE-2026-02-09] Collect result for summary

  # [CHANGE-2026-02-09] BEGIN - New TABLE VALIDATION SUMMARY section per unit testing feedback
  # This section lists each table considered for validation along with their status (SUCCESS/FAIL)
  buffer.append("TABLE VALIDATION SUMMARY\n")
  buffer.append("-" * 100 + "\n")
  buffer.append(f"{'Table Name':<60} {'Type':<20} {'Status':<10}\n")
  buffer.append("-" * 100 + "\n")
  
  success_count = 0
  failed_count = 0
  
  for result in validation_results:
    full_table_name = f"CUR_IBS.{result['schema']}.{result['table']}"
    status_str = result['status']
    table_type = result['table_type']
    buffer.append(f"{full_table_name:<60} {table_type:<20} {status_str:<15}\n")
    if result['status'] in ('SUCCESS', 'SUCCESS - NO DATA'):
      success_count += 1
    else:
      failed_count += 1
  
  # Summary counts
  buffer.append("-" * 100 + "\n")
  buffer.append(f"Total Tables Validated: {len(validation_results)}\n")
  buffer.append(f"Success: {success_count}\n")
  buffer.append(f"Failed: {failed_count}\n\n")
  buffer.append("=" * 100 + "\n\n")
  # [CHANGE-2026-02-09] END - TABLE VALIDATION SUMMARY section

  # [CHANGE-2026-02-09] Detailed failure report now appears after summary section
  # In case of any failure, we stick to the current template of showing failures
  if failure_buffer:
    buffer.append("DETAILED FAILURE REPORT\n")
    buffer.append("-" * 100 + "\n")
    buffer.extend(failure_buffer)
  else:
    buffer.append(f"All Required Audit fields are present in all tables under CUR_IBS.{SCHEMA_NAME}. No Mismatches or missing columns.\n")

# Write output ONCE
  with open(outfname, "w") as f:
    f.writelines(buffer)

  logging.info(f"Output written to : {outfname}")

  end_time = time.perf_counter()
  script_run_time = end_time - start_time

  if args.get('load_sf_meta') and validation_results:
      registry = TestCaseRegistry(sf_cur, SCRIPT_NAME,
                                  database=args.get('sf_meta_db'),
                                  schema=args.get('sf_meta_schema'))
      vr_list = []
      for result in validation_results:
          mis_count = len(result.get('missing_fields', [])) + len(result.get('mismatched_fields', []))
          mat_count = 1 if result['status'] == 'SUCCESS' else 0
          vr_list.append(registry.create_result(
              validation_key='ingestion_fields',
              test_scenario='Validate required audit fields exist with correct data types',
              appl_name=SCHEMA_NAME,
              appl_code=appl_code,
              tenant_id='ALL',
              table_name=f"{result['schema']}.{result['table']}",
              validation_status='SUCCESS' if result['status'] == 'SUCCESS' else 'FAIL',
              status_reason=f"Audit fields validation - {result['table_type']}",
              mismatched_count=mis_count if result['status'] != 'SUCCESS' else 0,
              matched_count=mat_count,
              execution_time_sec=script_run_time / max(len(validation_results), 1),
              additional_info={
                  'table_type': result['table_type'],
                  'missing_fields': result.get('missing_fields', []),
                  'mismatched_fields': [f.get('COLUMN_NAME') for f in result.get('mismatched_fields', [])],
                  'extra_fields': result.get('extra_fields', [])
              }
          ))
      
      try:
          loader = ValidationLoader(
              sf_cursor=sf_cur,
              arg_dict=args,
              script_name=SCRIPT_NAME,
              script_version=SCRIPT_VERSION,
              database=args.get('sf_meta_db'),
              schema=args.get('sf_meta_schema')
          )
          summary = ExecutionSummary(
              script_name=SCRIPT_NAME, appl_name=args.get('appl_name', SCHEMA_NAME),
              appl_code=appl_code, tenant_id='ALL',
              process_date=args.get('process_date', ''), script_version=SCRIPT_VERSION
          )
          summary.started_at = datetime.fromtimestamp(time.time() - (time.perf_counter() - start_time))
          summary.parameters_used = {k: str(v) for k, v in args.items() if k not in ('sf_cursor',)}
          summary.update_counts(vr_list)
          summary.execution_time_sec = script_run_time
          if os.path.exists(outfname):
              summary.read_and_store_output(outfname, file_type='audit_fields_report')
          exec_id = loader.insert_execution_summary(summary)

          run_ids = loader.insert_master_bulk(vr_list, execution_id=exec_id)
          run_id_map = {r.table_name: r.run_id for r in vr_list}

          detail_batch = []
          for result in validation_results:
              if result['status'] != 'SUCCESS':
                  rid = run_id_map.get(f"{result['schema']}.{result['table']}", 0)
                  if rid:
                      missing = result.get('missing_fields', [])
                      mismatched = [f.get('COLUMN_NAME') for f in result.get('mismatched_fields', [])]
                      extra = result.get('extra_fields', [])
                      detail_batch.append(ValidationDetailResult(
                          run_id=rid,
                          match_status='MISMATCH',
                          record_key=f"{result['schema']}.{result['table']}",
                          source_data={'missing_fields': missing, 'mismatched_fields': mismatched, 'extra_fields': extra},
                          target_data={},
                          detail_remarks=cap_details(str({'missing': missing, 'mismatched': mismatched, 'extra': extra}), 2000)[0]
                      ))
          if detail_batch:
              capped_batch, _, _ = cap_details(detail_batch)
              loader.insert_detail_bulk(capped_batch)

          summary.emit_summary_line()
          logging.info(f"Loaded {len(run_ids)} audit field validation records to VALIDATION_RUN_MASTER")
      except Exception as e:
          logging.error(f"Failed to load validation results to Snowflake: {str(e)}")
          traceback.print_exc()
  elif not args.get('load_sf_meta'):
      logging.info("Skipping metadata load to Snowflake (--load-sf-meta not specified)")

  sf_cur.close()
  sf_conn.close()

  logging.info(f"Validation completed in {script_run_time:.2f} seconds.")

if __name__ == "__main__":
  validate_default_fields_all_schemas()
