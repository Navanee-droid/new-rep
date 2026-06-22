# This Script Validate the Date Format i.e Timestamp data type for any application
# User Input provide SF APPL_NAME(e.g. DDW_CIS):
# Created:05/30/2025 
# Author: Charandeep
# Author: Barath - Modified to take appl_name as argument rather than an input. [06/26/2025]

import os
import re
import sys
import traceback
import yaml
import snowflake
import time
import toml
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
    parse_args,
    get_appl_code,
    open_sf_connection,
    load_yaml,
    get_tables_from_appl_table
)

SCRIPT_NAME = "DateFormatCheckDDW.py"
SCRIPT_VERSION = "v2.0"

def arg_parsing() -> dict:
    args = parse_args(
        required=['--a'],
        optional=['--load-sf-meta', '--sf-meta-db', '--sf-meta-schema'],
        description='Date format check for DDW tables.',
    )
    args['schema'] = args['appl_name']
    return args

def get_ccards_name(table_names):
   ccards = {}
   for name in table_names:
       parts = name.split('_')
       if len(parts) >= 3:
           first = parts[1].lower()
           second = parts[2].lower()
           value = f'dw{first}l{second}.pos'
           ccards[name] = value
       else:
           print(f"Invalid table name {name}.")
   return ccards

def extract_column_block(content):
   start = content.find('(')
   if start == -1:
       return None
   paren_level = 0
   for i in range(start, len(content)):
       if content[i] == '(':
           paren_level += 1
       elif content[i] == ')':
           paren_level -= 1
           if paren_level == 0:
               return content[start + 1:i]  # exclude outer parentheses
   return None

def split_column_definitions(text):
   defs = []
   current = ''
   paren_level = 0
   for char in text:
       if char == ',' and paren_level == 0:
           if current.strip():
               defs.append(current.strip())
           current = ''
       else:
           current += char
           if char == '(':
               paren_level += 1
           elif char == ')':
               paren_level = max(paren_level - 1, 0)
   if current.strip():
       defs.append(current.strip())
   return defs

def find_timestamp_columns(ccards_dict, base_path):
   timestamp_columns = {}
   for key, file_name in ccards_dict.items():
       file_path = os.path.join(base_path, file_name)
       # Fallback to .pos1 if .pos doesn't exist
       if not os.path.exists(file_path):
           if file_name.endswith('.pos'):
               fallback_file = file_name.replace('.pos', '.pos1')
               fallback_path = os.path.join(base_path, fallback_file)
               if os.path.exists(fallback_path):
                   file_path = fallback_path
               else:
                   print(f"[WARN] ccard not found: {file_name} or {fallback_file}")
                   timestamp_columns[key] = []
                   continue
           else:
               print(f"[WARN] ccard not found: {file_path}")
               timestamp_columns[key] = []
               continue
       try:
           with open(file_path, 'r') as file:
               content = file.read()
       except Exception as e:
           print(f"[ERROR] Could not read {file_path}: {e}")
           timestamp_columns[key] = []
           continue
       block = extract_column_block(content)
       if not block:
           print(f"[WARN] No valid column block in {file_name}")
           timestamp_columns[key] = []
           continue
       col_defs = split_column_definitions(block)
       result_cols = []
       for col_def in col_defs:
           if 'mi:ss' not in col_def.lower():
               continue
           tokens = col_def.strip().split()
           if tokens:
               col_name = tokens[0]
               result_cols.append(col_name)
       timestamp_columns[key] = result_cols
   return timestamp_columns

def get_column_data_types(table_name, columns,schema_name, conn):
    column_data_types = {}
    cursor = conn.cursor()
    try:
        for column in columns:
            query = f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}' AND COLUMN_NAME = '{column}' AND TABLE_SCHEMA='{schema_name}'"
            #print(query)
            cursor.execute(query)
            result = cursor.fetchone()
            if result:
                column_data_types[result[0]] = result[1]
            #print(f"return data type {column_data_types}")
            return column_data_types
    except Exception as e:
        print(f"Error fetching Datatype for {table_name}: {e}")

def validate_date_format(timestamp_cols, schema_name, conn, output_dir, mode='w'):
    """Returns list of outcome dicts per table."""
    valid_timestamp_types = {'timestamp_ntz', 'timestamp_ltz', 'timestamp_tz'}
    date_format_file = f"{output_dir}/{schema_name}_Date_format_validation_results.txt"
    outcomes = []
    with open(date_format_file, mode) as file:
        if timestamp_cols:
            for table, cols in timestamp_cols.items():
                if cols:
                    columns = [col for col in cols]
                    data_types = get_column_data_types(table, columns,schema_name,conn)
                    all_timestamp = all(
                            any(data_type.lower() == valid_type for valid_type in valid_timestamp_types) for data_type in
                            data_types.values())
                    if all_timestamp:
                        file.write("-" * 80 + "\n")
                        file.write(f"Oracle and SF DateFormats matched.\n{table}: Validation Successful!\n")
                        outcomes.append({'table': table, 'status': 'SUCCESS', 'reason': 'Date formats match'})
                    else:
                        file.write("-" * 80 + "\n")
                        file.write(f"Oracle and SF DateFormats not matched.\n{table}: Validation Failed!\n")
                        mismatched = []
                        for col, data_type in data_types.items():
                            if data_type.lower() not in valid_timestamp_types:
                                file.write(f"For Column: {col} in Oracle has timestamp, But SF has Data Type: {data_type}\n")
                                mismatched.append(col)
                        outcomes.append({'table': table, 'status': 'FAIL', 'reason': f'Date format mismatch in {", ".join(mismatched)}'})
                else:
                    file.write(f"Table: {table} has no timestamp columns to check.\n")
                    outcomes.append({'table': table, 'status': 'SUCCESS', 'reason': 'No timestamp columns'})
        else:
           print(f"\nNo Timestamp columns in oracle for {schema_name}.\n")
           file.write("-" * 80 + "\n")
           file.write(f"No Timestamp columns in oracle for {schema_name}.\n")
           file.write("-" * 80 + "\n")
    print(f"Validation for Date format completed.\n")
    print(f"Verify the results at- {date_format_file}")
    return outcomes

def main():
    args_dict = arg_parsing()
    schema_name = args_dict['schema']
    script_start = time.perf_counter()
    py_path = os.environ["PYTHONPATH"]
    ingest_yaml_file = f'{py_path}/IngestionConfig.yaml'
   
    ingest_cfg_dict = load_yaml(yaml_file=ingest_yaml_file)
    conn = open_sf_connection(ingest_cfg=ingest_cfg_dict)
    appl_code = get_appl_code(schema_name, conn.cursor())
    # Retrieve all application tables from T_APPL_TABLE, split into regular and TB_C2 [03/06/2026]
    all_tables = get_tables_from_appl_table(conn.cursor(), appl_code)
    # regular_tables = set(t for t in all_tables if not t.startswith('TB_C2') and t.endswith(('SCD', 'DIM', 'FACT')))
    regular_tables = set(t for t in all_tables if not t.startswith('TB_C2'))
    tb_c2_tables = set(t for t in all_tables if t.startswith('TB_C2') and t.endswith(('SCD', 'DIM', 'FACT')))
    env = os.environ["PRJ_ENVIRONMENT"]
    ccard_path = f"/mdw/{env}/tgt/scripts/cntlcards"

    script_name = os.path.splitext(os.path.basename(__file__))[0]
    file_path = f"{ingest_cfg_dict['snowflake_connection']['validation_path']}/{schema_name}/{script_name}"
    os.makedirs(file_path, exist_ok=True)

    table_outcomes = []
    ccards_dict = get_ccards_name(regular_tables)
    timestamp_cols = find_timestamp_columns(ccards_dict, ccard_path)
    timestamp_dict = {k: v for k, v in timestamp_cols.items() if v}
    table_outcomes.extend(validate_date_format(timestamp_dict, schema_name, conn, file_path, mode='w'))

    if tb_c2_tables:
        print(f"\nProcessing {len(tb_c2_tables)} TB_C2 tables against DDW_CNF_DIM schema...")
        c2_ccards_dict = get_ccards_name(tb_c2_tables)
        c2_timestamp_cols = find_timestamp_columns(c2_ccards_dict, ccard_path)
        c2_timestamp_dict = {k: v for k, v in c2_timestamp_cols.items() if v}
        table_outcomes.extend(validate_date_format(c2_timestamp_dict, 'DDW_CNF_DIM', conn, file_path, mode='a'))
    else:
        print("No TB_C2 tables found for date format validation.")
    # Script stats output
    script_end = time.perf_counter()
    script_run_time = script_end - script_start

    if args_dict.get('load_sf_meta'):
        try:
            meta_cur = conn.cursor()
            registry = TestCaseRegistry(meta_cur, SCRIPT_NAME,
                                        database=args_dict.get('sf_meta_db'),
                                        schema=args_dict.get('sf_meta_schema'))
            val_results = []
            for outcome in table_outcomes:
                mis_c = 1 if outcome['status'] == 'FAIL' else 0
                mat_c = 1 if outcome['status'] == 'SUCCESS' else 0
                val_results.append(registry.create_result(
                    validation_key='date_format',
                    test_scenario=f'Date format: {outcome["table"]}',
                    appl_name=schema_name,
                    appl_code=appl_code,
                    tenant_id='ALL',
                    table_name=outcome['table'],
                    validation_status=outcome['status'],
                    status_reason=outcome['reason'],
                    mismatched_count=mis_c,
                    matched_count=mat_c
                ))
            if not val_results:
                val_results.append(registry.create_result(
                    validation_key='date_format',
                    test_scenario='Date format validation',
                    appl_name=schema_name,
                    appl_code=appl_code,
                    tenant_id='ALL', table_name='ALL_TABLES',
                    validation_status='SUCCESS', status_reason='No timestamp columns found'
                ))
            loader = ValidationLoader(
                sf_cursor=meta_cur, arg_dict=args_dict,
                script_name=SCRIPT_NAME, script_version=SCRIPT_VERSION,
                database=args_dict.get('sf_meta_db'), schema=args_dict.get('sf_meta_schema')
            )
            summary = ExecutionSummary(
                script_name=SCRIPT_NAME, appl_name=schema_name,
                appl_code=appl_code, tenant_id='ALL',
                process_date=args_dict.get('process_date', ''), script_version=SCRIPT_VERSION
            )
            summary.started_at = datetime.fromtimestamp(time.time() - (time.perf_counter() - script_start))
            summary.parameters_used = {k: str(v) for k, v in args_dict.items() if k not in ('sf_cursor',)}
            summary.update_counts(val_results)
            summary.execution_time_sec = script_run_time
            import glob as _glob
            for df_file in _glob.glob(os.path.join(file_path, '*_Date_format_validation_results.txt')):
                if os.path.getsize(df_file) > 0:
                    summary.read_and_store_output(df_file, file_type='date_format_report')
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
                            source_data={'table': outcome['table'], 'validation': 'date_format_check'},
                            target_data={'status': outcome['status'], 'reason': outcome.get('reason', '')[:2000]},
                            detail_remarks=(outcome.get('reason', '') or '')[:2000]
                        ))
            if detail_batch:
                capped_batch, _, _ = cap_details(detail_batch)
                loader.insert_detail_bulk(capped_batch)

            summary.emit_summary_line()
            passed = sum(1 for o in table_outcomes if o['status'] == 'SUCCESS')
            failed = sum(1 for o in table_outcomes if o['status'] == 'FAIL')
            print(f"Loaded {len(val_results)} date format result(s) — {passed} SUCCESS, {failed} FAIL")
        except Exception as e:
            print(f"Failed to load validation results to Snowflake: {str(e)}")
            traceback.print_exc()
    else:
        print("Skipping metadata load to Snowflake (--load-sf-meta not specified)")

    print(f"\n{'-' * 50}")
    print(f'Script run time: {script_run_time} seconds\n')
    
if __name__ == '__main__':
    main()

