# Description : Format file comparison between Snowflake & Oracle
# Author : Krishnan , Charandeep & Naveen
# Date : Initial version ( 2025-05-26 )
import glob
import os
import snowflake
from snowflake import connector
import pandas as pd
import gzip
from datetime import datetime
import logging
import numpy as np
import csv
import glob
import os
import subprocess
import sys
import time
import traceback
from os import path
import yaml
import logging
import re
import ast
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake.connector import ProgrammingError
import toml

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
    cap_details,
    read_diff_file
)

from script_utils import(
    parse_args,
    get_appl_code,
    open_sf_connection,
    load_yaml
)

SCRIPT_NAME = "FMT_Field_Validation.py"
SCRIPT_VERSION = "v2.0"

def find_pos_files(filenames, search_path):
   found_map = {}
   extensions = ['.pos', '.pos1', '.pos2']
   for name in filenames:
       base = name[:-3]
       if len(base) >= 5:
           base = base[:4] + 'l' + base[5:]
       else:
           print(f"Skipping invalid name (too short): {name}")
           continue
       for ext in extensions:
           filename = f"{base}{ext}"
           full_path = os.path.join(search_path, filename)
           if os.path.isfile(full_path):
               found_map[name] = full_path
               break
   return found_map

def generate_fmt_headers(metadata_dict, driver_df, data_dir,conn,appl, prcs_dte):
    SKIP_COLS = {'tenant_id', 'load_ts', 'source_file', 'prcs_yr_mth_nbr'}
    cursor = conn.cursor()
    enhance_dict = {}
    error_list = []
    for _, row in driver_df.iterrows():
        table = row["TABLE_NAME"]
        original_file_prefix = row["FILE_NAME"]
        delimiter = row["DELIMITER"].encode().decode('unicode_escape')
        try:
            cursor.execute(f"""
                               SELECT COLUMN_NAME
                               FROM CUR_IBS.INFORMATION_SCHEMA.COLUMNS
                               WHERE TABLE_NAME = '{table.upper()}' 
                               AND TABLE_SCHEMA ='{appl}'
                               ORDER BY ORDINAL_POSITION
                           """)
            sf_cols = [r[0].lower() for r in cursor.fetchall()]
        except Exception as e:
            print(f"Error fetching schema for {table}: {e}")
            continue
        # Get SF columns (remove SKIP_COLS)
        #print(f"table is -{table} and its cols are - {sf_cols}")
        sf_columns = [col.lower() for col in sf_cols if col.lower() not in SKIP_COLS]
        # Match metadata by original name
        matched_meta = next((m for m in metadata_dict if m["orig_fmt_file_name"] == original_file_prefix), None)
        if not matched_meta:
            print(f"No metadata found for {original_file_prefix}")
            continue
        pos_columns = [col.lower() for col in matched_meta["cc_columns"]]
        no_of_pos_columns = len(pos_columns)
        #print(f"no. of pos cls--- {no_of_pos_columns}")
        if 'prcs_dte' not in pos_columns and 'prcs_dte' in sf_columns:
            sf_columns.remove('prcs_dte')
        # Write headers to matching .fmt files
        for filename in os.listdir(data_dir):
            if filename.startswith(original_file_prefix) and filename.endswith(f'{prcs_dte}.fmt'):
                fmt_path = os.path.join(data_dir, filename)
                # Check if header already present
                try:
                    with open(fmt_path, 'r') as fl:
                        header_line = fl.readline().strip()
                        if any(col in header_line.lower() for col in pos_columns):
                            print(f"Header already present in: {filename}")
                            hdr_columns = header_line.split(delimiter)
                            # if len(hdr_columns) == no_of_pos_columns:
                            #     if len(sf_columns) != no_of_pos_columns:
                            #         missing_counts = no_of_pos_columns - len(sf_columns)
                            #         missing_col = pos_columns[-missing_counts:]
                            #         enhance_dict[filename] = missing_col

                            if len(sf_columns) != len(hdr_columns):
                                hdr_count = len(hdr_columns) - len(sf_columns)
                                hdr_cols = hdr_columns[-hdr_count:]
                                enhance_dict[filename] = hdr_cols
                                # print(f"header present .testing derived scenario")
                            continue
                except Exception as e:
                    print(f"Error reading {filename}: {e}")
                    continue
                # Write header
                try:
                    with open(fmt_path, 'r') as f:
                        first_line = f.readline().rstrip('\n')
                        reader = csv.reader([first_line],delimiter=delimiter)
                        fmt_cols = next(reader)
                        print(f"no of fmt cols-->  {len(fmt_cols)}")
                        if len(fmt_cols) == no_of_pos_columns:
                            if len(sf_columns) != no_of_pos_columns:
                                missing_count = no_of_pos_columns - len(sf_columns)
                                missing_cols = pos_columns[-missing_count:]
                                enhance_dict[filename] = missing_cols
                                content = f.readlines()
                                with open(fmt_path, 'w') as f:
                                    f.write(delimiter.join(pos_columns) + '\n')
                                    f.write(first_line + '\n')
                                    f.writelines(content)
                                print(f"Header written to: {filename}")
                            else:
                                contents = f.readlines()
                                with open(fmt_path, 'w') as f:
                                    f.write(delimiter.join(pos_columns) + '\n')
                                    f.write(first_line + '\n')
                                    f.writelines(contents)
                                print(f"Header written to: {filename}")
                        else:
                            print("!!"* 60)
                            print(f"Warning: Unable to add header in {filename}. Please manually add headers to file to proceed with comparison")
                            print(
                                f"Warning: Comparison for  {filename}- will be skipped")
                            print("!!" * 60)
                            error_list.append(filename)
                except Exception as e:
                    print(f"Error writing to {filename}: {e}")
    # print(f"New columns in oracle : {enhance_dict}")
    # print(error_list)
    return enhance_dict, error_list

def extract_columns_from_pos_files(file_dict):
   results = []
   for key, filepath in file_dict.items():
       try:
           with open(filepath, 'r') as file:
               content = file.read()
           # Get content between first ( and last )
           start = content.find('(')
           end = content.rfind(')')
           if start == -1 or end == -1 or start >= end:
               raise ValueError("Column block not found properly.")
           block = content[start + 1:end].strip()
           # Smart split that handles nested parentheses like TO_DATE(...)
           def smart_split(block):
               columns = []
               current = ''
               paren_level = 0
               for char in block:
                   if char == ',' and paren_level == 0:
                       columns.append(current.strip())
                       current = ''
                   else:
                       current += char
                       if char == '(':
                           paren_level += 1
                       elif char == ')':
                           paren_level -= 1
               if current.strip():
                   columns.append(current.strip())
               return columns
           full_columns = smart_split(block)
           # Normalize whitespace and get only the base column name
           base_columns = []
           for col in full_columns:
               col = re.sub(r'\s+', ' ', col).strip()
               base_columns.append(col.split(' ', 1)[0])
           results.append({
               "orig_fmt_file_name": key,
               "ccard_file_path": filepath,
               "cc_columns": base_columns
           })
       except Exception as e:
           results.append({
               "orig_fmt_file_name": key,
               "ccard_file_path": filepath,
               "cc_columns": f"Error: {e}"
           })
   return results



def compare_data(table_name, file_df, sf_df, outfname, FILE_PRCS_DTE, TENANT_ID, PRCS_GRP, filt_pk_keys_list,fmt_path):
    """Returns dict with status, reason, src_count, tgt_count."""
    src_cnt = len(file_df)
    sf_cnt = len(sf_df)
    if path.exists(f"{outfname}"):
        os.remove(f"{outfname}")
    with open(file=outfname, mode='w') as p:
        if src_cnt != sf_cnt:
            p.write(
                f"{table_name} : Record count not matched <<< Record count SF : {sf_cnt} & Record count File : {src_cnt} >>>\n")
            count_not_match = "1"
            sf_df = sf_df.merge(file_df[filt_pk_keys_list], on=filt_pk_keys_list, how='inner')
        else:
            p.write(f"{table_name} : Count Validation Successful <<< Record count : {sf_cnt} >>> .\n")
            count_not_match = "0"

    if src_cnt == 0 and sf_cnt == 0:
        return {'status': 'SUCCESS', 'reason': 'Zero records on both sides', 'src_count': 0, 'tgt_count': 0}

    if src_cnt != 0 and sf_cnt != 0:
        try:
            arr1 = file_df.astype(str).to_numpy()
            arr2 = sf_df.astype(str).to_numpy()
            unequal_records = []
            unequal_indices = np.where(arr1 != arr2)
            for row, col in zip(unequal_indices[0], unequal_indices[1]):
                column_name = file_df.columns[col]
                value1 = arr1[row, col]
                value2 = arr2[row, col]
                if pd.notna(value1) and pd.notna(value2):
                    if value1 != "" and value2 is not None:
                        unequal_records.append({'row': row,
                                                'column': column_name,
                                                'value_ora_fmt': value1,
                                                'value_sf_fmt': value2})

            with open(file=outfname, mode='a') as f:
                if path.exists(f"{fmt_path}/DIFF_{table_name}_{TENANT_ID}{PRCS_GRP}_{FILE_PRCS_DTE}.txt"):
                    os.remove(f"{fmt_path}/DIFF_{table_name}_{TENANT_ID}{PRCS_GRP}_{FILE_PRCS_DTE}.txt")
                if unequal_records:
                    with open(
                            file=f"{fmt_path}/DIFF_{table_name}_{TENANT_ID}{PRCS_GRP}_{FILE_PRCS_DTE}.txt",
                            mode='w') as f1:
                        f1.write(f"Table: {table_name}\n")
                        for record in unequal_records:
                            f1.write(f"Column: {record['column']} Row: {int(record['row']) + 1}\n")
                            f1.write(f"value in ora fmt:{record['value_ora_fmt']}\n")
                            f1.write(f"value in SF fmt:{record['value_sf_fmt']}\n")
                    f.write(
                        f"{table_name} : Data Difference file is created <DIFF_{table_name}_{TENANT_ID}{PRCS_GRP}_{FILE_PRCS_DTE}.txt>")
                    _diff_path = f"{fmt_path}/DIFF_{table_name}_{TENANT_ID}{PRCS_GRP}_{FILE_PRCS_DTE}.txt"
                    reason = f'{len(unequal_records)} field difference(s) found'
                    if count_not_match == "1":
                        reason = f'Count mismatch + {reason}'
                    return {'status': 'FAIL', 'reason': reason, 'src_count': src_cnt, 'tgt_count': sf_cnt, 'diff_file': _diff_path}
                else:
                    logging.info(f"{table_name} : Data validation is successful")
                    if count_not_match == "1":
                        f.write(
                            f"{table_name} : Data Validation is Successful for only matched records between Snowflake & Oracle File\n")
                        return {'status': 'FAIL', 'reason': f'Count mismatch: File={src_cnt}, SF={sf_cnt} (matched rows OK)', 'src_count': src_cnt, 'tgt_count': sf_cnt}
                    else:
                        f.write(f"{table_name} : Data Validation is Successful\n")
                        return {'status': 'SUCCESS', 'reason': 'Data matches', 'src_count': src_cnt, 'tgt_count': sf_cnt}
        except ValueError:
            logging.info("Values for comparison are different between Oracle and SF")
            return {'status': 'FAIL', 'reason': 'ValueError during comparison', 'src_count': src_cnt, 'tgt_count': sf_cnt}
        except KeyError:
            logging.info("Count does not match between Oracle and SF")
            return {'status': 'FAIL', 'reason': 'KeyError during comparison', 'src_count': src_cnt, 'tgt_count': sf_cnt}
        except Exception as ex:
            logging.info("Error occurred during Validation")
            return {'status': 'FAIL', 'reason': f'Error: {type(ex).__name__}', 'src_count': src_cnt, 'tgt_count': sf_cnt}
    return {'status': 'FAIL', 'reason': f'Count mismatch: File={src_cnt}, SF={sf_cnt}', 'src_count': src_cnt, 'tgt_count': sf_cnt}


def date_column(control_file_path, column_names):
    result = {col: None for col in column_names}
    with open(control_file_path, 'r') as file:
        lines = file.readlines()
    # Match TO_DATE with any format string
    to_date_pattern = re.compile(
        r'TO_DATE\s*\([^,]+,\s*["\']([^"\']+)["\']\)',
        re.IGNORECASE
    )
    # Match DATE literals like: DATE 'YYYY-MM-DD'
    date_literal_pattern = re.compile(
        r'DATE\s+["\']([^"\']+)["\']',
        re.IGNORECASE
    )
    # Match any date-like string (fallback)
    generic_date_format_pattern = re.compile(
        r'["\']((?=.*YYYY)(?=.*(MM|MON))(?=.*DD)(?=.*(HH|MI|SS)?)[^"\']*)["\']',
        re.IGNORECASE
    )
    for col in column_names:
        for line in lines:
            if col.lower() in line.lower():
                normalized_line = re.sub(r'\s+', ' ', line.strip())
                # Primary: TO_DATE(..., 'format')
                to_date_match = to_date_pattern.search(normalized_line)
                if to_date_match:
                    result[col] = to_date_match.group(1)
                    break
                # Secondary: DATE 'YYYY-MM-DD'
                date_literal_match = date_literal_pattern.search(normalized_line)
                if date_literal_match:
                    result[col] = date_literal_match.group(1)
                    break
                # Fallback: any quoted date/time format
                generic_match = generic_date_format_pattern.search(normalized_line)
                if generic_match:
                    result[col] = generic_match.group(1)
                    break

    return result


def sort(sf_df, file_df, fmt_name, filt_pk_keys, enhance_dict, pos_dict, fmt_file_name):

    # Date columns formatting
    date_columns_to_find = [col for col in file_df if ('dte' in col.lower() or 'date' in col.lower()) and not (col.lower().startswith('dw_') and col.lower().endswith('id'))]
    control_file_path = pos_dict.get(fmt_file_name)

    if not control_file_path:
        print(f"Control card not found - {fmt_file_name}")
    else:

        result = date_column(control_file_path, date_columns_to_find)
        for col, fmt in result.items():
            if fmt == None:
                converted_dates = pd.to_datetime(file_df[col], format='%Y-%m-%d', errors='coerce')
                formatted_dates = converted_dates.dt.strftime('%Y-%m-%d')
                file_df[col] = formatted_dates.where(formatted_dates.notna(), file_df[col])
            else:
                fmt = fmt.replace('YYYY', '%Y').replace('MM', '%m').replace('DD', '%d')
                # file_df[col] = pd.to_datetime(file_df[col], format=fmt, errors='coerce')
                converted_dates = pd.to_datetime(file_df[col], format=fmt, errors='coerce')
                formatted_dates = converted_dates.dt.strftime(fmt)
                file_df[col] = formatted_dates.where(formatted_dates.notna(), file_df[col])
            # sf_df[col] = pd.to_datetime(sf_df[col], format='%Y-%m-%d', errors='coerce')
            sf_converted_dates = pd.to_datetime(sf_df[col], format='%Y-%m-%d', errors='coerce') if col in sf_df.columns else None
            sf_formatted_dates = sf_converted_dates.dt.strftime('%Y-%m-%d') if sf_converted_dates is not None else None
            if sf_converted_dates is not None:
                sf_df[col] = sf_formatted_dates.where(sf_formatted_dates.notna(), sf_df[col])

        # Changing key fields if its Numeric
        for key,value in filt_pk_keys.items():
            if value.upper() == "Y":
                file_df[key] = pd.to_numeric(file_df[key], errors='coerce')
                sf_df[key] = pd.to_numeric(sf_df[key], errors='coerce')

        # Creating keys as list from dict and sort based on the keys
        filt_pk_keys_list = list(filt_pk_keys.keys())


        sf_df = sf_df.sort_values(by=filt_pk_keys_list, ascending=True)
        file_df = file_df.sort_values(by=filt_pk_keys_list, ascending=True)


    # Remove DW_ID & Audit columns
    # File
    file_columns_to_drop = file_df.filter(like='dw_').columns
    column_check = "prcs_dte"
    if column_check in file_df.columns:
        file_df = file_df.drop(columns=[column_check])
    file_df = file_df.drop(columns=file_columns_to_drop)
    # Snowflake
    columns_to_drop = sf_df.filter(like='dw_').columns
    sf_df = sf_df.drop(columns=['tenant_id', 'load_ts', 'source_file', 'prcs_dte', 'prcs_yr_mth_nbr'])
    sf_df = sf_df.drop(columns=columns_to_drop)

    # Dropping the extra columns in Oracle which is added as part of enhancement
    for name,drop_col in enhance_dict.items():
        if fmt_name in name:
            file_df = file_df.drop(columns=drop_col)

    file_df = file_df.fillna("")
    return sf_df, file_df, filt_pk_keys_list


def dataframe(APPL_NAME, PRCS_DTE, FILE_PRCS_DTE, TENANT_ID, PRCS_GRP, map_df, conn, cs,fmt_path, enhance_dict, error_list, pos_dict):
    """Returns list of outcome dicts for each table processed."""
    outcomes = []
    for index, row in map_df.iterrows():
        table_name = row['TABLE_NAME']
        fmt_file_name = row['FILE_NAME']
        delim = row['DELIMITER']
        keys = row['KEYS']
        filt_pk_keys = ast.literal_eval(keys)
        file_path = f"{fmt_path}/{fmt_file_name}{TENANT_ID}{PRCS_GRP}_{FILE_PRCS_DTE}.fmt"
        fmt_name = os.path.basename(file_path)
        if fmt_name not in error_list:
            details = ""
            print(f"\nComparing table '{table_name}' with file '{file_path}'")
            try:
                with open(file_path, 'rt') as f:
                    try:
                        file_df = pd.read_csv(f, dtype=str, delimiter=delim, engine='python')
                    except Exception:
                        print("Unable to read Oracle FMT file")
                file_df.columns = file_df.columns.str.lower()
            except Exception as e:
                details = f"Error reading file file: {e}"
                outcomes.append({'table': table_name, 'status': 'FAIL', 'reason': details, 'src_count': 0, 'tgt_count': 0})
                continue
            try:
                sql = (f"SELECT * FROM cur_ibs.{APPL_NAME}.{table_name} where prcs_dte='{PRCS_DTE}' and TENANT_ID='{TENANT_ID}'and RIGHT(SPLIT_PART(SOURCE_FILE, '_', 1), 1)='{PRCS_GRP}'")
                cs.execute(sql)
                sf_d = cs.fetchall()
                column = [col[0] for col in cs.description]
                sf_df = pd.DataFrame(sf_d, columns=column)
                sf_df.columns = sf_df.columns.str.lower().str.strip()
            except Exception as e:
                details = f"Error fetching table data: {e}"
                print(details)
                outcomes.append({'table': table_name, 'status': 'FAIL', 'reason': details, 'src_count': 0, 'tgt_count': 0})
                continue

            try:
                sf_df, file_df, filt_pk_keys_list = sort(sf_df, file_df, fmt_name, filt_pk_keys, enhance_dict, pos_dict, fmt_file_name)
            except Exception as e:
                details = f"Error during sort/format for {table_name}: {e}"
                print(details)
                traceback.print_exc()
                outcomes.append({'table': table_name, 'status': 'FAIL', 'reason': details, 'src_count': 0, 'tgt_count': 0})
                continue
            outfname = f"{fmt_path}/Summary_{table_name}_{TENANT_ID}{PRCS_GRP}_{FILE_PRCS_DTE}.txt"
            print("data frame ready")
            outcome = compare_data(table_name, file_df, sf_df, outfname, FILE_PRCS_DTE, TENANT_ID, PRCS_GRP, filt_pk_keys_list,fmt_path)
            outcome['table'] = table_name
            outcomes.append(outcome)
    return outcomes


def arg_parsing() -> dict:
    args = parse_args(
        required=['--a', '--t', '--p'],
        optional=['--load-sf-meta', '--sf-meta-db', '--sf-meta-schema'],
        description='FMT file field validation.',
    )
    args['prcs_dte'] = args['process_date']
    args['tenant_group'] = args['tenant_id']
    return args

def main():
    script_start = time.perf_counter()
    argument_dict = arg_parsing()
    APPL_NAME = argument_dict['appl_name']
    FILE_PRCS_DTE = argument_dict['prcs_dte']
    TENANTGROUPIDs = argument_dict['tenant_group']
    date_obj = datetime.strptime(FILE_PRCS_DTE, "%Y%m%d")
    PRCS_DTE = date_obj.strftime("%Y-%m-%d")

    py_path = os.environ["PYTHONPATH"]
    ingest_yaml_file = f'{py_path}/IngestionConfig.yaml'
    ingest_cfg_dict = load_yaml(yaml_file=ingest_yaml_file)
    conn = open_sf_connection(ingest_cfg=ingest_cfg_dict)
    cs = conn.cursor()
    appl_code = get_appl_code(argument_dict['appl_name'],cs)
    env = os.environ["PRJ_ENVIRONMENT"]

    ccard_path = f"/mdw/{env}/tgt/scripts/cntlcards"
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    fmt_path = f"{ingest_cfg_dict['snowflake_connection']['validation_path']}/{APPL_NAME}/{script_name}"
    os.makedirs(fmt_path, exist_ok=True)
    file_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'configs')
    config_file = f'{file_dir}/FMT_Field_Validation_FILE_{APPL_NAME}.txt'
    if not os.path.exists(config_file):
        print(f"ERROR: Config file not found: {config_file}")
        sys.exit(1)
    else:
        print("Config file found")    
    map_df = pd.read_csv(config_file, quotechar='"')
    filenames = map_df['FILE_NAME'].tolist()
    pos_dict = find_pos_files(filenames, search_path=ccard_path)
    metadata_dict = extract_columns_from_pos_files(pos_dict)
    enhance_dict, error_list = generate_fmt_headers(metadata_dict, map_df, fmt_path, conn, APPL_NAME, FILE_PRCS_DTE)

    table_outcomes = []
    for TENANT_GROUPID in TENANTGROUPIDs.split(","):
        TENANT_ID = TENANT_GROUPID[:-1]
        PRCS_GRP = TENANT_GROUPID[-1]
        outcomes = dataframe(APPL_NAME, PRCS_DTE, FILE_PRCS_DTE, TENANT_ID, PRCS_GRP, map_df, conn, cs, fmt_path, enhance_dict, error_list, pos_dict)
        for o in outcomes:
            o['tenant_id'] = TENANT_ID
        table_outcomes.extend(outcomes)

    if argument_dict.get('load_sf_meta'):
        try:
            registry = TestCaseRegistry(cs, SCRIPT_NAME,
                                        database=argument_dict.get('sf_meta_db'),
                                        schema=argument_dict.get('sf_meta_schema'))
            val_results = []
            for outcome in table_outcomes:
                src_c = outcome.get('src_count', 0) or 0
                tgt_c = outcome.get('tgt_count', 0) or 0
                mis_c = abs(int(src_c) - int(tgt_c)) if outcome['status'] == 'FAIL' else 0
                mat_c = min(int(src_c), int(tgt_c)) if outcome['status'] == 'SUCCESS' else 0
                ai = read_diff_file(outcome.get('diff_file')) or None
                val_results.append(registry.create_result(
                    validation_key='field_validation',
                    test_scenario=f'FMT field: {outcome["table"]}',
                    appl_name=argument_dict['appl_name'],
                    appl_code=appl_code,
                    tenant_id=outcome.get('tenant_id', 'ALL'),
                    table_name=outcome['table'],
                    validation_status=outcome['status'],
                    status_reason=outcome['reason'],
                    source_count=outcome.get('src_count', 0),
                    target_count=outcome.get('tgt_count', 0),
                    mismatched_count=mis_c,
                    matched_count=mat_c,
                    additional_info=ai
                ))
            if not val_results:
                val_results.append(registry.create_result(
                    validation_key='field_validation',
                    test_scenario='FMT field validation',
                    appl_name=argument_dict['appl_name'],
                    appl_code=appl_code,
                    tenant_id='ALL', table_name='ALL_TABLES',
                    validation_status='SUCCESS', status_reason='No tables processed'
                ))
            loader = ValidationLoader(
                sf_cursor=cs, arg_dict=argument_dict,
                script_name=SCRIPT_NAME, script_version=SCRIPT_VERSION,
                database=argument_dict.get('sf_meta_db'), schema=argument_dict.get('sf_meta_schema')
            )
            summary = ExecutionSummary(
                script_name=SCRIPT_NAME, appl_name=argument_dict.get('appl_name', ''),
                appl_code=appl_code, tenant_id='ALL',
                process_date=argument_dict.get('process_date', ''), script_version=SCRIPT_VERSION
            )
            summary.started_at = datetime.fromtimestamp(time.time() - (time.perf_counter() - script_start))
            summary.parameters_used = {k: str(v) for k, v in argument_dict.items() if k not in ('sf_cursor',)}
            summary.update_counts(val_results)
            summary.execution_time_sec = time.perf_counter() - script_start
            import glob as _glob
            for sf in _glob.glob(os.path.join(fmt_path, 'Summary_*.txt')):
                if os.path.getsize(sf) > 0:
                    summary.read_and_store_output(sf, file_type='fmt_validation_report')
            exec_id = loader.insert_execution_summary(summary)

            run_id_map = {}
            for r in val_results:
                rid = loader.insert_master(r, execution_id=exec_id)
                run_id_map[(r.table_name, r.tenant_id)] = rid

            detail_batch = []
            for outcome in table_outcomes:
                if outcome['status'] == 'FAIL':
                    rid = run_id_map.get((outcome['table'], outcome.get('tenant_id', 'ALL')), 0)
                    if rid:
                        detail_batch.append(ValidationDetailResult(
                            run_id=rid,
                            match_status='MISMATCH',
                            record_key=outcome['table'],
                            source_data={'src_count': outcome.get('src_count'), 'table': outcome['table']},
                            target_data={'tgt_count': outcome.get('tgt_count')},
                            detail_remarks=cap_details(outcome.get('reason', '') or '', 2000)[0]
                        ))
            if detail_batch:
                capped_batch, _, _ = cap_details(detail_batch)
                loader.insert_detail_bulk(capped_batch)

            summary.emit_summary_line()
            passed = sum(1 for o in table_outcomes if o['status'] == 'SUCCESS')
            failed = sum(1 for o in table_outcomes if o['status'] == 'FAIL')
            print(f"Loaded {len(val_results)} FMT result(s) — {passed} SUCCESS, {failed} FAIL")
        except Exception as e:
            print(f"Failed to load validation results to Snowflake: {str(e)}")
            traceback.print_exc()
    else:
        print("Skipping metadata load to Snowflake (--load-sf-meta not specified)")

    cs.close()
    conn.close()
    # Script stats output
    script_end = time.perf_counter()
    script_run_time = script_end - script_start
    print(f"Script run time: {script_run_time} seconds")

if __name__ == '__main__':
    main()