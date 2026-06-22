# This script does a validation of table structure, or DDL, of what is currently in Oracle versus what is in Snowflake
# It also validates Date Format (Timestamp) columns by checking Oracle control-card files against Snowflake metadata.
# The arguments that this script takes are:
# --a Application Name as per Snowflake schema
# --o Output Directory (Where the log of run will output to)
# An example call to this script would look like: python -m DDLValidation --a DDW_LN --t 6A --l INFO --o /mdw/dvl/files9/logs/l6
# Sources used as a basis for this script was the FieldValidation.py script authored by Agalya Karikalan on 08/08/2024
# Script written by Nathan Gupton with help of Jyothi Aleti and Richard Pearse by date 01/14/2025
# Script modified to handle DW_ID Changes to NUMBER(38,0) (use label 'DW_ID Change' to navigate to changes)
# Updated: 02/11/2026 Author:Barath Lakshman A N
# Enhanced : Parameters SCD check, Incomplete Cycle Check; Snowflake metadata load
# Updated: 06/09/2026 Merged DateFormatCheckDDW.py (Author: Charandeep) into this script
# Updated: 06/19/2026 Fixed timestamp comparison logic (regex-based Oracle normalization,
#          DATETIME_PRECISION for Snowflake, TIMESTAMP WITH LOCAL TIME ZONE -> TIMESTAMP_LTZ)

import csv
import os.path
import re
import subprocess
import sys
import time
import traceback
import oracledb
import snowflake
import logging
import yaml
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake.connector import DictCursor
import toml
import pandas as pd
import numpy as np

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
    cap_details,
    read_diff_file,
    infer_registry_app_category,
)

from script_utils import (
    parse_args,
    get_appl_code,
    open_sf_connection,
    load_yaml,
    get_tables_from_appl_table,
    open_oracle_connection,
    logging_config
)

SCRIPT_NAME = "DDLValidation.py"
SCRIPT_VERSION = "v3.1"


def arg_parsing() -> dict:
    return parse_args(
        required=['--a'],
        optional=['--l', '--o', '--load-sf-meta', '--sf-meta-db', '--sf-meta-schema'],
        description='DDL structure validation between Oracle and Snowflake.',
    )


# ---------------------------------------------------------------------------
# Date Format (Timestamp) Validation Helpers
# (Merged from DateFormatCheckDDW.py)
# ---------------------------------------------------------------------------

def get_ccards_name(table_names):
    """Derive control-card file names from table names."""
    ccards = {}
    for name in table_names:
        parts = name.split('_')
        if len(parts) >= 3:
            first = parts[1].lower()
            second = parts[2].lower()
            value = f'dw{first}l{second}.pos'
            ccards[name] = value
        else:
            logging.warning(f"Invalid table name for ccard derivation: {name}")
    return ccards


def extract_column_block(content):
    """Extract the column definition block from a control-card file."""
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
                return content[start + 1:i]
    return None


def split_column_definitions(text):
    """Split column definitions on commas respecting parentheses."""
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
    """Identify timestamp columns from control-card files."""
    timestamp_columns = {}
    for key, file_name in ccards_dict.items():
        file_path = os.path.join(base_path, file_name)
        if not os.path.exists(file_path):
            if file_name.endswith('.pos'):
                fallback_file = file_name.replace('.pos', '.pos1')
                fallback_path = os.path.join(base_path, fallback_file)
                if os.path.exists(fallback_path):
                    file_path = fallback_path
                else:
                    logging.warning(f"[DateFormat] ccard not found: {file_name} or {fallback_file}")
                    timestamp_columns[key] = []
                    continue
            else:
                logging.warning(f"[DateFormat] ccard not found: {file_path}")
                timestamp_columns[key] = []
                continue

        try:
            with open(file_path, 'r') as file:
                content = file.read()
        except Exception as e:
            logging.error(f"[DateFormat] Could not read {file_path}: {e}")
            timestamp_columns[key] = []
            continue

        block = extract_column_block(content)
        if not block:
            logging.warning(f"[DateFormat] No valid column block in {file_name}")
            timestamp_columns[key] = []
            continue

        col_defs = split_column_definitions(block)
        result_cols = []
        for col_def in col_defs:
            if 'mi:ss' not in col_def.lower():
                continue
            tokens = col_def.strip().split()
            if tokens:
                result_cols.append(tokens[0])

        timestamp_columns[key] = result_cols
    return timestamp_columns


def validate_date_format(timestamp_cols, schema_name, sf_column_types_cache, outfname):
    """Validate that Oracle timestamp columns are also timestamp types in Snowflake.

    Uses cached SF column types from DDL phase.
    When outfname is provided, writes to file.
    Returns list of outcome dicts per table.
    """
    valid_timestamp_types = {'timestamp_ntz', 'timestamp_ltz', 'timestamp_tz'}
    outcomes = []

    if timestamp_cols:
        for table, cols in timestamp_cols.items():
            if cols:
                cached_types = sf_column_types_cache.get(table, {})
                data_types = {col: cached_types.get(col, 'UNKNOWN') for col in cols if col in cached_types}

                if not data_types:
                    outcomes.append({'table': table, 'status': 'SUCCESS',
                                     'reason': 'Timestamp columns not in SF metadata'})
                    continue

                all_timestamp = all(
                    any(data_type.lower() == valid_type for valid_type in valid_timestamp_types)
                    for data_type in data_types.values()
                )

                if all_timestamp:
                    outcomes.append({'table': table, 'status': 'SUCCESS', 'reason': 'Date formats match'})
                else:
                    mismatched = []
                    for col, data_type in data_types.items():
                        if data_type.lower() not in valid_timestamp_types:
                            mismatched.append(f"{col} (SF has: {data_type})")
                    outcomes.append({'table': table, 'status': 'FAIL',
                                     'reason': f'Date format mismatch in {", ".join([c.split(" ")[0] for c in mismatched])}'})
            else:
                outcomes.append({'table': table, 'status': 'SUCCESS', 'reason': 'No timestamp columns'})

    if outfname:
        with open(outfname, 'a') as file:
            for outcome in outcomes:
                if outcome['status'] == 'SUCCESS':
                    file.write(f"  {outcome['table']}: SUCCESS ({outcome['reason']})\n")
                else:
                    file.write(f"  {outcome['table']}: FAILED - {outcome['reason']}\n")

    logging.info(f"[DateFormat] Validation for date format completed for schema {schema_name}.")
    return outcomes


# ---------------------------------------------------------------------------
# Oracle Timestamp Type Normalization Helper
# ---------------------------------------------------------------------------

def normalize_oracle_datatype(dtype):
    """Normalize Oracle TIMESTAMP data types to Snowflake equivalents using regex.

    Handles all precisions (0-9) and all variants:
      TIMESTAMP(n)                       -> TIMESTAMP_NTZ
      TIMESTAMP(n) WITH LOCAL TIME ZONE  -> TIMESTAMP_LTZ
      TIMESTAMP(n) WITH TIME ZONE        -> TIMESTAMP_TZ
    """
    if re.match(r'^TIMESTAMP\(\d+\)$', dtype):
        return 'TIMESTAMP_NTZ'
    elif re.match(r'^TIMESTAMP\(\d+\) WITH LOCAL TIME ZONE$', dtype):
        return 'TIMESTAMP_LTZ'
    elif re.match(r'^TIMESTAMP\(\d+\) WITH TIME ZONE$', dtype):
        return 'TIMESTAMP_TZ'
    return dtype


# ---------------------------------------------------------------------------
# DDL Validation Functions
# ---------------------------------------------------------------------------

def orc_pk_cols(tablename, arg_dict, orc_cursor) -> tuple[list, str]:
    pk_cols = ""
    pk_list = []

    if arg_dict["appl_name"][0:3] == 'ADS' or (arg_dict["appl_name"][0:3] == 'DDW' and tablename.endswith('ARD')):
        pk_query = f'''select b.column_name from all_constraints a join all_cons_columns b on a.owner=\'DWQQ2\' and a.table_name = \'{tablename}\'and a.constraint_name = b.constraint_name and a.constraint_type = \'P\' and a.owner=b.owner and a.table_name = b.table_name order by b.column_name'''
    else:
        pk_query = f'''select b.column_name from all_constraints a join all_cons_columns b on a.owner=\'DWQQ1\' and a.table_name = \'{tablename}\'and a.constraint_name = b.constraint_name and a.constraint_type = \'P\' and a.owner=b.owner and a.table_name = b.table_name order by b.column_name'''

    orc_cursor.execute(pk_query)
    pk_result = orc_cursor.fetchall()

    if len(pk_result) != 0:
        for pk in pk_result:
            pk_cols += f"{pk[0]},"
            pk_list.append(pk[0])
        pk_cols = pk_cols[:-1]
    elif len(pk_result) == 0:
        syn_list = []
        if arg_dict["appl_name"][0:3] == 'ADS' or (arg_dict["appl_name"][0:3] == 'DDW' and tablename.endswith('ARD')):
            syn_query = (f'''SELECT TABLE_NAME FROM ALL_SYNONYMS WHERE SYNONYM_NAME = \'{tablename}\' AND TABLE_OWNER = \'DWQQ2\' AND TABLE_NAME <> SYNONYM_NAME''')
        else:
            syn_query = (f'''SELECT TABLE_NAME FROM ALL_SYNONYMS WHERE SYNONYM_NAME = \'{tablename}\' AND TABLE_OWNER = \'DWQQ1\' AND TABLE_NAME <> SYNONYM_NAME''')

        orc_cursor.execute(syn_query)
        syn_name = orc_cursor.fetchall()

        if len(syn_name) == 0:
            logging.info("This table has no primary keys to be compared.")
        else:
            for row in syn_name:
                syn_list.append(list(row))
            if arg_dict["appl_name"][0:3] == 'ADS' or (arg_dict["appl_name"][0:3] == 'DDW' and tablename.endswith('ARD')):
                pk_query = (f'''select b.column_name from all_constraints a join all_cons_columns b on a.owner=\'DWQQ2\' and a.table_name = \'{syn_list[0][0]}\'and a.constraint_name = b.constraint_name and a.constraint_type = \'P\' and a.owner=b.owner and a.table_name = b.table_name order by b.column_name''')
            else:
                pk_query = (f'''select b.column_name from all_constraints a join all_cons_columns b on a.owner=\'DWQQ1\' and a.table_name = \'{syn_list[0][0]}\'and a.constraint_name = b.constraint_name and a.constraint_type = \'P\' and a.owner=b.owner and a.table_name = b.table_name order by b.column_name''')

            try:
                orc_cursor.execute(pk_query)
                pk_result = orc_cursor.fetchall()
            except Exception as e:
                logging.info(f"The following Oracle Query has failed {e}")
                logging.info(pk_query)

            if len(pk_result) != 0:
                for pk in pk_result:
                    pk_cols += f"{pk[0]},"
                    pk_list.append(pk[0])
                pk_cols = pk_cols[:-1]

    return pk_list, pk_cols


def sf_pk_cols(tablename, arg_dict, sf_cursor) -> tuple[list, str]:
    pk_cols = ""
    pk_list = []
    pk_result1 = None

    sf_schema = "DDW_CNF_DIM" if tablename.startswith("TB_C2") else arg_dict["appl_name"]

    pk_query1 = f'''DESCRIBE TABLE CUR_IBS.{sf_schema}.{tablename};'''
    try:
        pk_result1 = sf_cursor.execute(pk_query1)
    except Exception:
        logging.info("Tablename not found in Snowflake.")
        pk_result1 = None

    pk_result2 = []
    if pk_result1:
        pk_query2 = f'''SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE "primary key" = \'Y\' ORDER BY "name";'''
        try:
            sf_cursor.execute(pk_query2)
            pk_result2 = sf_cursor.fetchall()
        except Exception as e:
            logging.info(f"Could not retrieve primary keys via RESULT_SCAN: {e}")
            pk_result2 = []

    if pk_result1:
        if pk_result2:
            for pk in pk_result2:
                if pk[0].lower() != 'tenant_id':
                    pk_cols += f'{pk[0]},'
                    pk_list.append(pk[0])
            pk_cols = pk_cols[:-1]

    return pk_list, pk_cols


def write_to_csv(ora_cursor, sf_cursor, arg_dict, table, file_path, sf_all, orc_all):
    # excluded fields
    sf_exclude_filed_list = ["TENANT_ID", "LOAD_TS", "SOURCE_FILE", "SRC_APPL_NAME"]
    orc_exclude_filed_list = []

    # Check DDW_CNF_DIM if TB_C2
    sf_schema = "DDW_CNF_DIM" if table.startswith("TB_C2") else arg_dict["appl_name"]

    # Snowflake query — now includes DATETIME_PRECISION for timestamp/date/time types
    get_sf_flds_qry = (f'''SELECT COLUMN_NAME,
        CASE WHEN DATA_TYPE='TEXT' THEN 'VARCHAR' ELSE DATA_TYPE END DATA_TYPE,
        CASE WHEN DATA_TYPE='NUMBER' THEN NUMERIC_PRECISION || ',' || NUMERIC_SCALE
             WHEN DATA_TYPE IN ('TIMESTAMP_NTZ','TIMESTAMP_LTZ','TIMESTAMP_TZ','DATE','TIME')
               THEN TO_CHAR(DATETIME_PRECISION)
             ELSE TO_CHAR(CHARACTER_MAXIMUM_LENGTH) END AS DATA_LENGTH,
        IS_NULLABLE NULLABLE
        FROM CUR_IBS.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = \'{table}\' AND TABLE_SCHEMA = \'{sf_schema}\'
        ORDER BY COLUMN_NAME;''')

    if arg_dict["appl_name"][0:3] == 'ADS' or (arg_dict["appl_name"][0:3] == 'DDW' and table.endswith('ARD')):
        get_orc_flds_qry = (f'''SELECT COLUMN_NAME,
            CASE WHEN DATA_TYPE IN ('CHAR','VARCHAR2') THEN 'VARCHAR' ELSE DATA_TYPE END DATA_TYPE,
            CASE WHEN DATA_TYPE = 'NUMBER' THEN DATA_PRECISION || ',' || DATA_SCALE
                 WHEN DATA_TYPE = 'DATE' THEN NULL
                 WHEN DATA_TYPE LIKE 'TIMESTAMP%' THEN TO_CHAR(DATA_SCALE)
                 ELSE TO_CHAR(DATA_LENGTH) END DATA_LENGTH,
            CASE WHEN COLUMN_NAME='PRCS_YR_MTH_NBR' AND NULLABLE='Y' THEN 'N' ELSE NULLABLE END NULLABLE
            FROM ALL_TAB_COLUMNS
            WHERE TABLE_NAME = \'{table}\' AND OWNER = 'DWQQ2'
            ORDER BY COLUMN_NAME''')
    else:
        get_orc_flds_qry = (f'''SELECT COLUMN_NAME,
            CASE WHEN DATA_TYPE IN ('CHAR','VARCHAR2') THEN 'VARCHAR' ELSE DATA_TYPE END DATA_TYPE,
            CASE WHEN DATA_TYPE = 'NUMBER' THEN DATA_PRECISION || ',' || DATA_SCALE
                 WHEN DATA_TYPE = 'DATE' THEN NULL
                 WHEN DATA_TYPE LIKE 'TIMESTAMP%' THEN TO_CHAR(DATA_SCALE)
                 ELSE TO_CHAR(DATA_LENGTH) END DATA_LENGTH,
            CASE WHEN COLUMN_NAME='PRCS_YR_MTH_NBR' AND NULLABLE='Y' THEN 'N' ELSE NULLABLE END NULLABLE
            FROM ALL_TAB_COLUMNS
            WHERE TABLE_NAME = \'{table}\' AND OWNER = 'DWQQ1'
            ORDER BY COLUMN_NAME''')

    sf_filename = f"{file_path}/SF_CUR_DDL.csv"
    orc_filename = f"{file_path}/ORC_DDL.csv"
    sf_fld_res = None
    orc_fld_res = None

    try:
        sf_cursor.execute(get_sf_flds_qry)
        sf_fld_res = sf_cursor.fetchall()
    except Exception:
        logging.info("The following Snowflake query has failed.")
        logging.info(get_sf_flds_qry)

    try:
        ora_cursor.execute(get_orc_flds_qry)
        orc_fld_res = ora_cursor.fetchall()
    except Exception as e:
        logging.info(f"The following Oracle Query has failed. {e}")
        logging.info(get_orc_flds_qry)

    if len(orc_fld_res) == 0:
        syn_list = []
        if arg_dict["appl_name"][0:3] == 'ADS' or (arg_dict["appl_name"][0:3] == 'DDW' and table.endswith('ARD')):
            syn_query = (f'''SELECT TABLE_NAME FROM ALL_SYNONYMS WHERE SYNONYM_NAME = \'{table}\' AND TABLE_OWNER = 'DWQQ2' AND TABLE_NAME <> SYNONYM_NAME''')
        else:
            syn_query = (f'''SELECT TABLE_NAME FROM ALL_SYNONYMS WHERE SYNONYM_NAME = \'{table}\' AND TABLE_OWNER = 'DWQQ1' AND TABLE_NAME <> SYNONYM_NAME''')

        ora_cursor.execute(syn_query)
        syn_name = ora_cursor.fetchall()
        for row in syn_name:
            syn_list.append(list(row))

        try:
            if arg_dict["appl_name"][0:3] == 'ADS' or (arg_dict["appl_name"][0:3] == 'DDW' and table.endswith('ARD')):
                get_orc_flds_qry = (f'''SELECT COLUMN_NAME,
                    CASE WHEN DATA_TYPE IN ('CHAR','VARCHAR2') THEN 'VARCHAR' ELSE DATA_TYPE END DATA_TYPE,
                    CASE WHEN DATA_TYPE = 'NUMBER' THEN DATA_PRECISION || ',' || DATA_SCALE
                         WHEN DATA_TYPE = 'DATE' THEN NULL
                         WHEN DATA_TYPE LIKE 'TIMESTAMP%' THEN TO_CHAR(DATA_SCALE)
                         ELSE TO_CHAR(DATA_LENGTH) END DATA_LENGTH,
                    CASE WHEN COLUMN_NAME='PRCS_YR_MTH_NBR' AND NULLABLE='Y' THEN 'N' ELSE NULLABLE END NULLABLE
                    FROM ALL_TAB_COLUMNS
                    WHERE TABLE_NAME = \'{syn_list[0][0]}\' AND OWNER = 'DWQQ2'
                    ORDER BY COLUMN_NAME''')
            else:
                get_orc_flds_qry = (f'''SELECT COLUMN_NAME,
                    CASE WHEN DATA_TYPE IN ('CHAR','VARCHAR2') THEN 'VARCHAR' ELSE DATA_TYPE END DATA_TYPE,
                    CASE WHEN DATA_TYPE = 'NUMBER' THEN DATA_PRECISION || ',' || DATA_SCALE
                         WHEN DATA_TYPE = 'DATE' THEN NULL
                         WHEN DATA_TYPE LIKE 'TIMESTAMP%' THEN TO_CHAR(DATA_SCALE)
                         ELSE TO_CHAR(DATA_LENGTH) END DATA_LENGTH,
                    CASE WHEN COLUMN_NAME='PRCS_YR_MTH_NBR' AND NULLABLE='Y' THEN 'N' ELSE NULLABLE END NULLABLE
                    FROM ALL_TAB_COLUMNS
                    WHERE TABLE_NAME = \'{syn_list[0][0]}\' AND OWNER = 'DWQQ1'
                    ORDER BY COLUMN_NAME''')
        except IndexError:
            logging.info(f"The Oracle queries were not able to find any DDL information for the following table: "
                         f"{table}.\n")
            with open(sf_filename, "w") as f1:
                f1.close()
            with open(orc_filename, "w") as f2:
                f2.close()
            return {}

        try:
            ora_cursor.execute(get_orc_flds_qry)
            orc_fld_res = ora_cursor.fetchall()
        except Exception as e:
            logging.info(f"The following Oracle Query has failed {e}")
            logging.info(get_orc_flds_qry)

    prcs_ind = 'n'
    prcs_yr_ind = 'n'
    for row in orc_fld_res:
        if row[0] == 'PRCS_DTE':
            prcs_ind = 'y'
        if row[0] == 'PRCS_YR_MTH_NBR':
            prcs_yr_ind = 'y'

    # Ignore SRC_APPL_NAME column ddl for TB_C2 tables
    sf_exclude_filed_list = ["SRC_APPL_NAME"] if sf_schema == "DDW_CNF_DIM" else []

    if prcs_ind == 'n' and prcs_yr_ind == 'n':
        sf_exclude_filed_list += ["TENANT_ID", "LOAD_TS", "SOURCE_FILE", "PRCS_DTE", "PRCS_YR_MTH_NBR"]
    elif prcs_ind == 'n' and prcs_yr_ind != 'n':
        sf_exclude_filed_list += ["TENANT_ID", "LOAD_TS", "SOURCE_FILE", "PRCS_DTE"]
    elif prcs_ind != 'n' and prcs_yr_ind == 'n':
        sf_exclude_filed_list += ["TENANT_ID", "LOAD_TS", "SOURCE_FILE", "PRCS_YR_MTH_NBR"]
    else:
        sf_exclude_filed_list = ["TENANT_ID", "LOAD_TS", "SOURCE_FILE"]

    if sf_fld_res:
        with open(sf_filename, "w") as f:
            writer = csv.writer(f, lineterminator="\n")
            col_names = ["COLUMN_NAME", "DATA_TYPE", "DATA_LENGTH", "NULLABLE"]
            writer.writerow(col_names)
            for row in sf_fld_res:
                row_list = list(row)
                if row_list[0] not in sf_exclude_filed_list:
                    if row_list[3] == 'YES':
                        row_list[3] = 'Y'
                    elif row_list[3] == 'NO':
                        row_list[3] = 'N'
                    writer.writerow(row_list)

        with open(sf_all, "a") as f1:
            writer = csv.writer(f1, lineterminator="\n")
            col_names = ["COLUMN_NAME", "DATA_TYPE", "DATA_LENGTH", "NULLABLE", f"Table: {table}"]
            writer.writerow(col_names)
            for row in sf_fld_res:
                row_list = list(row)
                if row_list[0] not in sf_exclude_filed_list:
                    if row_list[3] == 'YES':
                        row_list[3] = 'Y'
                    elif row_list[3] == 'NO':
                        row_list[3] = 'N'
                    writer.writerow(row_list)

    if orc_fld_res:
        with open(orc_filename, "w") as f:
            writer = csv.writer(f, lineterminator="\n")
            col_names = ["COLUMN_NAME", "DATA_TYPE", "DATA_LENGTH", "NULLABLE"]
            writer.writerow(col_names)
            for row in orc_fld_res:
                row_list = list(row)
                if row_list[0] not in orc_exclude_filed_list:
                    # Regex-based Oracle timestamp normalization
                    row_list[1] = normalize_oracle_datatype(row_list[1])
                    writer.writerow(row_list)

        with open(orc_all, "a") as f1:
            writer = csv.writer(f1, lineterminator="\n")
            col_names = ["COLUMN_NAME", "DATA_TYPE", "DATA_LENGTH", "NULLABLE", f"Table: {table}"]
            writer.writerow(col_names)
            for row in orc_fld_res:
                row_list = list(row)
                if row_list[0] not in orc_exclude_filed_list:
                    # Regex-based Oracle timestamp normalization
                    row_list[1] = normalize_oracle_datatype(row_list[1])
                    writer.writerow(row_list)

    # Return Snowflake column types for reuse in date format validation
    sf_col_types = {}
    if sf_fld_res:
        for row in sf_fld_res:
            sf_col_types[row[0]] = row[1]
    return sf_col_types


def primary_key_compare(arg_dict, table, file_path, outfname, orc_cursor, sf_cursor):
    orc_pk_list, orcpk_cols = orc_pk_cols(tablename=table, arg_dict=arg_dict, orc_cursor=orc_cursor)
    sf_pk_list, sfpk_cols = sf_pk_cols(tablename=table, arg_dict=arg_dict, sf_cursor=sf_cursor)

    result = {'status': 'SUCCESS', 'details': [], 'ora_pk_count': len(orc_pk_list), 'sf_pk_count': len(sf_pk_list)}
    missing_pk = []

    longer_list = list(orc_pk_list) if len(orc_pk_list) > len(sf_pk_list) else list(sf_pk_list)
    shorter_list = list(orc_pk_list) if len(orc_pk_list) < len(sf_pk_list) else list(sf_pk_list)
    more_pks = "Oracle" if len(longer_list) == len(orc_pk_list) else "Snowflake"
    less_pks = "Snowflake" if more_pks == "Oracle" else "Oracle"

    for i in range(len(longer_list)):
        try:
            if shorter_list[i] == longer_list[i]:
                continue
            missing_pk.append(longer_list[i])
            shorter_list.insert(i, missing_pk[-1])
        except IndexError:
            missing_pk.append(longer_list[i])

    if missing_pk:
        result['status'] = 'FAIL'
        result['details'] = [{'missing_from': less_pks, 'missing_keys': missing_pk,
                              'ora_count': len(orc_pk_list), 'sf_count': len(sf_pk_list)}]

    if outfname:
        with open(file=outfname, mode='a') as f:
            if missing_pk:
                f.write(f"PK mismatch: Oracle={len(orc_pk_list)}, SF={len(sf_pk_list)}\n")
                f.write(f"Missing from {less_pks}: {missing_pk}\n\n")
            else:
                f.write("Primary Key Validation was successful.\n")
                f.write(f"Oracle PKs: {len(orc_pk_list)}, SF PKs: {len(sf_pk_list)}\n\n")

    return result


def data_structure_compare(arg_dict, table, file_path, outfname):
    sfname = f"{file_path}/SF_CUR_DDL.csv"
    ofname = f"{file_path}/ORC_DDL.csv"

    sf_pd = pd.read_csv(sfname, dtype='str', keep_default_na=False)
    orc_pd = pd.read_csv(ofname, dtype='str', keep_default_na=False)

    arr1 = orc_pd.to_numpy()
    arr2 = sf_pd.to_numpy()

    result = {'ora_count': len(arr1), 'sf_count': len(arr2), 'status': 'SUCCESS', 'details': []}

    try:
        unequal_records = []
        unequal_indices = np.where(arr1 != arr2)

        for row, col in zip(unequal_indices[0], unequal_indices[1]):
            ddl_name = arr1[row][0]
            field_dtype = arr1[row][1]

            if str(ddl_name).upper().startswith("DW_") and str(ddl_name).upper().endswith("_ID"):
                if col in (1, 2):
                    continue

            value1 = arr1[row][col]
            value2 = arr2[row][col]

            if col == 0:
                ddl_mismatch = "Column Name"
            elif col == 1:
                ddl_mismatch = "Data Type"
            elif col == 2:
                ddl_mismatch = "Precision/Scale or Data Length"
            elif col == 3:
                ddl_mismatch = "Nullable"
            else:
                ddl_mismatch = "Unknown"

            if pd.notna(value1) and pd.notna(value2):
                unequal_records.append({
                    'column': ddl_name,
                    'dtype': field_dtype,
                    'ddl_mis': ddl_mismatch,
                    'value_ora': value1,
                    'value_sf': value2
                })

        dw_id_len_mis = []
        for rw in arr2:
            if str(rw[0]).upper().startswith("DW_") and str(rw[0]).upper().endswith("_ID"):
                if rw[1] != "NUMBER" or rw[2] != "38,0":
                    dw_id_len_mis.append({
                        'column': rw[0],
                        'dtype': rw[1],
                        'dlength': rw[2],
                        'ddl_mis': 'Snowflake DW_ID Datatype/Length Incorrect'
                    })

        if unequal_records or dw_id_len_mis:
            result['status'] = 'FAIL'
            result['details'] = unequal_records + dw_id_len_mis
        else:
            logging.info(f"{table} DDL validation is successful")

        if outfname:
            with open(file=outfname, mode='a') as f:
                f.write(f"Number of fields read for the DDL of the Oracle table are: {len(arr1)}\n")
                f.write(f"Number of fields read for the DDL of the Snowflake table are: {len(arr2)}\n\n")
                if unequal_records:
                    f.write("There were mismatches found in the data structure of the fields listed below:\n")
                    for record in unequal_records:
                        f.write(f"Affected Field: {record['column']}\n")
                        f.write(f"Data Type of the field: {record['dtype']}\n")
                        f.write(f"DDL Portion that mismatched: {record['ddl_mis']}\n")
                        f.write(f"Value stored in Oracle DDL: {record['value_ora']}\n")
                        f.write(f"Value stored in Snowflake DDL: {record['value_sf']}\n\n")
                if dw_id_len_mis:
                    f.write(f"Snowflake DW_ID's Datatype or Length Mismatches found for fields listed below:\n")
                    for record in dw_id_len_mis:
                        f.write(f"Affected Field: {record['column']}\n")
                        f.write(f"Data Type of the field: {record['dtype']}\n")
                        f.write(f"Data Length of the field: {record['dlength']}\n")
                        f.write(f"DDL Portion that mismatched: {record['ddl_mis']}\n")
                        f.write(f"Expected Datatype and Length: NUMBER(38,0)\n")
                if not unequal_records and not dw_id_len_mis:
                    f.write(f"Successful DDL Validation\n")

    except ValueError as e:
        logging.info(f"Values for comparison are different between Oracle and SF. {e}")
        longer_array = arr1 if len(arr1) > len(arr2) else arr2
        shorter_array = arr1 if len(arr1) < len(arr2) else arr2
        longer_table = "Oracle" if len(longer_array) == len(arr1) else "Snowflake"
        shorter_table = "Snowflake" if longer_table == "Oracle" else "Oracle"

        mismatch_columns = []
        for i in range(longer_array.shape[0]):
            try:
                if shorter_array[i, 0] == longer_array[i, 0]:
                    continue
                mismatch_columns.append(longer_array[i, 0])
                shorter_array = np.insert(shorter_array, i, mismatch_columns[-1], axis=0)
            except IndexError:
                mismatch_columns.append(longer_array[i, 0])

        result['status'] = 'FAIL'
        result['details'] = [{'ddl_mis': f'Column count mismatch: {longer_table} has extra columns',
                              'column': str(mismatch_columns),
                              'value_ora': str(len(arr1)),
                              'value_sf': str(len(arr2)),
                              'dtype': 'N/A'}]

        if outfname:
            with open(file=outfname, mode='a') as f:
                f.write(f"Fields in {longer_table} not in {shorter_table}.\n")
                f.write(f"Oracle fields: {len(arr1)}, Snowflake fields: {len(arr2)}\n")
                if mismatch_columns:
                    f.write(f"Missing columns from {shorter_table}: {mismatch_columns}\n\n")

    except Exception as e:
        logging.info(f"Error occurred during Validation: {e}")
        result['status'] = 'ERROR'
        result['details'] = [{'ddl_mis': f'Error: {str(e)}', 'column': 'N/A',
                              'value_ora': '', 'value_sf': '', 'dtype': 'N/A'}]
        if outfname:
            with open(file=outfname, mode='a') as f:
                f.write(f"Error during DDL Validation: {e}\n")
                f.write(f"Oracle fields: {len(arr1)}, Snowflake fields: {len(arr2)}\n\n")

    return result


# ---------------------------------------------------------------------------
# Main validation function
# ---------------------------------------------------------------------------

def ddl_validation():
    script_start = time.perf_counter()
    argument_dict = arg_parsing()

    file_len_checker = None

    py_path = os.environ["PYTHONPATH"]
    ingest_yaml_file = f'{py_path}/IngestionConfig.yaml'
    ingest_cfg_dict = load_yaml(yaml_file=ingest_yaml_file)

    sf_conn = open_sf_connection(ingest_cfg_dict)
    cur = sf_conn.cursor()

    script_name = os.path.splitext(os.path.basename(__file__))[0]
    appl_code = get_appl_code(argument_dict['appl_name'], cur)
    logging_config(argument_dict['logging_directory'], appl_code, script_name, ingest_cfg_dict)

    logging.info("Logged into Snowflake")

    all_tables_from_db = get_tables_from_appl_table(cur, appl_code)

    # Exclude DTM/DAY_TME/DAY_TIME tables not suitable for DDL validation
    _DTM_EXCLUSIONS = ('DTM', 'DAY_TME', 'DAY_TIME')
    all_tables_from_db = [t for t in all_tables_from_db if not any(ex in t.upper() for ex in _DTM_EXCLUSIONS)]

    tb_c2_tables = [t for t in all_tables_from_db if t.startswith('TB_C2')]

    logging.info("Grabbing tables related to application code:")
    tables = [t for t in all_tables_from_db if not t.startswith('TB_C2')]

    # Combine TB_C2 tables and application tables for validation
    all_tables = tb_c2_tables + tables
    print(f"Tables considered for validation: {all_tables}")

    ora_user = f'DWQQ'
    ora_conn = open_oracle_connection(myuser=ora_user)
    ora_cur = ora_conn.cursor()

    logging.info('Logged into Oracle')

    file_path = f"{ingest_cfg_dict['snowflake_connection']['validation_path']}/{argument_dict['appl_name']}/{script_name}"

    if not os.path.exists(file_path):
        os.makedirs(file_path, exist_ok=True)

    logging.info("Validation starts")

    run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    outfname = f"{file_path}/DDL_VALIDATION_RESULT_{run_timestamp}.txt"
    running_sf_outfname = f"{file_path}/SF_ALL_DDL.csv"
    running_orc_outfname = f"{file_path}/ORC_ALL_DDL.csv"
    orc_filename = f"{file_path}/ORC_DDL.csv"
    sf_ddl_file = f"{file_path}/SF_CUR_DDL.csv"

    with open(running_sf_outfname, "w") as f:
        pass
    with open(running_orc_outfname, "w") as f1:
        pass

    # -------------------------------------------------------------------
    # Batch Snowflake metadata
    # -------------------------------------------------------------------
    logging.info("Fetching Snowflake metadata in bulk for performance...")
    sf_bulk_ddl = {}
    sf_bulk_pks = {}

    main_schema = argument_dict['appl_name']
    schemas_to_fetch = [main_schema]
    if tb_c2_tables:
        schemas_to_fetch.append('DDW_CNF_DIM')

    for schema in schemas_to_fetch:
        try:
            bulk_col_query = (
                f"SELECT TABLE_NAME, COLUMN_NAME, "
                f"CASE WHEN DATA_TYPE='TEXT' THEN 'VARCHAR' ELSE DATA_TYPE END AS DATA_TYPE, "
                f"CASE WHEN DATA_TYPE='NUMBER' THEN NUMERIC_PRECISION || ',' || NUMERIC_SCALE "
                f"     WHEN DATA_TYPE IN ('TIMESTAMP_NTZ','TIMESTAMP_LTZ','TIMESTAMP_TZ','DATE','TIME') "
                f"       THEN TO_CHAR(DATETIME_PRECISION) "
                f"     ELSE TO_CHAR(CHARACTER_MAXIMUM_LENGTH) END AS DATA_LENGTH, "
                f"IS_NULLABLE "
                f"FROM CUR_IBS.INFORMATION_SCHEMA.COLUMNS "
                f"WHERE TABLE_SCHEMA = '{schema}' "
                f"ORDER BY TABLE_NAME, COLUMN_NAME"
            )
            cur.execute(bulk_col_query)
            for row in cur.fetchall():
                tbl = row[0]
                if tbl not in sf_bulk_ddl:
                    sf_bulk_ddl[tbl] = []
                sf_bulk_ddl[tbl].append((row[1], row[2], row[3], row[4]))
        except Exception as e:
            logging.error(f"Bulk column fetch failed for schema {schema}: {e}")

    for schema in schemas_to_fetch:
        try:
            cur.execute(f"SHOW PRIMARY KEYS IN SCHEMA CUR_IBS.{schema}")
            pk_results = cur.fetchall()
            for row in pk_results:
                tbl = row[3]
                col = row[4]
                if col.upper() == 'TENANT_ID':
                    continue
                if tbl not in sf_bulk_pks:
                    sf_bulk_pks[tbl] = []
                sf_bulk_pks[tbl].append(col)
        except Exception as e:
            logging.error(f"Bulk PK fetch failed for schema {schema}: {e}")

    for tbl in sf_bulk_pks:
        sf_bulk_pks[tbl].sort()

    logging.info(f"Fetched metadata for {len(sf_bulk_ddl)} tables, PKs for {len(sf_bulk_pks)} tables.")

    # -------------------------------------------------------------------
    # Field level validation
    # -------------------------------------------------------------------
    sf_column_types_cache = {}
    table_results = []

    for table in all_tables:
        logging.info(f"Validating: {table}")

        result = {
            'table': table,
            'ddl_status': None,
            'ddl_details': [],
            'pk_status': None,
            'pk_details': [],
            'ora_field_count': 0,
            'sf_field_count': 0,
            'skipped': False,
            'skip_reason': '',
        }

        open(orc_filename, "w").close()
        open(sf_ddl_file, "w").close()

        sf_col_types = write_to_csv(ora_cursor=ora_cur, sf_cursor=cur, arg_dict=argument_dict,
                                    table=table, file_path=file_path,
                                    sf_all=running_sf_outfname, orc_all=running_orc_outfname)
        sf_column_types_cache[table] = sf_col_types

        with open(orc_filename, mode='r') as file_check:
            file_len_checker = len(file_check.readlines())

        if file_len_checker < 1:
            result['skipped'] = True
            result['skip_reason'] = 'No Oracle DDL found in OPAC architecture tables'
            result['ddl_status'] = 'SKIP'
            result['pk_status'] = 'SKIP'
            table_results.append(result)
            continue

        if not os.path.exists(sf_ddl_file) or os.path.getsize(sf_ddl_file) == 0:
            result['skipped'] = True
            result['skip_reason'] = 'No Snowflake DDL found (table may not exist in CUR_IBS schema)'
            result['ddl_status'] = 'SKIP'
            result['pk_status'] = 'SKIP'
            logging.warning(f"Skipping DDL comparison for {table} — SF_CUR_DDL.csv not found or empty.")
            table_results.append(result)
            continue

        logging.info(f"Comparing data structures for {table}.")
        ddl_result = data_structure_compare(arg_dict=argument_dict, table=table,
                                            file_path=file_path, outfname=None)
        result['ora_field_count'] = ddl_result.get('ora_count', 0)
        result['sf_field_count'] = ddl_result.get('sf_count', 0)
        result['ddl_status'] = ddl_result.get('status', 'ERROR')
        result['ddl_details'] = ddl_result.get('details', [])

        logging.info(f"Comparing primary keys for {table}.")
        pk_result = primary_key_compare(arg_dict=argument_dict, table=table, file_path=file_path,
                                        outfname=None, orc_cursor=ora_cur, sf_cursor=cur)
        result['pk_status'] = pk_result.get('status', 'ERROR')
        result['pk_details'] = pk_result.get('details', [])

        table_results.append(result)

    logging.info("All DDL Validation Ends.")

    # -------------------------------------------------------------------
    # Date Format (Timestamp) Validation
    # -------------------------------------------------------------------
    logging.info("Starting Date Format (Timestamp) validation...")
    date_format_outcomes = []

    env = os.environ["PRJ_ENVIRONMENT"]
    ccard_path = f"/mdw/{env}/tgt/scripts/cntlcards"

    ccards_dict = get_ccards_name(tables)
    timestamp_cols = find_timestamp_columns(ccards_dict, ccard_path)
    timestamp_dict = {k: v for k, v in timestamp_cols.items() if v}
    date_format_outcomes.extend(validate_date_format(timestamp_dict, argument_dict['appl_name'],
                                                     sf_column_types_cache, outfname=None))

    if tb_c2_tables:
        logging.info(f"Processing {len(tb_c2_tables)} TB_C2 tables for date format against DDW_CNF_DIM schema...")
        c2_ccards_dict = get_ccards_name(tb_c2_tables)
        c2_timestamp_cols = find_timestamp_columns(c2_ccards_dict, ccard_path)
        c2_timestamp_dict = {k: v for k, v in c2_timestamp_cols.items() if v}
        date_format_outcomes.extend(validate_date_format(c2_timestamp_dict, 'DDW_CNF_DIM',
                                                         sf_column_types_cache, outfname=None))

    logging.info(f"Date Format validation complete. {len(date_format_outcomes)} table(s) checked.")

    # -------------------------------------------------------------------
    # Build date format status map
    # -------------------------------------------------------------------
    date_fmt_map = {}
    for df in date_format_outcomes:
        date_fmt_map[df['table']] = df

    # -------------------------------------------------------------------
    # Write the complete structured report
    # -------------------------------------------------------------------
    script_run_time = time.perf_counter() - script_start
    W = 100

    with open(outfname, 'w') as f:
        f.write('=' * W + '\n')
        f.write(f'DDL VALIDATION REPORT: {argument_dict["appl_name"]}\n')
        f.write(f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n')
        f.write(f'Script: {SCRIPT_NAME} {SCRIPT_VERSION}\n')
        f.write(f'Execution Time: {script_run_time:.2f} seconds\n')
        f.write('=' * W + '\n\n')

        # ----- VALIDATION COVERAGE SUMMARY -----
        f.write('VALIDATION COVERAGE SUMMARY\n')
        f.write('-' * W + '\n')
        f.write('This script performs the following validations between Oracle (source) and\n')
        f.write('Snowflake (target) to ensure DDL parity after migration:\n\n')

        f.write('  1. DDL STRUCTURE COMPARISON (Oracle vs Snowflake)\n')
        f.write('     - Column Names: verifies all columns exist in both systems\n')
        f.write('     - Data Types: compares types with normalization:\n')
        f.write('         Oracle CHAR/VARCHAR2      -> VARCHAR\n')
        f.write('         Oracle TIMESTAMP(n)       -> TIMESTAMP_NTZ\n')
        f.write('         Oracle TIMESTAMP(n) WITH TIME ZONE        -> TIMESTAMP_TZ\n')
        f.write('         Oracle TIMESTAMP(n) WITH LOCAL TIME ZONE  -> TIMESTAMP_LTZ\n')
        f.write('         Snowflake TEXT            -> VARCHAR\n')
        f.write('     - Data Length / Precision / Scale:\n')
        f.write('         NUMBER columns: PRECISION,SCALE\n')
        f.write('         VARCHAR columns: CHARACTER_MAXIMUM_LENGTH\n')
        f.write('         TIMESTAMP/DATE/TIME columns: DATETIME_PRECISION (fractional seconds)\n')
        f.write('     - Nullable Constraints: Y/N comparison\n\n')

        f.write('  2. PRIMARY KEY VALIDATION\n')
        f.write('     - Verifies primary key columns match between Oracle and Snowflake\n')
        f.write('     - Checks for missing or extra primary key columns\n')
        f.write('     - Resolves synonyms in Oracle if table not found directly\n')
        f.write('     - TENANT_ID is excluded from SF PK comparison (audit field)\n\n')

        f.write('  3. DW_ID FIELD VALIDATION (Snowflake-specific)\n')
        f.write('     - Ensures all columns matching DW_*_ID pattern are NUMBER(38,0)\n')
        f.write('     - DW_ID data type/length is NOT compared against Oracle (by design)\n')
        f.write('     - Only the Snowflake side is validated for conformance\n\n')

        f.write('  4. DATE FORMAT (TIMESTAMP) VALIDATION\n')
        f.write('     - Reads Oracle control-card files (.pos/.pos1) to identify timestamp columns\n')
        f.write('       (columns whose definition contains "mi:ss" pattern)\n')
        f.write('     - Verifies those columns are TIMESTAMP_NTZ, TIMESTAMP_LTZ, or TIMESTAMP_TZ\n')
        f.write('       in Snowflake metadata\n')
        f.write('     - TB_C2 tables are validated against DDW_CNF_DIM schema\n\n')

        f.write('  5. EXCLUDED FIELDS (not compared)\n')
        f.write('     - Snowflake audit columns: TENANT_ID, LOAD_TS, SOURCE_FILE\n')
        f.write('     - PRCS_DTE and PRCS_YR_MTH_NBR excluded only when absent from Oracle\n')
        f.write('     - SRC_APPL_NAME excluded for TB_C2 (DDW_CNF_DIM) tables\n')
        f.write('     - DTM/DAY_TME/DAY_TIME tables excluded from validation scope\n\n')

        f.write('  6. SYNONYM RESOLUTION\n')
        f.write('     - If a table has no DDL in Oracle directly, synonyms are resolved\n')
        f.write('       and the underlying table is used for comparison\n\n')
        f.write('=' * W + '\n\n')

        # ----- SUMMARY TABLE -----
        f.write('SUMMARY TABLE:\n')
        f.write('-' * W + '\n')
        hdr = f"{'Table Name':<45} | {'DDL':<6} | {'PK':<6} | {'Date Fmt':<10} | {'Fields (O/S)':<14}"
        f.write(hdr + '\n')
        f.write('-' * W + '\n')

        success_count = 0
        fail_count = 0
        skip_count = 0

        for r in table_results:
            tbl = r['table']
            ddl_s = r['ddl_status'] or 'N/A'
            pk_s = r['pk_status'] or 'N/A'
            df_entry = date_fmt_map.get(tbl, {})
            df_s = df_entry.get('status', 'N/A') if df_entry else 'N/A'
            fields = f"{r['ora_field_count']}/{r['sf_field_count']}" if r['ora_field_count'] else '-'

            if r['skipped']:
                skip_count += 1
            elif ddl_s == 'FAIL' or pk_s == 'FAIL' or df_s == 'FAIL':
                fail_count += 1
            else:
                success_count += 1

            f.write(f"{tbl:<45} | {ddl_s:<6} | {pk_s:<6} | {df_s:<10} | {fields:<14}\n")

        f.write('-' * W + '\n')
        f.write(f"Total: {len(table_results)} | Success: {success_count} | Fail: {fail_count} | Skip: {skip_count}\n")
        f.write('=' * W + '\n\n')

        # ----- DETAILED FAILURES -----
        failures = [r for r in table_results if r['ddl_status'] == 'FAIL' or r['pk_status'] == 'FAIL']
        if failures or any(d.get('status') == 'FAIL' for d in date_format_outcomes):
            f.write('=' * W + '\n')
            f.write('DETAILED FAILURES\n')
            f.write('=' * W + '\n\n')

            for r in failures:
                f.write(f"---------------------------------------\n")
                f.write(f"Table: {r['table']}\n")
                f.write(f"Oracle Fields: {r['ora_field_count']} | Snowflake Fields: {r['sf_field_count']}\n\n")

                if r['ddl_status'] == 'FAIL' and r['ddl_details']:
                    f.write("[DDL Mismatches]\n")
                    for d in r['ddl_details']:
                        if 'dlength' in d:
                            f.write(f"  Field: {d['column']} | Type: {d['dtype']} | "
                                    f"Issue: {d['ddl_mis']} | Expected: NUMBER(38,0)\n")
                        else:
                            f.write(f"  Field: {d['column']} | Type: {d.get('dtype', 'N/A')} | "
                                    f"Issue: {d['ddl_mis']}\n")
                            if d.get('value_ora') or d.get('value_sf'):
                                f.write(f"    Oracle: {d.get('value_ora', '')} | "
                                        f"Snowflake: {d.get('value_sf', '')}\n")
                    f.write('\n')

                if r['pk_status'] == 'FAIL' and r['pk_details']:
                    f.write("[Primary Key Mismatch]\n")
                    for d in r['pk_details']:
                        f.write(f"  Oracle PKs: {d.get('ora_count', '?')} | SF PKs: {d.get('sf_count', '?')}\n")
                        f.write(f"  Missing from {d.get('missing_from', '?')}: {d.get('missing_keys', [])}\n")
                    f.write('\n')

            df_failures = [d for d in date_format_outcomes if d.get('status') == 'FAIL']
            if df_failures:
                f.write("---------------------------------------\n")
                f.write("[Date Format (Timestamp) Failures]\n\n")
                for d in df_failures:
                    f.write(f"  {d['table']}: {d.get('reason', 'Unknown')}\n")
                f.write('\n')

            f.write('=' * W + '\n\n')

        # ----- SKIPPED TABLES -----
        skipped = [r for r in table_results if r['skipped']]
        if skipped:
            f.write('SKIPPED TABLES:\n')
            f.write('-' * W + '\n')
            for r in skipped:
                f.write(f"  {r['table']}: {r['skip_reason']}\n")
            f.write('\n')

        f.write('=' * W + '\n')
        f.write('END OF REPORT\n')
        f.write('=' * W + '\n')

    print(f"\nValidation complete. Results saved to: {outfname}")

    # -------------------------------------------------------------------
    # Build table_outcomes for Snowflake metadata loading
    # -------------------------------------------------------------------
    table_outcomes = []
    for r in table_results:
        tbl = r['table']
        df_entry = date_fmt_map.get(tbl, {})
        df_status = df_entry.get('status', 'N/A')

        if r['skipped']:
            table_outcomes.append({'table': tbl, 'status': 'SUCCESS',
                                   'reason': f'Skipped — {r["skip_reason"]}'})
        elif r['ddl_status'] == 'FAIL' or r['pk_status'] == 'FAIL' or df_status == 'FAIL':
            reasons = []
            if r['ddl_status'] == 'FAIL':
                reasons.append('DDL mismatch')
            if r['pk_status'] == 'FAIL':
                reasons.append('PK mismatch')
            if df_status == 'FAIL':
                reasons.append(df_entry.get('reason', 'Date format mismatch'))
            table_outcomes.append({'table': tbl, 'status': 'FAIL', 'reason': '; '.join(reasons)})
        else:
            table_outcomes.append({'table': tbl, 'status': 'SUCCESS',
                                   'reason': 'DDL and PK validation successful'})

    if argument_dict.get('load_sf_meta'):
        try:
            _ac = infer_registry_app_category(argument_dict.get('appl_name'))
            registry = TestCaseRegistry(
                cur, SCRIPT_NAME,
                database=argument_dict.get('sf_meta_db'),
                schema=argument_dict.get('sf_meta_schema'),
                app_category=_ac,
            )
            val_results = []
            passed = sum(1 for o in table_outcomes if o['status'] == 'SUCCESS')
            failed = sum(1 for o in table_outcomes if o['status'] == 'FAIL')

            ddl_diff_info = read_diff_file(outfname) if os.path.isfile(outfname) else {}

            for outcome in table_outcomes:
                ai = ddl_diff_info.copy() if outcome['status'] == 'FAIL' and ddl_diff_info else None
                val_results.append(registry.create_result(
                    validation_key='schema_validation',
                    test_scenario=f'DDL validation: {outcome["table"]}',
                    appl_name=argument_dict['appl_name'],
                    appl_code=appl_code,
                    tenant_id='ALL',
                    table_name=outcome['table'],
                    validation_status=outcome['status'],
                    status_reason=outcome['reason'],
                    mismatched_count=1 if outcome['status'] == 'FAIL' else 0,
                    matched_count=1 if outcome['status'] == 'SUCCESS' else 0,
                    additional_info=ai
                ))

            if not val_results:
                val_results.append(registry.create_result(
                    validation_key='schema_validation',
                    test_scenario='Oracle vs Snowflake DDL structure validation',
                    appl_name=argument_dict['appl_name'],
                    appl_code=appl_code,
                    tenant_id='ALL',
                    table_name='ALL_TABLES',
                    validation_status='SUCCESS',
                    status_reason='No tables processed'
                ))

            loader = ValidationLoader(
                sf_cursor=cur,
                arg_dict=argument_dict,
                script_name=SCRIPT_NAME,
                script_version=SCRIPT_VERSION,
                database=argument_dict.get('sf_meta_db'),
                schema=argument_dict.get('sf_meta_schema')
            )

            summary = ExecutionSummary(
                script_name=SCRIPT_NAME,
                appl_name=argument_dict.get('appl_name', ''),
                appl_code=appl_code,
                tenant_id='ALL',
                process_date=argument_dict.get('process_date', ''),
                script_version=SCRIPT_VERSION
            )
            summary.started_at = datetime.fromtimestamp(time.time() - (time.perf_counter() - script_start))
            summary.parameters_used = {k: str(v) for k, v in argument_dict.items() if k not in ('sf_cursor',)}
            summary.update_counts(val_results)
            summary.execution_time_sec = time.perf_counter() - script_start

            if os.path.isfile(outfname) and os.path.getsize(outfname) > 0:
                summary.read_and_store_output(outfname, file_type='ddl_validation_report')

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
                            source_data={},
                            target_data={},
                            detail_remarks=cap_details((outcome.get('reason', '') or '')[:2000], 2000)[0]
                        ))

            if detail_batch:
                capped_batch, _, _ = cap_details(detail_batch)
                loader.insert_detail_bulk(capped_batch)

            summary.emit_summary_line()
            logging.info(f"Loaded {len(val_results)} DDL result(s) — {passed} SUCCESS, {failed} FAIL")

        except Exception as e:
            logging.error(f"Failed to load validation results to Snowflake: {str(e)}")
            traceback.print_exc()
    else:
        logging.info("Skipping metadata load to Snowflake (--load-sf-meta not specified)")

    # Closing DB connections
    ora_cur.close()
    ora_conn.close()
    cur.close()
    sf_conn.close()

    script_end = time.perf_counter()
    script_run_time = script_end - script_start
    logging.info(f"\n{'-' * 50}")
    logging.info(f'Script run time: {script_run_time} seconds')


if __name__ == '__main__':
    ddl_validation()