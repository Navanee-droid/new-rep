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


# This script does a validation of table structure, or DDL, of what is currently in Oracle versus what is in Snowflake
# It also validates Date Format (Timestamp) columns by checking Oracle control-card files against Snowflake metadata.
# The arguments that this script takes are:
#   --a Application Name as per Snowflake schema
#   --o Output Directory (Where the log of run will output to)
# An example call to this script would look like: python -m DDLValidation --a DDW_LN --t 6A --l INFO --o /mdw/dvl/files9/logs/l6
# Sources used as a basis for this script was the FieldValidation.py script authored by Agalya Karikalan on 08/08/2024
# Script written by Nathan Gupton with help of Jyothi Aleti and Richard Pearse by date 01/14/2025
# Script modified to handle DW_ID Changes to NUMBER(38,0) (use label 'DW_ID Change' to navigate to changes)
# Updated: 02/11/2026 Author:Barath Lakshman A N
# Enhanced : Parameters SCD check, Incomplete Cycle Check; Snowflake metadata load
# Updated: 06/09/2026 Merged DateFormatCheckDDW.py (Author: Charandeep) into this script

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
SCRIPT_VERSION = "v3.0"


# Description: This function creates an instance of an ArgumentParser object from the argparse
#       library and adds the identifier and description of each parameter passed with the function. These parameters
#       get returned as a dictionary which gets returned.
# Variables:
# - args_dict: returned dictionary of parameters that are passed when calling the function
# - parser: instance of the ArgumentParser object
# What gets returned: A dictionary containing the information that is passed to the script at run-time.
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
    Uses cached SF column types from DDL phase. Writes results to the main output file.
    Returns list of outcome dicts per table."""
    valid_timestamp_types = {'timestamp_ntz', 'timestamp_ltz', 'timestamp_tz'}
    outcomes = []
    with open(outfname, 'a') as file:
        if timestamp_cols:
            for table, cols in timestamp_cols.items():
                if cols:
                    # Look up data types from cache
                    cached_types = sf_column_types_cache.get(table, {})
                    data_types = {col: cached_types.get(col, 'UNKNOWN') for col in cols if col in cached_types}
                    if not data_types:
                        file.write(f"  {table}: SUCCESS (timestamp columns not found in Snowflake metadata)\n")
                        outcomes.append({'table': table, 'status': 'SUCCESS', 'reason': 'Timestamp columns not in SF metadata'})
                        continue
                    all_timestamp = all(
                        any(data_type.lower() == valid_type for valid_type in valid_timestamp_types)
                        for data_type in data_types.values()
                    )
                    if all_timestamp:
                        col_list = ", ".join(cols)
                        file.write(f"  {table}: SUCCESS (all timestamp columns match)\n")
                        file.write(f"    Timestamp columns: {col_list}\n")
                        outcomes.append({'table': table, 'status': 'SUCCESS', 'reason': 'Date formats match'})
                    else:
                        mismatched = []
                        for col, data_type in data_types.items():
                            if data_type.lower() not in valid_timestamp_types:
                                mismatched.append(f"{col} (SF has: {data_type})")
                        file.write(f"  {table}: FAILED\n")
                        for m in mismatched:
                            file.write(f"    - Oracle has timestamp, but {m}\n")
                        outcomes.append({'table': table, 'status': 'FAIL',
                                         'reason': f'Date format mismatch in {", ".join([c.split(" ")[0] for c in mismatched])}'})
                else:
                    file.write(f"  {table}: SUCCESS (no timestamp columns to check)\n")
                    outcomes.append({'table': table, 'status': 'SUCCESS', 'reason': 'No timestamp columns'})
        else:
            file.write(f"  No timestamp columns found in Oracle control cards for schema {schema_name}.\n")
    logging.info(f"[DateFormat] Validation for date format completed for schema {schema_name}.")
    return outcomes


# ---------------------------------------------------------------------------
# DDL Validation Functions
# ---------------------------------------------------------------------------

# Description: This function will take a tablename, the dictionary of parameters, and an Oracle connection and try to
#       find the primary keys of that Oracle Table. They will then get returned in a list and a string
# Input Variables:
# - tablename: This is the name of the table that is being called on the iteration
# - arg_dict: This is the dictionary of the arguments passed at script run-time
# - orc_cursor: This is the cursor that will allow Snowflake to be queried through the Snowflake connector
# Variables:
# - pk_cols: a string of the primary key column names
# - pk_list: a list of the primary key column names
# - pk_query: the sql query to grab the primary key columns of the Oracle table
# - pk_result: stores the result of the Oracle SQL query
# What gets returned: A tuple of the list of Oracle primary key columns as well as a
#       string of the Oracle primary key columns
def orc_pk_cols(tablename, arg_dict, orc_cursor) -> tuple[list, str]:
    pk_cols = ""
    pk_list = []
    if arg_dict["appl_name"][0:3] == 'ADS':
        pk_query = f'''select b.column_name from all_constraints a join all_cons_columns b on a.owner=\'DWQQ2\'
        and a.table_name = \'{tablename}\'and a.constraint_name = b.constraint_name and a.constraint_type = \'P\' and a.owner=b.owner 
        and a.table_name = b.table_name order by b.column_name'''
    else:
        pk_query = f'''select b.column_name from all_constraints a join all_cons_columns b on a.owner=\'DWQQ1\'
        and a.table_name = \'{tablename}\'and a.constraint_name = b.constraint_name and a.constraint_type = \'P\' and a.owner=b.owner 
        and a.table_name = b.table_name order by b.column_name'''
    orc_cursor.execute(pk_query)
    pk_result = orc_cursor.fetchall()
    if len(pk_result) != 0:
        for pk in pk_result:
            pk_cols += f"{pk[0]},"
            pk_list.append(pk[0])
        pk_cols = pk_cols[:-1]
    elif len(pk_result) == 0:
        syn_list = []
        if arg_dict["appl_name"][0:3] == 'ADS':
            syn_query = (f'''SELECT TABLE_NAME FROM ALL_SYNONYMS WHERE SYNONYM_NAME = \'{tablename}\' 
            AND TABLE_OWNER = \'DWQQ2\' AND TABLE_NAME <> SYNONYM_NAME''')
        else:
            syn_query = (f'''SELECT TABLE_NAME FROM ALL_SYNONYMS WHERE SYNONYM_NAME = \'{tablename}\' 
            AND TABLE_OWNER = \'DWQQ1\' AND TABLE_NAME <> SYNONYM_NAME''')
        orc_cursor.execute(syn_query)
        syn_name = orc_cursor.fetchall()
        if len(syn_name) == 0:
            logging.info("This table has no primary keys to be compared.")
        else:
            for row in syn_name:
                syn_list.append(list(row))
            if arg_dict["appl_name"][0:3] == 'ADS':
                pk_query = (f'''select b.column_name from all_constraints a join all_cons_columns b on a.owner=\'DWQQ2\'
                and a.table_name = \'{syn_list[0][0]}\'and a.constraint_name = b.constraint_name and a.constraint_type = \'P\' 
                and a.owner=b.owner and a.table_name = b.table_name order by b.column_name''')
            else:
                pk_query = (f'''select b.column_name from all_constraints a join all_cons_columns b on a.owner=\'DWQQ1\'
                and a.table_name = \'{syn_list[0][0]}\'and a.constraint_name = b.constraint_name and a.constraint_type = \'P\' 
                and a.owner=b.owner and a.table_name = b.table_name order by b.column_name''')

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


# Description: This function will take a table name, the dictionary of parameters, and a Snowflake connection
#          and try to find the primary keys of that snowflake table. They will then get returned in a list and a string
#          of the column names.
# Input Variables:
# - tablename: This is the name of the table that is being called on the iteration
# - arg_dict: This is the dictionary of the arguments passed at script run-time
# - sf_cursor: This is the cursor that will allow Snowflake to be queried through the Snowflake connector
# Variables Used:
# - pk_cols: a string of the primary key column names -> This gets returned
# - pk_list: a list of the primary key column names -> This gets returned
# - pk_query: the sql query to grab the primary key columns of the Oracle table
# - pk_result: stores the result of the Oracle SQL query
# What gets returned: A tuple of the list of Snowflake primary key columns as well as a
#       string of the Snowflake primary key columns
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


# Description: This function will take an Oracle connection, a Snowflake connection, the dictionary of
#       parameters, the target table, and a filepath to store a csv of the table DDLs. The function does
#       does not return anything but it does create two csv files; one including the Oracle DDL and the
#       other including the SF DDL.
# Input Variables:
# - ora_cursor: This is the cursor that will allow Oracle to be queried through the Oracle connector
# - sf_cursor: This is the cursor that will allow Snowflake to be queried through the Snowflake connector
# - arg_dict: This is the dictionary of the arguments passed at script run-time
# - table: This is the name of the table that is being called on the iteration
# - file_path: This is the file path in the UNIX environment that will define where the csv files tracking the
#       current table run are stored
# - sf_all: This is the running csv file that stores the Snowflake DDLs of each table ran
# - orc_all: This is the running csv file that stores the Oracle DDLs of each table ran
# Variables:
# - sf_exclude_filed_list: This is a list of fields that will be included in Snowflake tables but not the
#       corresponding Oracle table; these fields will not be compared for DDL validation.
# - get_sf_flds_qry & get_orc_flds_qry: These are both SQL queries that will pull the DDL information for target tables
# - sf_filename, orc_filenames: respective filenames for the ddl csv output
# - sf_fld_res, orc_fld_res: These are both variables that will store the output of the SQL queries
# What gets returned: A dict of {COLUMN_NAME: DATA_TYPE} for the Snowflake table columns.
def write_to_csv(ora_cursor, sf_cursor, arg_dict, table, file_path, sf_all, orc_all):
    # excluded fields
    sf_exclude_filed_list = ["TENANT_ID", "LOAD_TS", "SOURCE_FILE", "SRC_APPL_NAME"]
    orc_exclude_filed_list = []

    # Check DDW_CNF_DIM if TB_C2 [02/11/2026]
    sf_schema = "DDW_CNF_DIM" if table.startswith("TB_C2") else arg_dict["appl_name"]

    # This query is going to pull the column names, data types, as well as the precision and scale of numeric fields from a specified snowflake table
    get_sf_flds_qry = (f'''SELECT COLUMN_NAME,
    CASE WHEN DATA_TYPE=\'TEXT\' THEN \'VARCHAR\' ELSE DATA_TYPE END DATA_TYPE,
    CASE WHEN DATA_TYPE=\'NUMBER\' THEN NUMERIC_PRECISION || \',\' || NUMERIC_SCALE ELSE TO_CHAR(CHARACTER_MAXIMUM_LENGTH) END AS DATA_LENGTH, IS_NULLABLE NULLABLE
    FROM CUR_IBS.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = \'{table}\' AND TABLE_SCHEMA = \'{sf_schema}\'
    ORDER BY COLUMN_NAME;''')
    if arg_dict["appl_name"][0:3] == 'ADS':
        get_orc_flds_qry = (f'''SELECT COLUMN_NAME,
        CASE WHEN DATA_TYPE IN (\'CHAR\',\'VARCHAR2\') THEN \'VARCHAR\' ELSE DATA_TYPE END DATA_TYPE,
        CASE WHEN DATA_TYPE = \'NUMBER\' THEN DATA_PRECISION || \',\' || DATA_SCALE WHEN DATA_TYPE = \'DATE\' THEN NULL ELSE TO_CHAR(DATA_LENGTH) END DATA_LENGTH,
        CASE WHEN COLUMN_NAME=\'PRCS_YR_MTH_NBR\' AND NULLABLE=\'Y\' THEN \'N\' ELSE NULLABLE END NULLABLE
        FROM ALL_TAB_COLUMNS
        WHERE TABLE_NAME = \'{table}\' AND OWNER = \'DWQQ2\'
        ORDER BY COLUMN_NAME''')
    else:
        get_orc_flds_qry = (f'''SELECT COLUMN_NAME,
        CASE WHEN DATA_TYPE IN (\'CHAR\',\'VARCHAR2\') THEN \'VARCHAR\' ELSE DATA_TYPE END DATA_TYPE,
        CASE WHEN DATA_TYPE = \'NUMBER\' THEN DATA_PRECISION || \',\' || DATA_SCALE WHEN DATA_TYPE = \'DATE\' THEN NULL ELSE TO_CHAR(DATA_LENGTH) END DATA_LENGTH,
        CASE WHEN COLUMN_NAME=\'PRCS_YR_MTH_NBR\' AND NULLABLE=\'Y\' THEN \'N\' ELSE NULLABLE END NULLABLE
        FROM ALL_TAB_COLUMNS
        WHERE TABLE_NAME = \'{table}\' AND OWNER = \'DWQQ1\'
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
        if arg_dict["appl_name"][0:3] == 'ADS':
            syn_query = (f'''SELECT TABLE_NAME FROM ALL_SYNONYMS WHERE SYNONYM_NAME = \'{table}\' 
            AND TABLE_OWNER = \'DWQQ2\' AND TABLE_NAME <> SYNONYM_NAME''')
        else:
            syn_query = (f'''SELECT TABLE_NAME FROM ALL_SYNONYMS WHERE SYNONYM_NAME = \'{table}\' 
            AND TABLE_OWNER = \'DWQQ1\' AND TABLE_NAME <> SYNONYM_NAME''')
        ora_cursor.execute(syn_query)
        syn_name = ora_cursor.fetchall()
        for row in syn_name:
            syn_list.append(list(row))
        try:
            if arg_dict["appl_name"][0:3] == 'ADS':
                get_orc_flds_qry = (f'''SELECT COLUMN_NAME,
                CASE WHEN DATA_TYPE IN (\'CHAR\',\'VARCHAR2\') THEN \'VARCHAR\' ELSE DATA_TYPE END DATA_TYPE,
                CASE WHEN DATA_TYPE = \'NUMBER\' THEN DATA_PRECISION || \',\' || DATA_SCALE WHEN DATA_TYPE = \'DATE\' THEN NULL ELSE TO_CHAR(DATA_LENGTH) END DATA_LENGTH,
                CASE WHEN COLUMN_NAME=\'PRCS_YR_MTH_NBR\' AND NULLABLE=\'Y\' THEN \'N\' ELSE NULLABLE END NULLABLE
                FROM ALL_TAB_COLUMNS
                WHERE TABLE_NAME = \'{syn_list[0][0]}\' AND OWNER = \'DWQQ2\'
                ORDER BY COLUMN_NAME''')
            else:
                get_orc_flds_qry = (f'''SELECT COLUMN_NAME,
                CASE WHEN DATA_TYPE IN (\'CHAR\',\'VARCHAR2\') THEN \'VARCHAR\' ELSE DATA_TYPE END DATA_TYPE,
                CASE WHEN DATA_TYPE = \'NUMBER\' THEN DATA_PRECISION || \',\' || DATA_SCALE WHEN DATA_TYPE = \'DATE\' THEN NULL ELSE TO_CHAR(DATA_LENGTH) END DATA_LENGTH,
                CASE WHEN COLUMN_NAME=\'PRCS_YR_MTH_NBR\' AND NULLABLE=\'Y\' THEN \'N\' ELSE NULLABLE END NULLABLE
                FROM ALL_TAB_COLUMNS
                WHERE TABLE_NAME = \'{syn_list[0][0]}\' AND OWNER = \'DWQQ1\'
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


    # Ignore SRC_APPL_NAME column ddl for TB_C2 tables [02/11/2026]
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
        with (open(sf_filename, "w") as f):
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
        f.close()
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
        f1.close()

    if orc_fld_res:
        with open(orc_filename, "w") as f:
            writer = csv.writer(f, lineterminator="\n")
            col_names = ["COLUMN_NAME", "DATA_TYPE", "DATA_LENGTH", "NULLABLE"]
            writer.writerow(col_names)
            for row in orc_fld_res:
                row_list = list(row)
                if row_list[0] not in orc_exclude_filed_list:
                    if row_list[1] in ['TIMESTAMP(0)', 'TIMESTAMP(3)', 'TIMESTAMP(6)', 'TIMESTAMP(9)']:
                        row_list[1] = 'TIMESTAMP_NTZ'
                    elif row_list[1] in ['TIMESTAMP(1) WITH TIME ZONE', 'TIMESTAMP(3) WITH TIME ZONE',
                                         'TIMESTAMP(6) WITH TIME ZONE', 'TIMESTAMP(9) WITH TIME ZONE']:
                        row_list[1] = 'TIMESTAMP_TZ'
                    writer.writerow(row_list)
        f.close()
        with open(orc_all, "a") as f1:
            writer = csv.writer(f1, lineterminator="\n")
            col_names = ["COLUMN_NAME", "DATA_TYPE", "DATA_LENGTH", "NULLABLE", f"Table: {table}"]
            writer.writerow(col_names)
            for row in orc_fld_res:
                row_list = list(row)
                if row_list[0] not in orc_exclude_filed_list:
                    if row_list[1] in ['TIMESTAMP(0)', 'TIMESTAMP(3)', 'TIMESTAMP(6)', 'TIMESTAMP(9)']:
                        row_list[1] = 'TIMESTAMP_NTZ'
                    elif row_list[1] in ['TIMESTAMP(1) WITH TIME ZONE', 'TIMESTAMP(3) WITH TIME ZONE',
                                         'TIMESTAMP(6) WITH TIME ZONE', 'TIMESTAMP(9) WITH TIME ZONE']:
                        row_list[1] = 'TIMESTAMP_TZ'
                    writer.writerow(row_list)
        f1.close()

    # Return Snowflake column types for reuse in date format validation
    sf_col_types = {}
    if sf_fld_res:
        for row in sf_fld_res:
            sf_col_types[row[0]] = row[1]
    return sf_col_types


# Description: This function will compare the primary Keys of the Snowflake table and the Oracle
#       table to make sure they have the same primary keys; if not, it will write a csv file with the differences
# Input Variables:
# - arg_dict: This is the dictionary of the arguments passed at script run-time
# - table: This is the name of the table that is being called on the iteration
# - outfname: This is the name of the primary key difference output file.
# - orc_cursor: This is the cursor that will allow Oracle to be queried through the Oracle connector
# - sf_cursor: This is the cursor that will allow Snowflake to be queried through the Snowflake connector
# Variables used:
# - orc_pk_list, orcpk_cols: stores for the Oracle primary key column names
# - sf_pk_list, sfpk_cols: stores for the Snowflake Primary key column names
# - unequal_pk: This is a list that will store the primary keys of oracle and snowflake that do not match
# What gets returned: Nothing gets returned by this function
def primary_key_compare(arg_dict, table, file_path, outfname, orc_cursor, sf_cursor):
    orc_pk_list, orcpk_cols = orc_pk_cols(tablename=table, arg_dict=arg_dict, orc_cursor=orc_cursor)
    sf_pk_list, sfpk_cols = sf_pk_cols(tablename=table, arg_dict=arg_dict, sf_cursor=sf_cursor)

    missing_pk = []

    longer_list = [i for i in orc_pk_list] if len(orc_pk_list) > len(sf_pk_list) else [i for i in sf_pk_list]
    shorter_list = [i for i in orc_pk_list] if len(orc_pk_list) < len(sf_pk_list) else [i for i in sf_pk_list]

    if len(longer_list) == len(orc_pk_list):
        more_pks = "Oracle"
        less_pks = "Snowflake"
    else:
        more_pks = "Snowflake"
        less_pks = "Oracle"

    for i in range(len(longer_list)):
        try:
            if shorter_list[i] == longer_list[i]:
                logging.info("Primary Key Matched")
                continue
            logging.info("Primary Key Mismatch.")
            missing_pk.append(longer_list[i])
            shorter_list.insert(i, missing_pk[-1])
        except IndexError as e:
            logging.info("The comparison lists are different lengths. "
                         "Please check DDL_VALIDATION_RESULT.txt for missing Primary Keys.")
            missing_pk.append(longer_list[i])

    with open(file=outfname, mode='a') as f:
        if missing_pk:
            f.write(f"There was a difference in primary keys between the Oracle table and the Snowflake table.\n")
            f.write(f"The number of Oracle primary keys returned are: {len(orc_pk_list)}\n")
            f.write(f"The number of Snowflake primary keys returned are: {len(sf_pk_list)}\n")
            f.write(f"Please look at information below: \n")
            f.write(f"The primary keys not marked as primary keys from the {less_pks} table are: {missing_pk}\n\n")
        else:
            f.write("Primary Key Validation was successful.\n")
            f.write(f"The number of Primary Keys received from Oracle are: {len(orc_pk_list)}\n")
            f.write(f"The number of primary keys received from Snowflake are: {len(sf_pk_list)}\n\n")
    f.close()


# Description: This function will compare the outputs of the DDL queries for Oracle and Snowflake
#       and then record any differences in a csv file.
# Input Variables:
# - arg_dict: This is the dictionary of the arguments passed at script run-time
# - table: This is the name of the table that is being called on the iteration
# - file_path: This is the file path in the UNIX environment that will define where the csv files tracking the
#       current table run are stored
# - outfname: This will be the name of the differentiating fields file that will get outputted
# Variables:
# - sf_pd, orc_pd: These are Pandas Library Dataframes of the DDL sql queries
# - arr1, arr2: These are numpy Library arrays of the Pandas library; This will allow for list comparison
#       using python comparison
# - unequal_records: this is a list that will store the unequal records that will get recorded in the final csv.
# - unequal_indicies: this is a list of row indecies and column indicies that will indicate the location of unequal
#      instances in the ddl queries
# What gets returned: Nothing gets returned by this function.
def data_structure_compare(arg_dict, table, file_path, outfname):
    sfname = f"{file_path}/SF_CUR_DDL.csv"
    ofname = f"{file_path}/ORC_DDL.csv"

    # Opening the csvs as a pandas dataframe
    sf_pd = pd.read_csv(sfname, dtype='str', keep_default_na=False)
    orc_pd = pd.read_csv(ofname, dtype='str', keep_default_na=False)
    # Converting the pandas dataframes to a numpy array
    arr1 = orc_pd.to_numpy()
    arr2 = sf_pd.to_numpy()

    try:
        # Making an array for unequal records and placing them using the numpy where
        is_dw_id = False
        unequal_records = []
        unequal_indices = np.where(arr1 != arr2)
        # iterating over a tuple of the (row, column) for each entry pairing in the arrays
        for row, col in zip(unequal_indices[0], unequal_indices[1]):
            # column names are equal to the columns of the oracle pandas dataframe
            ddl_name = arr1[row][0]
            field_dtype = arr1[row][1]
            # DW_ID Change: Checks whether column is DW_ID and skip validation for Datatype(col=1) and Length(col=2) as it will be NUMBER(38,0) in snowflake
            if str(ddl_name).upper().startswith("DW_") and str(ddl_name).upper().endswith("_ID"):
                if col in (1,2): continue
            # Saving each of the values
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

            row_num = row + 1

            # if the values are not NA...
            if pd.notna(value1) and pd.notna(value2):
                # appending to the unequal records the row, column, and the oracle and snowflake values
                unequal_records.append({'row nbr': row_num,
                                        'dtype': field_dtype,
                                        'ddl_mis': ddl_mismatch,
                                        'column': ddl_name,
                                        'value_ora': value1,
                                        'value_sf': value2})
        # DW_ID Change: Iterating over a Snowflake array to validate Datatype and length to be NUMBER(38,0) for DW_ID columns and making array if mismatch
        dw_id_len_mis = []
        for rw in arr2:
            if str(rw[0]).upper().startswith("DW_") and str(rw[0]).upper().endswith("_ID"):
                if rw[1] != "NUMBER" or rw[2] != "38,0":
                    dw_id_len_mis.append({'column':rw[0],
                                          'dtype':rw[1],
                                          'dlength':rw[2],
                                          'ddl_mis':'Snowflake DW_ID Datatype/Length Incorrect'})
        with open(file=outfname, mode='a') as f:
            # Opening the differentiating record files for appending
            f.write(f"Number of fields read for the DDL of the Oracle table are: {len(arr1)}\n")
            f.write(f"Number of fields read for the DDL of the Snowflake table are: {len(arr2)}\n\n")
            if unequal_records:
                logging.info("DDL Mismatches found, please check DDL_VALIDATION_RESULT.txt")
                f.write("There were mismatches found in the data structure of the fields listed below:\n")
                for record in unequal_records:
                    # for each record in the unequal records, record the column and row and the respective oracle value and snowflake value
                    f.write(f"Affected Field: {record['column']}\n")
                    f.write(f"Data Type of the field: {record['dtype']}\n")
                    f.write(f"DDL Portion that mismatched: {record['ddl_mis']}\n")
                    f.write(f"Value stored in Oracle DDL: {record['value_ora']}\n")
                    f.write(f"Value stored in Snowflake DDL: {record['value_sf']}\n\n")
            # DW_ID Change: Writes to log and output if DW_ID datatype & length is not NUMBER(38,0)
            if dw_id_len_mis:
                logging.info("Snowflake DW_ID's Datatype or Length Mismatches found, please check DDL_VALIDATION_RESULT.txt")
                f.write(f"Snowflake DW_ID's Datatype or Length Mismatches found for fields listed below:\n")
                for record in dw_id_len_mis:
                    # for each record in incorrect DW_ID datatype/length, record the column and its current and expected datatype & length
                    f.write(f"Affected Field: {record['column']}\n")
                    f.write(f"Data Type of the field: {record['dtype']}\n")
                    f.write(f"Data Length of the field: {record['dlength']}\n")
                    f.write(f"DDL Portion that mismatched: {record['ddl_mis']}\n")
                    f.write(f"Expected Datatype and Length: NUMBER(38,0)\n")
            if not unequal_records and not dw_id_len_mis:
                # f.write(f"---------------------------------------\nTable: {table}\n")
                logging.info(f"{table} DDL validation is successful")
                f.write(f"Successful DDL Validation\n")

        f.close()
    except ValueError as e:
        logging.info(f"Values for comparison are different between Oracle and SF. {e}")

        longer_array = arr1 if len(arr1) > len(arr2) else arr2
        shorter_array = arr1 if len(arr1) < len(arr2) else arr2

        if len(longer_array) == len(arr1):
            longer_table = "Oracle"
            shorter_table = "Snowflake"
        else:
            longer_table = "SnowFlake"
            shorter_table = "Oracle"

        present_columns = []
        mismatch_columns = []
        extra_column = None

        for i in range(longer_array.shape[0]):
            try:
                if shorter_array[i, 0] == longer_array[i, 0]:
                    present_columns.append(shorter_array[i, 0])
                    continue
                mismatch_columns.append(longer_array[i, 0])
                shorter_array = np.insert(shorter_array, i, mismatch_columns[-1], axis=0)
            except IndexError:
                extra_column = longer_array[i, 0]

        with open(file=outfname, mode='a') as f:
            f.write(f"A ValueError occurred which means there are fields in the {longer_table} table that are not in "
                    f"the {shorter_table} table.\n")
            f.write(f"The number of fields read for the DDL of the Oracle table are: {len(arr1)}\n")
            f.write(f"The number of fields read for the DDL of the Snowflake table are: {len(arr2)}\n")
            if len(mismatch_columns) > 0:
                f.write(f"The columns that are missing from the {shorter_table} table are: {mismatch_columns}\n")
            f.write("After ensuring all columns are successfully in both environments, please re-run this script to "
                    "ensure Data Type structure is matching\n\n")
        f.close()
    except KeyError as k:
        logging.info("Count does not match between Oracle and SF")
        with open(file=outfname, mode='a') as f:
            f.write(f"There was a KeyError that occurred with DDL Validation: {k}\n")
            f.write(f"The number of fields read for the DDL of the Oracle table are: {len(arr1)}\n")
            f.write(f"The number of fields read for the DDL of the Snowflake table are: {len(arr2)}\n\n")
        f.close()

    except Exception as e:
        logging.info(f"Error occurred during Validation: {e}")
        with open(file=outfname, mode='a') as f:
            f.write(f"There was an Error that occurred with DDL Validation: {e}\n")
            f.write(f"The number of fields read for the DDL of the Oracle table are: {len(arr1)}\n")
            f.write(f"The number of fields read for the DDL of the Snowflake table are: {len(arr2)}\n\n")
        f.close()

#Writes header information to the output file, including info on validation types and tables being validated. [02/11/2026]
def write_validation_header(outfname, appl_name, tables, tb_c2_tables):
    """Writes header section to DDL validation output file with validation info."""
    from datetime import datetime
    
    with open(file=outfname, mode='w') as f:
        f.write('='*100 + '\n')
        f.write(f'DDL VALIDATION REPORT: {appl_name}\n')
        f.write(f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n')
        f.write('='*100 + '\n\n')
        
        f.write('VALIDATION TYPES PERFORMED:\n')
        f.write('-'*100 + '\n')
        f.write('1. DDL Structure Comparison (Oracle vs Snowflake):\n')
        f.write('   - Column Names\n')
        f.write('   - Data Types (VARCHAR, NUMBER, DATE, TIMESTAMP, etc.)\n')
        f.write('   - Data Length/Precision/Scale\n')
        f.write('   - Nullable Constraints\n')
        f.write('\n')
        f.write('2. Primary Key Validation:\n')
        f.write('   - Verifies that primary keys match between Oracle and Snowflake\n')
        f.write('   - Checks for missing or extra primary key columns\n')
        f.write('\n')
        f.write('3. DW_ID Field Validation (Snowflake-specific):\n')
        f.write('   - Ensures all columns starting with "DW_" and ending with "_ID" are NUMBER(38,0)\n')
        f.write('   - DW_ID columns are NOT compared against Oracle (by design)\n')
        f.write('\n')
        f.write('4. Audit Fields are excluded (Snowflake-only fields not compared with Oracle)\n')
        f.write('\n')
        f.write('5. Date Format (Timestamp) Validation:\n')
        f.write('   - Reads Oracle control-card files (.pos/.pos1) to find timestamp columns\n')
        f.write('   - Verifies those columns are TIMESTAMP_NTZ, TIMESTAMP_LTZ, or TIMESTAMP_TZ in Snowflake\n')
        f.write('   - TB_C2 tables validated against DDW_CNF_DIM schema\n')
        f.write('\n')
        f.write('='*100 + '\n\n')
        
        # Tables being validated
        f.write('TABLES BEING VALIDATED:\n')
        f.write('-'*100 + '\n')
        f.write(f'Total Tables: {len(tables)}\n')
        if tb_c2_tables:
            f.write(f'  - TB_C2 Tables (from DDW_CNF_DIM schema): {len(tb_c2_tables)}\n')
            f.write(f'  - Application Tables (from {appl_name} schema): {len(tables) - len(tb_c2_tables)}\n')
        else:
            f.write(f'  - Application Tables (from {appl_name} schema): {len(tables)}\n')
        f.write('\n')
        f.write('='*100 + '\n\n')
        f.write('DETAILED VALIDATION RESULTS BY TABLE:\n')
        f.write('='*100 + '\n\n')
    f.close()

#Write summary of validation [02/11/2026]
def write_validation_summary(outfname, tables, script_run_time):
    """Writes summary section at the end of DDL validation output file.
    Returns (passed_tables, failed_tables, skipped_tables) lists."""
    from datetime import datetime
    
    # Read the output file to count passes and fails
    passed_tables = []
    failed_tables = []
    skipped_tables = []
    
    with open(file=outfname, mode='r') as f:
        content = f.read()
        
        for table in tables:
            table_section_start = f"Table: {table}\n"
            if table_section_start in content:
                # Find the section for this table
                start_idx = content.find(table_section_start)
                # Find the next table section or end of file
                next_table_idx = content.find("---------------------------------------\nTable:", start_idx + 1)
                if next_table_idx == -1:
                    table_content = content[start_idx:]
                else:
                    table_content = content[start_idx:next_table_idx]
                
                # Check for success/failure indicators
                if "NOTICE:::" in table_content and "no DDL information found" in table_content:
                    skipped_tables.append(table)
                elif "Successful DDL Validation" in table_content and "Primary Key Validation was successful" in table_content:
                    passed_tables.append(table)
                elif "mismatches found" in table_content.lower() or "difference in primary keys" in table_content.lower():
                    failed_tables.append(table)
                else:
                    # If we can't determine, check if there are any error indicators
                    if "successful" in table_content.lower():
                        passed_tables.append(table)
                    else:
                        failed_tables.append(table)
    
    # Write summary [02/11/2026]
    with open(file=outfname, mode='a') as f:
        f.write('\n\n')
        f.write('='*100 + '\n')
        f.write('VALIDATION SUMMARY\n')
        f.write('='*100 + '\n')
        f.write(f'Completed: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n')
        f.write(f'Total Execution Time: {script_run_time:.2f} seconds\n')
        f.write('\n')
        f.write('RESULTS OVERVIEW:\n')
        f.write('-'*100 + '\n')
        f.write(f'Total Tables Processed: {len(tables)}\n')
        f.write(f'  - PASSED: {len(passed_tables)}\n')
        f.write(f'  - FAILED: {len(failed_tables)}\n')
        f.write(f'  - SKIPPED (No Oracle DDL): {len(skipped_tables)}\n')
        f.write('\n')
        f.write('='*100 + '\n')
        f.write('END OF REPORT\n')
        f.write('='*100 + '\n')
    return passed_tables, failed_tables, skipped_tables

# Description: This is the main function for this file. This is where functions written in this script are called
#       to achieve the creation of the csv files as well as the log files and give a result of the script.
def ddl_validation():
    # Starting a timer to record script performance as well as reading arguments
    script_start = time.perf_counter()
    argument_dict = arg_parsing()

    # Declaring variable that will indicate if the file to store temporary DDL information is empty.
    file_len_checker = None

    # getting the ingestionconfig.yaml file from the python path
    py_path = os.environ["PYTHONPATH"]
    ingest_yaml_file = f'{py_path}/IngestionConfig.yaml'
    ingest_cfg_dict = load_yaml(yaml_file=ingest_yaml_file)

    # opening the snowflake connection
    sf_conn = open_sf_connection(ingest_cfg_dict)
    # getting a cursor for the sf connection
    cur = sf_conn.cursor()
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    appl_code = get_appl_code(argument_dict['appl_name'], cur)
    logging_config(argument_dict['logging_directory'], appl_code, script_name, ingest_cfg_dict)
    # logging an output
    logging.info("Logged into Snowflake")
    
    # collects all application tables (including TB_C2) from T_APPL_TABLE [02/11/2026]
    all_tables_from_db = get_tables_from_appl_table(cur, appl_code)

    # Exclude DTM/DAY_TME/DAY_TIME/ARD tables not suitable for DDL validation
    _DTM_EXCLUSIONS = ('DTM', 'DAY_TME', 'DAY_TIME')
    all_tables_from_db = [t for t in all_tables_from_db
                          if not any(ex in t.upper() for ex in _DTM_EXCLUSIONS)]

    tb_c2_tables = [t for t in all_tables_from_db if t.startswith('TB_C2')]

    # getting the tables for the application name passed as a parameter
    logging.info("Grabbing tables related to application code:")
    tables = [t for t in all_tables_from_db if not t.startswith('TB_C2')]
    
    # Combine TB_C2 tables and application tables for validation
    all_tables = tb_c2_tables + tables
    print(f"Tables considered for validation: {all_tables}")

    # getting the oracle Owner and password for that owner
    ora_user = f'DWQQ'
    ora_conn = open_oracle_connection(myuser=ora_user)
    ora_cur = ora_conn.cursor()

    # telling the log that we are logged into oracle
    logging.info('Logged into Oracle')

    # Identifying the file path for validation files
    file_path = f"{ingest_cfg_dict['snowflake_connection']['validation_path']}/{argument_dict['appl_name']}/{script_name}"
    
    # if the file path does not exist make it
    if not os.path.exists(file_path):
        os.makedirs(file_path, exist_ok=True)
    # Test 1: oracle to snowflake record counts
    logging.info("Validation starts")

    # Declaring filenames and removing Difference file if already exists
    outfname = f"{file_path}/DDL_VALIDATION_RESULT.txt"
    running_sf_outfname = f"{file_path}/SF_ALL_DDL.csv"
    running_orc_outfname = f"{file_path}/ORC_ALL_DDL.csv"
    orc_filename = f"{file_path}/ORC_DDL.csv"

    with open(running_sf_outfname, "w") as f:
        f.close()
    with open(running_orc_outfname, "w") as f1:
        f1.close()

    if os.path.exists(outfname):
        os.remove(outfname)
    
    # Write header section with validation info
    write_validation_header(outfname=outfname, appl_name=argument_dict['appl_name'], 
                           tables=all_tables, tb_c2_tables=tb_c2_tables)
    
    # Field level validation - write detailed results
    sf_column_types_cache = {}  # {table_name: {COLUMN_NAME: DATA_TYPE}}
    for table in all_tables:
        logging.info("Writing DDL Info to csv.")
        with open(file=outfname, mode='a') as f:
            f.write(f"---------------------------------------\nTable: {table}\n")
        f.close()

        sf_col_types = write_to_csv(ora_cursor=ora_cur, sf_cursor=cur, arg_dict=argument_dict, table=table,
                     file_path=file_path, sf_all=running_sf_outfname, orc_all=running_orc_outfname)
        sf_column_types_cache[table] = sf_col_types

        sf_ddl_file = f"{file_path}/SF_CUR_DDL.csv"
        with open(file=orc_filename, mode='r') as file_check:
            file_len_checker = len(file_check.readlines())
        file_check.close()
        if file_len_checker < 1:
            with open(file=outfname, mode='a') as update_output:
                update_output.write(f"NOTICE:::\n"
                                    f"There was no DDL information found in the OPAC architecture tables for this "
                                    f"table: {table}.\nIf this table ends in _TEMP it is possible that there is "
                                    f"a similarly named table under this SF schema that has the same underlying DDL.\n"
                                    f"If the DDL for that table matched correctly, it is likely that the _TEMP DDL is "
                                    f"correct. However, please double check this _TEMP table DDL matches the DDL of "
                                    f"the similarly named table on Snowflake.\n\n")
            update_output.close()
            continue
        if not os.path.exists(sf_ddl_file) or os.path.getsize(sf_ddl_file) == 0:
            with open(file=outfname, mode='a') as update_output:
                update_output.write(f"NOTICE:::\n"
                                    f"There was no DDL information found in Snowflake for this "
                                    f"table: {table}.\nThe table may not exist in the CUR_IBS schema, "
                                    f"or it may exist under a different name (e.g. VW_ prefix instead of TB_).\n"
                                    f"Please verify the table exists in Snowflake and is accessible.\n\n")
            update_output.close()
            logging.warning(f"Skipping DDL comparison for {table} — SF_CUR_DDL.csv not found or empty.")
            continue
        logging.info("Comparing data structures.")
        data_structure_compare(arg_dict=argument_dict, table=table, file_path=file_path, outfname=outfname)

        logging.info("Comparing the primary keys of the tables.")
        primary_key_compare(arg_dict=argument_dict, table=table, file_path=file_path, outfname=outfname,
                            orc_cursor=ora_cur, sf_cursor=cur)

    logging.info("All DDL Validation Ends. Non-Matching results in SFValidation/<app code> path.")

    # -----------------------------------------------------------------------
    # Date Format (Timestamp) Validation
    # -----------------------------------------------------------------------
    logging.info("Starting Date Format (Timestamp) validation...")
    date_format_outcomes = []
    env = os.environ["PRJ_ENVIRONMENT"]
    ccard_path = f"/mdw/{env}/tgt/scripts/cntlcards"

    # Write date format section header to the main output file
    with open(outfname, 'a') as f:
        f.write('\n')
        f.write('=' * 100 + '\n')
        f.write('DATE FORMAT (TIMESTAMP) VALIDATION\n')
        f.write('=' * 100 + '\n')
        f.write('Validates Oracle control-card timestamp columns exist as TIMESTAMP types in Snowflake.\n\n')
        f.write(f'Application Schema: {argument_dict["appl_name"]}\n')
        f.write('-' * 100 + '\n')

    # Regular tables (non-TB_C2)
    ccards_dict = get_ccards_name(tables)
    timestamp_cols = find_timestamp_columns(ccards_dict, ccard_path)
    timestamp_dict = {k: v for k, v in timestamp_cols.items() if v}
    date_format_outcomes.extend(validate_date_format(timestamp_dict, argument_dict['appl_name'], sf_column_types_cache, outfname))

    # TB_C2 tables validated against DDW_CNF_DIM schema
    if tb_c2_tables:
        logging.info(f"Processing {len(tb_c2_tables)} TB_C2 tables for date format against DDW_CNF_DIM schema...")
        with open(outfname, 'a') as f:
            f.write(f'\nTB_C2 Tables (validated against DDW_CNF_DIM schema):\n')
            f.write('-' * 100 + '\n')
        c2_ccards_dict = get_ccards_name(tb_c2_tables)
        c2_timestamp_cols = find_timestamp_columns(c2_ccards_dict, ccard_path)
        c2_timestamp_dict = {k: v for k, v in c2_timestamp_cols.items() if v}
        date_format_outcomes.extend(validate_date_format(c2_timestamp_dict, 'DDW_CNF_DIM', sf_column_types_cache, outfname))
    else:
        logging.info("No TB_C2 tables found for date format validation.")

    with open(outfname, 'a') as f:
        f.write('\n' + '=' * 100 + '\n')

    logging.info(f"Date Format validation complete. {len(date_format_outcomes)} table(s) checked.")

    # -----------------------------------------------------------------------
    # Write validation summary at the end and capture per-table outcomes
    passed_tables, failed_tables, skipped_tables = write_validation_summary(
        outfname=outfname, tables=all_tables, script_run_time=time.perf_counter() - script_start
    )
    table_outcomes = []
    for t in passed_tables:
        table_outcomes.append({'table': t, 'status': 'SUCCESS', 'reason': 'DDL and PK validation passed'})
    for t in failed_tables:
        table_outcomes.append({'table': t, 'status': 'FAIL', 'reason': 'DDL or PK mismatches found'})
    for t in skipped_tables:
        table_outcomes.append({'table': t, 'status': 'SUCCESS', 'reason': 'Skipped — no Oracle DDL found'})

    # Merge date format outcomes into table_outcomes (single entry per table)
    outcome_map = {o['table']: o for o in table_outcomes}
    for df_outcome in date_format_outcomes:
        tbl = df_outcome['table']
        if tbl in outcome_map:
            if df_outcome['status'] == 'FAIL':
                outcome_map[tbl]['status'] = 'FAIL'
                outcome_map[tbl]['reason'] += f'; {df_outcome["reason"]}'
            elif outcome_map[tbl]['status'] == 'SUCCESS' and 'Date formats match' in df_outcome.get('reason', ''):
                outcome_map[tbl]['reason'] += '; Date format OK'
        else:
            table_outcomes.append(df_outcome)

    print(f"\nValidation complete. Results saved to: {outfname}")

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
                    tenant_id='ALL', table_name='ALL_TABLES',
                    validation_status='SUCCESS', status_reason='No tables processed'
                ))
            loader = ValidationLoader(
                sf_cursor=cur, arg_dict=argument_dict,
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
            # Attach the human-readable report (not raw per-table DDL CSVs).
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

    # Script stats output
    script_end = time.perf_counter()
    script_run_time = script_end - script_start
    logging.info(f"\n{'-' * 50}")
    logging.info(f'Script run time: {script_run_time} seconds')


# Script driver
if __name__ == '__main__':
    ddl_validation()



