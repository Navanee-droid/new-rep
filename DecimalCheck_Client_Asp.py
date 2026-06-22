# This Script check CLNT_ID and DW_ASP_ID columns for decimal positions in all the intermediate table
# Input Python DecimalCheck_CLNT_ASP --a appl_code 
# Created: 17/11/2025 Developer: Krishnan Ravisankar

import snowflake.connector
import yaml
import os
import sys
import toml
import time
import re
import traceback
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

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
    load_yaml
)

SCRIPT_NAME = "DecimalCheck_Client_Asp.py"
SCRIPT_VERSION = "v2.0"


def arg_parsing() -> dict:
    return parse_args(
        required=['--a'],
        optional=['--load-sf-meta', '--sf-meta-db', '--sf-meta-schema'],
        description='Decimal precision check for client and ASP columns.',
    )


def check_decimal_columns(cursor, schema_name):
    """
    Check if CLNT_ID and DW_ASP_ID columns have decimal positions in any tables
    
    Args:
        cursor: Snowflake cursor
        schema_name: Schema name to check
    
    Returns:
        Dictionary with results for each table and column
    """
    # Get all tables in the schema that have CLNT_ID or DW_ASP_ID columns with numeric scale > 0
    column_query = f"""
    SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, NUMERIC_SCALE
    FROM CUR_IBS.information_schema.columns 
    WHERE table_schema='{schema_name}' 
    AND table_catalog='CUR_IBS' 
    AND (COLUMN_NAME = 'CLNT_ID' OR COLUMN_NAME = 'DW_ASP_ID')
    AND NUMERIC_SCALE > 0
    ORDER BY TABLE_NAME, COLUMN_NAME
    """
    
    cursor.execute(column_query)
    columns_info = cursor.fetchall()
    
    if not columns_info:
        return []
    
    results = []
    
    for table_name, column_name, data_type, numeric_scale in columns_info:
        
        try:
            # Get row count and sample values
            check_query = f"""
            SELECT 
                COUNT(*) as total_rows
            FROM CUR_IBS.{schema_name}.{table_name}
            WHERE {column_name} IS NOT NULL
            """
            
            cursor.execute(check_query)
            result = cursor.fetchone()
            total_rows = result[0]
            
            # Get sample records
            sample_records = []
            sample_query = f"""
            SELECT {column_name}
            FROM CUR_IBS.{schema_name}.{table_name}
            WHERE {column_name} IS NOT NULL
            LIMIT 10
            """
            cursor.execute(sample_query)
            sample_records = [row[0] for row in cursor.fetchall()]
            
            result_dict = {
                'table': table_name,
                'column': column_name,
                'data_type': data_type,
                'numeric_scale': numeric_scale,
                'total_rows': total_rows,
                'sample_records': sample_records,
                'status': 'SUCCESS'
            }
            
            # Add all results where schema has decimal precision
            results.append(result_dict)
                
        except Exception as e:
            # Still report errors
            results.append({
                'table': table_name,
                'column': column_name,
                'data_type': data_type,
                'numeric_scale': numeric_scale,
                'error': str(e),
                'status': 'ERROR'
            })
    
    return results


def print_summary_report(results, schema_name, file_path):
    """
    Print and save summary report of decimal check results
    
    Args:
        results: List of check results
        schema_name: Schema name checked
        file_path: Output directory path
    """
    output_filename = f"{file_path}/DecimalCheck_CLNT_ASP_{schema_name}.txt"
    
    summary_lines = []
    summary_lines.append("=" * 150)
    summary_lines.append("")
    summary_lines.append("*** DECIMAL POSITION CHECK REPORT - CLNT_ID & DW_ASP_ID ***")
    summary_lines.append("")
    summary_lines.append(f"*** SCHEMA: {schema_name} ***")
    summary_lines.append("")
    summary_lines.append("=" * 150)
    summary_lines.append("")
    
    # Separate results by status
    decimal_precision = [r for r in results if r.get('status') == 'SUCCESS']
    errors = [r for r in results if r.get('status') == 'ERROR']
    
    summary_lines.append("*** SUMMARY ***")
    summary_lines.append("")
    summary_lines.append(f"Tables/Columns with Decimal Precision Defined: {len(decimal_precision)}")
    summary_lines.append(f"Errors: {len(errors)}")
    summary_lines.append("")
    
    # BLOCK 1: List all tables with decimal precision
    if decimal_precision:
        summary_lines.append("=" * 150)
        summary_lines.append("")
        summary_lines.append("*** TABLES WITH DECIMAL PRECISION - LIST ***")
        summary_lines.append("")
        summary_lines.append("=" * 150)
        header = f"{'Table Name':<50} {'Column Name':<20} {'Data Type':<20} {'Scale':>10} {'Total Rows':>20}"
        summary_lines.append(header)
        summary_lines.append("-" * 150)
        
        for result in decimal_precision:
            row = f"{result['table']:<50} {result['column']:<20} {result['data_type']:<20} {result['numeric_scale']:>10} {result['total_rows']:>20,}"
            summary_lines.append(row)
        
        summary_lines.append("=" * 150)
        summary_lines.append("")
        summary_lines.append("")
        
        # BLOCK 2: Sample records for each table
        summary_lines.append("=" * 150)
        summary_lines.append("")
        summary_lines.append("*** SAMPLE RECORDS FOR EACH TABLE ***")
        summary_lines.append("")
        summary_lines.append("=" * 150)
        
        for result in decimal_precision:
            summary_lines.append("")
            summary_lines.append(f"TABLE: {result['table']} | COLUMN: {result['column']}")
            summary_lines.append("-" * 150)
            
            if result['sample_records']:
                for idx, value in enumerate(result['sample_records'], 1):
                    summary_lines.append(f"  {idx:2d}. {value}")
            else:
                summary_lines.append("  No records available")
            
            summary_lines.append("-" * 150)
        
        summary_lines.append("")
        summary_lines.append("=" * 150)
        summary_lines.append("")
    else:
        summary_lines.append("=" * 150)
        summary_lines.append("")
        summary_lines.append("*** NO DECIMAL PRECISION FOUND ***")
        summary_lines.append("")
        summary_lines.append("All CLNT_ID and DW_ASP_ID columns have NUMERIC_SCALE = 0.")
        summary_lines.append("")
        summary_lines.append("=" * 150)
        summary_lines.append("")
    
    # Errors
    if errors:
        summary_lines.append("=" * 150)
        summary_lines.append("")
        summary_lines.append("*** ERRORS ***")
        summary_lines.append("")
        summary_lines.append("=" * 150)
        header = f"{'Table Name':<40} {'Column Name':<20} {'Error Message':<80}"
        summary_lines.append(header)
        summary_lines.append("-" * 150)
        
        for result in errors:
            error_msg = result['error'].replace('\n', ' ').replace('  ', ' ')
            row = f"{result['table']:<40} {result['column']:<20} {error_msg:<80}"
            summary_lines.append(row)
        
        summary_lines.append("=" * 150)
    
    # Write to file
    try:
        with open(output_filename, 'w', encoding='utf-8') as f:
            f.write("\n".join(summary_lines))
        print(f"Report saved to: {output_filename}")
    except Exception as e:
        print(f"Error saving report: {e}")
        # Fallback to current directory
        fallback_filename = f"DecimalCheck_CLNT_ASP_{schema_name}.txt"
        try:
            with open(fallback_filename, 'w', encoding='utf-8') as f:
                f.write("\n".join(summary_lines))
            print(f"Report saved to fallback location: {fallback_filename}")
        except Exception as fallback_error:
            print(f"Error saving to fallback location: {fallback_error}")


def generate_fix_sql(cursor, results, schema_name, file_path):
    """
    Generate SQL script to fix decimal precision in columns
    
    Args:
        cursor: Snowflake cursor
        results: List of check results
        schema_name: Schema name
        file_path: Output directory path
    """
    sql_filename = f"{file_path}/Fix_DecimalPrecision_{schema_name}.sql"
    
    sql_lines = []
    sql_lines.append("-- ====================================================================================================")
    sql_lines.append("-- SQL Script to Fix Decimal Precision for CLNT_ID and DW_ASP_ID Columns")
    sql_lines.append(f"-- Schema: {schema_name}")
    sql_lines.append("-- Generated: Automatically by DecimelCheck_CLNT_ASP.py")
    sql_lines.append("-- ====================================================================================================")
    sql_lines.append("")
    sql_lines.append("-- IMPORTANT: Review this script before execution")
    sql_lines.append("-- This script will:")
    sql_lines.append("--   1. Create backup tables")
    sql_lines.append("--   2. Create or replace tables with corrected decimal precision (table will be truncated)")
    sql_lines.append("--   3. Copy data from backup to new tables")
    sql_lines.append("--   4. Drop backup tables")
    sql_lines.append("")
    sql_lines.append("-- ====================================================================================================")
    sql_lines.append("")
    
    # Group results by table
    tables_dict = {}
    for result in results:
        if result.get('status') == 'SUCCESS':
            table_name = result['table']
            if table_name not in tables_dict:
                tables_dict[table_name] = []
            tables_dict[table_name].append(result['column'])
    
    # Generate SQL for each table
    for table_name, columns in tables_dict.items():
        sql_lines.append("")
        sql_lines.append("-- ====================================================================================================")
        sql_lines.append(f"-- Processing Table: {table_name}")
        sql_lines.append(f"-- Columns to fix: {', '.join(columns)}")
        sql_lines.append("-- ====================================================================================================")
        sql_lines.append("")
        
        backup_table = f"{table_name}_BACKUP"
        
        # Step 1: Create backup table
        sql_lines.append("-- Step 1: Create backup of the table")
        sql_lines.append(f"CREATE OR REPLACE TABLE CUR_IBS.{schema_name}.{backup_table} AS")
        sql_lines.append(f"SELECT * FROM CUR_IBS.{schema_name}.{table_name};")
        sql_lines.append("")
        
        # Step 2: Create or replace table with corrected DDL (without decimal precision)
        sql_lines.append("-- Step 2: Create or replace table without decimal precision")
        
        try:
            # Get table DDL
            ddl_query = f"SELECT GET_DDL('TABLE', 'CUR_IBS.{schema_name}.{table_name}')"
            cursor.execute(ddl_query)
            ddl_result = cursor.fetchone()
            
            if ddl_result:
                original_ddl = ddl_result[0]
                
                # Modify DDL to remove decimal precision for CLNT_ID and DW_ASP_ID
                # Change "create table" to "create or replace table"
                modified_ddl = original_ddl.replace('create table', 'create or replace table', 1)
                
                # Ensure fully qualified table name in DDL
                # Replace table name with fully qualified name if not already qualified
                modified_ddl = re.sub(
                    r'(create or replace table\s+)(?!CUR_IBS\.)(\w+)',
                    rf'\1CUR_IBS.{schema_name}.\2',
                    modified_ddl,
                    flags=re.IGNORECASE
                )
                
                for col in columns:
                    # Replace NUMBER(x,y) with NUMBER(x,0) for the specific columns
                    # Pattern to match the column definition with decimal precision
                    pattern = rf'({col}\s+NUMBER\s*\(\s*\d+\s*,\s*)\d+(\s*\))'
                    modified_ddl = re.sub(pattern, r'\g<1>0\2', modified_ddl, flags=re.IGNORECASE)
                
                sql_lines.append(modified_ddl + ";")
                sql_lines.append("")
                
        except Exception as e:
            sql_lines.append(f"-- ERROR: Could not retrieve DDL for {table_name}")
            sql_lines.append(f"-- Error message: {str(e)}")
            sql_lines.append(f"-- Please manually create the table definition for {table_name}")
            sql_lines.append("")
        
        # Step 3: Insert data from backup to new table
        sql_lines.append("-- Step 3: Insert all records from backup table to newly created table")
        sql_lines.append(f"INSERT INTO CUR_IBS.{schema_name}.{table_name}")
        sql_lines.append(f"SELECT * FROM CUR_IBS.{schema_name}.{backup_table};")
        sql_lines.append("")
        
        # Step 4: Drop backup table
        sql_lines.append("-- Step 4: Drop the backup table")
        sql_lines.append(f"DROP TABLE CUR_IBS.{schema_name}.{backup_table};")
        sql_lines.append("")
        sql_lines.append("")
    
    sql_lines.append("-- ====================================================================================================")
    sql_lines.append("-- End of SQL Script")
    sql_lines.append("-- ====================================================================================================")
    
    # Write SQL file
    try:
        with open(sql_filename, 'w', encoding='utf-8') as f:
            f.write("\n".join(sql_lines))
        print(f"SQL script saved to: {sql_filename}")
    except Exception as e:
        print(f"Error saving SQL script: {e}")
        # Fallback to current directory
        fallback_sql_filename = f"Fix_DecimalPrecision_{schema_name}.sql"
        try:
            with open(fallback_sql_filename, 'w', encoding='utf-8') as f:
                f.write("\n".join(sql_lines))
            print(f"SQL script saved to fallback location: {fallback_sql_filename}")
        except Exception as fallback_error:
            print(f"Error saving SQL to fallback location: {fallback_error}")

    
if __name__ == "__main__":
    script_start = time.perf_counter()
    py_path = os.environ["PYTHONPATH"]
    ingest_yaml_file = f'{py_path}/IngestionConfig.yaml'
    ingest_cfg_dict = load_yaml(yaml_file=ingest_yaml_file)
    argument_dict = arg_parsing()
    appl_nme = argument_dict["appl_name"]
    print(f"Application Name: {appl_nme}")

    script_name = os.path.splitext(os.path.basename(__file__))[0]
    file_path = f"{ingest_cfg_dict['snowflake_connection']['validation_path']}/{appl_nme}/{script_name}"
    os.makedirs(file_path, exist_ok=True)

    #Snowflake connection
    sf_connection = open_sf_connection(ingest_cfg=ingest_cfg_dict)
    sf_cursor = sf_connection.cursor()
    appl_code = get_appl_code(appl_nme, sf_cursor)
    # Check for decimal positions
    results = check_decimal_columns(sf_cursor, appl_nme)
    
    # Print summary report (only shows tables with decimals)
    output_filename = f"{file_path}/DecimalCheck_CLNT_ASP_{appl_nme}.txt"
    print_summary_report(results, appl_nme, file_path)
    
    # Generate SQL script to fix decimal precision
    if results:
        generate_fix_sql(sf_cursor, results, appl_nme, file_path)
    else:
        print("No decimal precision issues found. No SQL fix script generated.")
    
    if argument_dict.get('load_sf_meta'):
        try:
            registry = TestCaseRegistry(sf_cursor, SCRIPT_NAME,
                                        database=argument_dict.get('sf_meta_db'),
                                        schema=argument_dict.get('sf_meta_schema'))
            _tables_with_issues = len([r for r in results if r.get('status') == 'SUCCESS'])
            _errors = len([r for r in results if r.get('status') == 'ERROR'])
            result = registry.create_result(
                validation_key='decimal_precision',
                test_scenario='Check CLNT_ID and DW_ASP_ID decimal positions',
                appl_name=argument_dict['appl_name'],
                appl_code=appl_code,
                tenant_id='ALL',
                table_name='ALL_TABLES',
                validation_status='SUCCESS' if not results else 'FAIL',
                status_reason=f"Found {_tables_with_issues} tables with decimal issues" if results else 'No decimal issues found',
                matched_count=0 if results else 1,
                mismatched_count=_tables_with_issues,
                additional_info={'tables_with_issues': _tables_with_issues, 'errors': _errors}
            )
            loader = ValidationLoader(
                sf_cursor=sf_cursor,
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
            summary.update_counts([result])
            summary.execution_time_sec = time.perf_counter() - script_start
            if os.path.exists(output_filename) and os.path.getsize(output_filename) > 0:
                summary.read_and_store_output(output_filename, file_type='decimal_check_report')
            exec_id = loader.insert_execution_summary(summary)

            run_id = loader.insert_master(result, execution_id=exec_id)
            if run_id and results:
                detail_batch = []
                for r in results:
                    if r.get('status') == 'SUCCESS':
                        rem = f"Table: {r.get('table', '')} Column: {r.get('column', '')} Scale: {r.get('numeric_scale', 0)}"[:500]
                        detail_batch.append(ValidationDetailResult(
                            run_id=run_id,
                            match_status='MISMATCH',
                            record_key=f"{r.get('table', '')}.{r.get('column', '')}",
                            record_key_columns='TABLE_NAME.COLUMN_NAME',
                            source_data={'scale': r.get('numeric_scale'), 'total_rows': r.get('total_rows')},
                            detail_remarks=rem
                        ))
                if detail_batch:
                    capped, _, _ = cap_details(detail_batch, 500)
                    loader.insert_detail_bulk(capped)
            summary.emit_summary_line()
            print("Loaded decimal check result to Snowflake")
        except Exception as e:
            print(f"Failed to load validation results to Snowflake: {str(e)}")
            traceback.print_exc()
    else:
        print("Skipping metadata load to Snowflake (--load-sf-meta not specified)")

    # Close connections
    sf_cursor.close()
    sf_connection.close()
    print("Process completed.")
    # Script stats output
    script_end = time.perf_counter()
    script_run_time = script_end - script_start
    script_run_time_minutes = script_run_time / 60
    print(f"Script run time: {script_run_time_minutes:.2f} minutes\n")