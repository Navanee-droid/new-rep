# This Script validates the Data between oracle table & snowflake views for all the DTM/BRDG/DAY_ARD tables
# Input Python DTM_Data_Validation --p prcs_dte --a sf_schema
# Created: 24/11/2025 Developer: Nikita Patidar

import sys
import yaml
import oracledb
import snowflake.connector
from datetime import datetime
import csv
import toml
import time
import os
import traceback
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import subprocess

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
    load_yaml,
    open_sf_connection,
    get_appl_code,
    open_oracle_connection,
    get_tenants,
    get_snowflake_dtm_views
)

SCRIPT_NAME = "DTM_Data_Validation.py"
SCRIPT_VERSION = "v2.0"

def arg_parsing() -> dict:
    args = parse_args(
        required=['--a', '--p'],
        optional=['--t','--load-sf-meta', '--sf-meta-db', '--sf-meta-schema'],
        description='DTM data validation between Oracle and Snowflake.',
    )
    args['prcs_dte'] = args['process_date_ts']
    args['sf_schema'] = args['appl_name']
    return args

def convert_views_to_tables(view_list):
    """Replace VW_ with TB_ in view names"""
    table_list = [view.replace('VW_', 'TB_') for view in view_list]
    print(f"✓ Converted {len(table_list)} view names to table names")
    return table_list

def get_snowflake_data(sf_conn, database, schema, view_name, tenant_id, process_date):
    """Get data from Snowflake view for specific tenant and process date"""
    # Determine date column based on view type
    if 'DTM_DIM' in view_name:
        date_column = 'PRCS_DTE'
    elif 'DTM_ARD' in view_name or 'DAY_ARD' in view_name:
        date_column = 'PRCS_DTE'
    else:
        date_column = 'PRCS_DTE'  # default
   # First, get column count to build proper ORDER BY clause
    count_query = f"""
    SELECT COUNT(*) as col_count
    FROM {database}.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '{schema}'
    AND TABLE_NAME = '{view_name}'
    """
    
    cursor_temp = sf_conn.cursor()
    cursor_temp.execute(count_query)
    col_count = cursor_temp.fetchone()[0]
    cursor_temp.close()
    
    # Build ORDER BY clause for all columns
    order_by_clause = ', '.join([str(i) for i in range(1, col_count + 1)])                                                          
    
    query = f"""
    SELECT *
    FROM {database}.{schema}.{view_name}
    WHERE TENANT_ID = %s
    AND {date_column} = %s
    ORDER BY {order_by_clause}
    """
    
    try:
        cursor = sf_conn.cursor()
        cursor.execute(query, (tenant_id, process_date))
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        return columns, rows
    except Exception as e:
        print(f"  ✗ Error querying Snowflake view {view_name}: {e}")
        return None, None

def get_oracle_data(oracle_conn, schema, table_name, tenant_id, process_date):
    """Get data from Oracle table for specific tenant and process date"""
    # Determine date column based on table type
    if 'DTM_DIM' in table_name:
        date_column = 'FULL_DTE'
    elif 'DTM_ARD' in table_name or 'DAY_ARD' in table_name:
        date_column = 'PRCS_DTE'
    else:
        date_column = 'PRCS_DTE'  # default
    # First, get column count to build proper ORDER BY clause
    count_query = f"""
    SELECT COUNT(*) as col_count
    FROM ALL_TAB_COLUMNS
    WHERE OWNER = '{schema}'
    AND TABLE_NAME = '{table_name}'
    """
    
    cursor_temp = oracle_conn.cursor()
    cursor_temp.execute(count_query)
    col_count = cursor_temp.fetchone()[0]
    cursor_temp.close()
    
    # Build ORDER BY clause for all columns
    order_by_clause = ', '.join([str(i) for i in range(1, col_count + 1)])
    
    query = f"""
    SELECT *
    FROM {schema}.{table_name}
    WHERE {date_column} = TO_DATE(:process_date, 'YYYY-MM-DD')
    ORDER BY {order_by_clause}
    """
    
    try:
        cursor = oracle_conn.cursor()
        cursor.execute(query, {'process_date': process_date})
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        return columns, rows
    except Exception as e:
        print(f"  ✗ Error querying Oracle table {table_name}: {e}")
        return None, None

def compare_data(sf_columns, sf_rows, oracle_columns, oracle_rows, tenant_id, view_name, table_name):
    """Compare data between Snowflake and Oracle for a specific tenant"""
    print(f"\n  Comparing data for Tenant {tenant_id}: {view_name} <-> {table_name}")
    
    issues = []
    
    # Filter out TENANT_ID column from comparison (only in Snowflake)
    oracle_columns_set = set(oracle_columns)
    sf_cols_no_tenant = [col for col in sf_columns if col != 'TENANT_ID']
    
    # Compare row counts
    sf_count = len(sf_rows)
    oracle_count = len(oracle_rows)
    
    print(f"    Snowflake rows: {sf_count}, Oracle rows: {oracle_count}")
    
    if sf_count != oracle_count:
        issues.append(f"Row count mismatch: Snowflake={sf_count}, Oracle={oracle_count}")
        print(f"    ✗ Row count mismatch!")
    else:
        print(f"    ✓ Row counts match")
    
    # Compare column lists (excluding TENANT_ID)
    if set(sf_cols_no_tenant) != set(oracle_columns):
        missing_in_oracle = set(sf_cols_no_tenant) - set(oracle_columns)
        missing_in_sf = set(oracle_columns) - set(sf_cols_no_tenant)
        if missing_in_oracle:
            issues.append(f"Columns in Snowflake but not in Oracle: {missing_in_oracle}")
            print(f"    ✗ Columns missing in Oracle: {missing_in_oracle}")
        if missing_in_sf:
            issues.append(f"Columns in Oracle but not in Snowflake: {missing_in_sf}")
            print(f"    ✗ Columns missing in Snowflake: {missing_in_sf}")
    else:
        print(f"    ✓ Column lists match")
    
    # Compare actual data row by row (limit to first 10 mismatches to avoid overwhelming output)
    mismatch_count = 0
    max_mismatches = 10
    data_mismatches = []
    
    for i in range(min(sf_count, oracle_count)):
        if mismatch_count >= max_mismatches:
            remaining = min(sf_count, oracle_count) - i
            issues.append(f"... and {remaining} more rows not checked")
            break
            
        sf_row = sf_rows[i]
        oracle_row = oracle_rows[i]
        
        # Create dictionaries for easier comparison
        sf_dict = dict(zip(sf_columns, sf_row))
        oracle_dict = dict(zip(oracle_columns, oracle_row))
        
        # Compare common columns (excluding TENANT_ID)
        for col in sf_cols_no_tenant:
            if col in oracle_dict:
                sf_val = sf_dict[col]
                oracle_val = oracle_dict[col]
                
                # Normalize values for comparison
                sf_val_norm = str(sf_val).strip() if sf_val is not None else None
                oracle_val_norm = str(oracle_val).strip() if oracle_val is not None else None
                
                # Handle date/datetime normalization - remove time component if it's 00:00:00
                if sf_val_norm and ' 00:00:00' in sf_val_norm:
                    sf_val_norm = sf_val_norm.replace(' 00:00:00', '')
                if oracle_val_norm and ' 00:00:00' in oracle_val_norm:
                    oracle_val_norm = oracle_val_norm.replace(' 00:00:00', '')
                
                if sf_val_norm != oracle_val_norm:
                    mismatch_msg = f"Row {i+1}, Column {col}: SF='{sf_val_norm}' vs Oracle='{oracle_val_norm}'"
                    issues.append(mismatch_msg)
                    data_mismatches.append(mismatch_msg)
                    mismatch_count += 1
                    if mismatch_count >= max_mismatches:
                        break
    
    if data_mismatches:
        print(f"    ✗ Found {len(data_mismatches)} data mismatches (showing first {min(len(data_mismatches), 5)}):")
        for mismatch in data_mismatches[:5]:
            print(f"      - {mismatch}")
        if len(data_mismatches) > 5:
            print(f"      ... and {len(data_mismatches) - 5} more")
    elif sf_count > 0 and oracle_count > 0:
        print(f"    ✓ All data values match")
    
    status = "SUCCESS" if not issues else "FAIL"
    print(f"    Status: {status}")
    
    return issues, status

def display_results(sf_views, oracle_tables, tenants, process_date, sf_schema):
    """Display the collected lists in a formatted output"""
    print("\n" + "="*120)
    print("DTM DATA VALIDATION - METADATA COLLECTION")
    print("="*120)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Process Date: {process_date}")
    print(f"Snowflake Schema: {sf_schema}")
    print("="*120)
    
    # Display Snowflake DTM Views
    print(f"\n{'─'*120}")
    print(f"SNOWFLAKE DTM VIEWS (Total: {len(sf_views)})")
    print(f"{'─'*120}")
    for i, view in enumerate(sf_views, 1):
        print(f"  {i:2d}. {view}")
    
    # Display Oracle DTM Tables
    print(f"\n{'─'*120}")
    print(f"ORACLE DTM TABLES (Total: {len(oracle_tables)})")
    print(f"{'─'*120}")
    for i, table in enumerate(oracle_tables, 1):
        print(f"  {i:2d}. {table}")
    
    # Display Distinct Tenants
    print(f"\n{'─'*120}")
    print(f"DISTINCT TENANTS FOR SCHEMA '{sf_schema}' (Total: {len(tenants)})")
    print(f"{'─'*120}")
    if len(tenants) <= 20:
        # If 20 or fewer tenants, display all
        for i, tenant in enumerate(tenants, 1):
            print(f"  {i:2d}. Tenant ID: {tenant}")
    else:
        # If more than 20, display first 10 and last 10
        for i, tenant in enumerate(tenants[:10], 1):
            print(f"  {i:2d}. Tenant ID: {tenant}")
        print(f"  ... ({len(tenants) - 20} tenants omitted)")
        for i, tenant in enumerate(tenants[-10:], len(tenants) - 9):
            print(f"  {i:2d}. Tenant ID: {tenant}")
    
    print("\n" + "="*120)
    print("SUMMARY")
    print("="*120)
    print(f"Total Snowflake Views:  {len(sf_views)}")
    print(f"Total Oracle Tables:    {len(oracle_tables)}")
    print(f"Total Tenants:          {len(tenants)}")
    print(f"Total Comparisons:      {len(sf_views) * len(tenants)}")
    print("="*120)

def main():
    """Main execution function"""
    script_start = time.perf_counter()
    py_path = os.environ["PYTHONPATH"]
    ingest_yaml_file = f'{py_path}/IngestionConfig.yaml'
    ingest_cfg_dict = load_yaml(yaml_file=ingest_yaml_file)
    argument_dict = arg_parsing()
    sf_schema = argument_dict["sf_schema"]
    process_date = argument_dict["prcs_dte"]
    
    # Validate date format
    try:
        datetime.strptime(process_date, '%Y-%m-%d')
    except ValueError:
        print(f"✗ Invalid date format: {process_date}. Expected YYYYMMDD or YYYY-MM-DD.")
        sys.exit(1)
    
    print("="*120)
    print("DTM DATA VALIDATION - METADATA COLLECTION")
    print("="*120)
    print(f"Process Date: {process_date}")
    print(f"Snowflake Schema: {sf_schema}")
    print("="*120 + "\n")
    
    #Snowflake connection
    sf_conn = open_sf_connection(ingest_cfg=ingest_cfg_dict)
    sf_database = 'CUR_IBS'
    cs = sf_conn.cursor()
    appl_code = get_appl_code(sf_schema, cs)
    
    print("\nCollecting metadata...")
    
    # Get list of DTM_DIM views from Snowflake
    sf_views = get_snowflake_dtm_views(sf_conn, sf_schema, sf_database)
    
    if not sf_views:
        print("\n✗ No DTM_DIM views found in Snowflake. Exiting.")
        sf_conn.close()
        sys.exit(0)
    
    # Convert view names to table names
    oracle_tables = convert_views_to_tables(sf_views)
    
    # Get list of tenants for the schema
    tenant_id_str = argument_dict.get('tenant_id')
    if tenant_id_str:
        tenants = [t.strip() for t in tenant_id_str.split(',') if t.strip()]
    else:
        tenants = get_tenants(sf_schema, sf_conn)
    
    if not tenants:
        print("\n✗ No tenants found for the schema. Exiting.")
        sf_conn.close()
        sys.exit(0)
    
    # Display results
    display_results(sf_views, oracle_tables, tenants, process_date, sf_schema)
    
    # Perform data validation for each tenant
    print("\n" + "="*120)
    print("DATA VALIDATION - COMPARING SNOWFLAKE AND ORACLE DATA")
    print("="*120)
    
    all_results = []
    skipped_tenants = []
    
    # Determine Oracle schema based on schema type
    schema_type = 'ddw' if 'DDW' in sf_schema.upper() else 'ads'
    
    for tenant_id in tenants:
        # Determine Oracle schema based on tenant and schema type
        ora_user = f"DW{tenant_id}"
        
        # Attempt Oracle connection - skip tenant if it fails
        try:
            oracle_conn = open_oracle_connection(myuser=ora_user)
        except Exception as e:
            print(f"\n  ✗ Failed to connect to Oracle for tenant {tenant_id} (user: {ora_user}): {e}")
            print(f"    Skipping tenant {tenant_id} and proceeding with next tenant...")
            skipped_tenants.append({'tenant_id': tenant_id, 'reason': str(e)})
            
            # Determine oracle_schema for reporting
            if schema_type == 'ads':
                oracle_schema = f"DW{tenant_id}2"
            else:
                oracle_schema = f"DW{tenant_id}1"
            
            # Record SKIPPED for all views for this tenant
            for sf_view, oracle_table in zip(sf_views, oracle_tables):
                all_results.append({
                    'tenant_id': tenant_id,
                    'view_name': sf_view,
                    'table_name': oracle_table,
                    'sf_count': 'N/A',
                    'oracle_count': 'N/A',
                    'oracle_schema': oracle_schema,
                    'status': 'SKIPPED',
                    'issues': [f'Oracle connection failed for user {ora_user}: {e}']
                })
            continue
        
        ocs = oracle_conn.cursor()
        if schema_type == 'ads':
            oracle_schema = f"DW{tenant_id}2"
        else:  # ddw
            oracle_schema = f"DW{tenant_id}1"
        
        print(f"\n{'─'*120}")
        print(f"TENANT {tenant_id} - Oracle Schema: {oracle_schema}")
        print(f"{'─'*120}")
        
        for sf_view, oracle_table in zip(sf_views, oracle_tables):
            try:
                # Get data from Snowflake
                sf_cols, sf_rows = get_snowflake_data(sf_conn, sf_database, sf_schema, sf_view, tenant_id, process_date)
                
                # Get data from Oracle
                oracle_cols, oracle_rows = get_oracle_data(oracle_conn, oracle_schema, oracle_table, tenant_id, process_date)
                
                if sf_cols is None or oracle_cols is None:
                    all_results.append({
                        'tenant_id': tenant_id,
                        'view_name': sf_view,
                        'table_name': oracle_table,
                        'oracle_schema': oracle_schema,
                        'status': 'ERROR',
                        'issues': ['Failed to retrieve data']
                    })
                    continue
                
                # Compare data
                issues, status = compare_data(sf_cols, sf_rows, oracle_cols, oracle_rows, tenant_id, sf_view, oracle_table)
                
                all_results.append({
                    'tenant_id': tenant_id,
                    'view_name': sf_view,
                    'table_name': oracle_table,
                    'sf_count': len(sf_rows) if sf_rows else 0,
                    'oracle_count': len(oracle_rows) if oracle_rows else 0,
                    'status': status,
                    'oracle_schema': oracle_schema,
                    'issues': issues
                })
                
            except Exception as e:
                print(f"  ✗ Error comparing {sf_view} <-> {oracle_table}: {e}")
                all_results.append({
                    'tenant_id': tenant_id,
                    'view_name': sf_view,
                    'table_name': oracle_table,
                    'oracle_schema': oracle_schema,
                    'status': 'ERROR',
                    'issues': [str(e)]
                })
        
        # Close this tenant's Oracle connection
        try:
            oracle_conn.close()
        except Exception:
            pass
    
    # Generate final report
    print("\n" + "="*120)
    print("FINAL VALIDATION REPORT - TENANT WISE SUMMARY")
    print("="*120)
    
    success = sum(1 for r in all_results if r['status'] == 'SUCCESS')
    failed = sum(1 for r in all_results if r['status'] == 'FAIL')
    errors = sum(1 for r in all_results if r['status'] == 'ERROR')
    skipped = sum(1 for r in all_results if r['status'] == 'SKIPPED')
    
    print(f"Total Comparisons: {len(all_results)}")
    print(f"Success: {success}")
    print(f"Failed: {failed}")
    print(f"Errors: {errors}")
    print(f"Skipped (Connection Failed): {skipped}")
    print("="*120)
    
    # Print skipped tenants summary
    if skipped_tenants:
        print(f"\n{'─'*120}")
        print(f"SKIPPED TENANTS (Oracle Connection Failed): {len(skipped_tenants)}")
        print(f"{'─'*120}")
        for st in skipped_tenants:
            print(f"  ✗ Tenant {st['tenant_id']}: {st['reason']}")
    
    # Group results by tenant
    tenant_results = {}
    for result in all_results:
        tenant = result['tenant_id']
        if tenant not in tenant_results:
            tenant_results[tenant] = []
        tenant_results[tenant].append(result)
    
    # Print tabular format for each tenant
    print("\n" + "="*120)
    print("TENANT-WISE VALIDATION RESULTS")
    print("="*120)
    
    for tenant_id in sorted(tenant_results.keys()):
        results = tenant_results[tenant_id]
        oracle_schema = results[0]['oracle_schema']
        
        print(f"\n{'─'*120}")
        print(f"TENANT: {tenant_id} | Oracle Schema: {oracle_schema}")
        print(f"{'─'*120}")
        
        # Print table header
        print(f"{'Table Name':<40} | {'SF Rows':<10} | {'Oracle Rows':<12} | {'Status':<10} | {'Issues'}")
        print(f"{'-'*40}-+-{'-'*10}-+-{'-'*12}-+-{'-'*10}-+-{'-'*40}")
        
        # Print each table result
        for result in results:
            table_name = result['table_name']
            sf_count = result.get('sf_count', 'N/A')
            oracle_count = result.get('oracle_count', 'N/A')
            status = result['status']
            issues = result.get('issues', [])
            
            # Status symbol
            if status == 'SUCCESS':
                status_symbol = '✓ SUCCESS'
            elif status == 'FAIL':
                status_symbol = '✗ FAIL'
            elif status == 'SKIPPED':
                status_symbol = '⊘ SKIPPED'
            else:
                status_symbol = '⚠ ERROR'
            
            # Issues summary
            if issues:
                issue_summary = issues[0] if len(issues) == 1 else f"{len(issues)} issues"
            else:
                issue_summary = 'None'
            
            print(f"{table_name:<40} | {str(sf_count):<10} | {str(oracle_count):<12} | {status_symbol:<10} | {issue_summary}")
            
            # Print additional issues if any
            if len(issues) > 1:
                for issue in issues[1:]:
                    print(f"{'':<40} | {'':<10} | {'':<12} | {'':<10} | {issue}")
    
    # Show failed comparisons summary
    if failed > 0:
        print("\n" + "="*120)
        print("FAILED COMPARISONS SUMMARY")
        print("="*120)
        for result in all_results:
            if result['status'] == 'FAIL':
                print(f"\n  ✗ Tenant {result['tenant_id']} - {result['view_name']} <-> {result['table_name']}")
                print(f"    Oracle Schema: {result['oracle_schema']}")
                print(f"    Snowflake Rows: {result.get('sf_count', 'N/A')}, Oracle Rows: {result.get('oracle_count', 'N/A')}")
                print(f"    Issues:")
                for issue in result.get('issues', []):
                    print(f"      - {issue}")
    
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    file_path = f"{ingest_cfg_dict['snowflake_connection']['validation_path']}/{sf_schema}/{script_name}"
    
    # if the file path does not exist make it
    if not os.path.exists(file_path):
        os.makedirs(file_path, exist_ok=True)
    # Save results to CSV with dynamic filename
    csv_filename = f"{file_path}/Record_validation_{sf_schema}_{process_date}.csv"
    try:
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['Tenant_ID', 'Table_Name', 'SF_Rows', 'Oracle_Rows', 'Status', 'Mismatch']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            
            # Sort by tenant and then by table
            sorted_results = sorted(all_results, key=lambda x: (x['tenant_id'], x['table_name']))
            
            for result in sorted_results:
                issues_str = '; '.join(result.get('issues', [])) if result.get('issues') else 'None'
                
                writer.writerow({
                    'Tenant_ID': result['tenant_id'],
                    'Table_Name': result['table_name'],
                    'SF_Rows': result.get('sf_count', 'N/A'),
                    'Oracle_Rows': result.get('oracle_count', 'N/A'),
                    'Status': result['status'],
                    'Mismatch': issues_str
                })
        
        print(f"\n✓ Results saved to CSV: {csv_filename}")
    except Exception as e:
        print(f"\n✗ Failed to save CSV file: {e}")
    
    if argument_dict.get('load_sf_meta') and all_results:
        try:
            registry = TestCaseRegistry(cs, SCRIPT_NAME,
                                        database=argument_dict.get('sf_meta_db'),
                                        schema=argument_dict.get('sf_meta_schema'))
            meta_results = []
            for r in all_results:
                ora_c = r.get('oracle_count', 0) or 0
                sf_c = r.get('sf_count', 0) or 0
                # Handle 'N/A' values from skipped tenants
                try:
                    ora_c_int = int(ora_c)
                except (ValueError, TypeError):
                    ora_c_int = 0
                try:
                    sf_c_int = int(sf_c)
                except (ValueError, TypeError):
                    sf_c_int = 0
                mis_count = abs(ora_c_int - sf_c_int) if r['status'] == 'FAIL' else 0
                mat_count = min(ora_c_int, sf_c_int) if r['status'] == 'SUCCESS' else 0
                meta_results.append(registry.create_result(
                    validation_key='dtm_data_validation',
                    test_scenario='DTM/BRDG/DAY_ARD data validation Oracle vs Snowflake',
                    appl_name=sf_schema,
                    appl_code=appl_code,
                    tenant_id=r['tenant_id'],
                    table_name=r['table_name'],
                    validation_status=r['status'],
                    source_count=r.get('oracle_count', 0),
                    target_count=r.get('sf_count', 0),
                    mismatched_count=mis_count,
                    matched_count=mat_count,
                    status_reason='; '.join(r.get('issues', [])) if r.get('issues') else 'None',
                    additional_info={'view_name': r.get('view_name'), 'oracle_schema': r.get('oracle_schema')}
                ))
            if meta_results:
                loader = ValidationLoader(
                    sf_cursor=cs,
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
                summary.update_counts(meta_results)
                summary.execution_time_sec = time.perf_counter() - script_start
                if os.path.exists(csv_filename) and os.path.getsize(csv_filename) > 0:
                    summary.read_and_store_output(csv_filename, file_type='dtm_validation_csv')
                exec_id = loader.insert_execution_summary(summary)

                run_ids = loader.insert_master_bulk(meta_results, execution_id=exec_id)
                run_id_map = {(r.tenant_id, r.table_name): r.run_id for r in meta_results}

                detail_batch = []
                for r in all_results:
                    if r['status'] == 'FAIL':
                        rid = run_id_map.get((r['tenant_id'], r['table_name']), 0)
                        if rid:
                            issues_str = '; '.join(r.get('issues', [])) if r.get('issues') else 'None'
                            detail_batch.append(ValidationDetailResult(
                                run_id=rid,
                                match_status='MISMATCH',
                                record_key=r['table_name'],
                                source_data={'oracle_count': r.get('oracle_count'), 'tenant_id': r['tenant_id']},
                                target_data={'sf_count': r.get('sf_count'), 'issues': r.get('issues', [])},
                                detail_remarks=cap_details(issues_str, 2000)[0]
                            ))
                if detail_batch:
                    capped_batch, _, _ = cap_details(detail_batch)
                    loader.insert_detail_bulk(capped_batch)

                summary.emit_summary_line()
                print(f"Loaded {len(run_ids)} DTM validation records to Snowflake")
        except Exception as e:
            print(f"Failed to load validation results to Snowflake: {str(e)}")
            traceback.print_exc()
    elif not argument_dict.get('load_sf_meta'):
        print("Skipping metadata load to Snowflake (--load-sf-meta not specified)")

    # Close Snowflake connection
    sf_conn.close()
    
    print("\n✓ Data validation completed successfully!")
    
    # Print final note about skipped tenants if any
    if skipped_tenants:
        print(f"\nNote: {len(skipped_tenants)} tenant(s) were skipped due to Oracle connection failures.")
        print("These are marked as 'SKIPPED' in the output CSV file.")

if __name__ == "__main__":
    main()