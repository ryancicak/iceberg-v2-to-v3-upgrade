#!/usr/bin/env python3
"""
Core upgrade logic for Iceberg V2 to V3.
Handles ALTER TABLE and compaction via EMR.
"""

import argparse
import subprocess
import sys
import boto3
from config import load_config, validate_config


def get_emr_master_dns(config):
    """Get the EMR cluster master node DNS."""
    emr = boto3.client(
        'emr',
        region_name=config["AWS_REGION"],
        aws_access_key_id=config["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=config["AWS_SECRET_ACCESS_KEY"]
    )
    
    cluster_id = config["EMR_CLUSTER_ID"]
    response = emr.describe_cluster(ClusterId=cluster_id)
    
    state = response['Cluster']['Status']['State']
    if state not in ['WAITING', 'RUNNING']:
        raise RuntimeError(f"EMR cluster {cluster_id} is not ready. State: {state}")
    
    return response['Cluster']['MasterPublicDnsName']


def get_table_info(config, database, table):
    """Get table information from Glue."""
    glue = boto3.client(
        'glue',
        region_name=config["AWS_REGION"],
        aws_access_key_id=config["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=config["AWS_SECRET_ACCESS_KEY"]
    )
    
    try:
        response = glue.get_table(DatabaseName=database, Name=table)
        tbl = response['Table']
        
        params = tbl.get('Parameters', {})
        return {
            'name': tbl['Name'],
            'database': database,
            'location': tbl.get('StorageDescriptor', {}).get('Location'),
            'table_type': params.get('table_type', 'UNKNOWN'),
            'format_version': params.get('format-version', 'UNKNOWN'),
            'metadata_location': params.get('metadata_location'),
        }
    except glue.exceptions.EntityNotFoundException:
        return None


def run_spark_sql_on_emr(config, sql_commands, dry_run=False):
    """Execute Spark SQL commands on EMR cluster."""
    master_dns = get_emr_master_dns(config)
    pem_path = config["EMR_PEM_PATH"]
    
    # Build the spark-sql command
    spark_sql_cmd = f"""spark-sql \\
        --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog \\
        --conf spark.sql.catalog.glue_catalog.warehouse=s3://{config.get('S3_BUCKET', 'default')}/warehouse \\
        --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \\
        --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \\
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \\
        -e "{sql_commands}"
    """
    
    if dry_run:
        print("\n[DRY RUN] Would execute on EMR:")
        print(f"  Host: {master_dns}")
        print(f"  SQL: {sql_commands}")
        return True
    
    # SSH and execute
    ssh_cmd = [
        "ssh",
        "-i", pem_path,
        "-o", "StrictHostKeyChecking=no",
        f"hadoop@{master_dns}",
        spark_sql_cmd
    ]
    
    print(f"\nExecuting on EMR ({master_dns})...")
    result = subprocess.run(ssh_cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"[ERROR] Error: {result.stderr}")
        return False
    
    print(result.stdout)
    return True


def upgrade_table_to_v3(config, database, table, dry_run=False):
    """Upgrade a single table from V2 to V3."""
    print(f"\n{'=' * 60}")
    print(f"UPGRADING: {database}.{table}")
    print(f"{'=' * 60}")
    
    # Get table info
    info = get_table_info(config, database, table)
    if not info:
        print(f"[ERROR] Table {database}.{table} not found in Glue")
        return False
    
    print(f"  Location: {info['location']}")
    print(f"  Table Type: {info['table_type']}")
    print(f"  Current Format Version: {info['format_version']}")
    
    # Check if it's an Iceberg table
    if info['table_type'].upper() != 'ICEBERG':
        print(f"[WARN] Skipping - not an Iceberg table (type: {info['table_type']})")
        return False
    
    # Check if already V3
    if info['format_version'] == '3':
        print(f"[OK] Already on V3 - skipping ALTER, running compaction only")
        sql = f"CALL glue_catalog.system.rewrite_data_files(table => '{database}.{table}', options => map('rewrite-all', 'true'));"
    else:
        # Upgrade to V3 and compact
        sql = f"""
ALTER TABLE glue_catalog.{database}.{table} SET TBLPROPERTIES ('format-version' = '3');
CALL glue_catalog.system.rewrite_data_files(table => '{database}.{table}', options => map('rewrite-all', 'true'));
SELECT 'Upgrade complete for {database}.{table}' as status;
"""
    
    print(f"\n📋 SQL to execute:")
    print(f"  {sql.strip()}")
    
    # Execute
    success = run_spark_sql_on_emr(config, sql.strip().replace('\n', ' '), dry_run=dry_run)
    
    if success and not dry_run:
        print(f"\n[OK] Successfully upgraded {database}.{table} to V3!")
    elif success and dry_run:
        print(f"\n[DRY RUN] Would upgrade {database}.{table} to V3")
    else:
        print(f"\n[ERROR] Failed to upgrade {database}.{table}")
    
    return success


def list_tables_in_database(config, database):
    """List all Iceberg tables in a database."""
    glue = boto3.client(
        'glue',
        region_name=config["AWS_REGION"],
        aws_access_key_id=config["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=config["AWS_SECRET_ACCESS_KEY"]
    )
    
    tables = []
    paginator = glue.get_paginator('get_tables')
    
    for page in paginator.paginate(DatabaseName=database):
        for tbl in page.get('TableList', []):
            params = tbl.get('Parameters', {})
            if params.get('table_type', '').upper() == 'ICEBERG':
                tables.append({
                    'name': tbl['Name'],
                    'format_version': params.get('format-version', 'UNKNOWN')
                })
    
    return tables


def main():
    parser = argparse.ArgumentParser(description="Upgrade Iceberg tables from V2 to V3")
    parser.add_argument("-d", "--database", required=True, help="Glue database name")
    parser.add_argument("-t", "--table", help="Single table to upgrade")
    parser.add_argument("--tables", help="Comma-separated list of tables")
    parser.add_argument("--all", action="store_true", help="Upgrade all Iceberg tables in database")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without executing")
    parser.add_argument("--list", action="store_true", help="List tables and their versions")
    
    args = parser.parse_args()
    config = load_config()
    validate_config(config)
    
    # List mode
    if args.list:
        print(f"\nIceberg tables in {args.database}:")
        tables = list_tables_in_database(config, args.database)
        for t in tables:
            version = t['format_version']
            status = "[OK]" if version == '3' else "[WARN] V2"
            print(f"  {status} {t['name']} (format-version: {version})")
        return
    
    # Determine tables to upgrade
    tables_to_upgrade = []
    
    if args.table:
        tables_to_upgrade = [args.table]
    elif args.tables:
        tables_to_upgrade = [t.strip() for t in args.tables.split(',')]
    elif args.all:
        all_tables = list_tables_in_database(config, args.database)
        tables_to_upgrade = [t['name'] for t in all_tables if t['format_version'] != '3']
        print(f"\nFound {len(tables_to_upgrade)} tables to upgrade in {args.database}")
    else:
        print("[ERROR] Please specify --table, --tables, or --all")
        sys.exit(1)
    
    if not tables_to_upgrade:
        print("[OK] No tables need upgrading!")
        return
    
    # Upgrade each table
    results = []
    for table in tables_to_upgrade:
        success = upgrade_table_to_v3(config, args.database, table, dry_run=args.dry_run)
        results.append((table, success))
    
    # Summary
    print(f"\n{'=' * 60}")
    print("UPGRADE SUMMARY")
    print(f"{'=' * 60}")
    
    for table, success in results:
        status = "[OK]" if success else "[ERROR]"
        print(f"  {status} {args.database}.{table}")


if __name__ == "__main__":
    main()

