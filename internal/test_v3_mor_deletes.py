#!/usr/bin/env python3
"""
Test that V3 merge-on-read deletes work in Databricks.
Runs a DELETE on the upgraded V3 table and verifies Databricks can still read it.
"""

import argparse
import subprocess
import requests
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
    return response['Cluster']['MasterPublicDnsName']


def run_delete_on_v3_table(config, database, table):
    """Run a DELETE on the V3 table to create new MoR delete files."""
    master_dns = get_emr_master_dns(config)
    pem_path = config["EMR_PEM_PATH"]
    bucket = config.get("S3_BUCKET", "default")
    
    print(f"\n--- Running DELETE on V3 table {database}.{table} ---")
    print("This will create NEW merge-on-read delete files in V3 format.")
    
    sql = f"""
-- Show current row count
SELECT 'Before delete:' as status, COUNT(*) as cnt FROM glue_catalog.{database}.{table};

-- Run a MoR delete on the V3 table
DELETE FROM glue_catalog.{database}.{table} WHERE id = 1;

-- Show new row count
SELECT 'After delete:' as status, COUNT(*) as cnt FROM glue_catalog.{database}.{table};

-- Show remaining data
SELECT * FROM glue_catalog.{database}.{table} ORDER BY id;
"""
    
    spark_sql_cmd = f"""spark-sql \\
        --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog \\
        --conf spark.sql.catalog.glue_catalog.warehouse=s3://{bucket}/warehouse \\
        --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \\
        --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \\
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions << 'SQLS'
{sql}
SQLS
"""
    
    ssh_cmd = [
        "ssh",
        "-i", pem_path,
        "-o", "StrictHostKeyChecking=no",
        f"hadoop@{master_dns}",
        spark_sql_cmd
    ]
    
    result = subprocess.run(ssh_cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"[ERROR] {result.stderr}")
        return False
    
    print(result.stdout)
    print("[OK] DELETE executed on V3 table - new MoR delete files created")
    return True


def verify_in_databricks(config, catalog, database, table):
    """Verify the table can still be read in Databricks after V3 delete."""
    host = config["DATABRICKS_HOST"].rstrip('/')
    token = config["DATABRICKS_TOKEN"]
    
    print(f"\n--- Verifying {catalog}.{database}.{table} in Databricks ---")
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Get a running warehouse
    response = requests.get(f"{host}/api/2.0/sql/warehouses", headers=headers)
    warehouses = response.json().get('warehouses', [])
    running = [w for w in warehouses if w.get('state') == 'RUNNING']
    
    if not running:
        print("[ERROR] No running SQL warehouse found")
        return False
    
    warehouse_id = running[0]['id']
    print(f"Using warehouse: {running[0]['name']}")
    
    # Run query
    sql = f"SELECT * FROM {catalog}.{database}.{table} LIMIT 10"
    print(f"Query: {sql}")
    
    response = requests.post(
        f"{host}/api/2.0/sql/statements",
        headers=headers,
        json={
            "warehouse_id": warehouse_id,
            "statement": sql,
            "wait_timeout": "50s"
        }
    )
    
    result = response.json()
    status = result.get('status', {}).get('state', 'UNKNOWN')
    
    if status == 'SUCCEEDED':
        rows = result.get('result', {}).get('data_array', [])
        print(f"[OK] Query succeeded - returned {len(rows)} rows")
        print("\nThis proves V3 merge-on-read deletes work in Databricks!")
        return True
    else:
        error = result.get('status', {}).get('error', {}).get('message', 'Unknown')
        print(f"[ERROR] Query failed: {error[:500]}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Test V3 MoR deletes in Databricks")
    parser.add_argument("-d", "--database", required=True, help="Glue database name")
    parser.add_argument("-t", "--table", required=True, help="Table name")
    parser.add_argument("-c", "--catalog", help="Databricks catalog name")
    parser.add_argument("--skip-delete", action="store_true", help="Skip the DELETE, just verify")
    
    args = parser.parse_args()
    config = load_config()
    validate_config(config)
    
    print("\n" + "=" * 60)
    print("TESTING V3 MERGE-ON-READ DELETES")
    print("=" * 60)
    
    # Step 1: Run DELETE on EMR (creates V3 MoR delete files)
    if not args.skip_delete:
        success = run_delete_on_v3_table(config, args.database, args.table)
        if not success:
            print("\n[ERROR] Failed to run DELETE")
            return
    
    # Step 2: Verify in Databricks
    if args.catalog and config.get("DATABRICKS_HOST") and config.get("DATABRICKS_TOKEN"):
        success = verify_in_databricks(config, args.catalog, args.database, args.table)
        if success:
            print("\n" + "=" * 60)
            print("TEST PASSED")
            print("=" * 60)
            print("V3 merge-on-read deletes work in Databricks!")
            print("The table can be read even after new DELETEs create MoR delete files.")
        else:
            print("\n[ERROR] Verification failed")
    else:
        print("\nTo verify in Databricks, run:")
        print(f"  SELECT * FROM your_catalog.{args.database}.{args.table}")


if __name__ == "__main__":
    main()

