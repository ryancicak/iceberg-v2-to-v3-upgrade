#!/usr/bin/env python3
"""
Verify that upgraded tables can be read in Databricks.
"""

import argparse
import requests
import time
from config import load_config


def run_databricks_query(config, sql, warehouse_id=None):
    """Run a SQL query on Databricks SQL warehouse."""
    host = config["DATABRICKS_HOST"].rstrip('/')
    token = config["DATABRICKS_TOKEN"]
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Get warehouse ID if not provided
    if not warehouse_id:
        response = requests.get(
            f"{host}/api/2.0/sql/warehouses",
            headers=headers
        )
        warehouses = response.json().get('warehouses', [])
        running = [w for w in warehouses if w.get('state') == 'RUNNING']
        if not running:
            print("❌ No running SQL warehouse found")
            return None
        warehouse_id = running[0]['id']
        print(f"Using warehouse: {running[0]['name']} ({warehouse_id})")
    
    # Execute query
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
    
    return {
        'status': status,
        'result': result.get('result', {}),
        'error': result.get('status', {}).get('error', {})
    }


def verify_table(config, catalog, database, table):
    """Verify a table can be read in Databricks."""
    full_name = f"{catalog}.{database}.{table}"
    
    print(f"\n{'=' * 60}")
    print(f"VERIFYING: {full_name}")
    print(f"{'=' * 60}")
    
    # Test 1: DESCRIBE
    print("\n1. Testing DESCRIBE TABLE...")
    sql = f"DESCRIBE TABLE {full_name}"
    result = run_databricks_query(config, sql)
    
    if result['status'] == 'SUCCEEDED':
        print("   ✅ DESCRIBE succeeded")
    else:
        error_msg = result['error'].get('message', 'Unknown error')
        print(f"   ❌ DESCRIBE failed: {error_msg[:200]}")
        return False
    
    # Test 2: SELECT
    print("\n2. Testing SELECT query...")
    sql = f"SELECT * FROM {full_name} LIMIT 5"
    result = run_databricks_query(config, sql)
    
    if result['status'] == 'SUCCEEDED':
        rows = result['result'].get('data_array', [])
        print(f"   ✅ SELECT succeeded - returned {len(rows)} rows")
        for row in rows[:3]:
            print(f"      {row}")
        if len(rows) > 3:
            print(f"      ... and {len(rows) - 3} more")
    else:
        error_msg = result['error'].get('message', 'Unknown error')
        print(f"   ❌ SELECT failed: {error_msg[:300]}")
        
        # Check for specific errors
        if 'ICEBERG' in error_msg.upper():
            print("\n   🔴 This appears to be an Iceberg format issue.")
            print("   💡 Try running the upgrade: python internal/upgrade_table.py")
        
        return False
    
    # Test 3: COUNT
    print("\n3. Testing COUNT query...")
    sql = f"SELECT COUNT(*) as cnt FROM {full_name}"
    result = run_databricks_query(config, sql)
    
    if result['status'] == 'SUCCEEDED':
        count = result['result'].get('data_array', [[0]])[0][0]
        print(f"   ✅ COUNT succeeded - {count} total rows")
    else:
        print(f"   ⚠️ COUNT failed (non-critical)")
    
    print(f"\n✅ Table {full_name} is fully readable in Databricks!")
    return True


def main():
    parser = argparse.ArgumentParser(description="Verify tables in Databricks")
    parser.add_argument("-c", "--catalog", required=True, help="Databricks catalog name")
    parser.add_argument("-d", "--database", required=True, help="Database/schema name")
    parser.add_argument("-t", "--table", required=True, help="Table name")
    parser.add_argument("-w", "--warehouse", help="SQL warehouse ID (optional)")
    
    args = parser.parse_args()
    config = load_config()
    
    if not config.get("DATABRICKS_HOST") or not config.get("DATABRICKS_TOKEN"):
        print("❌ DATABRICKS_HOST and DATABRICKS_TOKEN must be set")
        return
    
    success = verify_table(config, args.catalog, args.database, args.table)
    
    if success:
        print("\n🎉 Verification passed!")
    else:
        print("\n❌ Verification failed")


if __name__ == "__main__":
    main()

