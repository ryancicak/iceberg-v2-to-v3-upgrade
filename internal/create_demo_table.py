#!/usr/bin/env python3
"""
Create a demo V2 Iceberg table with merge-on-read deletes.
This simulates the scenario where Databricks can't read the delete files.
"""

import subprocess
import boto3
from config import load_config, validate_config


def create_s3_bucket(config):
    """Create S3 bucket for demo if it doesn't exist."""
    s3 = boto3.client(
        's3',
        region_name=config["AWS_REGION"],
        aws_access_key_id=config["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=config["AWS_SECRET_ACCESS_KEY"]
    )
    
    bucket = config["S3_BUCKET"]
    region = config["AWS_REGION"]
    
    try:
        if region == 'us-east-1':
            s3.create_bucket(Bucket=bucket)
        else:
            s3.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={'LocationConstraint': region}
            )
        print(f"[OK] Created S3 bucket: {bucket}")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print(f"[OK] S3 bucket already exists: {bucket}")
    except s3.exceptions.BucketAlreadyExists:
        print(f"[OK] S3 bucket already exists: {bucket}")
    except Exception as e:
        print(f"[WARN] Bucket creation: {e}")


def create_glue_database(config):
    """Create Glue database for demo if it doesn't exist."""
    glue = boto3.client(
        'glue',
        region_name=config["AWS_REGION"],
        aws_access_key_id=config["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=config["AWS_SECRET_ACCESS_KEY"]
    )
    
    database = config["GLUE_DATABASE"]
    bucket = config["S3_BUCKET"]
    
    try:
        glue.create_database(
            DatabaseInput={
                'Name': database,
                'Description': 'Demo database for Iceberg V2 to V3 upgrade',
                'LocationUri': f's3://{bucket}/warehouse'
            }
        )
        print(f"[OK] Created Glue database: {database}")
    except glue.exceptions.AlreadyExistsException:
        print(f"[OK] Glue database already exists: {database}")
    except Exception as e:
        print(f"[WARN] Database creation: {e}")


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


def create_demo_table_on_emr(config):
    """Create a V2 Iceberg table with merge-on-read deletes on EMR."""
    master_dns = get_emr_master_dns(config)
    pem_path = config["EMR_PEM_PATH"]
    database = config["GLUE_DATABASE"]
    bucket = config["S3_BUCKET"]
    
    table_name = "v2_mor_demo"
    
    # SQL to create V2 table with merge-on-read deletes
    sql_commands = f"""
-- Drop existing demo table
DROP TABLE IF EXISTS glue_catalog.{database}.{table_name};

-- Create V2 Iceberg table with merge-on-read delete mode
CREATE TABLE glue_catalog.{database}.{table_name} (
    id INT,
    name STRING,
    category STRING,
    amount DECIMAL(10,2),
    created_at TIMESTAMP
) USING iceberg
PARTITIONED BY (category)
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read'
);

-- Insert sample data
INSERT INTO glue_catalog.{database}.{table_name} VALUES
(1, 'Product A', 'electronics', 199.99, current_timestamp()),
(2, 'Product B', 'electronics', 299.99, current_timestamp()),
(3, 'Product C', 'clothing', 49.99, current_timestamp()),
(4, 'Product D', 'clothing', 79.99, current_timestamp()),
(5, 'Product E', 'furniture', 599.99, current_timestamp()),
(6, 'Product F', 'furniture', 899.99, current_timestamp()),
(7, 'Product G', 'electronics', 149.99, current_timestamp()),
(8, 'Product H', 'clothing', 29.99, current_timestamp()),
(9, 'Product I', 'furniture', 449.99, current_timestamp()),
(10, 'Product J', 'electronics', 399.99, current_timestamp());

-- Perform merge-on-read deletes (creates delete files!)
DELETE FROM glue_catalog.{database}.{table_name} WHERE id IN (2, 4, 6);

-- Perform merge-on-read updates (also creates delete files!)
UPDATE glue_catalog.{database}.{table_name} SET amount = amount * 1.1 WHERE category = 'electronics';

-- Show final state
SELECT 'Demo table created with MoR deletes!' as status;
SELECT * FROM glue_catalog.{database}.{table_name} ORDER BY id;
"""
    
    print(f"\nCreating demo V2 table with merge-on-read deletes...")
    print(f"  Database: {database}")
    print(f"  Table: {table_name}")
    print(f"  EMR: {master_dns}")
    
    # Build spark-sql command
    spark_sql_cmd = f"""spark-sql \\
        --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog \\
        --conf spark.sql.catalog.glue_catalog.warehouse=s3://{bucket}/warehouse \\
        --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \\
        --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \\
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions << 'SQLS'
{sql_commands}
SQLS
"""
    
    # SSH and execute
    ssh_cmd = [
        "ssh",
        "-i", pem_path,
        "-o", "StrictHostKeyChecking=no",
        f"hadoop@{master_dns}",
        spark_sql_cmd
    ]
    
    result = subprocess.run(ssh_cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"[ERROR] Error: {result.stderr}")
        return None
    
    print(result.stdout)
    print(f"\n[OK] Demo table created: {database}.{table_name}")
    print(f"   This table has V2 merge-on-read delete files that Databricks cannot read.")
    
    return table_name


def main():
    print("\n" + "=" * 60)
    print("CREATING DEMO V2 ICEBERG TABLE WITH MoR DELETES")
    print("=" * 60)
    
    config = load_config()
    validate_config(config)
    
    print(f"\nConfiguration:")
    print(f"  S3 Bucket: {config['S3_BUCKET']}")
    print(f"  Database: {config['GLUE_DATABASE']}")
    print(f"  EMR Cluster: {config['EMR_CLUSTER_ID']}")
    
    # Create infrastructure
    print("\n--- Setting up infrastructure ---")
    create_s3_bucket(config)
    create_glue_database(config)
    
    # Create demo table
    print("\n--- Creating demo table on EMR ---")
    table_name = create_demo_table_on_emr(config)
    
    if table_name:
        print("\n" + "=" * 60)
        print("NEXT STEPS")
        print("=" * 60)
        print(f"""
1. Try to query in Databricks (will fail due to V2 MoR delete files):
   SELECT * FROM your_catalog.{config['GLUE_DATABASE']}.{table_name}

2. Run the upgrade:
   python internal/upgrade_table.py -d {config['GLUE_DATABASE']} -t {table_name}

3. Query again in Databricks (should work now!)
""")


if __name__ == "__main__":
    main()

