# Iceberg V2 to V3 Upgrade Tool

Upgrade Iceberg tables from V2 to V3 format to enable full row-level delete support in Databricks Unity Catalog Federation.

## The Problem

Databricks Unity Catalog Federation does not support row-level deletes on V2 Iceberg tables when using Merge-on-Read (MoR) mode.

| Delete Mode | V2 Support | V3 Support |
|-------------|------------|------------|
| Copy-on-Write | Yes | Yes |
| Merge-on-Read | No | Yes |

### Why V3?

V3 Iceberg tables use a new delete file format that Databricks can properly interpret. But simply converting to V3 is not enough. You must also compact the table to remove the existing V2 delete files.

```
UPGRADE PROCESS

STEP 1: ALTER TABLE ... SET TBLPROPERTIES ('format-version' = '3')
- Table metadata updated to V3
- Existing delete files remain (V2 format)
- Databricks still can not read deletes properly

STEP 2: CALL system.rewrite_data_files(..., options => map('rewrite-all', 'true'))
- All data files rewritten
- Delete files applied and removed
- Databricks can now read the table correctly
```

## Quick Start

### Prerequisites

- Python 3.8+
- AWS CLI configured
- Active EMR Cluster with Iceberg 1.4+ (for V3 support)
- Databricks workspace with Unity Catalog
- Lake Formation permissions configured

### Installation

```bash
git clone https://github.com/ryancicak/iceberg-v2-to-v3-upgrade.git
cd iceberg-v2-to-v3-upgrade
pip install -r requirements.txt
```

### Configuration

```bash
cp env.example .env
# Edit .env with your credentials
```

## Usage

### Demo Mode

Creates a new demo table, generates synthetic data with deletes, upgrades to V3, and compacts.

```bash
./demo.sh
```

This will:
1. Create a V2 Iceberg table in Glue Catalog
2. Insert sample data
3. Perform merge-on-read deletes (creates delete files)
4. Show the table fails to read in Databricks
5. Upgrade to V3 format
6. Run full compaction
7. Verify the table now works in Databricks

### Upgrade Existing Tables

```bash
# Single table
./upgrade.sh --database my_database --table my_table

# Multiple tables
./upgrade.sh --database my_database --tables "table1,table2,table3"

# All tables in a database
./upgrade.sh --database my_database --all

# Dry run (show what would happen)
./upgrade.sh --database my_database --table my_table --dry-run

# List tables and their current versions
./upgrade.sh --database my_database --list
```

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `AWS_ACCESS_KEY_ID` | AWS access key | Yes |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | Yes |
| `AWS_DEFAULT_REGION` | AWS region (e.g., us-west-2) | Yes |
| `DATABRICKS_HOST` | Databricks workspace URL | Yes |
| `DATABRICKS_TOKEN` | Databricks PAT | Yes |
| `EMR_CLUSTER_ID` | EMR cluster ID (e.g., j-XXXXX) | Yes |
| `EMR_PEM_PATH` | Path to EMR SSH key | Yes |
| `S3_BUCKET` | S3 bucket for demo table | For demo |
| `GLUE_DATABASE` | Glue database name | For targeted upgrade |

## What Gets Upgraded

The upgrade process:

1. Checks current format version (skips tables already on V3)
2. Verifies table uses Iceberg format (skips non-Iceberg tables)
3. Runs ALTER TABLE to set format-version to 3
4. Runs full compaction to apply and remove all delete files
5. Verifies in Databricks that the table is now readable

## Risks

### Compaction Impact

- Storage temporarily increases (old + new files exist until cleanup)
- Large tables may take significant time to compact
- EMR cluster needs sufficient resources

### Lake Formation

Ensure your EMR role has Lake Formation permissions on:
- The Glue database
- All tables being upgraded
- The S3 locations

### Best Practices

1. Test on non-production first using demo mode
2. Run during low-traffic periods since compaction is resource-intensive
3. Monitor EMR cluster for memory/disk issues on large tables
4. Always verify tables work in Databricks after upgrade

## Troubleshooting

### "Table is not an Iceberg table"

The table in Glue must have `table_type=ICEBERG` in its parameters.

### Lake Formation Permission Denied

Run the Lake Formation setup:
```bash
python internal/lake_formation_setup.py --database my_db --principal YOUR_EMR_ROLE_ARN
```

### EMR SSH Connection Failed

Check that:
1. PEM file has correct permissions (`chmod 600`)
2. EMR security group allows SSH (port 22)
3. Cluster is in WAITING or RUNNING state

### Compaction Takes Too Long

For very large tables:
- Use a larger EMR cluster
- Compact by partition: `where => 'date >= "2024-01-01"'`

## Author

Ryan Cicak - ryan.cicak@databricks.com

## License

MIT
