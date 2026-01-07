# Iceberg V2 to V3 Upgrade Tool 🚀

Upgrade Iceberg tables from V2 to V3 format to enable full row-level delete support in Databricks Unity Catalog Federation.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## The Problem

Databricks Unity Catalog Federation does **not** support row-level deletes on V2 Iceberg tables when using **Merge-on-Read (MoR)** mode.

| Delete Mode | V2 Support | V3 Support |
|-------------|------------|------------|
| Copy-on-Write | ✅ Supported | ✅ Supported |
| Merge-on-Read | ❌ Not Supported | ✅ Supported |

### Why V3?

V3 Iceberg tables use a new delete file format that Databricks can properly interpret. However, simply converting to V3 isn't enough - you must also **compact the table** to remove existing V2 delete files.

```
UPGRADE PROCESS
────────────────────────────────────────────────────────────────────────────────

STEP 1: ALTER TABLE ... SET TBLPROPERTIES ('format-version' = '3')
────────────────────────────────────────────────────────────────────────────────
- Table metadata updated to V3
- Existing delete files remain (V2 format)
- Databricks STILL can't read deletes properly

STEP 2: CALL system.rewrite_data_files(..., options => map('rewrite-all', 'true'))
────────────────────────────────────────────────────────────────────────────────
- All data files rewritten
- Delete files applied and removed
- Databricks can now read the table correctly ✅
```

---

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

Copy the example environment file and fill in your values:

```bash
cp env.example .env
# Edit .env with your credentials
```

---

## Usage

### Option 1: Demo Mode (Recommended First)

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

### Option 2: Upgrade Existing Tables

Upgrade specific tables from your Glue Catalog:

```bash
# Upgrade a single table
./upgrade.sh --database my_database --table my_table

# Upgrade multiple tables
./upgrade.sh --database my_database --tables "table1,table2,table3"

# Upgrade all tables in a database
./upgrade.sh --database my_database --all

# Dry run (show what would happen)
./upgrade.sh --database my_database --table my_table --dry-run
```

---

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

---

## Project Structure

```
iceberg-v2-to-v3-upgrade/
├── demo.sh                    # Demo mode - full end-to-end example
├── upgrade.sh                 # Production mode - upgrade existing tables
├── requirements.txt           # Python dependencies
├── env.example                # Environment template
├── README.md
├── LICENSE
└── internal/
    ├── config.py              # Shared configuration loader
    ├── lake_formation_setup.py # AWS Lake Formation permissions
    ├── create_demo_table.py   # Creates demo V2 table with MoR deletes
    ├── upgrade_table.py       # Core upgrade logic (ALTER + compact)
    └── verify_in_databricks.py # Verify upgrade in Databricks
```

---

## What Gets Upgraded

The upgrade process:

1. **Checks current format version** - Skips tables already on V3
2. **Verifies table uses Iceberg format** - Skips non-Iceberg tables
3. **Runs ALTER TABLE** - Sets `format-version` to `3`
4. **Runs full compaction** - Applies and removes all delete files
5. **Verifies in Databricks** - Confirms the table is now readable

---

## Risks & Considerations

### ⚠️ Compaction Impact

- **Storage**: Temporarily increases storage (old + new files)
- **Time**: Large tables may take significant time to compact
- **Compute**: EMR cluster needs sufficient resources

### 🔒 Lake Formation

Ensure your EMR role has Lake Formation permissions on:
- The Glue database
- All tables being upgraded
- The S3 locations

### 📋 Best Practices

1. **Test on non-production first** - Use demo mode to understand the process
2. **Run during low-traffic periods** - Compaction is resource-intensive
3. **Monitor EMR cluster** - Watch for memory/disk issues on large tables
4. **Verify after upgrade** - Always confirm tables work in Databricks

---

## Troubleshooting

### "Table is not an Iceberg table"

The table in Glue must have `table_type=ICEBERG` in its parameters.

### Lake Formation Permission Denied

Run the Lake Formation setup:
```bash
python internal/lake_formation_setup.py --database my_db --principal YOUR_EMR_ROLE_ARN
```

### EMR SSH Connection Failed

Ensure:
1. PEM file has correct permissions (`chmod 600`)
2. EMR security group allows SSH (port 22)
3. Cluster is in WAITING or RUNNING state

### Compaction Takes Too Long

For very large tables, consider:
- Using a larger EMR cluster
- Compacting in partitions: `where => 'date >= "2024-01-01"'`

---

## Author

**Ryan Cicak** - ryan.cicak@databricks.com

## License

MIT License - see [LICENSE](LICENSE) for details.

