# Iceberg V2 to V3 Upgrade Tool

Scripts to upgrade Iceberg tables from V2 to V3 so Databricks can read merge-on-read deletes.

## Background

Databricks UC Federation can't read V2 Iceberg tables that use merge-on-read (MoR) deletes. V3 fixes this, but you also need to compact after upgrading to clear out the old delete files.

```
Step 1: ALTER TABLE ... SET TBLPROPERTIES ('format-version' = '3')
Step 2: CALL system.rewrite_data_files(..., options => map('rewrite-all', 'true'))
```

## Setup

```bash
git clone https://github.com/ryancicak/iceberg-v2-to-v3-upgrade.git
cd iceberg-v2-to-v3-upgrade
pip install -r requirements.txt
cp env.example .env
# fill in .env
```

Required env vars:
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`
- `EMR_CLUSTER_ID`, `EMR_PEM_PATH`
- `DATABRICKS_HOST`, `DATABRICKS_TOKEN`

## Usage

**Demo mode** - full end-to-end test:
```bash
./demo.sh
```

This runs through:
1. Create V2 table with MoR deletes (Databricks fails to read)
2. Upgrade to V3 + compact (Databricks can read)
3. Run NEW delete on V3 (Databricks can still read - proves V3 MoR works)

**Upgrade existing tables:**
```bash
# single table
./upgrade.sh -d my_database -t my_table

# multiple tables
./upgrade.sh -d my_database --tables "table1,table2"

# all iceberg tables in a database
./upgrade.sh -d my_database --all

# dry run
./upgrade.sh -d my_database --all --dry-run

# list tables and versions
./upgrade.sh -d my_database --list
```

## Notes

- Compaction temporarily doubles storage until old files are cleaned up
- Large tables take a while to compact
- Make sure your EMR role has Lake Formation permissions on the database/tables
- Test on non-prod first

## Troubleshooting

**Lake Formation errors:** Run `python internal/lake_formation_setup.py -d my_db -p YOUR_EMR_ROLE_ARN`

**EMR SSH fails:** Check PEM permissions (`chmod 600`), security group allows port 22, cluster is running

**Compaction slow:** Use a bigger cluster or compact by partition

## Author

Ryan Cicak
