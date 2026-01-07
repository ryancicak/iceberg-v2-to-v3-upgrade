# Iceberg V2 to V3 Upgrade Tool

Upgrade Iceberg tables from V2 to V3 for Databricks UC Federation MoR delete support.

## The Problem

Databricks UC Federation can't read V2 Iceberg tables with merge-on-read (MoR) delete files.

## The Fix

Upgrade to V3 + compact. **Must use EMR 7.12+** (Iceberg 1.10+).

| EMR Version | Iceberg | V3 Support |
|-------------|---------|------------|
| < 7.12 | < 1.10 | BROKEN - missing `next-row-id` |
| **7.12+** | **1.10+** | Works |

## Upgrade Steps (EMR 7.12+ only!)

```sql
-- Step 1: Upgrade to V3
ALTER TABLE glue_catalog.your_db.your_table 
SET TBLPROPERTIES ('format-version' = '3');

-- Step 2: Compact with delete file removal
CALL glue_catalog.system.rewrite_data_files(
  table => 'your_db.your_table',
  options => map('rewrite-all', 'true', 'delete-file-threshold', '1')
);
```

**Optional - clean up old snapshots for storage savings:**
```sql
CALL glue_catalog.system.expire_snapshots(
  table => 'your_db.your_table',
  older_than => TIMESTAMP '2030-01-01 00:00:00',
  retain_last => 1
);
```

## Warning: Upgrading on Old EMR Breaks Tables!

If you upgrade to V3 on EMR < 7.12, the table becomes unreadable by both Databricks AND EMR 7.12+. The V3 metadata will be missing the required `next-row-id` field.

**Fix for broken V3 tables:** Metadata surgery to add `next-row-id`, then compact on EMR 7.12+.

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

**Demo mode:**
```bash
./demo.sh
```

**Upgrade existing tables:**
```bash
./upgrade.sh -d my_database -t my_table
./upgrade.sh -d my_database --all
./upgrade.sh -d my_database --all --dry-run
```

## Troubleshooting

**Lake Formation errors:** `python internal/lake_formation_setup.py -d my_db -p YOUR_EMR_ROLE_ARN`

**EMR SSH fails:** Check PEM permissions (`chmod 600`), security group, cluster state

## Author

Ryan Cicak
