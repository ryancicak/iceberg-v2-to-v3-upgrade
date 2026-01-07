#!/bin/bash
#
# Demo script for Iceberg V2 to V3 upgrade
# Creates a demo table, shows the problem, and demonstrates the fix
#

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo -e "${BLUE}=== ICEBERG V2 TO V3 UPGRADE - DEMO MODE ===${NC}"
echo ""

# Load environment
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
    echo -e "${GREEN}[OK] Loaded .env file${NC}"
else
    echo -e "${YELLOW}[WARN] No .env file found. Using environment variables.${NC}"
fi

# Check required variables
REQUIRED_VARS="AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION EMR_CLUSTER_ID EMR_PEM_PATH"
MISSING=""
for var in $REQUIRED_VARS; do
    if [ -z "${!var}" ]; then
        MISSING="$MISSING $var"
    fi
done

if [ -n "$MISSING" ]; then
    echo -e "${RED}[ERROR] Missing required environment variables:${MISSING}${NC}"
    echo ""
    echo "Please set these in .env or export them:"
    echo "  cp env.example .env"
    echo "  # Edit .env with your values"
    exit 1
fi

echo ""
echo -e "${BLUE}--- STEP 1: Create Demo V2 Table with Merge-on-Read Deletes ---${NC}"
echo ""

cd internal
python3 create_demo_table.py
cd ..

# Get the generated values
if [ -z "$GLUE_DATABASE" ]; then
    GLUE_DATABASE=$(python3 -c "from internal.config import load_config; c=load_config(); print(c['GLUE_DATABASE'])")
fi
TABLE_NAME="v2_mor_demo"

echo ""
echo -e "${YELLOW}The demo table has been created with V2 merge-on-read deletes.${NC}"
echo ""
echo "In Databricks, try to query:"
echo "  SELECT * FROM your_catalog.${GLUE_DATABASE}.${TABLE_NAME}"
echo ""
echo "You should see an error about unsupported delete files!"
echo ""

read -p "Press Enter after you've seen the error in Databricks (or 's' to skip)... " response
if [ "$response" = "s" ]; then
    echo "Skipping verification..."
fi

echo ""
echo -e "${BLUE}--- STEP 2: Upgrade Table to V3 and Run Compaction ---${NC}"
echo ""

cd internal
python3 upgrade_table.py -d "$GLUE_DATABASE" -t "$TABLE_NAME"
cd ..

echo ""
echo -e "${GREEN}[OK] Upgrade complete - table is now V3 and compacted${NC}"
echo ""
echo "Try the query again in Databricks - it should work now."
echo ""

read -p "Press Enter after you've verified it works (or 's' to skip)... " response
if [ "$response" = "s" ]; then
    echo "Skipping..."
fi

echo ""
echo -e "${BLUE}--- STEP 3: Test NEW Deletes on V3 Table ---${NC}"
echo ""
echo "Now we'll run a DELETE on the V3 table to create NEW merge-on-read delete files."
echo "This proves that V3 MoR deletes work in Databricks (not just cleaned-up tables)."
echo ""

cd internal
python3 test_v3_mor_deletes.py -d "$GLUE_DATABASE" -t "$TABLE_NAME" ${CATALOG_NAME:+-c "$CATALOG_NAME"}
cd ..

echo ""
echo -e "${GREEN}=== DEMO COMPLETE ===${NC}"
echo ""
echo "Summary:"
echo "  1. Created V2 table with MoR deletes -> Databricks could NOT read it"
echo "  2. Upgraded to V3 + compacted -> Databricks CAN read it"
echo "  3. Ran NEW delete on V3 table -> Databricks can STILL read it"
echo ""
echo "This proves V3 merge-on-read is fully supported in Databricks UC Federation."
