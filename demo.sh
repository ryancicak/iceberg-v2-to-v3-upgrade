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

echo -e "${BLUE}"
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║         ICEBERG V2 TO V3 UPGRADE - DEMO MODE                   ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo -e "${NC}"

# Load environment
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
    echo -e "${GREEN}✓ Loaded .env file${NC}"
else
    echo -e "${YELLOW}⚠ No .env file found. Using environment variables.${NC}"
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
    echo -e "${RED}❌ Missing required environment variables:${MISSING}${NC}"
    echo ""
    echo "Please set these in .env or export them:"
    echo "  cp env.example .env"
    echo "  # Edit .env with your values"
    exit 1
fi

echo ""
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}STEP 1: Create Demo V2 Table with Merge-on-Read Deletes${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo ""

cd internal
python3 create_demo_table.py
cd ..

# Get the generated values
if [ -z "$GLUE_DATABASE" ]; then
    # Try to extract from Python output
    GLUE_DATABASE=$(python3 -c "from internal.config import load_config; c=load_config(); print(c['GLUE_DATABASE'])")
fi
TABLE_NAME="v2_mor_demo"

echo ""
echo -e "${YELLOW}════════════════════════════════════════════════════════════════${NC}"
echo -e "${YELLOW}The demo table has been created with V2 merge-on-read deletes.${NC}"
echo -e "${YELLOW}${NC}"
echo -e "${YELLOW}In Databricks, try to query:${NC}"
echo -e "${YELLOW}  SELECT * FROM your_catalog.${GLUE_DATABASE}.${TABLE_NAME}${NC}"
echo -e "${YELLOW}${NC}"
echo -e "${YELLOW}You should see an error about unsupported delete files!${NC}"
echo -e "${YELLOW}════════════════════════════════════════════════════════════════${NC}"
echo ""

read -p "Press Enter after you've seen the error in Databricks (or 's' to skip)... " response
if [ "$response" = "s" ]; then
    echo "Skipping verification..."
fi

echo ""
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}STEP 2: Upgrade Table to V3 and Run Compaction${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo ""

cd internal
python3 upgrade_table.py -d "$GLUE_DATABASE" -t "$TABLE_NAME"
cd ..

echo ""
echo -e "${GREEN}════════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}✅ UPGRADE COMPLETE!${NC}"
echo -e "${GREEN}${NC}"
echo -e "${GREEN}The table has been upgraded to V3 and compacted.${NC}"
echo -e "${GREEN}All merge-on-read delete files have been applied and removed.${NC}"
echo -e "${GREEN}${NC}"
echo -e "${GREEN}Now try the query again in Databricks:${NC}"
echo -e "${GREEN}  SELECT * FROM your_catalog.${GLUE_DATABASE}.${TABLE_NAME}${NC}"
echo -e "${GREEN}${NC}"
echo -e "${GREEN}It should work now! 🎉${NC}"
echo -e "${GREEN}════════════════════════════════════════════════════════════════${NC}"

# Optional: Verify in Databricks
if [ -n "$DATABRICKS_HOST" ] && [ -n "$DATABRICKS_TOKEN" ] && [ -n "$CATALOG_NAME" ]; then
    echo ""
    read -p "Would you like to verify automatically in Databricks? (y/n) " verify
    if [ "$verify" = "y" ]; then
        cd internal
        python3 verify_in_databricks.py -c "$CATALOG_NAME" -d "$GLUE_DATABASE" -t "$TABLE_NAME"
        cd ..
    fi
fi

echo ""
echo -e "${BLUE}Demo complete!${NC}"

