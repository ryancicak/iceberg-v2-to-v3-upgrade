#!/bin/bash
#
# Upgrade existing Iceberg tables from V2 to V3
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

# Help function
show_help() {
    echo ""
    echo "Iceberg V2 to V3 Upgrade Tool"
    echo ""
    echo "Usage:"
    echo "  ./upgrade.sh --database <db> --table <table>    # Upgrade single table"
    echo "  ./upgrade.sh --database <db> --tables <t1,t2>   # Upgrade multiple tables"
    echo "  ./upgrade.sh --database <db> --all              # Upgrade all tables in database"
    echo "  ./upgrade.sh --database <db> --list             # List tables and versions"
    echo ""
    echo "Options:"
    echo "  -d, --database    Glue database name (required)"
    echo "  -t, --table       Single table to upgrade"
    echo "  --tables          Comma-separated list of tables"
    echo "  --all             Upgrade all Iceberg tables in database"
    echo "  --list            List tables and their format versions"
    echo "  --dry-run         Show what would be done without executing"
    echo "  -h, --help        Show this help"
    echo ""
    echo "Examples:"
    echo "  ./upgrade.sh -d my_database -t my_table"
    echo "  ./upgrade.sh -d my_database --tables 'table1,table2,table3'"
    echo "  ./upgrade.sh -d my_database --all --dry-run"
    echo "  ./upgrade.sh -d my_database --list"
    echo ""
}

# Parse arguments
DATABASE=""
TABLE=""
TABLES=""
ALL_FLAG=""
LIST_FLAG=""
DRY_RUN=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -d|--database)
            DATABASE="$2"
            shift 2
            ;;
        -t|--table)
            TABLE="$2"
            shift 2
            ;;
        --tables)
            TABLES="$2"
            shift 2
            ;;
        --all)
            ALL_FLAG="--all"
            shift
            ;;
        --list)
            LIST_FLAG="--list"
            shift
            ;;
        --dry-run)
            DRY_RUN="--dry-run"
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            show_help
            exit 1
            ;;
    esac
done

# Validate arguments
if [ -z "$DATABASE" ]; then
    echo -e "${RED}❌ --database is required${NC}"
    show_help
    exit 1
fi

if [ -z "$TABLE" ] && [ -z "$TABLES" ] && [ -z "$ALL_FLAG" ] && [ -z "$LIST_FLAG" ]; then
    echo -e "${RED}❌ Please specify --table, --tables, --all, or --list${NC}"
    show_help
    exit 1
fi

echo -e "${BLUE}=== ICEBERG V2 TO V3 UPGRADE TOOL ===${NC}"
echo ""

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
    exit 1
fi

# Build command
CMD="python3 internal/upgrade_table.py -d $DATABASE"

if [ -n "$TABLE" ]; then
    CMD="$CMD -t $TABLE"
fi

if [ -n "$TABLES" ]; then
    CMD="$CMD --tables '$TABLES'"
fi

if [ -n "$ALL_FLAG" ]; then
    CMD="$CMD $ALL_FLAG"
fi

if [ -n "$LIST_FLAG" ]; then
    CMD="$CMD $LIST_FLAG"
fi

if [ -n "$DRY_RUN" ]; then
    CMD="$CMD $DRY_RUN"
    echo -e "${YELLOW}🔍 DRY RUN MODE - No changes will be made${NC}"
fi

echo ""
echo -e "${BLUE}Database: ${DATABASE}${NC}"
if [ -n "$TABLE" ]; then
    echo -e "${BLUE}Table: ${TABLE}${NC}"
fi
if [ -n "$TABLES" ]; then
    echo -e "${BLUE}Tables: ${TABLES}${NC}"
fi
if [ -n "$ALL_FLAG" ]; then
    echo -e "${BLUE}Mode: All Iceberg tables${NC}"
fi
echo ""

# Execute
eval $CMD

echo ""
if [ -z "$DRY_RUN" ] && [ -z "$LIST_FLAG" ]; then
    echo -e "${GREEN}Upgrade complete!${NC}"
    echo ""
    echo "Tables have been upgraded to V3 and compacted."
    echo "You can now query them in Databricks."
fi

