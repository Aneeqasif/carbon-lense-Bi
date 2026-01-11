#!/bin/bash
# =============================================================================
# CDC Pipeline Setup & Test Script
# =============================================================================
# This script sets up the CDC pipeline in the correct order:
#   1. Initial incremental load (loads all existing data)
#   2. Set CDC bookmark with full-refresh (no data, just sets cursor)
#   3. Test CDC with inserts/updates/deletes
#   4. Verify CDC captured the changes
#   5. Create BI-ready views
# =============================================================================

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DUCKDB_PATH="$PROJECT_DIR/output/duckdb/warehouse.duckdb"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_step() { echo -e "\n${BLUE}▶ $1${NC}"; }
log_success() { echo -e "${GREEN}✅ $1${NC}"; }
log_warning() { echo -e "${YELLOW}⚠️  $1${NC}"; }
log_error() { echo -e "${RED}❌ $1${NC}"; }

cd "$PROJECT_DIR"

# =============================================================================
# Helper Functions
# =============================================================================

count_duckdb() {
    local table=$1
    duckdb "$DUCKDB_PATH" -noheader -c "SELECT COUNT(*) FROM rawjd.$table;" 2>/dev/null || echo "0"
}

show_usage() {
    echo "Usage: $0 <command>"
    echo ""
    echo "Commands:"
    echo "  setup              Run full initial setup (incremental + set bookmark)"
    echo "  incremental        Run incremental load only (LOAD_METHOD=INCREMENTAL)"
    echo "  set-bookmark       Set CDC bookmark with full-refresh (no data load)"
    echo "  cdc-run            Run CDC log-based sync (default mode)"
    echo "  test-cdc           Full CDC test (insert → sync → update → sync → delete → sync)"
    echo "  create-views       Create BI-ready views in DuckDB"
    echo "  status             Show current state (record counts, etc.)"
    echo "  cleanup            Delete test records from MongoDB"
    echo ""
}

# =============================================================================
# Commands
# =============================================================================

cmd_status() {
    log_step "Current Pipeline Status"
    
    echo -e "\n📊 DuckDB Record Counts (rawjd schema):"
    duckdb "$DUCKDB_PATH" -c "
        SELECT 
            REPLACE(table_name, 'carbonlens_', '') as collection,
            (SELECT COUNT(*) FROM rawjd.stationarycombustions) as stationarycombustions
        FROM information_schema.tables 
        WHERE table_schema = 'rawjd' AND table_name = 'stationarycombustions'
        LIMIT 1;
    " 2>/dev/null || log_warning "DuckDB not initialized yet"
    
    echo -e "\n📊 MongoDB Test Records:"
    uv run "$SCRIPT_DIR/test_cdc_changes.py" --count
    
    echo -e "\n📊 Meltano State:"
    meltano state list 2>/dev/null | head -5 || log_warning "No state found"
}

cmd_incremental() {
    log_step "Running INCREMENTAL load (jsondocs)"
    echo "This will load all existing data from MongoDB..."
    
    LOAD_METHOD=INCREMENTAL meltano run tap-mongodb--jsondocs target-duckdb--jsondocs
    
    log_success "Incremental load complete"
    count=$(count_duckdb "stationarycombustions")
    echo "   stationarycombustions: $count records"
}

cmd_set_bookmark() {
    log_step "Setting CDC bookmark with --full-refresh"
    echo "This sets the change stream cursor without loading data..."
    
    # Default LOAD_METHOD=LOG_BASED from .env
    meltano run tap-mongodb--jsondocs target-duckdb--jsondocs --full-refresh
    
    log_success "CDC bookmark set - pipeline will now capture changes"
}

cmd_cdc_run() {
    log_step "Running CDC log-based sync"
    
    # Default LOAD_METHOD=LOG_BASED from .env
    meltano run tap-mongodb--jsondocs target-duckdb--jsondocs
    
    log_success "CDC sync complete"
}

cmd_setup() {
    log_step "FULL SETUP: Initial Load + Set CDC Bookmark"
    
    echo ""
    echo "This will:"
    echo "  1. Run INCREMENTAL load to get all existing data"
    echo "  2. Set CDC bookmark with --full-refresh"
    echo "  3. Create BI-ready views"
    echo ""
    read -p "Continue? [y/N] " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Aborted."
        exit 1
    fi
    
    # Step 1: Incremental load
    cmd_incremental
    
    # Step 2: Set bookmark
    cmd_set_bookmark
    
    # Step 3: Create views
    cmd_create_views
    
    log_success "Setup complete! Pipeline is ready for CDC mode."
    echo ""
    echo "Next steps:"
    echo "  1. Start Airflow scheduler: meltano invoke airflow scheduler"
    echo "  2. Or run manual CDC sync: $0 cdc-run"
}

cmd_test_cdc() {
    log_step "CDC Test: Full Insert → Sync → Update → Sync → Delete → Sync Cycle"
    
    # Get initial count
    initial_count=$(count_duckdb "stationarycombustions")
    echo "Initial DuckDB count: $initial_count"
    
    # Step 1: Insert test records
    log_step "Step 1: Inserting test records into MongoDB"
    uv run "$SCRIPT_DIR/test_cdc_changes.py" --insert
    
    # Step 2: Run CDC sync
    log_step "Step 2: Running CDC sync to capture inserts"
    meltano run tap-mongodb--jsondocs target-duckdb--jsondocs
    
    # Check count after insert
    after_insert=$(count_duckdb "stationarycombustions")
    echo "DuckDB count after insert sync: $after_insert (expected: $((initial_count + 3)))"
    
    if [ "$after_insert" -eq "$((initial_count + 3))" ]; then
        log_success "INSERT CDC verified! +3 records captured."
    else
        log_warning "Count mismatch after insert"
    fi
    
    # Step 3: Update test records
    log_step "Step 3: Updating test records in MongoDB"
    uv run "$SCRIPT_DIR/test_cdc_changes.py" --update
    
    # Step 4: Run CDC sync
    log_step "Step 4: Running CDC sync to capture updates"
    meltano run tap-mongodb--jsondocs target-duckdb--jsondocs
    
    # Verify updates (check for updated notes in view)
    log_step "Verifying updates in DuckDB"
    duckdb "$DUCKDB_PATH" -c "
        SELECT _id, year, month, notes 
        FROM rawjd.v_current_stationarycombustions 
        WHERE source = 'CDC_TEST'
        ORDER BY month;
    " 2>/dev/null || echo "(Views not created yet)"
    
    # Step 5: Delete test records
    log_step "Step 5: Deleting test records from MongoDB"
    uv run "$SCRIPT_DIR/test_cdc_changes.py" --delete
    
    # Step 6: Run CDC sync
    log_step "Step 6: Running CDC sync to capture deletes"
    meltano run tap-mongodb--jsondocs target-duckdb--jsondocs
    
    # Check count after delete - raw table may still have records, view should filter
    after_delete=$(count_duckdb "stationarycombustions")
    echo "Raw table count: $after_delete"
    
    # Check view (which filters deleted)
    view_count=$(duckdb "$DUCKDB_PATH" -noheader -c "SELECT COUNT(*) FROM rawjd.v_current_stationarycombustions WHERE source = 'CDC_TEST';" 2>/dev/null || echo "N/A")
    echo "View count (CDC_TEST records, excluding deleted): $view_count"
    
    if [ "$view_count" = "0" ]; then
        log_success "DELETE CDC verified! Test records filtered out in view."
    else
        log_warning "View still shows $view_count test records"
    fi
    
    log_success "CDC Test Complete!"
}

cmd_create_views() {
    log_step "Creating BI-ready views in DuckDB"
    
    duckdb "$DUCKDB_PATH" < "$SCRIPT_DIR/01_create_all_views.sql"
    
    log_success "Views created"
    
    # Show view counts
    duckdb "$DUCKDB_PATH" -c "
        SELECT table_schema, COUNT(*) as view_count 
        FROM information_schema.tables 
        WHERE table_type = 'VIEW' AND table_schema IN ('rawjd', 'rawmp')
        GROUP BY table_schema;
    "
}

cmd_cleanup() {
    log_step "Cleaning up test records from MongoDB"
    uv run "$SCRIPT_DIR/test_cdc_changes.py" --delete
}

# =============================================================================
# Main
# =============================================================================

case "${1:-}" in
    setup)          cmd_setup ;;
    incremental)    cmd_incremental ;;
    set-bookmark)   cmd_set_bookmark ;;
    cdc-run)        cmd_cdc_run ;;
    test-cdc)       cmd_test_cdc ;;
    create-views)   cmd_create_views ;;
    status)         cmd_status ;;
    cleanup)        cmd_cleanup ;;
    *)              show_usage; exit 1 ;;
esac
