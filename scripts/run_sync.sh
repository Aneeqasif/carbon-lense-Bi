#!/bin/bash
# Carbon Lens MongoDB → PostgreSQL Sync Script
# Run this manually or via cron/systemd

set -e

# Configuration
MELTANO_PROJECT_DIR="/home/aneeq/Documents/Fiver/react-dashboard/CL_site/mongo-pg"
LOG_DIR="${MELTANO_PROJECT_DIR}/logs"
LOG_FILE="${LOG_DIR}/sync_$(date +%Y%m%d_%H%M%S).log"

# Create logs directory if it doesn't exist
mkdir -p "${LOG_DIR}"

# Navigate to project directory
cd "${MELTANO_PROJECT_DIR}"

# Export password (or source from .env file)
# Option 1: Set directly (not recommended for production)
# export TARGET_POSTGRES_PASSWORD="your_password_here"

# Option 2: Source from .env file (recommended)
if [ -f ".env" ]; then
    source .env
fi

echo "========================================" | tee -a "${LOG_FILE}"
echo "Starting Carbon Lens sync at $(date)" | tee -a "${LOG_FILE}"
echo "========================================" | tee -a "${LOG_FILE}"

# Run the sync
if meltano run tap-mongodb target-postgres 2>&1 | tee -a "${LOG_FILE}"; then
    echo "✅ Sync completed successfully at $(date)" | tee -a "${LOG_FILE}"
    exit 0
else
    echo "❌ Sync failed at $(date)" | tee -a "${LOG_FILE}"
    exit 1
fi
