# Meltano MongoDB → PostgreSQL Migration Guide

> **Carbon Lens BI Data Pipeline Configuration**

This guide provides recommendations for configuring Meltano to migrate MongoDB collections to PostgreSQL for your BI solution.

---

## Table of Contents

1. [Database Setup from Scratch](#1-database-setup-from-scratch)
   - [MongoDB User Setup](#mongodb-user-setup)
   - [PostgreSQL Database & Role Setup](#postgresql-database--role-setup)
2. [Entity Selection](#2-entity-selection)
3. [Replication Strategy Recommendation](#3-replication-strategy-recommendation)
4. [Replication Key Selection](#4-replication-key-selection)
5. [Operation Types Configuration](#5-operation-types-configuration)
6. [Meltano Select Commands](#6-meltano-select-commands)
7. [Complete meltano.yml Configuration](#7-complete-meltanoyml-configuration)

---

## 1. Database Setup from Scratch

### MongoDB User Setup

#### Step 1: Connect to MongoDB as Admin

```bash
# Connect to MongoDB shell
mongosh "mongodb://localhost:27017"

# Or if authentication is already enabled
mongosh "mongodb://admin:adminpassword@localhost:27017/admin"
```

#### Step 2: Create a Read-Only User for Meltano

For data extraction, Meltano only needs **read access** to your database:

```javascript
// Switch to admin database first
use admin

// Create a user with read-only access to carbonLens database
db.createUser({
  user: "meltano_reader",
  pwd: "your_secure_password_here",  // Change this!
  roles: [
    { role: "read", db: "carbonLens" }
  ]
})
```

#### Step 3: Verify the User

```javascript
// List all users
use admin
db.getUsers()

// Test authentication
db.auth("meltano_reader", "your_secure_password_here")
```

#### MongoDB Built-in Roles Reference

| Role | Description | Use Case |
|------|-------------|----------|
| `read` | Read-only access to all non-system collections | ✅ **Meltano extraction** |
| `readWrite` | Read and write access | App backend |
| `dbAdmin` | Schema management, indexing | DBA tasks |
| `dbOwner` | Full control over database | Database owner |
| `userAdmin` | Create/manage users | Security admin |

#### Step 4: Enable Authentication (if not already enabled)

Edit your MongoDB config file (`/etc/mongod.conf` or `mongod.cfg`):

```yaml
security:
  authorization: enabled
```

Restart MongoDB:

```bash
# Linux
sudo systemctl restart mongod

# macOS with Homebrew
brew services restart mongodb-community
```

#### Step 5: Update Meltano Connection String

```yaml
# In meltano.yml
config:
  mongodb_connection_string: mongodb://meltano_reader:your_secure_password_here@localhost:27017/carbonLens?authSource=admin
```

#### Optional: Create User with Change Streams Access (for LOG_BASED)

If you later switch to a replica set and want Change Streams:

```javascript
use admin

db.createUser({
  user: "meltano_cdc",
  pwd: "your_secure_password_here",
  roles: [
    { role: "read", db: "carbonLens" },
    { role: "read", db: "local" }  // Required for oplog access
  ]
})
```

---

### PostgreSQL Database & Role Setup

#### Step 1: Connect as Superuser

```bash
# Connect as postgres superuser
psql -U postgres

# Or with password
psql -U postgres -h localhost -W
```

#### Step 2: Create a Dedicated Role for Meltano

```sql
-- Create a role (user) for Meltano
CREATE ROLE loader_admin WITH 
  LOGIN 
  PASSWORD 'loaderadd99'
  NOSUPERUSER 
  NOCREATEDB 
  NOCREATEROLE;

-- Verify the role was created
\du loader_admin
```

#### Step 3: Create the Target Database

```sql
-- Create the database
CREATE DATABASE carbon_lens_bi
  WITH 
  OWNER = loader_admin
  ENCODING = 'UTF8'
  LC_COLLATE = 'en_US.UTF-8'
  LC_CTYPE = 'en_US.UTF-8'
  TEMPLATE = template0;

-- Verify
\l carbon_lens_bi
```

#### Step 4: Create Schemas

Connect to the new database and create schemas:

```sql
-- Connect to the new database
\c carbon_lens_bi

-- Create schema for raw data from MongoDB
CREATE SCHEMA IF NOT EXISTS rawcl;

-- Create schema for transformed/mart data (optional)
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- Set ownership
ALTER SCHEMA rawcl OWNER TO loader_admin;
ALTER SCHEMA staging OWNER TO loader_admin;
ALTER SCHEMA marts OWNER TO loader_admin;

-- Verify schemas
\dn
```

#### Step 5: Grant Privileges

```sql
-- Grant all privileges on the rawcl schema
GRANT ALL PRIVILEGES ON SCHEMA rawcl TO loader_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA rawcl TO loader_admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA rawcl TO loader_admin;

-- Set default privileges for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA rawcl 
  GRANT ALL PRIVILEGES ON TABLES TO loader_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA rawcl 
  GRANT ALL PRIVILEGES ON SEQUENCES TO loader_admin;

-- Repeat for other schemas if needed
GRANT ALL PRIVILEGES ON SCHEMA staging TO loader_admin;
GRANT ALL PRIVILEGES ON SCHEMA marts TO loader_admin;
```

#### Step 6: Create a Read-Only Role for BI Tools (Optional)

```sql
-- Create a read-only role for BI 
CREATE ROLE bi_reader WITH 
  LOGIN 
  PASSWORD 'bireaderadd99'
  NOSUPERUSER 
  NOCREATEDB 
  NOCREATEROLE;

-- Grant read access to all schemas
GRANT USAGE ON SCHEMA rawcl TO bi_reader;
GRANT USAGE ON SCHEMA staging TO bi_reader;
GRANT USAGE ON SCHEMA marts TO bi_reader;

GRANT SELECT ON ALL TABLES IN SCHEMA rawcl TO bi_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA staging TO bi_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA marts TO bi_reader;

-- Set default privileges for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA rawcl 
  GRANT SELECT ON TABLES TO bi_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging 
  GRANT SELECT ON TABLES TO bi_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts 
  GRANT SELECT ON TABLES TO bi_reader;
```

#### Step 7: Set Search Path (Optional)

```sql
-- Set default search path for the role
ALTER ROLE loader_admin SET search_path TO rawcl, staging, marts, public;
ALTER ROLE bi_reader SET search_path TO marts, staging, rawcl, public;
```

#### Step 8: Test the Connection

```bash
# Test loader_admin connection
psql -U loader_admin -d carbon_lens_bi -h localhost -W

# Once connected, verify access
\dn                    -- List schemas
\dt rawcl.*            -- List tables in rawcl schema (empty initially)
SELECT current_user;   -- Verify connected user
```

#### Complete Setup Script

Save this as `setup_postgres.sql` and run with `psql -U postgres -f setup_postgres.sql`:

```sql
-- ============================================
-- PostgreSQL Setup Script for Carbon Lens BI
-- ============================================

-- 1. Create Roles
CREATE ROLE loader_admin WITH 
  LOGIN 
  PASSWORD 'meltano_secure_pwd_123'
  NOSUPERUSER NOCREATEDB NOCREATEROLE;

CREATE ROLE bi_reader WITH 
  LOGIN 
  PASSWORD 'bi_reader_pwd_456'
  NOSUPERUSER NOCREATEDB NOCREATEROLE;

-- 2. Create Database
CREATE DATABASE carbon_lens_bi
  WITH OWNER = loader_admin
  ENCODING = 'UTF8'
  TEMPLATE = template0;

-- 3. Connect to new database
\c carbon_lens_bi

-- 4. Create Schemas
CREATE SCHEMA IF NOT EXISTS rawcl;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- 5. Set Ownership
ALTER SCHEMA rawcl OWNER TO loader_admin;
ALTER SCHEMA staging OWNER TO loader_admin;
ALTER SCHEMA marts OWNER TO loader_admin;

-- 6. Grant Privileges to loader_admin
GRANT ALL ON SCHEMA rawcl, staging, marts TO loader_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA rawcl GRANT ALL ON TABLES TO loader_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL ON TABLES TO loader_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT ALL ON TABLES TO loader_admin;

-- 7. Grant Privileges to bi_reader
GRANT USAGE ON SCHEMA rawcl, staging, marts TO bi_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA rawcl TO bi_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA staging TO bi_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA marts TO bi_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA rawcl GRANT SELECT ON TABLES TO bi_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT SELECT ON TABLES TO bi_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT SELECT ON TABLES TO bi_reader;

-- 8. Set Search Paths
ALTER ROLE loader_admin SET search_path TO rawcl, staging, marts, public;
ALTER ROLE bi_reader SET search_path TO marts, staging, rawcl, public;

-- 9. Verify Setup
\echo '=== Roles ==='
\du loader_admin
\du bi_reader

\echo '=== Schemas ==='
\dn

\echo '=== Setup Complete ==='
```

#### Update Meltano Configuration

After setup, update your `meltano.yml`:

```yaml
loaders:
  - name: target-postgres
    config:
      host: localhost
      port: 5432
      user: loader_admin
      password: meltano_secure_pwd_123  # Or use ${POSTGRES_PASSWORD}
      database: carbon_lens_bi
      default_target_schema: rawcl
```

#### PostgreSQL Roles & Privileges Quick Reference

| Privilege | Description |
|-----------|-------------|
| `SELECT` | Read data from tables |
| `INSERT` | Add new rows |
| `UPDATE` | Modify existing rows |
| `DELETE` | Remove rows |
| `TRUNCATE` | Empty tables |
| `REFERENCES` | Create foreign keys |
| `TRIGGER` | Create triggers |
| `ALL` | All of the above |

| Role Attribute | Description |
|----------------|-------------|
| `LOGIN` | Can connect to database |
| `SUPERUSER` | Bypass all permission checks |
| `CREATEDB` | Can create databases |
| `CREATEROLE` | Can create other roles |
| `INHERIT` | Inherits privileges from member roles |

---

## 2. Entity Selection

### Collections Required for BI (25 total)

Based on analysis of your React dashboard charts and backend APIs, these are the collections needed for BI analytics:

#### Scope 1 - Direct Emissions (4 collections)
| Collection | Description | Document Fields |
|------------|-------------|-----------------|
| `stationarycombustions` | Stationary combustion emissions | 33 fields |
| `mobilecombustions` | Mobile combustion emissions | 30 fields |
| `fugitiveemissions` | Fugitive emissions data | ~25 fields |
| `fire extinguishers` | Fire extinguisher emissions | ~20 fields |

#### Scope 2 - Indirect Energy Emissions (2 collections)
| Collection | Description | Document Fields |
|------------|-------------|-----------------|
| `purchasedelectricities` | Purchased electricity data | 25 fields |
| `renewableelectricities` | Renewable electricity data | ~25 fields |

#### Scope 3 - Value Chain Emissions (16 collections)
| Collection | Description |
|------------|-------------|
| `ferastationaries` | FERA stationary sources |
| `feramobiles` | FERA mobile sources |
| `feraelectricities` | FERA electricity data |
| `flighttravels` | Business flight travel |
| `groundtravels` | Ground transportation |
| `seatravels` | Sea transportation |
| `accomodations` | Business accommodation |
| `employeecommutings` | Employee commuting |
| `wastegenerations` | Waste generation data |
| `endoflifetreatments` | End-of-life treatment |
| `upstreamtransportations` | Upstream transportation |
| `dtds` | Downstream distribution |
| `capitalgoods` | Capital goods emissions |
| `rawmaterials` | Raw materials data |
| `packagings` | Packaging emissions |
| `services` | Purchased services |

#### Reference Tables (3 collections)
| Collection | Description | Usage |
|------------|-------------|-------|
| `companies` | Company master data | Join for company names |
| `sites` | Site/facility data | Join for site names, locations |
| `users` | User accounts | Join for user names |

### Collections to EXCLUDE (15 collections)

These are configuration/lookup tables or admin data not needed for BI:

| Collection | Reason to Exclude |
|------------|-------------------|
| `categories` | App configuration |
| `subcategories` | App configuration |
| `fields` | Form configuration |
| `sections` | UI configuration |
| `kpis` | KPI definitions |
| `fuels` | Fuel type lookup |
| `units` | Unit conversions |
| `unitesgs` | ESG unit data |
| `vehicletypes` | Vehicle type lookup |
| `vehiclespecifications` | Vehicle specs lookup |
| `products` | Product catalog |
| `custommappings` | Custom field mappings |
| `customemissionfactors` | Custom EF overrides |
| `emaillogs` | Email audit logs |
| `superadminusers` | Admin accounts |

---

## 3. Replication Strategy Recommendation

### TL;DR: Use **INCREMENTAL** Replication

Based on your MongoDB collection structure, here's the comparison:

| Factor | INCREMENTAL | LOG_BASED (Change Streams) |
|--------|-------------|---------------------------|
| **Setup Complexity** | ✅ Simple | ⚠️ Requires Replica Set |
| **MongoDB Requirements** | ✅ Works with standalone | ❌ Requires replica set |
| **Delete Detection** | ❌ No | ✅ Yes |
| **Real-time Sync** | ⚠️ Near-real-time | ✅ Real-time |
| **Initial Sync** | ✅ Automatic | ⚠️ Separate full load needed |
| **Your Use Case** | ✅ BI dashboards | ⚡ Real-time apps |

### Why INCREMENTAL is Best for Your BI Use Case:

1. **Your MongoDB is standalone** (based on connection string `localhost:27017`)
   - Change Streams require a replica set, which you don't have

2. **BI dashboards don't need real-time sync**
   - Nightly or hourly batch updates are sufficient for reporting

3. **Your collections have `createdAt` and `updatedAt` fields**
   - Perfect for tracking new and modified records

4. **Deletes are rare in emission data**
   - Emission records are typically soft-deleted or archived, not hard deleted

5. **Simpler operations**
   - No need to manage change stream resume tokens

### When to Consider LOG_BASED:

Only consider LOG_BASED if:
- You upgrade to MongoDB Atlas or set up a replica set
- You need real-time data (< 1 minute latency)
- You need to capture hard deletes

---

## 4. Replication Key Selection

### ⚠️ Important: Only `_id` is Supported

From the official tap-mongodb documentation:
> **Note that the tap currently only supports the replication key `_id`** - the tap assumes that every collection in the database has an ObjectId field named `_id`, and that that field is indexed.

### What This Means

| Replication Key | Status | Behavior |
|-----------------|--------|----------|
| `_id` | ✅ Supported | Extracts records where `_id > last_extracted_id` |
| `updatedAt` | ❌ NOT Supported | Will cause errors |
| `createdAt` | ❌ NOT Supported | Will cause errors |

### Required Configuration

```yaml
metadata:
  '*':
    replication-key: _id
    replication-method: INCREMENTAL
```

### Limitation: Updates Not Captured

With `_id` as the replication key, **only NEW records** are captured in incremental syncs:

```
First sync:     Extracts all records
Second sync:    Only extracts records where _id > last_id
                (Updated records are NOT re-extracted)
```

### How `upsert` Still Helps

Even though updates aren't re-extracted, the `load_method: upsert` ensures:
- New records are inserted
- If you ever do a full resync, updates will be applied
- No duplicate key errors on re-runs

### Workaround: Periodic Full Refresh

To capture updates, schedule periodic full refreshes:

```bash
# Clear state to force full extraction
meltano state clear --state-id tap-mongodb-to-target-postgres

# Run full extraction
meltano run tap-mongodb target-postgres
```

Consider running full refresh weekly or monthly depending on your update frequency.

### Your MongoDB Collections are Ready

All your collections have proper `_id` fields:
- Type: `ObjectId` ✅
- Index: `_id_` exists on all collections ✅

No additional indexes needed for incremental extraction with `_id`.

---

## 5. Operation Types Configuration

### Default Configuration is Fine

The default `operation_types` in tap-mongodb includes:
```yaml
operation_types: ["create", "delete", "insert", "replace", "update"]
```

### Do You Need to Configure This?

**No, for INCREMENTAL replication.**

The `operation_types` setting is only relevant for **LOG_BASED** (Change Streams) replication. When using INCREMENTAL:
- The tap performs `find()` queries based on your replication key
- Operation types are not considered
- You're querying a point-in-time snapshot, not streaming changes

### If You Later Switch to LOG_BASED:

Consider these operation types:

| Operation | Include? | Reason |
|-----------|----------|--------|
| `insert` | ✅ Yes | New emission records |
| `update` | ✅ Yes | Approvals, corrections |
| `replace` | ✅ Yes | Full document updates |
| `create` | ✅ Yes | Collection creation |
| `delete` | ⚠️ Optional | Only if you hard-delete records |

---

## 6. Meltano Select Commands

### Step 1: Exclude All Collections First

```bash
meltano select tap-mongodb --exclude '*'
```

### Step 2: Select Required Collections

```bash
# Scope 1 - Direct Emissions
meltano select tap-mongodb stationarycombustions "*"
meltano select tap-mongodb mobilecombustions "*"
meltano select tap-mongodb fugitiveemissions "*"
meltano select tap-mongodb "fire extinguishers" "*"

# Scope 2 - Indirect Energy
meltano select tap-mongodb purchasedelectricities "*"
meltano select tap-mongodb renewableelectricities "*"

# Scope 3 - Value Chain
meltano select tap-mongodb ferastationaries "*"
meltano select tap-mongodb feramobiles "*"
meltano select tap-mongodb feraelectricities "*"
meltano select tap-mongodb flighttravels "*"
meltano select tap-mongodb groundtravels "*"
meltano select tap-mongodb seatravels "*"
meltano select tap-mongodb accomodations "*"
meltano select tap-mongodb employeecommutings "*"
meltano select tap-mongodb wastegenerations "*"
meltano select tap-mongodb endoflifetreatments "*"
meltano select tap-mongodb upstreamtransportations "*"
meltano select tap-mongodb dtds "*"
meltano select tap-mongodb capitalgoods "*"
meltano select tap-mongodb rawmaterials "*"
meltano select tap-mongodb packagings "*"
meltano select tap-mongodb services "*"

# Reference Tables
meltano select tap-mongodb companies "*"
meltano select tap-mongodb sites "*"
meltano select tap-mongodb users "*"
```

### Alternative: Single Script

```bash
#!/bin/bash
# select_bi_collections.sh

# Clear all selections
meltano select tap-mongodb --exclude '*'

# BI Collections to include
collections=(
  "stationarycombustions"
  "mobilecombustions"
  "fugitiveemissions"
  "fire extinguishers"
  "purchasedelectricities"
  "renewableelectricities"
  "ferastationaries"
  "feramobiles"
  "feraelectricities"
  "flighttravels"
  "groundtravels"
  "seatravels"
  "accomodations"
  "employeecommutings"
  "wastegenerations"
  "endoflifetreatments"
  "upstreamtransportations"
  "dtds"
  "capitalgoods"
  "rawmaterials"
  "packagings"
  "services"
  "companies"
  "sites"
  "users"
)

for coll in "${collections[@]}"; do
  echo "Selecting: $coll"
  meltano select tap-mongodb "$coll" "*"
done

echo "Done! Run 'meltano select tap-mongodb --list' to verify."
```

### Verify Selection

```bash
meltano select tap-mongodb --list --all
```

---

## 7. Complete meltano.yml Configuration

Here's the recommended complete configuration:

```yaml
version: 1
default_environment: dev

project_id: carbon-lens-bi

environments:
  - name: dev
  - name: staging
  - name: prod

plugins:
  extractors:
    - name: tap-mongodb
      variant: meltanolabs
      pip_url: git+https://github.com/MeltanoLabs/tap-mongodb.git
      config:
        database: carbonLens
        mongodb_connection_string: mongodb://meltano_reader:your_password@localhost:27017/carbonLens?authSource=admin
        flattening_enabled: true
        flattening_max_depth: 1
      metadata:
        # IMPORTANT: tap-mongodb only supports _id as replication key
        '*':
          replication-method: INCREMENTAL
          replication-key: _id
      
      select:
        # Scope 1
        - stationarycombustions.*
        - mobilecombustions.*
        - fugitiveemissions.*
        - "fire extinguishers.*"
        
        # Scope 2
        - purchasedelectricities.*
        - renewableelectricities.*
        
        # Scope 3
        - ferastationaries.*
        - feramobiles.*
        - feraelectricities.*
        - flighttravels.*
        - groundtravels.*
        - seatravels.*
        - accomodations.*
        - employeecommutings.*
        - wastegenerations.*
        - endoflifetreatments.*
        - upstreamtransportations.*
        - dtds.*
        - capitalgoods.*
        - rawmaterials.*
        - packagings.*
        - services.*
        
        # Reference Tables
        - companies.*
        - sites.*
        - users.*

  loaders:
    - name: target-postgres
      variant: meltanolabs
      pip_url: meltanolabs-target-postgres
      config:
        host: localhost
        port: 5432
        user: loader_admin
        password: ${TARGET_POSTGRES_PASSWORD}  # Set via: export TARGET_POSTGRES_PASSWORD=xxx
        database: carbon_lens_bi
        default_target_schema: rawcl
        ssl_mode: allow
        load_method: overwrite          # Full replace each sync (captures deletes)
        add_record_metadata: true       # Adds _sdc_extracted_at, _sdc_received_at
        activate_version: true          # Required for delete detection
        hard_delete: true               # Actually removes deleted records

schedules:
  - name: carbon-lens-hourly-sync
    interval: '@hourly'
    extractor: tap-mongodb
    loader: target-postgres

jobs:
  - name: full-sync
    tasks:
      - tap-mongodb target-postgres
```

---

## 8. Running the Pipeline

### First Run (Test)

```bash
# Set the password environment variable
export TARGET_POSTGRES_PASSWORD="your_loader_admin_password"

# Test configurations
meltano config tap-mongodb test
meltano config target-postgres test

# Run initial sync
meltano run tap-mongodb target-postgres
```

### Manual Sync

```bash
# Run sync manually anytime
meltano run tap-mongodb target-postgres

# Or use the job name
meltano run full-sync
```

### Check Logs

```bash
# View last run logs
meltano invoke tap-mongodb --about
tail -f logs/sync.log
```

---

## 9. Scheduling Options

### Option 1: Meltano Built-in Schedule (Recommended for Dev)

Start the Meltano scheduler daemon:

```bash
# Start scheduler in foreground
meltano schedule run

# Or in background
nohup meltano schedule run > logs/scheduler.log 2>&1 &
```

### Option 2: Cron (Recommended for Production)

```bash
# Edit crontab
crontab -e

# Add hourly sync (runs at minute 0 of every hour)
0 * * * * cd /home/aneeq/Documents/Fiver/react-dashboard/CL_site/mongo-pg && /home/aneeq/Documents/Fiver/react-dashboard/CL_site/mongo-pg/scripts/run_sync.sh >> logs/cron.log 2>&1
```

### Option 3: Systemd Timer (Best for Production Linux)

Install the systemd service and timer:

```bash
# Copy service files
sudo cp scripts/carbon-lens-sync.service /etc/systemd/system/
sudo cp scripts/carbon-lens-sync.timer /etc/systemd/system/

# Create .env file with password
cp .env.example .env
nano .env  # Add your TARGET_POSTGRES_PASSWORD

# Create logs directory
mkdir -p logs

# Reload systemd and enable timer
sudo systemctl daemon-reload
sudo systemctl enable carbon-lens-sync.timer
sudo systemctl start carbon-lens-sync.timer

# Check timer status
systemctl list-timers | grep carbon-lens

# Run sync manually via systemd
sudo systemctl start carbon-lens-sync.service

# Check logs
journalctl -u carbon-lens-sync.service -f
```

### Option 4: Docker with Cron

```dockerfile
# Dockerfile for scheduled sync
FROM python:3.11-slim

WORKDIR /app
COPY . .

RUN pip install meltano
RUN meltano install

# Install cron
RUN apt-get update && apt-get install -y cron

# Add crontab
RUN echo "0 * * * * cd /app && meltano run tap-mongodb target-postgres >> /app/logs/sync.log 2>&1" | crontab -

CMD ["cron", "-f"]
```

---

## 10. Delete Handling

### How Deletes Work with FULL_TABLE + Overwrite

With `replication-method: FULL_TABLE` and `load_method: overwrite`:

1. **Every sync** extracts ALL documents from MongoDB
2. **PostgreSQL table is replaced** with the current MongoDB data
3. **Deleted records in MongoDB** will be removed from PostgreSQL

```
MongoDB (before):  [A, B, C, D, E]
PostgreSQL:        [A, B, C, D, E]

MongoDB (delete C): [A, B, D, E]
After sync:
PostgreSQL:        [A, B, D, E]  ← C is gone!
```

### Why FULL_TABLE Instead of INCREMENTAL?

| Scenario | INCREMENTAL (`_id`) | FULL_TABLE |
|----------|---------------------|------------|
| New records | ✅ Captured | ✅ Captured |
| Updated records | ❌ Missed | ✅ Captured |
| Deleted records | ❌ Never removed | ✅ Removed |
| Performance | Faster (delta only) | Slower (all data) |
| Your data size (~100 docs) | Overkill | ✅ Perfect |

### Performance Considerations

Your current data size:
- ~16 documents in largest collection
- ~10 KB total per collection
- **Full sync takes < 5 seconds**

FULL_TABLE is ideal for your data volume. Consider INCREMENTAL only if:
- You have 100,000+ documents per collection
- Network bandwidth is limited
- You don't need delete sync

---

## 11. Monitoring & Troubleshooting

### Verify Sync Worked

```bash
# Connect to PostgreSQL
psql -U loader_admin -d carbon_lens_bi -h localhost

# Check row counts
SELECT 
    schemaname,
    relname as table_name,
    n_live_tup as row_count
FROM pg_stat_user_tables
WHERE schemaname = 'rawcl'
ORDER BY n_live_tup DESC;

# Check latest sync time (if add_record_metadata is true)
SELECT MAX(_sdc_extracted_at) as last_sync 
FROM rawcl.stationarycombustions;
```

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| `connection refused` | MongoDB/Postgres not running | `sudo systemctl start mongod postgresql` |
| `authentication failed` | Wrong password | Check `.env` file |
| `permission denied` | User lacks privileges | Run PostgreSQL setup script |
| `table doesn't exist` | First run needed | Run `meltano run tap-mongodb target-postgres` |
| `no state found` | Normal for FULL_TABLE | FULL_TABLE doesn't use state |

### Force Re-sync

```bash
# For FULL_TABLE, just run again
meltano run tap-mongodb target-postgres

# If you switch to INCREMENTAL and need reset:
meltano state clear --state-id tap-mongodb-to-target-postgres
```

---

## 9. Quick Reference

### Key Fields for BI Queries

All emission collections share these common fields:

| Field | Type | Purpose |
|-------|------|---------|
| `_id` | ObjectId | Primary key |
| `companyId` | ObjectId | FK to companies |
| `siteId` | ObjectId | FK to sites |
| `userId` | ObjectId | FK to users |
| `year` | String | Fiscal year |
| `month` | String | Month name |
| `calculatedEmission` | Number | **PRIMARY METRIC** |
| `approved` | String | Approval status |
| `createdAt` | Date | Record created |
| `updatedAt` | Date | Last modified |

### PostgreSQL Schema After Loading

After Meltano loads the data, you'll have tables like:

```
rawcl.stationarycombustions
rawcl.mobilecombustions
rawcl.companies
rawcl.sites
...
```

### Sample BI Query

```sql
SELECT 
    s.siteName,
    sc.year,
    sc.month,
    SUM(sc.calculatedEmission) as total_emissions
FROM rawcl.stationarycombustions sc
JOIN rawcl.sites s ON sc.siteId = s._id
WHERE sc.approved = 'approved'
GROUP BY s.siteName, sc.year, sc.month
ORDER BY sc.year, sc.month;
```

---

## Summary Checklist

- [ ] Add `updatedAt` indexes to MongoDB collections
- [ ] Update meltano.yml with recommended configuration
- [ ] Run `meltano select` commands to include only BI collections
- [ ] Perform initial full extraction
- [ ] Schedule regular incremental syncs
- [ ] Verify data in PostgreSQL
- [ ] Build BI dashboards on PostgreSQL tables
