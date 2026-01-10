# MongoDB to DuckDB Data Verification Report

**Generated:** January 10, 2026 (Updated: Jan 11, 2026)  
**Comparison Method:** MongoDB MCP direct query vs DuckDB warehouse

---

## Summary

| Status | Description |
|--------|-------------|
| ⚠️ | Minor discrepancies detected - 1 record missing per collection due to incremental sync start point |
| ✅ | BI-ready views created with CDC deduplication for both rawjd and rawmp schemas |

---

## BI-Ready Views

### Running the View Creation Script
```bash
cd mongo-pg
duckdb output/duckdb/warehouse.duckdb < scripts/01_create_all_views.sql
```

### View Naming Convention
- **Pattern:** `v_current_<tablename>` - Returns only current state of each document
- **CDC Deduplication:** `QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp DESC) = 1`
- **Deleted Filtering:** `WHERE _sdc_deleted_at IS NULL`

### rawjd Schema Views (JSON Documents → Parsed Columns)
| View Name | Purpose | Scope |
|-----------|---------|-------|
| v_current_users | User master data | Dimension |
| v_current_companies | Company master data | Dimension |
| v_current_sites | Site/Facility master data | Dimension |
| v_current_stationarycombustions | Boilers, furnaces, turbines | Scope 1 |
| v_current_mobilecombustions | Company vehicles, equipment | Scope 1 |
| v_current_fugitiveemissions | Refrigerants, AC systems | Scope 1 |
| v_current_purchasedelectricities | Grid electricity | Scope 2 |
| v_current_renewableelectricities | Renewable energy | Scope 2 |
| v_current_wastegenerations | Waste disposal | Scope 3 |
| v_current_flighttravels | Business travel - air | Scope 3 |
| v_current_groundtravels | Business travel - land | Scope 3 |
| v_current_seatravels | Business travel - sea | Scope 3 |
| v_current_rawmaterials | Purchased goods | Scope 3 |
| v_current_capitalgoods | Capital assets | Scope 3 |
| v_current_upstreamtransportations | Supply chain transport | Scope 3 |
| v_current_employeecommutings | Employee commuting | Scope 3 |
| v_current_accomodations | Business travel lodging | Scope 3 |
| v_current_endoflifetreatments | End of life treatment | Scope 3 |
| v_emissions_summary_by_scope | Aggregate summary | Analytics |

### rawmp Schema Views (Pre-flattened Columns)
| View Name | Purpose | Notes |
|-----------|---------|-------|
| v_current_users | User master data | Has site_id not company_id |
| v_current_companies | Company master data | Has baseline_year, city |
| v_current_sites | Site/Facility master data | No company_id column |
| v_current_stationarycombustions | Stationary combustion | Full column set |
| v_current_purchasedelectricities | Purchased electricity | consumption→activity aliased |
| v_current_renewableelectricities | Renewable electricity | consumption→activity aliased |

---

## Collection Count Comparison

| Collection | MongoDB Count | DuckDB Count | Difference | Status |
|------------|---------------|--------------|------------|--------|
| stationarycombustions | 184 | 183 | 1 | ⚠️ |
| purchasedelectricities | 166 | 165 | 1 | ⚠️ |
| users | 73 | 72 | 1 | ⚠️ |
| sites | 12 | 11 | 1 | ⚠️ |
| companies | 6 | 5 | 1 | ⚠️ |
| mobilecombustions | 110 | 109 | 1 | ⚠️ |
| fugitiveemissions | 91 | 90 | 1 | ⚠️ |
| renewableelectricities | 57 | 56 | 1 | ⚠️ |
| wastegenerations | 14 | 13 | 1 | ⚠️ |

---

## Root Cause Analysis

The difference of **exactly 1 record per collection** is caused by the **incremental sync start point**.

### Example: stationarycombustions

- **MongoDB earliest record ID:** `688cd2facb5199ba2c3d810b` (created: August 1, 2025)
- **DuckDB earliest record ID:** `689319def66c4680bf8736bd` (created: August 6, 2025)
- **Missing record:** The record created on August 1, 2025 was created BEFORE Meltano began tracking the change stream

This is expected behavior with **incremental replication** using MongoDB Change Streams. Records created before the sync was initialized are not captured automatically.

---

## Year Breakdown Comparison (stationarycombustions)

| Year | MongoDB Count | DuckDB Count | Match |
|------|---------------|--------------|-------|
| 2023 | 25 | 24 | ❌ (-1) |
| 2024 | 62 | 62 | ✅ |
| 2025 | 97 | 97 | ✅ |

---

## Schema Consistency Check

| Comparison | Result |
|------------|--------|
| rawjd vs rawmp (stationarycombustions) | ✅ 183 = 183 |
| rawjd vs rawmp (users) | ✅ 72 = 72 |
| Cross-schema emissions totals | ✅ All match |

---

## Emissions Totals Verification

| Metric | DuckDB rawmp | DuckDB rawjd | Match |
|--------|--------------|--------------|-------|
| Stationary Scope 1 Total | 141,541.75 | 141,541.75 | ✅ |
| Purchased Scope 2 Total | 2,844,995.57 | 2,844,995.57 | ✅ |
| Renewable Scope 2 Total | 18,658.58 | 18,658.58 | ✅ |

---

## Recommendations

1. **For complete sync:** Run a full load (non-incremental) to capture all historical records
2. **Alternative:** Manually backfill the missing records from before the sync start date
3. **Going forward:** The incremental sync will capture all new changes correctly

---

## Technical Details

- **Meltano Jobs Used:** `mongo-duckdb-mapped`, `mongo-duckdb-jsondocs`
- **DuckDB Schemas:** `rawmp` (mapped/flattened), `rawjd` (raw JSON documents)
- **Tap Configuration:** `tap-mongodb` with envelope serialization strategy
- **Replication Method:** Incremental via MongoDB Change Streams
- **CDC Handling:** QUALIFY ROW_NUMBER() deduplication for updates

---

## Verified With

- **MongoDB MCP:** Direct source database queries
- **DuckDB CLI:** Data warehouse queries
- **Meltano:** ELT orchestration logs
