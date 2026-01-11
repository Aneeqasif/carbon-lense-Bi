# MongoDB → DuckDB Pipeline Summary

## Overview
Pipeline extracts data from MongoDB (carbonLens) and loads to DuckDB/MotherDuck using Meltano.

**Approach**: Direct JSON document storage (`jsondocs`) - simpler, preserves MongoDB document structure.

**Replication Mode**: INCREMENTAL - syncs all data each run, reliable for scheduled batch loads.

## Environments
| Environment | Target | Use Case |
|-------------|--------|----------|
| `dev` | Local DuckDB file | Development, local testing |
| `prod` | MotherDuck cloud | Production, Docker deployment |

---

## Quick Start

```bash
# Local development (dev environment)
meltano --environment=dev run tap-mongodb--jsondocs target-duckdb--jsondocs --force

# Production / Docker (prod environment - MotherDuck)
docker compose --profile sync run --rm meltano-sync

# Clear state for fresh start
meltano state clear dev:tap-mongodb--jsondocs-to-target-duckdb--jsondocs --force
```

---

## Testing Results

### ✅ Incremental Load

| Test | Local | Docker + MotherDuck |
|------|-------|---------------------|
| Connection to MongoDB | ✅ | ✅ (via `host.docker.internal`) |
| All 40 collections synced | ✅ | ✅ |
| Record counts match | ✅ | ✅ |

**Sample counts**: 80 users, 183 stationarycombustions, 31 units, 5 companies

### ✅ Docker Containerization

**Key configurations for Docker:**
1. MongoDB connection requires `directConnection=true` (bypasses replica set host discovery)
2. Use `host.docker.internal` instead of `localhost` for MongoDB host
3. Linux Docker needs `extra_hosts: ["host.docker.internal:host-gateway"]`
4. Environment variable naming for inherited plugins: `TAP_MONGODB__JSONDOCS_*` (double underscore before suffix)

---

## Architecture Decision: INCREMENTAL Mode

We use **INCREMENTAL mode** instead of CDC (LOG_BASED) for the following reasons:

### Why Not CDC?
- tap-mongodb processes collections **sequentially** with 10-second idle timeouts
- With 40 collections, changes are often missed (only captured during narrow sync windows)
- Collection-level change streams don't scale well for multi-collection databases

### INCREMENTAL Benefits
- ✅ **Reliable** - Always captures all data
- ✅ **Simple** - No complex bookmark management
- ✅ **Predictable** - Same behavior every run
- ✅ **Suitable for scheduled syncs** - Run every 15-60 minutes

### If Real-time CDC is Needed
Consider alternatives:
- Debezium with Kafka Connect
- MongoDB Atlas Triggers
- Custom change stream application with `database.watch()`

---

## Project Structure

```
mongo-pg/
├── meltano.yml              # Main config with environments
├── docker-compose.yml       # Docker services
├── extract/
│   └── mongo-extract-jsondocs.yml   # MongoDB tap config
├── load/
│   └── duckdb-loader.yml    # DuckDB target config
├── output/duckdb/           # Local DuckDB files (dev)
└── docs/
    └── PIPELINE_TESTING_SUMMARY.md
```

---

*Last updated: January 12, 2026*
