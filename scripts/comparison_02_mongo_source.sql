-- =============================================================================
-- MongoDB Source vs DuckDB Comparison Script
-- Compares data counts between MongoDB source and DuckDB targets
-- =============================================================================
-- USAGE: 
-- 1. First run this script to get DuckDB counts
-- 2. Compare with MongoDB counts (use mongosh or MongoDB MCP tools)
-- 
-- MongoDB commands to get comparable counts:
--   db.stationarycombustions.countDocuments({})
--   db.purchasedelectricities.countDocuments({})
--   db.renewableelectricities.countDocuments({})
--   db.users.countDocuments({})
--   db.companies.countDocuments({})
--   db.sites.countDocuments({})
-- =============================================================================

-- =============================================================================
-- SECTION 1: DuckDB Counts for Comparison with MongoDB
-- =============================================================================
.print '============================================='
.print 'SECTION 1: DuckDB Row Counts for MongoDB Comparison'
.print '============================================='

SELECT 'DuckDB counts (compare with MongoDB countDocuments)' as section;

-- Mapped schema counts (should match MongoDB document counts)
SELECT 
    'rawmp (mapped)' as schema,
    'stationarycombustions' as collection,
    count(*) as duckdb_count,
    '-- Compare with: db.stationarycombustions.countDocuments({})' as mongo_cmd
FROM rawmp.carbonlens_stationarycombustions 
WHERE _sdc_deleted_at IS NULL

UNION ALL

SELECT 
    'rawmp (mapped)',
    'purchasedelectricities',
    count(*),
    '-- Compare with: db.purchasedelectricities.countDocuments({})'
FROM rawmp.carbonlens_purchasedelectricities 
WHERE _sdc_deleted_at IS NULL

UNION ALL

SELECT 
    'rawmp (mapped)',
    'renewableelectricities',
    count(*),
    '-- Compare with: db.renewableelectricities.countDocuments({})'
FROM rawmp.carbonlens_renewableelectricities 
WHERE _sdc_deleted_at IS NULL

UNION ALL

SELECT 
    'rawmp (mapped)',
    'fugitiveemissions',
    (SELECT count(*) FROM rawjd.fugitiveemissions WHERE _sdc_deleted_at IS NULL),
    '-- Compare with: db.fugitiveemissions.countDocuments({})'

UNION ALL

SELECT 
    'rawmp (mapped)',
    'mobilecombustions',
    (SELECT count(*) FROM rawjd.mobilecombustions WHERE _sdc_deleted_at IS NULL),
    '-- Compare with: db.mobilecombustions.countDocuments({})'

UNION ALL

SELECT 
    'rawmp (mapped)',
    'wastegenerations',
    count(*),
    '-- Compare with: db.wastegenerations.countDocuments({})'
FROM rawmp.carbonlens_wastegenerations 
WHERE _sdc_deleted_at IS NULL

UNION ALL

SELECT 
    'rawmp (mapped)',
    'users',
    count(*),
    '-- Compare with: db.users.countDocuments({})'
FROM rawmp.carbonlens_users 
WHERE _sdc_deleted_at IS NULL

UNION ALL

SELECT 
    'rawmp (mapped)',
    'companies',
    count(*),
    '-- Compare with: db.companies.countDocuments({})'
FROM rawmp.carbonlens_companies 
WHERE _sdc_deleted_at IS NULL

UNION ALL

SELECT 
    'rawmp (mapped)',
    'sites',
    count(*),
    '-- Compare with: db.sites.countDocuments({})'
FROM rawmp.carbonlens_sites 
WHERE _sdc_deleted_at IS NULL

ORDER BY collection;

-- =============================================================================
-- SECTION 2: Emissions Aggregation for MongoDB Comparison
-- =============================================================================
.print ''
.print '============================================='
.print 'SECTION 2: Emissions Aggregations for MongoDB Comparison'
.print '============================================='

SELECT 'DuckDB aggregations (compare with MongoDB aggregation)' as section;

-- Total emissions by year (Stationary Combustion)
-- MongoDB: db.stationarycombustions.aggregate([
--   { $group: { _id: "$year", total: { $sum: "$calculatedEmission" }, count: { $sum: 1 } } },
--   { $sort: { _id: 1 } }
-- ])
SELECT 
    'stationarycombustions' as collection,
    year,
    count(*) as doc_count,
    round(sum(calculated_emission), 2) as total_emission
FROM rawmp.carbonlens_stationarycombustions 
WHERE _sdc_deleted_at IS NULL
GROUP BY year
ORDER BY year;

-- Total emissions by year (Purchased Electricity)
-- MongoDB: db.purchasedelectricities.aggregate([
--   { $group: { _id: "$year", total: { $sum: "$calculatedEmission" }, count: { $sum: 1 } } },
--   { $sort: { _id: 1 } }
-- ])
SELECT 
    'purchasedelectricities' as collection,
    year,
    count(*) as doc_count,
    round(sum(calculated_emission), 2) as total_emission
FROM rawmp.carbonlens_purchasedelectricities 
WHERE _sdc_deleted_at IS NULL
GROUP BY year
ORDER BY year;

-- =============================================================================
-- SECTION 3: Site Distribution for MongoDB Comparison
-- =============================================================================
.print ''
.print '============================================='
.print 'SECTION 3: Records by Site for MongoDB Comparison'
.print '============================================='

SELECT 'Stationary combustion distribution by site' as section;

-- MongoDB: db.stationarycombustions.aggregate([
--   { $group: { _id: "$siteId", count: { $sum: 1 }, total: { $sum: "$calculatedEmission" } } },
--   { $sort: { count: -1 } }
-- ])
SELECT 
    site_id,
    s.site_name,
    count(*) as doc_count,
    round(sum(sc.calculated_emission), 2) as total_emission
FROM rawmp.carbonlens_stationarycombustions sc
LEFT JOIN rawmp.carbonlens_sites s ON sc.site_id = s._id
WHERE sc._sdc_deleted_at IS NULL
GROUP BY site_id, s.site_name
ORDER BY doc_count DESC;

-- =============================================================================
-- SECTION 4: Distinct Values Check
-- =============================================================================
.print ''
.print '============================================='
.print 'SECTION 4: Distinct Values for MongoDB Comparison'
.print '============================================='

SELECT 'Distinct value counts (compare with MongoDB distinct)' as section;

-- MongoDB: db.stationarycombustions.distinct("year").length
-- MongoDB: db.stationarycombustions.distinct("fuelType").length
SELECT 
    'stationarycombustions' as collection,
    'distinct_years' as field,
    count(DISTINCT year) as count
FROM rawmp.carbonlens_stationarycombustions WHERE _sdc_deleted_at IS NULL

UNION ALL

SELECT 
    'stationarycombustions',
    'distinct_fuel_types',
    count(DISTINCT fuel_type)
FROM rawmp.carbonlens_stationarycombustions WHERE _sdc_deleted_at IS NULL

UNION ALL

SELECT 
    'stationarycombustions',
    'distinct_sites',
    count(DISTINCT site_id)
FROM rawmp.carbonlens_stationarycombustions WHERE _sdc_deleted_at IS NULL

UNION ALL

SELECT 
    'stationarycombustions',
    'distinct_users',
    count(DISTINCT user_id)
FROM rawmp.carbonlens_stationarycombustions WHERE _sdc_deleted_at IS NULL

UNION ALL

SELECT 
    'users',
    'distinct_roles',
    count(DISTINCT role)
FROM rawmp.carbonlens_users WHERE _sdc_deleted_at IS NULL

ORDER BY collection, field;

-- =============================================================================
-- SECTION 5: Sample Unique IDs for Spot Checking
-- =============================================================================
.print ''
.print '============================================='
.print 'SECTION 5: Sample IDs for MongoDB Spot Checking'
.print '============================================='

SELECT 'Sample document IDs to verify in MongoDB' as section;

-- Get sample IDs to spot check in MongoDB
-- MongoDB: db.stationarycombustions.findOne({ _id: ObjectId("<id>") })
SELECT 
    'stationarycombustions' as collection,
    _id,
    year,
    month,
    round(calculated_emission, 2) as emission
FROM rawmp.carbonlens_stationarycombustions 
WHERE _sdc_deleted_at IS NULL
ORDER BY random()
LIMIT 5;

SELECT 
    'users' as collection,
    _id,
    name,
    email
FROM rawmp.carbonlens_users 
WHERE _sdc_deleted_at IS NULL
ORDER BY random()
LIMIT 5;

.print ''
.print '============================================='
.print 'MongoDB Comparison Script Complete'
.print '============================================='
.print ''
.print 'To compare with MongoDB, run these commands in mongosh:'
.print '  use carbonLens'
.print '  db.stationarycombustions.countDocuments({})'
.print '  db.purchasedelectricities.countDocuments({})'
.print '  db.users.countDocuments({})'
.print ''
