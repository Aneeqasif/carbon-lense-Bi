-- =============================================================================
-- Cross-Schema Comparison Script
-- Compares data between rawmp (mapped) and rawjd (json docs) schemas
-- =============================================================================
-- This script verifies that both ingestion approaches produce consistent
-- results. Run this after the views are created in rawjd schema.
-- =============================================================================

-- =============================================================================
-- SECTION 1: Row Count Comparison (Core Tables)
-- =============================================================================
.print '============================================='
.print 'SECTION 1: Row Count Comparison'
.print '============================================='

SELECT 'Comparing row counts between schemas' as section;

WITH mapped_counts AS (
    SELECT 'stationarycombustions' as table_name, count(*) as mapped_count 
    FROM rawmp.carbonlens_stationarycombustions WHERE _sdc_deleted_at IS NULL UNION ALL
    SELECT 'purchasedelectricities', count(*) 
    FROM rawmp.carbonlens_purchasedelectricities WHERE _sdc_deleted_at IS NULL UNION ALL
    SELECT 'renewableelectricities', count(*) 
    FROM rawmp.carbonlens_renewableelectricities WHERE _sdc_deleted_at IS NULL UNION ALL
    SELECT 'wastegenerations', count(*) 
    FROM rawmp.carbonlens_wastegenerations WHERE _sdc_deleted_at IS NULL UNION ALL
    SELECT 'rawmaterials', count(*) 
    FROM rawmp.carbonlens_rawmaterials WHERE _sdc_deleted_at IS NULL UNION ALL
    SELECT 'flighttravels', count(*) 
    FROM rawmp.carbonlens_flighttravels WHERE _sdc_deleted_at IS NULL UNION ALL
    SELECT 'groundtravels', count(*) 
    FROM rawmp.carbonlens_groundtravels WHERE _sdc_deleted_at IS NULL UNION ALL
    SELECT 'seatravels', count(*) 
    FROM rawmp.carbonlens_seatravels WHERE _sdc_deleted_at IS NULL UNION ALL
    SELECT 'users', count(*) 
    FROM rawmp.carbonlens_users WHERE _sdc_deleted_at IS NULL UNION ALL
    SELECT 'companies', count(*) 
    FROM rawmp.carbonlens_companies WHERE _sdc_deleted_at IS NULL UNION ALL
    SELECT 'sites', count(*) 
    FROM rawmp.carbonlens_sites WHERE _sdc_deleted_at IS NULL
),
direct_counts AS (
    SELECT 'stationarycombustions' as table_name, count(*) as direct_count 
    FROM rawjd.v_current_stationarycombustions UNION ALL
    SELECT 'purchasedelectricities', count(*) 
    FROM rawjd.v_current_purchasedelectricities UNION ALL
    SELECT 'renewableelectricities', count(*) 
    FROM rawjd.v_current_renewableelectricities UNION ALL
    SELECT 'wastegenerations', count(*) 
    FROM rawjd.v_current_wastegenerations UNION ALL
    SELECT 'rawmaterials', count(*) 
    FROM rawjd.v_current_rawmaterials UNION ALL
    SELECT 'flighttravels', count(*) 
    FROM rawjd.v_current_flighttravels UNION ALL
    SELECT 'groundtravels', count(*) 
    FROM rawjd.v_current_groundtravels UNION ALL
    SELECT 'seatravels', count(*) 
    FROM rawjd.v_current_seatravels UNION ALL
    SELECT 'users', count(*) 
    FROM rawjd.v_current_users UNION ALL
    SELECT 'companies', count(*) 
    FROM rawjd.v_current_companies UNION ALL
    SELECT 'sites', count(*) 
    FROM rawjd.v_current_sites
)
SELECT 
    m.table_name,
    m.mapped_count,
    d.direct_count,
    m.mapped_count - d.direct_count as difference,
    CASE 
        WHEN m.mapped_count = d.direct_count THEN '✓ MATCH'
        ELSE '✗ MISMATCH'
    END as status
FROM mapped_counts m
JOIN direct_counts d ON m.table_name = d.table_name
ORDER BY m.table_name;

-- =============================================================================
-- SECTION 2: Emissions Total Comparison (Critical Metrics)
-- =============================================================================
.print ''
.print '============================================='
.print 'SECTION 2: Emissions Total Comparison'
.print '============================================='

SELECT 'Comparing total emissions between schemas' as section;

WITH mapped_emissions AS (
    SELECT 'stationary_scope1' as metric, round(sum(calculated_emission), 2) as mapped_total
    FROM rawmp.carbonlens_stationarycombustions WHERE _sdc_deleted_at IS NULL UNION ALL
    SELECT 'purchased_scope2', round(sum(calculated_emission), 2)
    FROM rawmp.carbonlens_purchasedelectricities WHERE _sdc_deleted_at IS NULL UNION ALL
    SELECT 'renewable_scope2', round(sum(calculated_emission), 2)
    FROM rawmp.carbonlens_renewableelectricities WHERE _sdc_deleted_at IS NULL
),
direct_emissions AS (
    SELECT 'stationary_scope1' as metric, round(sum(calculated_emission), 2) as direct_total
    FROM rawjd.v_current_stationarycombustions UNION ALL
    SELECT 'purchased_scope2', round(sum(calculated_emission), 2)
    FROM rawjd.v_current_purchasedelectricities UNION ALL
    SELECT 'renewable_scope2', round(sum(calculated_emission), 2)
    FROM rawjd.v_current_renewableelectricities
)
SELECT 
    m.metric,
    m.mapped_total,
    d.direct_total,
    round(m.mapped_total - d.direct_total, 2) as difference,
    CASE 
        WHEN abs(m.mapped_total - d.direct_total) < 0.01 THEN '✓ MATCH'
        ELSE '✗ MISMATCH'
    END as status
FROM mapped_emissions m
JOIN direct_emissions d ON m.metric = d.metric
ORDER BY m.metric;

-- =============================================================================
-- SECTION 3: Emissions by Year Comparison
-- =============================================================================
.print ''
.print '============================================='
.print 'SECTION 3: Emissions by Year Comparison'
.print '============================================='

SELECT 'Stationary combustion emissions by year comparison' as section;

WITH mapped_by_year AS (
    SELECT 
        year,
        count(*) as mapped_records,
        round(sum(calculated_emission), 2) as mapped_tco2e
    FROM rawmp.carbonlens_stationarycombustions 
    WHERE _sdc_deleted_at IS NULL
    GROUP BY year
),
direct_by_year AS (
    SELECT 
        year,
        count(*) as direct_records,
        round(sum(calculated_emission), 2) as direct_tco2e
    FROM rawjd.v_current_stationarycombustions
    GROUP BY year
)
SELECT 
    COALESCE(m.year, d.year) as year,
    m.mapped_records,
    d.direct_records,
    m.mapped_tco2e,
    d.direct_tco2e,
    CASE 
        WHEN m.mapped_records = d.direct_records 
            AND abs(COALESCE(m.mapped_tco2e, 0) - COALESCE(d.direct_tco2e, 0)) < 0.01 
        THEN '✓ MATCH'
        ELSE '✗ MISMATCH'
    END as status
FROM mapped_by_year m
FULL OUTER JOIN direct_by_year d ON m.year = d.year
ORDER BY year;

-- =============================================================================
-- SECTION 4: Missing Records Detection
-- =============================================================================
.print ''
.print '============================================='
.print 'SECTION 4: Missing Records Detection'
.print '============================================='

SELECT 'Records in mapped but not in direct (stationarycombustions)' as section;

SELECT 
    'In mapped, not in direct' as direction,
    count(*) as missing_count
FROM rawmp.carbonlens_stationarycombustions m
LEFT JOIN rawjd.v_current_stationarycombustions d ON m._id = d._id
WHERE d._id IS NULL AND m._sdc_deleted_at IS NULL;

SELECT 
    'In direct, not in mapped' as direction,
    count(*) as missing_count
FROM rawjd.v_current_stationarycombustions d
LEFT JOIN rawmp.carbonlens_stationarycombustions m ON d._id = m._id
WHERE m._id IS NULL;

-- Show actual missing IDs if any (limit to 10)
SELECT 'Sample missing IDs (mapped -> direct)' as section;
SELECT 
    m._id as missing_id,
    m.year,
    m.month,
    m.fuel_type
FROM rawmp.carbonlens_stationarycombustions m
LEFT JOIN rawjd.v_current_stationarycombustions d ON m._id = d._id
WHERE d._id IS NULL AND m._sdc_deleted_at IS NULL
LIMIT 10;

SELECT 'Sample missing IDs (direct -> mapped)' as section;
SELECT 
    d._id as missing_id,
    d.year,
    d.month,
    d.fuel_type
FROM rawjd.v_current_stationarycombustions d
LEFT JOIN rawmp.carbonlens_stationarycombustions m ON d._id = m._id
WHERE m._id IS NULL
LIMIT 10;

-- =============================================================================
-- SECTION 5: User/Company/Site Referential Integrity Comparison
-- =============================================================================
.print ''
.print '============================================='
.print 'SECTION 5: Referential Integrity Comparison'
.print '============================================='

SELECT 'Comparing referential integrity between schemas' as section;

-- Users referenced in emissions
WITH mapped_users AS (
    SELECT DISTINCT user_id FROM rawmp.carbonlens_stationarycombustions WHERE _sdc_deleted_at IS NULL
),
direct_users AS (
    SELECT DISTINCT user_id FROM rawjd.v_current_stationarycombustions
)
SELECT 
    'unique_user_ids_in_emissions' as metric,
    (SELECT count(*) FROM mapped_users) as mapped_count,
    (SELECT count(*) FROM direct_users) as direct_count;

-- Sites referenced in emissions
WITH mapped_sites AS (
    SELECT DISTINCT site_id FROM rawmp.carbonlens_stationarycombustions WHERE _sdc_deleted_at IS NULL
),
direct_sites AS (
    SELECT DISTINCT site_id FROM rawjd.v_current_stationarycombustions
)
SELECT 
    'unique_site_ids_in_emissions' as metric,
    (SELECT count(*) FROM mapped_sites) as mapped_count,
    (SELECT count(*) FROM direct_sites) as direct_count;

-- =============================================================================
-- SECTION 6: Summary Report
-- =============================================================================
.print ''
.print '============================================='
.print 'SECTION 6: Summary Report'
.print '============================================='

SELECT 'Overall comparison summary' as section;

WITH comparison AS (
    SELECT 
        'stationarycombustions' as table_name,
        (SELECT count(*) FROM rawmp.carbonlens_stationarycombustions WHERE _sdc_deleted_at IS NULL) as mapped,
        (SELECT count(*) FROM rawjd.v_current_stationarycombustions) as direct
    UNION ALL
    SELECT 
        'purchasedelectricities',
        (SELECT count(*) FROM rawmp.carbonlens_purchasedelectricities WHERE _sdc_deleted_at IS NULL),
        (SELECT count(*) FROM rawjd.v_current_purchasedelectricities)
    UNION ALL
    SELECT 
        'renewableelectricities',
        (SELECT count(*) FROM rawmp.carbonlens_renewableelectricities WHERE _sdc_deleted_at IS NULL),
        (SELECT count(*) FROM rawjd.v_current_renewableelectricities)
    UNION ALL
    SELECT 
        'users',
        (SELECT count(*) FROM rawmp.carbonlens_users WHERE _sdc_deleted_at IS NULL),
        (SELECT count(*) FROM rawjd.v_current_users)
    UNION ALL
    SELECT 
        'sites',
        (SELECT count(*) FROM rawmp.carbonlens_sites WHERE _sdc_deleted_at IS NULL),
        (SELECT count(*) FROM rawjd.v_current_sites)
    UNION ALL
    SELECT 
        'companies',
        (SELECT count(*) FROM rawmp.carbonlens_companies WHERE _sdc_deleted_at IS NULL),
        (SELECT count(*) FROM rawjd.v_current_companies)
)
SELECT 
    (SELECT count(*) FROM comparison WHERE mapped = direct) as matching_tables,
    (SELECT count(*) FROM comparison WHERE mapped != direct) as mismatched_tables,
    (SELECT count(*) FROM comparison) as total_tables,
    CASE 
        WHEN (SELECT count(*) FROM comparison WHERE mapped != direct) = 0 
        THEN '✓ ALL TABLES MATCH'
        ELSE '✗ SOME TABLES MISMATCH'
    END as overall_status;

.print ''
.print '============================================='
.print 'Cross-Schema Comparison Complete'
.print '============================================='
