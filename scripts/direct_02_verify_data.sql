-- =============================================================================
-- Direct JSON Docs Schema - Data Verification Script
-- Schema: rawjd (raw json documents)
-- =============================================================================
-- This script runs verification queries on the rawjd schema to validate
-- data integrity after ingestion. Run this after direct_01_create_views.sql
-- =============================================================================

-- =============================================================================
-- SECTION 1: Raw Table Row Counts (with potential duplicates from CDC)
-- =============================================================================
.print '============================================='
.print 'SECTION 1: Raw Table Row Counts (rawjd schema)'
.print '============================================='

SELECT 'Raw Table Counts (may include CDC duplicates)' as section;

SELECT 'stationarycombustions' as table_name, count(*) as raw_count FROM rawjd.stationarycombustions UNION ALL
SELECT 'purchasedelectricities', count(*) FROM rawjd.purchasedelectricities UNION ALL
SELECT 'renewableelectricities', count(*) FROM rawjd.renewableelectricities UNION ALL
SELECT 'fugitiveemissions', count(*) FROM rawjd.fugitiveemissions UNION ALL
SELECT 'mobilecombustions', count(*) FROM rawjd.mobilecombustions UNION ALL
SELECT 'wastegenerations', count(*) FROM rawjd.wastegenerations UNION ALL
SELECT 'rawmaterials', count(*) FROM rawjd.rawmaterials UNION ALL
SELECT 'flighttravels', count(*) FROM rawjd.flighttravels UNION ALL
SELECT 'groundtravels', count(*) FROM rawjd.groundtravels UNION ALL
SELECT 'seatravels', count(*) FROM rawjd.seatravels UNION ALL
SELECT 'users', count(*) FROM rawjd.users UNION ALL
SELECT 'companies', count(*) FROM rawjd.companies UNION ALL
SELECT 'sites', count(*) FROM rawjd.sites
ORDER BY table_name;

-- =============================================================================
-- SECTION 2: View Row Counts (deduplicated current state)
-- =============================================================================
.print ''
.print '============================================='
.print 'SECTION 2: View Row Counts (deduplicated)'
.print '============================================='

SELECT 'Current State Counts (deduplicated via views)' as section;

SELECT 'v_current_stationarycombustions' as view_name, count(*) as current_count FROM rawjd.v_current_stationarycombustions UNION ALL
SELECT 'v_current_purchasedelectricities', count(*) FROM rawjd.v_current_purchasedelectricities UNION ALL
SELECT 'v_current_renewableelectricities', count(*) FROM rawjd.v_current_renewableelectricities UNION ALL
SELECT 'v_current_fugitiveemissions', count(*) FROM rawjd.v_current_fugitiveemissions UNION ALL
SELECT 'v_current_mobilecombustions', count(*) FROM rawjd.v_current_mobilecombustions UNION ALL
SELECT 'v_current_wastegenerations', count(*) FROM rawjd.v_current_wastegenerations UNION ALL
SELECT 'v_current_rawmaterials', count(*) FROM rawjd.v_current_rawmaterials UNION ALL
SELECT 'v_current_flighttravels', count(*) FROM rawjd.v_current_flighttravels UNION ALL
SELECT 'v_current_groundtravels', count(*) FROM rawjd.v_current_groundtravels UNION ALL
SELECT 'v_current_seatravels', count(*) FROM rawjd.v_current_seatravels UNION ALL
SELECT 'v_current_users', count(*) FROM rawjd.v_current_users UNION ALL
SELECT 'v_current_companies', count(*) FROM rawjd.v_current_companies UNION ALL
SELECT 'v_current_sites', count(*) FROM rawjd.v_current_sites
ORDER BY view_name;

-- =============================================================================
-- SECTION 3: Duplicate Detection (CDC history check)
-- =============================================================================
.print ''
.print '============================================='
.print 'SECTION 3: Duplicate Detection (CDC History)'
.print '============================================='

SELECT 'Documents with multiple versions (CDC updates)' as section;

-- Check for documents with multiple versions in stationarycombustions
SELECT 
    'stationarycombustions' as collection,
    count(*) as docs_with_history,
    sum(version_count) as total_versions
FROM (
    SELECT object_id, count(*) as version_count
    FROM rawjd.stationarycombustions
    GROUP BY object_id
    HAVING count(*) > 1
);

-- Check for documents with multiple versions in users
SELECT 
    'users' as collection,
    count(*) as docs_with_history,
    sum(version_count) as total_versions
FROM (
    SELECT object_id, count(*) as version_count
    FROM rawjd.users
    GROUP BY object_id
    HAVING count(*) > 1
);

-- =============================================================================
-- SECTION 4: Deleted Documents Check
-- =============================================================================
.print ''
.print '============================================='
.print 'SECTION 4: Deleted Documents Check'
.print '============================================='

SELECT 'Deleted documents (soft deletes)' as section;

SELECT 'stationarycombustions' as table_name, count(*) as deleted_count 
FROM rawjd.stationarycombustions WHERE _sdc_deleted_at IS NOT NULL UNION ALL
SELECT 'purchasedelectricities', count(*) 
FROM rawjd.purchasedelectricities WHERE _sdc_deleted_at IS NOT NULL UNION ALL
SELECT 'users', count(*) 
FROM rawjd.users WHERE _sdc_deleted_at IS NOT NULL UNION ALL
SELECT 'companies', count(*) 
FROM rawjd.companies WHERE _sdc_deleted_at IS NOT NULL UNION ALL
SELECT 'sites', count(*) 
FROM rawjd.sites WHERE _sdc_deleted_at IS NOT NULL
ORDER BY table_name;

-- =============================================================================
-- SECTION 5: Emissions Summary by Year (Scope 1 & 2)
-- =============================================================================
.print ''
.print '============================================='
.print 'SECTION 5: Total Emissions by Year (tCO2e)'
.print '============================================='

SELECT 'Total calculated emissions by year and source' as section;

-- Stationary Combustion (Scope 1) by Year
SELECT 
    'Stationary Combustion (Scope 1)' as emission_type,
    year,
    count(*) as records,
    round(sum(calculated_emission), 2) as total_tco2e
FROM rawjd.v_current_stationarycombustions
GROUP BY year
ORDER BY year;

-- Purchased Electricity (Scope 2) by Year
SELECT 
    'Purchased Electricity (Scope 2)' as emission_type,
    year,
    count(*) as records,
    round(sum(calculated_emission), 2) as total_tco2e
FROM rawjd.v_current_purchasedelectricities
GROUP BY year
ORDER BY year;

-- Fugitive Emissions (Scope 1) by Year
SELECT 
    'Fugitive Emissions (Scope 1)' as emission_type,
    year,
    count(*) as records,
    round(sum(calculated_emission), 2) as total_tco2e
FROM rawjd.v_current_fugitiveemissions
GROUP BY year
ORDER BY year;

-- =============================================================================
-- SECTION 6: Site-Level Aggregations
-- =============================================================================
.print ''
.print '============================================='
.print 'SECTION 6: Emissions by Site'
.print '============================================='

SELECT 'Stationary combustion emissions by site' as section;

SELECT 
    s.name as site_name,
    sc.year,
    count(*) as records,
    round(sum(sc.calculated_emission), 2) as total_tco2e
FROM rawjd.v_current_stationarycombustions sc
LEFT JOIN rawjd.v_current_sites s ON sc.site_id = s._id
GROUP BY s.name, sc.year
ORDER BY s.name, sc.year;

-- =============================================================================
-- SECTION 7: Data Quality Checks
-- =============================================================================
.print ''
.print '============================================='
.print 'SECTION 7: Data Quality Checks'
.print '============================================='

SELECT 'Data quality issues' as section;

-- Null emission values in stationarycombustions
SELECT 
    'stationarycombustions' as table_name,
    'null_calculated_emission' as issue,
    count(*) as count
FROM rawjd.v_current_stationarycombustions
WHERE calculated_emission IS NULL;

-- Null activity values in stationarycombustions  
SELECT 
    'stationarycombustions' as table_name,
    'null_activity' as issue,
    count(*) as count
FROM rawjd.v_current_stationarycombustions
WHERE activity IS NULL;

-- Missing site_id references
SELECT 
    'stationarycombustions' as table_name,
    'orphan_site_id' as issue,
    count(*) as count
FROM rawjd.v_current_stationarycombustions sc
LEFT JOIN rawjd.v_current_sites s ON sc.site_id = s._id
WHERE s._id IS NULL AND sc.site_id IS NOT NULL;

-- Missing user_id references
SELECT 
    'stationarycombustions' as table_name,
    'orphan_user_id' as issue,
    count(*) as count
FROM rawjd.v_current_stationarycombustions sc
LEFT JOIN rawjd.v_current_users u ON sc.user_id = u._id
WHERE u._id IS NULL AND sc.user_id IS NOT NULL;

-- =============================================================================
-- SECTION 8: Extraction Timestamps (Data Freshness)
-- =============================================================================
.print ''
.print '============================================='
.print 'SECTION 8: Data Freshness'
.print '============================================='

SELECT 'Latest extraction timestamps per table' as section;

SELECT 
    'stationarycombustions' as table_name,
    max(_sdc_extracted_at) as last_extracted,
    min(_sdc_extracted_at) as first_extracted
FROM rawjd.stationarycombustions UNION ALL
SELECT 
    'purchasedelectricities',
    max(_sdc_extracted_at),
    min(_sdc_extracted_at)
FROM rawjd.purchasedelectricities UNION ALL
SELECT 
    'users',
    max(_sdc_extracted_at),
    min(_sdc_extracted_at)
FROM rawjd.users UNION ALL
SELECT 
    'companies',
    max(_sdc_extracted_at),
    min(_sdc_extracted_at)
FROM rawjd.companies UNION ALL
SELECT 
    'sites',
    max(_sdc_extracted_at),
    min(_sdc_extracted_at)
FROM rawjd.sites
ORDER BY table_name;

.print ''
.print '============================================='
.print 'Verification Complete for rawjd schema'
.print '============================================='
