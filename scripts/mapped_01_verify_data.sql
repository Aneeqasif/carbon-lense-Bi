-- =============================================================================
-- Mapped Schema - Data Verification Script
-- Schema: rawmp (pre-processed mapped data)
-- =============================================================================
-- This script runs verification queries on the rawmp schema to validate
-- data integrity after ingestion. The mapped schema has already flattened
-- JSON into columns via Meltano mapper, so no views are needed.
-- =============================================================================

-- =============================================================================
-- SECTION 1: Table Row Counts
-- =============================================================================
.print '============================================='
.print 'SECTION 1: Table Row Counts (rawmp schema)'
.print '============================================='

SELECT 'Mapped Table Counts' as section;

SELECT 'carbonlens_stationarycombustions' as table_name, count(*) as row_count FROM rawmp.carbonlens_stationarycombustions UNION ALL
SELECT 'carbonlens_purchasedelectricities', count(*) FROM rawmp.carbonlens_purchasedelectricities UNION ALL
SELECT 'carbonlens_renewableelectricities', count(*) FROM rawmp.carbonlens_renewableelectricities UNION ALL
SELECT 'carbonlens_wastegenerations', count(*) FROM rawmp.carbonlens_wastegenerations UNION ALL
SELECT 'carbonlens_rawmaterials', count(*) FROM rawmp.carbonlens_rawmaterials UNION ALL
SELECT 'carbonlens_flighttravels', count(*) FROM rawmp.carbonlens_flighttravels UNION ALL
SELECT 'carbonlens_groundtravels', count(*) FROM rawmp.carbonlens_groundtravels UNION ALL
SELECT 'carbonlens_seatravels', count(*) FROM rawmp.carbonlens_seatravels UNION ALL
SELECT 'carbonlens_accomodations', count(*) FROM rawmp.carbonlens_accomodations UNION ALL
SELECT 'carbonlens_employeecommutings', count(*) FROM rawmp.carbonlens_employeecommutings UNION ALL
SELECT 'carbonlens_upstreamtransportations', count(*) FROM rawmp.carbonlens_upstreamtransportations UNION ALL
SELECT 'carbonlens_dtds', count(*) FROM rawmp.carbonlens_dtds UNION ALL
SELECT 'carbonlens_capitalgoods', count(*) FROM rawmp.carbonlens_capitalgoods UNION ALL
SELECT 'carbonlens_endoflifetreatments', count(*) FROM rawmp.carbonlens_endoflifetreatments UNION ALL
SELECT 'carbonlens_packagings', count(*) FROM rawmp.carbonlens_packagings UNION ALL
SELECT 'carbonlens_services', count(*) FROM rawmp.carbonlens_services UNION ALL
SELECT 'carbonlens_ferastationaries', count(*) FROM rawmp.carbonlens_ferastationaries UNION ALL
SELECT 'carbonlens_feramobiles', count(*) FROM rawmp.carbonlens_feramobiles UNION ALL
SELECT 'carbonlens_feraelectricities', count(*) FROM rawmp.carbonlens_feraelectricities UNION ALL
SELECT 'carbonlens_users', count(*) FROM rawmp.carbonlens_users UNION ALL
SELECT 'carbonlens_companies', count(*) FROM rawmp.carbonlens_companies UNION ALL
SELECT 'carbonlens_sites', count(*) FROM rawmp.carbonlens_sites
ORDER BY table_name;

-- =============================================================================
-- SECTION 2: Deleted Records Check
-- =============================================================================
.print ''
.print '============================================='
.print 'SECTION 2: Deleted Records Check'
.print '============================================='

SELECT 'Soft-deleted records (via CDC)' as section;

SELECT 'carbonlens_stationarycombustions' as table_name, count(*) as deleted_count 
FROM rawmp.carbonlens_stationarycombustions WHERE _sdc_deleted_at IS NOT NULL UNION ALL
SELECT 'carbonlens_purchasedelectricities', count(*) 
FROM rawmp.carbonlens_purchasedelectricities WHERE _sdc_deleted_at IS NOT NULL UNION ALL
SELECT 'carbonlens_users', count(*) 
FROM rawmp.carbonlens_users WHERE _sdc_deleted_at IS NOT NULL UNION ALL
SELECT 'carbonlens_companies', count(*) 
FROM rawmp.carbonlens_companies WHERE _sdc_deleted_at IS NOT NULL UNION ALL
SELECT 'carbonlens_sites', count(*) 
FROM rawmp.carbonlens_sites WHERE _sdc_deleted_at IS NOT NULL
ORDER BY table_name;

-- =============================================================================
-- SECTION 3: Total Emissions by Year (Scope 1, 2, 3)
-- =============================================================================
.print ''
.print '============================================='
.print 'SECTION 3: Total Emissions by Year (tCO2e)'
.print '============================================='

SELECT 'Emissions by source and year' as section;

-- Stationary Combustion (Scope 1) by Year
SELECT 
    'Stationary Combustion' as emission_type,
    'Scope 1' as scope,
    year,
    count(*) as records,
    round(sum(calculated_emission), 2) as total_tco2e
FROM rawmp.carbonlens_stationarycombustions
WHERE _sdc_deleted_at IS NULL
GROUP BY year
ORDER BY year;

-- Purchased Electricity (Scope 2) by Year
SELECT 
    'Purchased Electricity' as emission_type,
    'Scope 2' as scope,
    year,
    count(*) as records,
    round(sum(calculated_emission), 2) as total_tco2e
FROM rawmp.carbonlens_purchasedelectricities
WHERE _sdc_deleted_at IS NULL
GROUP BY year
ORDER BY year;

-- Renewable Electricity (Scope 2) by Year
SELECT 
    'Renewable Electricity' as emission_type,
    'Scope 2' as scope,
    year,
    count(*) as records,
    round(sum(calculated_emission), 2) as total_tco2e
FROM rawmp.carbonlens_renewableelectricities
WHERE _sdc_deleted_at IS NULL
GROUP BY year
ORDER BY year;

-- =============================================================================
-- SECTION 4: Emissions Summary by Site
-- =============================================================================
.print ''
.print '============================================='
.print 'SECTION 4: Emissions by Site'
.print '============================================='

SELECT 'Stationary combustion emissions by site' as section;

SELECT 
    s.site_name,
    sc.year,
    count(*) as records,
    round(sum(sc.calculated_emission), 2) as total_tco2e,
    round(avg(sc.calculated_emission), 4) as avg_tco2e
FROM rawmp.carbonlens_stationarycombustions sc
LEFT JOIN rawmp.carbonlens_sites s ON sc.site_id = s._id
WHERE sc._sdc_deleted_at IS NULL
GROUP BY s.site_name, sc.year
ORDER BY s.site_name, sc.year;

-- =============================================================================
-- SECTION 5: Fuel Type Analysis
-- =============================================================================
.print ''
.print '============================================='
.print 'SECTION 5: Stationary Combustion by Fuel Type'
.print '============================================='

SELECT 'Emissions breakdown by fuel type' as section;

SELECT 
    fuel_type,
    fuel_state,
    count(*) as records,
    round(sum(activity), 2) as total_activity,
    round(sum(calculated_emission), 2) as total_tco2e,
    round(avg(emission_factor), 6) as avg_emission_factor
FROM rawmp.carbonlens_stationarycombustions
WHERE _sdc_deleted_at IS NULL
GROUP BY fuel_type, fuel_state
ORDER BY total_tco2e DESC;

-- =============================================================================
-- SECTION 6: Company-Level Summary
-- =============================================================================
.print ''
.print '============================================='
.print 'SECTION 6: Emissions by Company'
.print '============================================='

SELECT 'Total emissions by company' as section;

SELECT 
    COALESCE(c.company_name, 'Unknown') as company_name,
    count(DISTINCT sc.site_id) as sites,
    count(sc._id) as records,
    round(sum(sc.calculated_emission), 2) as total_tco2e
FROM rawmp.carbonlens_stationarycombustions sc
LEFT JOIN rawmp.carbonlens_companies c ON sc.company_id = c._id
WHERE sc._sdc_deleted_at IS NULL
GROUP BY c.company_name
ORDER BY total_tco2e DESC;

-- =============================================================================
-- SECTION 7: Data Quality Checks
-- =============================================================================
.print ''
.print '============================================='
.print 'SECTION 7: Data Quality Checks'
.print '============================================='

SELECT 'Data quality issues' as section;

-- Null emission values
SELECT 
    'carbonlens_stationarycombustions' as table_name,
    'null_calculated_emission' as issue,
    count(*) as count
FROM rawmp.carbonlens_stationarycombustions
WHERE calculated_emission IS NULL AND _sdc_deleted_at IS NULL;

-- Null activity values
SELECT 
    'carbonlens_stationarycombustions' as table_name,
    'null_activity' as issue,
    count(*) as count
FROM rawmp.carbonlens_stationarycombustions
WHERE activity IS NULL AND _sdc_deleted_at IS NULL;

-- Orphan site references (site_id not in sites table)
SELECT 
    'carbonlens_stationarycombustions' as table_name,
    'orphan_site_id' as issue,
    count(*) as count
FROM rawmp.carbonlens_stationarycombustions sc
LEFT JOIN rawmp.carbonlens_sites s ON sc.site_id = s._id
WHERE s._id IS NULL AND sc.site_id IS NOT NULL AND sc._sdc_deleted_at IS NULL;

-- Orphan user references (user_id not in users table)
SELECT 
    'carbonlens_stationarycombustions' as table_name,
    'orphan_user_id' as issue,
    count(*) as count
FROM rawmp.carbonlens_stationarycombustions sc
LEFT JOIN rawmp.carbonlens_users u ON sc.user_id = u._id
WHERE u._id IS NULL AND sc.user_id IS NOT NULL AND sc._sdc_deleted_at IS NULL;

-- Negative emissions (data anomaly)
SELECT 
    'carbonlens_stationarycombustions' as table_name,
    'negative_emission' as issue,
    count(*) as count
FROM rawmp.carbonlens_stationarycombustions
WHERE calculated_emission < 0 AND _sdc_deleted_at IS NULL;

-- =============================================================================
-- SECTION 8: Approval Status Check
-- =============================================================================
.print ''
.print '============================================='
.print 'SECTION 8: Approval Status'
.print '============================================='

SELECT 'Records by approval status' as section;

SELECT 
    'carbonlens_stationarycombustions' as table_name,
    approved,
    count(*) as count
FROM rawmp.carbonlens_stationarycombustions
WHERE _sdc_deleted_at IS NULL
GROUP BY approved
ORDER BY approved;

-- =============================================================================
-- SECTION 9: Monthly Distribution
-- =============================================================================
.print ''
.print '============================================='
.print 'SECTION 9: Monthly Distribution (2025)'
.print '============================================='

SELECT 'Emissions by month for 2025' as section;

SELECT 
    month,
    count(*) as records,
    round(sum(calculated_emission), 2) as total_tco2e
FROM rawmp.carbonlens_stationarycombustions
WHERE year = '2025' AND _sdc_deleted_at IS NULL
GROUP BY month
ORDER BY 
    CASE month
        WHEN 'January' THEN 1
        WHEN 'February' THEN 2
        WHEN 'March' THEN 3
        WHEN 'April' THEN 4
        WHEN 'May' THEN 5
        WHEN 'June' THEN 6
        WHEN 'July' THEN 7
        WHEN 'August' THEN 8
        WHEN 'September' THEN 9
        WHEN 'October' THEN 10
        WHEN 'November' THEN 11
        WHEN 'December' THEN 12
    END;

-- =============================================================================
-- SECTION 10: Data Freshness
-- =============================================================================
.print ''
.print '============================================='
.print 'SECTION 10: Data Freshness'
.print '============================================='

SELECT 'Latest extraction timestamps per table' as section;

SELECT 
    'carbonlens_stationarycombustions' as table_name,
    max(_sdc_extracted_at) as last_extracted,
    min(_sdc_extracted_at) as first_extracted
FROM rawmp.carbonlens_stationarycombustions UNION ALL
SELECT 
    'carbonlens_purchasedelectricities',
    max(_sdc_extracted_at),
    min(_sdc_extracted_at)
FROM rawmp.carbonlens_purchasedelectricities UNION ALL
SELECT 
    'carbonlens_users',
    max(_sdc_extracted_at),
    min(_sdc_extracted_at)
FROM rawmp.carbonlens_users UNION ALL
SELECT 
    'carbonlens_companies',
    max(_sdc_extracted_at),
    min(_sdc_extracted_at)
FROM rawmp.carbonlens_companies UNION ALL
SELECT 
    'carbonlens_sites',
    max(_sdc_extracted_at),
    min(_sdc_extracted_at)
FROM rawmp.carbonlens_sites
ORDER BY table_name;

.print ''
.print '============================================='
.print 'Verification Complete for rawmp schema'
.print '============================================='
