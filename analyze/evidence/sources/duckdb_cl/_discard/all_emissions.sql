-- Comprehensive emissions data for use in page queries via chaining
-- This extracts all data from v_emissions_wide to be referenced as ${duckdb_cl.all_emissions}
SELECT 
    -- IDs
    id,
    site_id,
    company_id,
    unit_id,
    created_by,
    
    -- Names
    site_name,
    company_name,
    unit_name,
    
    -- Location
    site_country,
    
    -- Time dimensions
    year,
    month,
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
        ELSE 0
    END as month_number,
    
    -- Scope classification
    scope,
    CASE 
        WHEN scope = 1 THEN 'Scope 1 - Direct'
        WHEN scope = 2 THEN 'Scope 2 - Indirect'
        WHEN scope = 3 THEN 'Scope 3 - Value Chain'
        WHEN scope = 0 THEN 'Scope 0 - Renewables'
        ELSE 'Unknown'
    END as scope_label,
    emission_source,
    
    -- Fuel and activity data
    fuel_type,
    activity_value,
    activity_unit,
    emission_factor,
    emission_factor_unit,
    
    -- Emission value
    calculated_emission,
    
    -- Status
    approval_status,
    
    -- Timestamps
    created_at,
    updated_at
FROM mongo_carbonlens.v_emissions_wide
WHERE approval_status = 'approved'
