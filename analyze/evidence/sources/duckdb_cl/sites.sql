-- Get all unique sites for filter dropdown
SELECT DISTINCT 
    site_id,
    site_name
FROM mongo_carbonlens.v_emissions_wide
WHERE site_name IS NOT NULL
ORDER BY site_name
