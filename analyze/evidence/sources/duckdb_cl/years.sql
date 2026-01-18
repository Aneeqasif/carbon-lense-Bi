-- Get all unique years for filter dropdown
SELECT DISTINCT 
    year
FROM mongo_carbonlens.v_emissions_wide
WHERE year IS NOT NULL
ORDER BY year DESC
