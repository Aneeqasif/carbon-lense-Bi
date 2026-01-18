-- Get all unique companies for filter dropdown
SELECT DISTINCT 
    company_id,
    company_name
FROM mongo_carbonlens.v_emissions_wide
WHERE company_name IS NOT NULL
ORDER BY company_name
