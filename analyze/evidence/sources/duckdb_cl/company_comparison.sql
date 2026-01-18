-- Company performance comparison
SELECT 
    company_name,
    ROUND(SUM(calculated_emission), 2) as total_emissions,
    COUNT(*) as record_count,
    ROUND(SUM(CASE WHEN scope = 1 THEN calculated_emission ELSE 0 END), 2) as scope1,
    ROUND(SUM(CASE WHEN scope = 2 THEN calculated_emission ELSE 0 END), 2) as scope2,
    ROUND(SUM(CASE WHEN scope = 3 THEN calculated_emission ELSE 0 END), 2) as scope3
FROM mongo_carbonlens.v_emissions_wide
WHERE approval_status = 'approved' AND company_name IS NOT NULL
GROUP BY company_name
ORDER BY total_emissions DESC
