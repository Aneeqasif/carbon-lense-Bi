-- Emissions by site
SELECT 
    site_id,
    site_name,
    site_country,
    ROUND(SUM(calculated_emission), 2) as total_emissions,
    COUNT(*) as record_count,
    ROUND(SUM(CASE WHEN scope = 1 THEN calculated_emission ELSE 0 END), 2) as scope1_emissions,
    ROUND(SUM(CASE WHEN scope = 2 THEN calculated_emission ELSE 0 END), 2) as scope2_emissions,
    ROUND(SUM(CASE WHEN scope = 3 THEN calculated_emission ELSE 0 END), 2) as scope3_emissions
FROM mongo_carbonlens.v_emissions_wide
WHERE approval_status = 'approved'
GROUP BY site_id, site_name, site_country
ORDER BY total_emissions DESC

