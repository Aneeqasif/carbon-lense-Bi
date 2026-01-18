-- Summary KPIs for dashboard
SELECT 
    ROUND(SUM(calculated_emission), 0) as total_emissions,
    COUNT(*) as total_records,
    COUNT(DISTINCT site_id) as sites_count,
    COUNT(DISTINCT company_id) as companies_count,
    ROUND(SUM(CASE WHEN scope = 1 THEN calculated_emission ELSE 0 END), 0) as scope1_total,
    ROUND(SUM(CASE WHEN scope = 2 THEN calculated_emission ELSE 0 END), 0) as scope2_total,
    ROUND(SUM(CASE WHEN scope = 3 THEN calculated_emission ELSE 0 END), 0) as scope3_total,
    ROUND(SUM(CASE WHEN scope = 0 THEN calculated_emission ELSE 0 END), 0) as renewables_total
FROM mongo_carbonlens.v_emissions_wide
WHERE approval_status = 'approved'
