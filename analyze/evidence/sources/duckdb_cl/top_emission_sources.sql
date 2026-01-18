-- Top 10 emission sources by total emissions
SELECT 
    emission_source,
    scope_label,
    fuel_type,
    ROUND(SUM(calculated_emission), 2) as total_emissions,
    COUNT(*) as records
FROM mongo_carbonlens.v_emissions_wide
WHERE approval_status = 'approved'
GROUP BY emission_source, scope_label, fuel_type
ORDER BY total_emissions DESC
LIMIT 15
