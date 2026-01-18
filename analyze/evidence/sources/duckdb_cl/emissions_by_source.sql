-- Emissions by source type
SELECT 
    emission_source,
    scope_label,
    ROUND(SUM(calculated_emission), 2) as total_emissions,
    COUNT(*) as record_count,
    ROUND(AVG(calculated_emission), 2) as avg_emission
FROM mongo_carbonlens.v_emissions_wide
WHERE approval_status = 'approved'
GROUP BY emission_source, scope_label
ORDER BY total_emissions DESC
