-- Total emissions by scope (all data)
SELECT 
    scope,
    scope_label,
    ROUND(SUM(calculated_emission), 2) as total_emissions,
    COUNT(*) as record_count
FROM mongo_carbonlens.v_emissions_wide
WHERE approval_status = 'approved'
GROUP BY scope, scope_label
ORDER BY scope
