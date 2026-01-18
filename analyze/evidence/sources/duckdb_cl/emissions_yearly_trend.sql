-- Yearly emissions trend
SELECT 
    year,
    scope_label,
    ROUND(SUM(calculated_emission), 2) as total_emissions,
    COUNT(*) as records
FROM mongo_carbonlens.v_emissions_wide
WHERE approval_status = 'approved' AND year IS NOT NULL
GROUP BY year, scope_label
ORDER BY year, scope_label
