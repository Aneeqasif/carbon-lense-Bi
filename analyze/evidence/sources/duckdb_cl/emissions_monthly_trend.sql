-- Monthly emissions trend by scope
SELECT 
    year,
    month,
    month_number,
    scope_label,
    ROUND(SUM(calculated_emission), 2) as total_emissions,
    COUNT(*) as records
FROM mongo_carbonlens.v_emissions_wide
WHERE approval_status = 'approved'
GROUP BY year, month, month_number, scope_label
ORDER BY year, month_number, scope_label
