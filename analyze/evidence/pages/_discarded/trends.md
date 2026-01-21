---
title: Trends & Insights
---

# 📈 Trends & Insights

Year-over-year analysis and performance trends.

```sql yearly_comparison
SELECT 
    year,
    scope_label,
    ROUND(SUM(calculated_emission), 2) as total_emissions,
    COUNT(DISTINCT site_name) as sites_reporting,
    COUNT(DISTINCT emission_source) as sources_tracked,
    COUNT(*) as total_records
FROM duckdb_cl.full_emissions
GROUP BY year, scope_label
ORDER BY year, scope_label
```

```sql yearly_totals
SELECT 
    year,
    ROUND(SUM(calculated_emission), 2) as total_emissions,
    ROUND(SUM(CASE WHEN scope = 1 THEN calculated_emission ELSE 0 END), 2) as scope1,
    ROUND(SUM(CASE WHEN scope = 2 THEN calculated_emission ELSE 0 END), 2) as scope2,
    ROUND(SUM(CASE WHEN scope = 3 THEN calculated_emission ELSE 0 END), 2) as scope3,
    COUNT(DISTINCT site_name) as active_sites,
    COUNT(DISTINCT emission_source) as emission_sources
FROM duckdb_cl.full_emissions
GROUP BY year
ORDER BY year
```

```sql yoy_change
WITH yearly AS (
    SELECT 
        year,
        ROUND(SUM(calculated_emission), 2) as total_emissions
    FROM duckdb_cl.full_emissions
    GROUP BY year
)
SELECT 
    y1.year,
    y1.total_emissions as current_year_emissions,
    y2.total_emissions as previous_year_emissions,
    ROUND(y1.total_emissions - COALESCE(y2.total_emissions, 0), 2) as absolute_change,
    CASE 
        WHEN y2.total_emissions IS NULL OR y2.total_emissions = 0 THEN NULL
        ELSE ROUND(((y1.total_emissions - y2.total_emissions) / y2.total_emissions) * 100, 2)
    END as percent_change
FROM yearly y1
LEFT JOIN yearly y2 ON y1.year = y2.year + 1
ORDER BY y1.year
```

```sql quarterly_trend
SELECT 
    year,
    quarter,
    scope_label,
    ROUND(SUM(calculated_emission), 2) as total_emissions
FROM duckdb_cl.full_emissions
GROUP BY year, quarter, scope_label
ORDER BY year, quarter, scope_label
```

```sql monthly_avg
SELECT 
    month,
    month_number,
    ROUND(AVG(calculated_emission), 2) as avg_daily_emission,
    ROUND(SUM(calculated_emission), 2) as total_emissions,
    COUNT(*) as records
FROM duckdb_cl.full_emissions
GROUP BY month, month_number
ORDER BY month_number
```

```sql source_trend
SELECT 
    year,
    emission_source,
    ROUND(SUM(calculated_emission), 2) as total_emissions
FROM duckdb_cl.full_emissions
GROUP BY year, emission_source
ORDER BY year, total_emissions DESC
```

---

## Yearly Overview

<DataTable data={yearly_totals} rows=10>
    <Column id=year title="Year"/>
    <Column id=total_emissions title="Total (tCO₂e)" fmt="#,##0.00"/>
    <Column id=scope1 title="Scope 1" fmt="#,##0.00"/>
    <Column id=scope2 title="Scope 2" fmt="#,##0.00"/>
    <Column id=scope3 title="Scope 3" fmt="#,##0.00"/>
    <Column id=active_sites title="Sites"/>
    <Column id=emission_sources title="Sources"/>
</DataTable>

---

## Year-over-Year Change

<BarChart 
    data={yoy_change}
    x=year
    y=percent_change
    title="YoY Emission Change (%)"
    yFmt="+#,##0.0;-#,##0.0"
/>

<DataTable data={yoy_change} rows=10>
    <Column id=year title="Year"/>
    <Column id=current_year_emissions title="Current (tCO₂e)" fmt="#,##0.00"/>
    <Column id=previous_year_emissions title="Previous (tCO₂e)" fmt="#,##0.00"/>
    <Column id=absolute_change title="Change (tCO₂e)" fmt="+#,##0.00;-#,##0.00"/>
    <Column id=percent_change title="Change (%)" fmt="+#,##0.0;-#,##0.0"/>
</DataTable>

---

## Annual Emissions by Scope

<LineChart 
    data={yearly_comparison}
    x=year
    y=total_emissions
    series=scope_label
    title="Yearly Trend by Scope (tCO₂e)"
    yFmt="#,##0"
    markers=true
/>

<AreaChart 
    data={yearly_comparison}
    x=year
    y=total_emissions
    series=scope_label
    title="Stacked Yearly Emissions"
    type=stacked
    yFmt="#,##0"
/>

---

## Quarterly Performance

<BarChart 
    data={quarterly_trend}
    x=quarter
    y=total_emissions
    series=scope_label
    title="Quarterly Emissions by Scope"
    yFmt="#,##0"
    type=grouped
/>

---

## Monthly Seasonality

Understanding emission patterns across months.

<BarChart 
    data={monthly_avg}
    x=month
    y=total_emissions
    title="Total Emissions by Month (All Years)"
    yFmt="#,##0"
    xSort=month_number
/>

---

## Emission Source Trends

<AreaChart 
    data={source_trend}
    x=year
    y=total_emissions
    series=emission_source
    title="Emission Sources Over Time"
    yFmt="#,##0"
    type=stacked100
/>

---

<!-- <Alert status=info>
    **Insight Tips:**
    - Look for consistent patterns across years to identify reduction opportunities
    - Compare quarterly data to spot seasonal variations
    - Track source contributions to prioritize mitigation efforts
</Alert> -->

---

[← Dashboard](/) | [Scope Analysis →](/scope-analysis)
