---
title: Trends & Insights
---

# 📈 Trends & Insights Dashboard

Year-over-year analysis, seasonal patterns, and anomaly detection. Track progress toward reduction targets.

```sql all_sites
SELECT DISTINCT site_name FROM duckdb_cl.full_emissions ORDER BY site_name
```

```sql all_years
SELECT DISTINCT year FROM duckdb_cl.full_emissions ORDER BY year DESC
```

```sql all_scopes
SELECT DISTINCT scope_label FROM duckdb_cl.full_emissions ORDER BY scope_label
```

## Filters

<Grid cols=3>

<Dropdown 
    name=selected_site
    data={all_sites}
    value=site_name
    title="🏭 Site"
>
    <DropdownOption value="%" valueLabel="All Sites"/>
</Dropdown>

<Dropdown 
    name=scope_filter
    data={all_scopes}
    value=scope_label
    title="🎯 Scope"
>
    <DropdownOption value="%" valueLabel="All Scopes"/>
</Dropdown>

<Dropdown 
    name=base_year
    data={all_years}
    value=year
    title="📅 Base Year (for comparison)"
>
</Dropdown>

</Grid>

---

## 📊 Year-over-Year Comparison

```sql yearly_summary
SELECT 
    year,
    ROUND(SUM(calculated_emission), 2) as total_emissions,
    ROUND(SUM(CASE WHEN scope = 1 THEN calculated_emission ELSE 0 END), 2) as scope1,
    ROUND(SUM(CASE WHEN scope = 2 THEN calculated_emission ELSE 0 END), 2) as scope2,
    ROUND(SUM(CASE WHEN scope = 3 THEN calculated_emission ELSE 0 END), 2) as scope3,
    COUNT(DISTINCT site_name) as sites,
    COUNT(DISTINCT emission_source) as sources,
    COUNT(*) as records
FROM duckdb_cl.full_emissions
WHERE (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
  AND (scope_label LIKE '${inputs.scope_filter.value}' OR '${inputs.scope_filter.value}' = '%')
GROUP BY year
ORDER BY year
```

```sql yoy_change
WITH yearly AS (
    SELECT 
        year,
        ROUND(SUM(calculated_emission), 2) as total_emissions
    FROM duckdb_cl.full_emissions
    WHERE (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
      AND (scope_label LIKE '${inputs.scope_filter.value}' OR '${inputs.scope_filter.value}' = '%')
    GROUP BY year
)
SELECT 
    y1.year,
    y1.total_emissions as current_emissions,
    y2.total_emissions as previous_emissions,
    ROUND(y1.total_emissions - COALESCE(y2.total_emissions, 0), 2) as absolute_change,
    CASE 
        WHEN y2.total_emissions IS NULL OR y2.total_emissions = 0 THEN NULL
        ELSE ROUND(((y1.total_emissions - y2.total_emissions) / y2.total_emissions) * 100, 1)
    END as percent_change
FROM yearly y1
LEFT JOIN yearly y2 ON y1.year = y2.year + 1
ORDER BY y1.year
```

<Grid cols=2>

<div>

### Annual Emissions Overview

<LineChart 
    data={yearly_summary}
    x=year
    y=total_emissions
    title="Total Emissions by Year"
    yFmt="#,##0"
    markers=true
    colorPalette={['#236aa4']}
/>

</div>

<div>

### Year-over-Year Change (%)

<BarChart 
    data={yoy_change}
    x=year
    y=percent_change
    title="YoY Emission Change (%)"
    yFmt="+#,##0.0;-#,##0.0"
/>

</div>

</Grid>

<DataTable data={yoy_change} rows=10>
    <Column id=year title="Year"/>
    <Column id=current_emissions title="Emissions (tCO₂e)" fmt="#,##0.00"/>
    <Column id=previous_emissions title="Previous Year" fmt="#,##0.00"/>
    <Column id=absolute_change title="Change (tCO₂e)" fmt="+#,##0.00;-#,##0.00"/>
    <Column id=percent_change title="Change (%)" fmt="+#,##0.0%;-#,##0.0%"/>
</DataTable>

---

## 📈 Emissions by Scope - Annual Trend

```sql scope_yearly
SELECT 
    year,
    scope_label,
    ROUND(SUM(calculated_emission), 2) as total_emissions
FROM duckdb_cl.full_emissions
WHERE (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
GROUP BY year, scope_label
ORDER BY year, scope_label
```

<Grid cols=2>

<LineChart 
    data={scope_yearly}
    x=year
    y=total_emissions
    series=scope_label
    title="Scope Trends (Multi-Line)"
    yFmt="#,##0"
    markers=true
    colorPalette={['#236aa4', '#45a1bf', '#85c7c6']}
/>

<AreaChart 
    data={scope_yearly}
    x=year
    y=total_emissions
    series=scope_label
    title="Scope Trends (Stacked Area)"
    type=stacked
    yFmt="#,##0"
    colorPalette={['#236aa4', '#45a1bf', '#85c7c6']}
/>

</Grid>

---

## 🗓️ Seasonal Pattern Analysis (Heatmap)

```sql monthly_heatmap
SELECT 
    site_name,
    month,
    month_number,
    ROUND(SUM(calculated_emission), 2) as total_emissions
FROM duckdb_cl.full_emissions
WHERE (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
  AND (scope_label LIKE '${inputs.scope_filter.value}' OR '${inputs.scope_filter.value}' = '%')
GROUP BY site_name, month, month_number
ORDER BY site_name, month_number
```

<Heatmap 
    data={monthly_heatmap}
    x=month
    y=site_name
    value=total_emissions
    valueFmt="#,##0"
    colorScale={['#f0f9ff', '#236aa4']}
    title="Emissions by Site and Month (Seasonal Patterns)"
    xSort=month_number
/>

---

## 📊 Quarterly Trend Analysis

```sql quarterly_data
SELECT 
    year,
    quarter,
    scope_label,
    ROUND(SUM(calculated_emission), 2) as total_emissions
FROM duckdb_cl.full_emissions
WHERE (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
  AND (scope_label LIKE '${inputs.scope_filter.value}' OR '${inputs.scope_filter.value}' = '%')
GROUP BY year, quarter, scope_label
ORDER BY year, quarter, scope_label
```

```sql quarterly_totals
SELECT 
    year || '-Q' || quarter as period,
    year,
    quarter,
    ROUND(SUM(calculated_emission), 2) as total_emissions
FROM duckdb_cl.full_emissions
WHERE (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
  AND (scope_label LIKE '${inputs.scope_filter.value}' OR '${inputs.scope_filter.value}' = '%')
GROUP BY year, quarter
ORDER BY year, quarter
```

<LineChart 
    data={quarterly_totals}
    x=period
    y=total_emissions
    title="Quarterly Emissions Trend"
    yFmt="#,##0"
    markers=true
/>

---

## 🎯 Progress Toward Reduction Targets

```sql baseline_comparison
WITH baseline AS (
    SELECT ROUND(SUM(calculated_emission), 2) as baseline_emissions
    FROM duckdb_cl.full_emissions
    WHERE year = ${inputs.base_year.value}
      AND (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
      AND (scope_label LIKE '${inputs.scope_filter.value}' OR '${inputs.scope_filter.value}' = '%')
),
yearly AS (
    SELECT 
        year,
        ROUND(SUM(calculated_emission), 2) as emissions
    FROM duckdb_cl.full_emissions
    WHERE (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
      AND (scope_label LIKE '${inputs.scope_filter.value}' OR '${inputs.scope_filter.value}' = '%')
    GROUP BY year
)
SELECT 
    y.year,
    y.emissions as current_emissions,
    b.baseline_emissions,
    ROUND(y.emissions - b.baseline_emissions, 2) as change_from_baseline,
    CASE WHEN b.baseline_emissions > 0 
        THEN ROUND(((y.emissions - b.baseline_emissions) / b.baseline_emissions) * 100, 1)
        ELSE NULL
    END as pct_change_from_baseline
FROM yearly y
CROSS JOIN baseline b
ORDER BY y.year
```

<LineChart 
    data={baseline_comparison}
    x=year
    y=pct_change_from_baseline
    title="% Change from Base Year ({inputs.base_year.value})"
    yFmt="+#,##0.0;-#,##0.0"
    markers=true
/>

<DataTable data={baseline_comparison} rows=10>
    <Column id=year title="Year"/>
    <Column id=current_emissions title="Emissions (tCO₂e)" fmt="#,##0.00"/>
    <Column id=baseline_emissions title="Base Year Emissions" fmt="#,##0.00"/>
    <Column id=change_from_baseline title="Change (tCO₂e)" fmt="+#,##0.00;-#,##0.00"/>
    <Column id=pct_change_from_baseline title="% from Baseline" fmt="+#,##0.0%;-#,##0.0%"/>
</DataTable>

---

## ⚠️ Anomaly Detection - Unusual Spikes or Drops

```sql monthly_stats
WITH monthly AS (
    SELECT 
        site_name,
        year,
        month,
        month_number,
        ROUND(SUM(calculated_emission), 2) as emissions
    FROM duckdb_cl.full_emissions
    WHERE (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
      AND (scope_label LIKE '${inputs.scope_filter.value}' OR '${inputs.scope_filter.value}' = '%')
    GROUP BY site_name, year, month, month_number
),
site_avg AS (
    SELECT 
        site_name,
        ROUND(AVG(emissions), 2) as avg_emissions,
        ROUND(STDDEV(emissions), 2) as std_emissions
    FROM monthly
    GROUP BY site_name
)
SELECT 
    m.site_name,
    m.year,
    m.month,
    m.emissions,
    s.avg_emissions,
    ROUND((m.emissions - s.avg_emissions) / NULLIF(s.std_emissions, 0), 2) as z_score,
    CASE 
        WHEN (m.emissions - s.avg_emissions) / NULLIF(s.std_emissions, 0) > 2 THEN '⬆️ Spike'
        WHEN (m.emissions - s.avg_emissions) / NULLIF(s.std_emissions, 0) < -2 THEN '⬇️ Drop'
        ELSE '✓ Normal'
    END as anomaly_status
FROM monthly m
JOIN site_avg s ON m.site_name = s.site_name
WHERE ABS((m.emissions - s.avg_emissions) / NULLIF(s.std_emissions, 0)) > 1.5
ORDER BY ABS((m.emissions - s.avg_emissions) / NULLIF(s.std_emissions, 0)) DESC
LIMIT 20
```

<DataTable data={monthly_stats} rows=20 search=true>
    <Column id=site_name title="Site"/>
    <Column id=year title="Year"/>
    <Column id=month title="Month"/>
    <Column id=emissions title="Emissions (tCO₂e)" fmt="#,##0.00"/>
    <Column id=avg_emissions title="Site Average" fmt="#,##0.00"/>
    <Column id=z_score title="Z-Score"/>
    <Column id=anomaly_status title="Status"/>
</DataTable>

---

## 🔥 Source Trends Over Time

```sql source_trend
SELECT 
    year,
    emission_source,
    ROUND(SUM(calculated_emission), 2) as total_emissions
FROM duckdb_cl.full_emissions
WHERE (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
  AND (scope_label LIKE '${inputs.scope_filter.value}' OR '${inputs.scope_filter.value}' = '%')
GROUP BY year, emission_source
ORDER BY year, total_emissions DESC
```

<LineChart 
    data={source_trend}
    x=year
    y=total_emissions
    series=emission_source
    title="Emission Source Trends Over Time"
    yFmt="#,##0"
    markers=true
/>

---

## 📅 Monthly Average Analysis

```sql monthly_avg
SELECT 
    month,
    month_number,
    ROUND(AVG(calculated_emission), 4) as avg_emission_per_record,
    ROUND(SUM(calculated_emission), 2) as total_emissions,
    COUNT(*) as records
FROM duckdb_cl.full_emissions
WHERE (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
  AND (scope_label LIKE '${inputs.scope_filter.value}' OR '${inputs.scope_filter.value}' = '%')
GROUP BY month, month_number
ORDER BY month_number
```

<BarChart 
    data={monthly_avg}
    x=month
    y=total_emissions
    title="Total Emissions by Month (All Years)"
    yFmt="#,##0"
    xSort=month_number
/>

<DataTable data={monthly_avg} rows=12>
    <Column id=month title="Month"/>
    <Column id=total_emissions title="Total Emissions (tCO₂e)" fmt="#,##0.00"/>
    <Column id=avg_emission_per_record title="Avg per Record" fmt="#,##0.0000"/>
    <Column id=records title="Records"/>
</DataTable>

