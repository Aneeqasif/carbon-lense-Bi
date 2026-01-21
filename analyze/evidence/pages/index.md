---
title: Executive Summary
---

# 🌍 Carbon Lens - Executive Summary

Welcome to the Carbon Lens Business Intelligence Dashboard. Get a comprehensive overview of your organization's greenhouse gas emissions.

```sql summary
SELECT * FROM duckdb_cl.summary_kpis
```

```sql scope_totals
SELECT * FROM duckdb_cl.emissions_by_scope
```

```sql yearly_data
SELECT * FROM duckdb_cl.emissions_yearly_trend
```

```sql monthly_trend
SELECT * FROM duckdb_cl.emissions_monthly_trend
```

```sql sites_ranking
SELECT * FROM duckdb_cl.emissions_by_site
```

```sql years
SELECT * FROM duckdb_cl.years
```

```sql top_sources
SELECT * FROM duckdb_cl.top_emission_sources
```

```sql full_data
SELECT * FROM duckdb_cl.full_emissions
```

## Global Filters

```sql all_sites
SELECT DISTINCT site_name FROM duckdb_cl.full_emissions ORDER BY site_name
```

```sql all_years
SELECT DISTINCT year FROM duckdb_cl.full_emissions ORDER BY year DESC
```

<Grid cols=3>

<Dropdown 
    name=selected_year
    data={all_years}
    value=year
    title="📅 Year"
>
    <DropdownOption value="%" valueLabel="All Years"/>
</Dropdown>

<Dropdown 
    name=selected_site
    data={all_sites}
    value=site_name
    title="🏭 Site"
>
    <DropdownOption value="%" valueLabel="All Sites"/>
</Dropdown>

<Dropdown 
    name=metric_view
    title="📊 Metric View"
>
    <DropdownOption value="absolute" valueLabel="Absolute Values"/>
    <DropdownOption value="percentage" valueLabel="% of Total"/>
</Dropdown>

</Grid>

---

```sql current_period
SELECT 
    ROUND(SUM(calculated_emission), 2) as total_emissions,
    COUNT(DISTINCT site_name) as sites_count,
    COUNT(DISTINCT emission_source) as sources_count,
    COUNT(*) as record_count
FROM duckdb_cl.full_emissions
WHERE (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
  AND (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
```

```sql period_comparison
WITH current_year_data AS (
    SELECT MAX(year) as max_year FROM duckdb_cl.full_emissions
),
current AS (
    SELECT 
        year,
        ROUND(SUM(calculated_emission), 2) as emissions
    FROM duckdb_cl.full_emissions
    WHERE year = (SELECT max_year FROM current_year_data)
      AND (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
    GROUP BY year
),
previous AS (
    SELECT 
        year,
        ROUND(SUM(calculated_emission), 2) as emissions
    FROM duckdb_cl.full_emissions
    WHERE year = (SELECT max_year - 1 FROM current_year_data)
      AND (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
    GROUP BY year
)
SELECT 
    c.year as current_year,
    c.emissions as current_emissions,
    p.year as previous_year,
    p.emissions as previous_emissions,
    ROUND(c.emissions - COALESCE(p.emissions, 0), 2) as absolute_change,
    CASE WHEN p.emissions > 0 
        THEN ROUND(((c.emissions - p.emissions) / p.emissions) * 100, 1)
        ELSE NULL
    END as percent_change
FROM current c
LEFT JOIN previous p ON 1=1
```

```sql highest_site
SELECT 
    site_name,
    ROUND(SUM(calculated_emission), 2) as total_emissions
FROM duckdb_cl.full_emissions
WHERE (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
GROUP BY site_name
ORDER BY total_emissions DESC
LIMIT 1
```

```sql emissions_intensity
SELECT 
    ROUND(SUM(calculated_emission) / NULLIF(COUNT(DISTINCT site_name), 0), 2) as intensity_per_site
FROM duckdb_cl.full_emissions
WHERE (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
  AND (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
```

## 📊 Key Performance Indicators

<Grid cols=4>

<BigValue 
  data={current_period}
  value=total_emissions
  title="Total Emissions"
  fmt="#,##0"
/>

<BigValue 
  data={period_comparison}
  value=percent_change
  title="vs Previous Year"
  fmt="+#,##0.0%;-#,##0.0%"
/>

<BigValue 
  data={highest_site}
  value=site_name
  title="Highest Emitting Site"
/>

<BigValue 
  data={emissions_intensity}
  value=intensity_per_site
  title="Emissions per Site"
  fmt="#,##0.0"
/>

</Grid>

<Grid cols=3>

<BigValue 
  data={current_period}
  value=sites_count
  title="Active Sites"
/>

<BigValue 
  data={current_period}
  value=sources_count
  title="Emission Sources"
/>

<BigValue 
  data={current_period}
  value=record_count
  title="Total Records"
  fmt="#,##0"
/>

</Grid>

---

## Emissions by Scope

```sql scope_filtered
SELECT 
    scope_label,
    ROUND(SUM(calculated_emission), 2) as total_emissions,
    COUNT(*) as record_count
FROM duckdb_cl.full_emissions
WHERE (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
  AND (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
GROUP BY scope_label
ORDER BY scope_label
```

<Grid cols=2>

<div>

### Scope Distribution (Donut Chart)

<ECharts config={{
    tooltip: {
        trigger: 'item',
        formatter: '{b}: {c} tCO₂e ({d}%)'
    },
    legend: {
        orient: 'horizontal',
        bottom: '0%'
    },
    series: [{
        type: 'pie',
        radius: ['45%', '75%'],
        avoidLabelOverlap: false,
        itemStyle: {
            borderRadius: 10,
            borderColor: '#fff',
            borderWidth: 2
        },
        label: {
            show: false
        },
        emphasis: {
            label: {
                show: true,
                fontSize: 16,
                fontWeight: 'bold'
            }
        },
        data: [...scope_filtered].map(d => ({name: d.scope_label, value: d.total_emissions}))
    }]
}}/>

</div>

<div>

### Scope Breakdown

<BarChart 
    data={scope_filtered}
    x=scope_label
    y=total_emissions
    yFmt="#,##0"
    colorPalette={['#236aa4', '#45a1bf', '#85c7c6', '#f4b548']}
/>

<DataTable data={scope_filtered} rows=5>
    <Column id=scope_label title="Scope"/>
    <Column id=total_emissions title="Emissions (tCO₂e)" fmt="#,##0.00"/>
    <Column id=record_count title="Records"/>
</DataTable>

</div>

</Grid>

---

## 📈 Monthly Emissions Trend

```sql monthly_by_scope
SELECT 
    year,
    month,
    month_number,
    scope_label,
    ROUND(SUM(calculated_emission), 2) as total_emissions
FROM duckdb_cl.full_emissions
WHERE (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
  AND (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
GROUP BY year, month, month_number, scope_label
ORDER BY year, month_number
```

<LineChart 
    data={monthly_by_scope}
    x=month
    y=total_emissions
    series=scope_label
    title="Monthly Emissions by Scope"
    yFmt="#,##0"
    xSort=month_number
    colorPalette={['#236aa4', '#45a1bf', '#85c7c6', '#f4b548']}
/>

---

## 🏭 Site Performance Ranking

```sql sites_filtered
SELECT 
    site_name,
    site_country,
    ROUND(SUM(calculated_emission), 2) as total_emissions,
    ROUND(SUM(CASE WHEN scope = 1 THEN calculated_emission ELSE 0 END), 2) as scope1,
    ROUND(SUM(CASE WHEN scope = 2 THEN calculated_emission ELSE 0 END), 2) as scope2,
    ROUND(SUM(CASE WHEN scope = 3 THEN calculated_emission ELSE 0 END), 2) as scope3
FROM duckdb_cl.full_emissions
WHERE (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
  AND (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
GROUP BY site_name, site_country
ORDER BY total_emissions DESC
```

<BarChart 
    data={sites_filtered}
    x=site_name
    y={['scope1', 'scope2', 'scope3']}
    title="Sites Ranked by Emissions (Stacked by Scope)"
    type=stacked
    yFmt="#,##0"
    swapXY=true
    colorPalette={['#236aa4', '#45a1bf', '#85c7c6']}
/>

---

## 🔥 Top Emission Sources

```sql sources_filtered
SELECT 
    emission_source,
    scope_label,
    ROUND(SUM(calculated_emission), 2) as total_emissions,
    COUNT(*) as record_count
FROM duckdb_cl.full_emissions
WHERE (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
  AND (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
GROUP BY emission_source, scope_label
ORDER BY total_emissions DESC
LIMIT 15
```

<Grid cols=2>

<div>

<BarChart 
    data={sources_filtered}
    x=emission_source
    y=total_emissions
    series=scope_label
    title="Top Emission Sources"
    type=stacked
    yFmt="#,##0"
    swapXY=true
/>

</div>

<div>

<DataTable data={sources_filtered} rows=10 search=true>
    <Column id=emission_source title="Source"/>
    <Column id=scope_label title="Scope"/>
    <Column id=total_emissions title="Emissions (tCO₂e)" fmt="#,##0.00"/>
    <Column id=record_count title="Records"/>
</DataTable>

</div>

</Grid>

---

## 📅 Yearly Comparison

```sql yearly_filtered
SELECT 
    year,
    scope_label,
    ROUND(SUM(calculated_emission), 2) as total_emissions
FROM duckdb_cl.full_emissions
WHERE (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
GROUP BY year, scope_label
ORDER BY year, scope_label
```

<AreaChart 
    data={yearly_filtered}
    x=year
    y=total_emissions
    series=scope_label
    title="Annual Emissions Trend by Scope"
    type=stacked
    yFmt="#,##0"
    colorPalette={['#236aa4', '#45a1bf', '#85c7c6', '#f4b548']}
/>

---

## Quick Navigation

<Grid cols=4>
    <BigLink href="/scope-analysis">📊 Scope Analysis</BigLink>
    <BigLink href="/site-analysis">🏭 Site Performance</BigLink>
    <BigLink href="/trends">📈 Trends & Insights</BigLink>
    <BigLink href="/category-analysis">📦 Category Analysis</BigLink>
</Grid>

