---
title: Site Analysis
---

# 🏭 Site Analysis

Compare emissions performance across different facilities and locations.

```sql sites
SELECT * FROM duckdb_cl.sites
```

```sql years
SELECT * FROM duckdb_cl.years
```

## Filters

<Dropdown 
    name=selected_year
    data={years}
    value=year
    title="Select Year"
>
    <DropdownOption value="%" valueLabel="All Years"/>
</Dropdown>

```sql site_emissions
SELECT 
    site_name,
    site_country,
    ROUND(SUM(calculated_emission), 2) as total_emissions,
    COUNT(*) as record_count,
    ROUND(SUM(CASE WHEN scope = 1 THEN calculated_emission ELSE 0 END), 2) as scope1,
    ROUND(SUM(CASE WHEN scope = 2 THEN calculated_emission ELSE 0 END), 2) as scope2,
    ROUND(SUM(CASE WHEN scope = 3 THEN calculated_emission ELSE 0 END), 2) as scope3
FROM duckdb_cl.full_emissions
WHERE (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
GROUP BY site_name, site_country
ORDER BY total_emissions DESC
```

```sql site_monthly
SELECT 
    site_name,
    month,
    month_number,
    ROUND(SUM(calculated_emission), 2) as total_emissions
FROM duckdb_cl.full_emissions
WHERE (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
GROUP BY site_name, month, month_number
ORDER BY site_name, month_number
```

```sql site_sources
SELECT 
    site_name,
    emission_source,
    ROUND(SUM(calculated_emission), 2) as total_emissions
FROM duckdb_cl.full_emissions
WHERE (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
GROUP BY site_name, emission_source
ORDER BY site_name, total_emissions DESC
```

---

## Site Performance Overview

<BarChart 
    data={site_emissions}
    x=site_name
    y=total_emissions
    title="Total Emissions by Site (tCO₂e)"
    yFmt="#,##0"
    swapXY=true
    colorPalette={['#236aa4']}
/>

---

## Site Breakdown by Scope

<BarChart 
    data={site_emissions}
    x=site_name
    y={['scope1', 'scope2', 'scope3']}
    title="Emissions by Site and Scope (tCO₂e)"
    type=stacked
    yFmt="#,##0"
    swapXY=true
/>

<DataTable data={site_emissions} rows=10 search=true>
    <Column id=site_name title="Site"/>
    <Column id=site_country title="Country"/>
    <Column id=total_emissions title="Total (tCO₂e)" fmt="#,##0.00"/>
    <Column id=scope1 title="Scope 1" fmt="#,##0.00"/>
    <Column id=scope2 title="Scope 2" fmt="#,##0.00"/>
    <Column id=scope3 title="Scope 3" fmt="#,##0.00"/>
    <Column id=record_count title="Records"/>
</DataTable>

---

## Monthly Trend by Site

<LineChart 
    data={site_monthly}
    x=month
    y=total_emissions
    series=site_name
    title="Monthly Emissions Trend by Site"
    yFmt="#,##0"
    xSort=month_number
/>

---

## Emission Sources by Site

<BarChart 
    data={site_sources}
    x=site_name
    y=total_emissions
    series=emission_source
    title="Emission Sources Breakdown by Site"
    type=stacked
    yFmt="#,##0"
    swapXY=true
/>

---

## Site Distribution

```sql site_pie_data
SELECT site_name as name, ROUND(SUM(calculated_emission), 2) as value 
FROM duckdb_cl.full_emissions
WHERE (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
GROUP BY site_name
```

<ECharts config={{
    tooltip: {
        formatter: '{b}: {c} ({d}%)'
    },
    series: [{
        type: 'pie',
        radius: ['40%', '70%'],
        data: [...site_pie_data]
    }]
}}/>

---

[← Scope Analysis](/scope-analysis) | [Fuel Analysis →](/fuel-analysis)
