---
title: Site Performance
---

# 🏭 Site Performance Dashboard

Compare emissions performance across different facilities and locations. Identify hotspots and track site-level progress.

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

<Grid cols=4>

<Dropdown 
    name=selected_year
    data={all_years}
    value=year
    title="📅 Year"
>
    <DropdownOption value="%" valueLabel="All Years"/>
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
    name=compare_sites
    data={all_sites}
    value=site_name
    title="📊 Compare Sites"
    multiple=true
>
</Dropdown>

<Dropdown 
    name=metric_view
    title="📈 View"
>
    <DropdownOption value="absolute" valueLabel="Absolute Values"/>
    <DropdownOption value="percentage" valueLabel="% of Total"/>
</Dropdown>

</Grid>

---

```sql site_performance
SELECT 
    site_name,
    site_country,
    ROUND(SUM(calculated_emission), 2) as total_emissions,
    COUNT(*) as record_count,
    ROUND(SUM(CASE WHEN scope = 1 THEN calculated_emission ELSE 0 END), 2) as scope1,
    ROUND(SUM(CASE WHEN scope = 2 THEN calculated_emission ELSE 0 END), 2) as scope2,
    ROUND(SUM(CASE WHEN scope = 3 THEN calculated_emission ELSE 0 END), 2) as scope3,
    COUNT(DISTINCT emission_source) as emission_sources
FROM duckdb_cl.full_emissions
WHERE (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
  AND (scope_label LIKE '${inputs.scope_filter.value}' OR '${inputs.scope_filter.value}' = '%')
GROUP BY site_name, site_country
ORDER BY total_emissions DESC
```

```sql total_company_emissions
SELECT ROUND(SUM(calculated_emission), 2) as total
FROM duckdb_cl.full_emissions
WHERE (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
  AND (scope_label LIKE '${inputs.scope_filter.value}' OR '${inputs.scope_filter.value}' = '%')
```

```sql site_with_percentage
SELECT 
    site_name,
    site_country,
    total_emissions,
    scope1,
    scope2,
    scope3,
    record_count,
    emission_sources,
    ROUND((total_emissions / NULLIF((SELECT total FROM ${total_company_emissions}), 0)) * 100, 1) as pct_of_total
FROM ${site_performance}
ORDER BY total_emissions DESC
```

## 🏆 Site Ranking by Total Emissions

<BarChart 
    data={site_performance}
    x=site_name
    y=total_emissions
    title="Sites Ranked by Emissions (Highest to Lowest)"
    yFmt="#,##0"
    swapXY=true
    colorPalette={['#236aa4']}
    sort=false
/>

---

## 📊 Site Performance Table

<DataTable data={site_with_percentage} rows=all search=true>
    <Column id=site_name title="Site"/>
    <Column id=site_country title="Country"/>
    <Column id=total_emissions title="Total (tCO₂e)" fmt="#,##0.00"/>
    <Column id=pct_of_total title="% of Company Total" fmt="#,##0.0%"/>
    <Column id=scope1 title="Scope 1" fmt="#,##0.00"/>
    <Column id=scope2 title="Scope 2" fmt="#,##0.00"/>
    <Column id=scope3 title="Scope 3" fmt="#,##0.00"/>
    <Column id=record_count title="Records"/>
    <Column id=emission_sources title="Sources"/>
</DataTable>

---

## 📈 Site Breakdown by Scope

<BarChart 
    data={site_performance}
    x=site_name
    y={['scope1', 'scope2', 'scope3']}
    title="Emissions by Site (Stacked by Scope)"
    type=stacked
    yFmt="#,##0"
    swapXY=true
    colorPalette={['#236aa4', '#45a1bf', '#85c7c6']}
/>

---

## 📅 Monthly Trend by Site

```sql site_monthly
SELECT 
    site_name,
    month,
    month_number,
    ROUND(SUM(calculated_emission), 2) as total_emissions
FROM duckdb_cl.full_emissions
WHERE (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
  AND (scope_label LIKE '${inputs.scope_filter.value}' OR '${inputs.scope_filter.value}' = '%')
GROUP BY site_name, month, month_number
ORDER BY site_name, month_number
```

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

## 🔍 Site Comparison

{#if inputs.compare_sites.value && inputs.compare_sites.value.length > 0}

```sql comparison_data
SELECT 
    site_name,
    scope_label,
    ROUND(SUM(calculated_emission), 2) as total_emissions
FROM duckdb_cl.full_emissions
WHERE site_name IN ${inputs.compare_sites.value}
  AND (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
GROUP BY site_name, scope_label
ORDER BY site_name, scope_label
```

```sql comparison_monthly
SELECT 
    site_name,
    month,
    month_number,
    ROUND(SUM(calculated_emission), 2) as total_emissions
FROM duckdb_cl.full_emissions
WHERE site_name IN ${inputs.compare_sites.value}
  AND (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
GROUP BY site_name, month, month_number
ORDER BY month_number, site_name
```

```sql comparison_sources
SELECT 
    site_name,
    emission_source,
    ROUND(SUM(calculated_emission), 2) as total_emissions
FROM duckdb_cl.full_emissions
WHERE site_name IN ${inputs.compare_sites.value}
  AND (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
GROUP BY site_name, emission_source
ORDER BY total_emissions DESC
```

<Grid cols=2>

<div>

### Scope Comparison

<BarChart 
    data={comparison_data}
    x=site_name
    y=total_emissions
    series=scope_label
    title="Selected Sites by Scope"
    type=grouped
    yFmt="#,##0"
/>

</div>

<div>

### Monthly Comparison

<LineChart 
    data={comparison_monthly}
    x=month
    y=total_emissions
    series=site_name
    title="Monthly Trend Comparison"
    yFmt="#,##0"
    xSort=month_number
/>

</div>

</Grid>

### Emission Sources Comparison

<BarChart 
    data={comparison_sources}
    x=emission_source
    y=total_emissions
    series=site_name
    title="Emission Sources by Site"
    type=grouped
    yFmt="#,##0"
    swapXY=true
/>

{:else}

<Alert status="info">
    Select 2-4 sites from the "Compare Sites" dropdown above to see a detailed side-by-side comparison.
</Alert>

{/if}

---

## 🗺️ Site Emission Sources Breakdown

```sql site_sources
SELECT 
    site_name,
    emission_source,
    scope_label,
    ROUND(SUM(calculated_emission), 2) as total_emissions
FROM duckdb_cl.full_emissions
WHERE (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
  AND (scope_label LIKE '${inputs.scope_filter.value}' OR '${inputs.scope_filter.value}' = '%')
GROUP BY site_name, emission_source, scope_label
ORDER BY site_name, total_emissions DESC
```

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

## 🥧 Site Distribution

```sql site_pie
SELECT 
    site_name as name, 
    ROUND(SUM(calculated_emission), 2) as value 
FROM duckdb_cl.full_emissions
WHERE (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
  AND (scope_label LIKE '${inputs.scope_filter.value}' OR '${inputs.scope_filter.value}' = '%')
GROUP BY site_name
ORDER BY value DESC
```

<ECharts config={{
    tooltip: {
        trigger: 'item',
        formatter: '{b}: {c} tCO₂e ({d}%)'
    },
    legend: {
        type: 'scroll',
        orient: 'vertical',
        right: '5%',
        top: 'center'
    },
    series: [{
        type: 'pie',
        radius: ['40%', '70%'],
        center: ['35%', '50%'],
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
                fontSize: 14,
                fontWeight: 'bold'
            }
        },
        data: [...site_pie]
    }]
}}/>

