---
title: Scope Analysis
---

# 📊 Scope Analysis Dashboard

Deep dive into emissions by GHG Protocol scope. Understand the breakdown of Scope 1, 2, and 3 emissions with drill-down capabilities.

```sql all_sites
SELECT DISTINCT site_name FROM duckdb_cl.full_emissions ORDER BY site_name
```

```sql all_years
SELECT DISTINCT year FROM duckdb_cl.full_emissions ORDER BY year DESC
```

```sql all_scopes
SELECT DISTINCT scope, scope_label FROM duckdb_cl.full_emissions ORDER BY scope
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
    name=selected_site
    data={all_sites}
    value=site_name
    title="🏭 Site"
>
    <DropdownOption value="%" valueLabel="All Sites"/>
</Dropdown>

<Dropdown 
    name=scope_focus
    data={all_scopes}
    value=scope_label
    title="🎯 Focus Scope"
>
    <DropdownOption value="%" valueLabel="All Scopes"/>
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

```sql scope_summary
SELECT 
    scope,
    scope_label,
    ROUND(SUM(calculated_emission), 2) as total_emissions,
    COUNT(*) as record_count,
    COUNT(DISTINCT site_name) as sites_count,
    COUNT(DISTINCT emission_source) as sources_count
FROM duckdb_cl.full_emissions
WHERE (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
  AND (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
GROUP BY scope, scope_label
ORDER BY scope
```

## 📈 Key Metrics by Scope

<Grid cols=3>

{#each scope_summary as row}
<BigValue 
  data={[row]}
  value=total_emissions
  title={row.scope_label}
  fmt="#,##0"
/>
{/each}

</Grid>

---

## 📊 Scope Distribution

<Grid cols=2>

<div>

### Emissions by Scope (Donut)

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
        data: [...scope_summary].map(d => ({name: d.scope_label, value: d.total_emissions}))
    }]
}}/>

</div>

<div>

### Scope Breakdown Table

<DataTable data={scope_summary} rows=5>
    <Column id=scope_label title="Scope"/>
    <Column id=total_emissions title="Emissions (tCO₂e)" fmt="#,##0.00"/>
    <Column id=sites_count title="Sites"/>
    <Column id=sources_count title="Sources"/>
    <Column id=record_count title="Records"/>
</DataTable>

<BarChart 
    data={scope_summary}
    x=scope_label
    y=total_emissions
    yFmt="#,##0"
    colorPalette={['#236aa4', '#45a1bf', '#85c7c6']}
/>

</div>

</Grid>

---

## 📈 Scope Trends Over Time (Stacked Area)

```sql scope_yearly_trend
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
    data={scope_yearly_trend}
    x=year
    y=total_emissions
    series=scope_label
    title="Scope Emissions Trend (Stacked Area)"
    type=stacked
    yFmt="#,##0"
    colorPalette={['#236aa4', '#45a1bf', '#85c7c6']}
/>

---

## 📅 Monthly Trend by Scope

```sql scope_monthly
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

<AreaChart 
    data={scope_monthly}
    x=month
    y=total_emissions
    series=scope_label
    title="Monthly Emissions by Scope (Stacked)"
    type=stacked
    yFmt="#,##0"
    xSort=month_number
    colorPalette={['#236aa4', '#45a1bf', '#85c7c6']}
/>

---

## 📦 Scope 3 Categories Breakdown

```sql scope3_breakdown
SELECT 
    emission_source as category,
    ROUND(SUM(calculated_emission), 2) as total_emissions,
    COUNT(*) as record_count,
    COUNT(DISTINCT site_name) as sites
FROM duckdb_cl.full_emissions
WHERE scope = 3
  AND (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
  AND (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
GROUP BY emission_source
ORDER BY total_emissions DESC
```

### Scope 3 Categories (Horizontal Bar)

<BarChart 
    data={scope3_breakdown}
    x=category
    y=total_emissions
    title="Scope 3 Emissions by Category"
    yFmt="#,##0"
    swapXY=true
    colorPalette={['#85c7c6']}
/>

<DataTable data={scope3_breakdown} rows=15 search=true>
    <Column id=category title="Category"/>
    <Column id=total_emissions title="Emissions (tCO₂e)" fmt="#,##0.00"/>
    <Column id=sites title="Sites"/>
    <Column id=record_count title="Records"/>
</DataTable>

---

## 🔍 Drill-Down: Sites Contributing to Each Scope

```sql scope_by_site
SELECT 
    scope_label,
    site_name,
    ROUND(SUM(calculated_emission), 2) as total_emissions,
    COUNT(DISTINCT emission_source) as sources
FROM duckdb_cl.full_emissions
WHERE (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
  AND (scope_label LIKE '${inputs.scope_focus.value}' OR '${inputs.scope_focus.value}' = '%')
GROUP BY scope_label, site_name
ORDER BY scope_label, total_emissions DESC
```

<BarChart 
    data={scope_by_site}
    x=site_name
    y=total_emissions
    series=scope_label
    title="Site Contributions by Scope"
    type=grouped
    yFmt="#,##0"
    swapXY=true
/>

<DataTable data={scope_by_site} rows=20 search=true>
    <Column id=scope_label title="Scope"/>
    <Column id=site_name title="Site"/>
    <Column id=total_emissions title="Emissions (tCO₂e)" fmt="#,##0.00"/>
    <Column id=sources title="Emission Sources"/>
</DataTable>

---

## 🔥 Emission Sources by Scope

```sql sources_by_scope
SELECT 
    emission_source,
    scope_label,
    ROUND(SUM(calculated_emission), 2) as total_emissions,
    COUNT(*) as record_count
FROM duckdb_cl.full_emissions
WHERE (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
  AND (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
  AND (scope_label LIKE '${inputs.scope_focus.value}' OR '${inputs.scope_focus.value}' = '%')
GROUP BY emission_source, scope_label
ORDER BY total_emissions DESC
```

<BarChart 
    data={sources_by_scope}
    x=emission_source
    y=total_emissions
    series=scope_label
    title="Emission Sources (Grouped by Scope)"
    type=stacked
    yFmt="#,##0"
    swapXY=true
    colorPalette={['#236aa4', '#45a1bf', '#85c7c6']}
/>

---

## 📊 Scope Comparison Matrix

```sql scope_comparison
SELECT 
    scope_label,
    year,
    ROUND(SUM(calculated_emission), 2) as emissions
FROM duckdb_cl.full_emissions
WHERE (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
GROUP BY scope_label, year
ORDER BY scope_label, year
```

<Heatmap 
    data={scope_comparison}
    x=year
    y=scope_label
    value=emissions
    valueFmt="#,##0"
    colorScale={['#f0f9ff', '#236aa4']}
    title="Scope Emissions by Year (Heatmap)"
/>

