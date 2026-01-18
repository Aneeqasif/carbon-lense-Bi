---
title: Scope Analysis
---

# 📊 Scope Analysis

Analyze emissions by GHG Protocol scope with site and year filters.

```sql sites
SELECT * FROM duckdb_cl.sites
```

```sql years
SELECT * FROM duckdb_cl.years
```

## Filters

<Dropdown 
    name=selected_site
    data={sites}
    value=site_name
    title="Select Site"
>
    <DropdownOption value="%" valueLabel="All Sites"/>
</Dropdown>

<Dropdown 
    name=selected_year
    data={years}
    value=year
    title="Select Year"
>
    <DropdownOption value="%" valueLabel="All Years"/>
</Dropdown>

```sql filtered_by_scope
SELECT 
    scope,
    scope_label,
    ROUND(SUM(calculated_emission), 2) as total_emissions,
    COUNT(*) as record_count
FROM duckdb_cl.full_emissions
WHERE (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
  AND (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
GROUP BY scope, scope_label
ORDER BY scope
```

```sql filtered_by_source
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
```

```sql filtered_monthly
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

---

## Emissions Summary by Scope

<BarChart 
    data={filtered_by_scope}
    x=scope_label
    y=total_emissions
    title="Emissions by Scope (tCO₂e)"
    yFmt="#,##0"
    colorPalette={['#85c7c6', '#236aa4', '#45a1bf', '#f4b548']}
/>

<DataTable data={filtered_by_scope} rows=5>
    <Column id=scope_label title="Scope"/>
    <Column id=total_emissions title="Total Emissions (tCO₂e)" fmt="#,##0.00"/>
    <Column id=record_count title="Records"/>
</DataTable>

---

## Emissions by Source Type

<BarChart 
    data={filtered_by_source}
    x=emission_source
    y=total_emissions
    series=scope_label
    title="Emissions by Source (tCO₂e)"
    yFmt="#,##0"
    swapXY=true
    type=stacked
/>

<DataTable data={filtered_by_source} rows=15 search=true>
    <Column id=emission_source title="Source"/>
    <Column id=scope_label title="Scope"/>
    <Column id=total_emissions title="Emissions (tCO₂e)" fmt="#,##0.00"/>
    <Column id=record_count title="Records"/>
</DataTable>

---

## Monthly Trend

<LineChart 
    data={filtered_monthly}
    x=month
    y=total_emissions
    series=scope_label
    title="Monthly Emissions by Scope"
    yFmt="#,##0"
    xSort=month_number
/>

---

## Scope Distribution

```sql pie_data
SELECT scope_label as name, ROUND(SUM(calculated_emission), 2) as value 
FROM duckdb_cl.full_emissions
WHERE (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
  AND (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
GROUP BY scope_label
```

<ECharts config={{
    tooltip: {
        formatter: '{b}: {c} ({d}%)'
    },
    series: [{
        type: 'pie',
        radius: ['40%', '70%'],
        data: [...pie_data]
    }]
}}/>

---

[← Back to Dashboard](/) | [Site Analysis →](/site-analysis)
