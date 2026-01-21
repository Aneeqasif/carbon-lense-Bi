---
title: Category Analysis
---

# 📦 Category Analysis Dashboard

Analyze emissions by category across all scopes. Compare categories, track performance, and identify reduction opportunities.

```sql all_sites
SELECT DISTINCT site_name FROM duckdb_cl.full_emissions ORDER BY site_name
```

```sql all_years
SELECT DISTINCT year FROM duckdb_cl.full_emissions ORDER BY year DESC
```

```sql all_scopes
SELECT DISTINCT scope_label FROM duckdb_cl.full_emissions ORDER BY scope_label
```

```sql all_categories
SELECT DISTINCT emission_source as category FROM duckdb_cl.full_emissions ORDER BY emission_source
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
    name=scope_filter
    data={all_scopes}
    value=scope_label
    title="🎯 Scope"
>
    <DropdownOption value="%" valueLabel="All Scopes"/>
</Dropdown>

<Dropdown 
    name=focus_category
    data={all_categories}
    value=category
    title="📦 Focus Category"
>
    <DropdownOption value="%" valueLabel="All Categories"/>
</Dropdown>

</Grid>

---

## 📊 Category Comparison Matrix

```sql category_matrix
WITH current_data AS (
    SELECT 
        emission_source as category,
        scope_label,
        ROUND(SUM(calculated_emission), 2) as current_emissions,
        COUNT(*) as records
    FROM duckdb_cl.full_emissions
    WHERE (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
      AND (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
      AND (scope_label LIKE '${inputs.scope_filter.value}' OR '${inputs.scope_filter.value}' = '%')
    GROUP BY emission_source, scope_label
),
previous_data AS (
    SELECT 
        emission_source as category,
        scope_label,
        ROUND(SUM(calculated_emission), 2) as previous_emissions
    FROM duckdb_cl.full_emissions
    WHERE (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
      AND year = (SELECT MAX(year) - 1 FROM duckdb_cl.full_emissions)
      AND (scope_label LIKE '${inputs.scope_filter.value}' OR '${inputs.scope_filter.value}' = '%')
    GROUP BY emission_source, scope_label
)
SELECT 
    c.category,
    c.scope_label,
    c.current_emissions,
    COALESCE(p.previous_emissions, 0) as previous_emissions,
    ROUND(c.current_emissions - COALESCE(p.previous_emissions, 0), 2) as absolute_change,
    CASE 
        WHEN p.previous_emissions IS NULL OR p.previous_emissions = 0 THEN NULL
        ELSE ROUND(((c.current_emissions - p.previous_emissions) / p.previous_emissions) * 100, 1)
    END as pct_change,
    c.records
FROM current_data c
LEFT JOIN previous_data p ON c.category = p.category AND c.scope_label = p.scope_label
ORDER BY c.current_emissions DESC
```

<DataTable data={category_matrix} rows=all search=true>
    <Column id=category title="Category"/>
    <Column id=scope_label title="Scope"/>
    <Column id=current_emissions title="Current (tCO₂e)" fmt="#,##0.00"/>
    <Column id=previous_emissions title="Previous (tCO₂e)" fmt="#,##0.00"/>
    <Column id=absolute_change title="Change (tCO₂e)" fmt="+#,##0.00;-#,##0.00"/>
    <Column id=pct_change title="% Change" fmt="+#,##0.0%;-#,##0.0%"/>
    <Column id=records title="Records"/>
</DataTable>

---

## 📈 Category Rankings

```sql category_totals
SELECT 
    emission_source as category,
    scope_label,
    ROUND(SUM(calculated_emission), 2) as total_emissions,
    COUNT(DISTINCT site_name) as sites,
    COUNT(*) as records
FROM duckdb_cl.full_emissions
WHERE (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
  AND (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
  AND (scope_label LIKE '${inputs.scope_filter.value}' OR '${inputs.scope_filter.value}' = '%')
GROUP BY emission_source, scope_label
ORDER BY total_emissions DESC
```

<BarChart 
    data={category_totals}
    x=category
    y=total_emissions
    series=scope_label
    title="Category Rankings by Emissions"
    type=stacked
    yFmt="#,##0"
    swapXY=true
    colorPalette={['#236aa4', '#45a1bf', '#85c7c6']}
/>

---

## 🌳 Category Treemap (Hierarchical View)

```sql treemap_data
SELECT 
    scope_label as scope,
    emission_source as category,
    site_name as site,
    ROUND(SUM(calculated_emission), 2) as value
FROM duckdb_cl.full_emissions
WHERE (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
  AND (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
  AND (scope_label LIKE '${inputs.scope_filter.value}' OR '${inputs.scope_filter.value}' = '%')
GROUP BY scope_label, emission_source, site_name
ORDER BY scope_label, value DESC
```

```sql treemap_by_scope
SELECT 
    scope_label as name,
    ROUND(SUM(calculated_emission), 2) as value
FROM duckdb_cl.full_emissions
WHERE (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
  AND (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
  AND (scope_label LIKE '${inputs.scope_filter.value}' OR '${inputs.scope_filter.value}' = '%')
GROUP BY scope_label
ORDER BY value DESC
```

<ECharts config={{
    title: {
        text: 'Emissions Treemap by Scope',
        left: 'center'
    },
    tooltip: {
        formatter: '{b}: {c} tCO₂e'
    },
    series: [{
        type: 'treemap',
        visibleMin: 300,
        label: {
            show: true,
            formatter: '{b}'
        },
        itemStyle: {
            borderColor: '#fff'
        },
        roam: false,
        nodeClick: false,
        data: [...treemap_by_scope],
        breadcrumb: {
            show: false
        }
    }]
}}/>

---

## 🔍 Category Performance Dashboard

{#if inputs.focus_category.value !== '%'}

```sql category_detail
SELECT 
    emission_source as category,
    ROUND(SUM(calculated_emission), 2) as total_emissions,
    COUNT(DISTINCT site_name) as sites,
    COUNT(DISTINCT year) as years,
    COUNT(*) as records
FROM duckdb_cl.full_emissions
WHERE emission_source LIKE '${inputs.focus_category.value}'
  AND (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
GROUP BY emission_source
```

```sql category_total_company
SELECT ROUND(SUM(calculated_emission), 2) as total
FROM duckdb_cl.full_emissions
WHERE (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
  AND (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
```

```sql category_pct
SELECT 
    ROUND((SELECT total_emissions FROM ${category_detail}) / NULLIF((SELECT total FROM ${category_total_company}), 0) * 100, 1) as pct_of_total
```

```sql category_trend
SELECT 
    year,
    month,
    month_number,
    ROUND(SUM(calculated_emission), 2) as emissions
FROM duckdb_cl.full_emissions
WHERE emission_source LIKE '${inputs.focus_category.value}'
  AND (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
GROUP BY year, month, month_number
ORDER BY year, month_number
```

```sql category_by_site
SELECT 
    site_name,
    ROUND(SUM(calculated_emission), 2) as emissions,
    COUNT(*) as records
FROM duckdb_cl.full_emissions
WHERE emission_source LIKE '${inputs.focus_category.value}'
  AND (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
  AND (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
GROUP BY site_name
ORDER BY emissions DESC
```

### Category: {inputs.focus_category.value}

<Grid cols=3>

<BigValue 
  data={category_detail}
  value=total_emissions
  title="Total Emissions (tCO₂e)"
  fmt="#,##0"
/>

<BigValue 
  data={category_pct}
  value=pct_of_total
  title="% of Company Total"
  fmt="#,##0.0%"
/>

<BigValue 
  data={category_detail}
  value=sites
  title="Sites Contributing"
/>

</Grid>

### Trend Over Time

<LineChart 
    data={category_trend}
    x=month
    y=emissions
    title="{inputs.focus_category.value} - Monthly Trend"
    yFmt="#,##0"
    xSort=month_number
    markers=true
/>

### Site Breakdown

<BarChart 
    data={category_by_site}
    x=site_name
    y=emissions
    title="Emissions by Site"
    yFmt="#,##0"
    swapXY=true
/>

<DataTable data={category_by_site} rows=10>
    <Column id=site_name title="Site"/>
    <Column id=emissions title="Emissions (tCO₂e)" fmt="#,##0.00"/>
    <Column id=records title="Records"/>
</DataTable>

{:else}

<Alert status="info">
    Select a category from the "Focus Category" dropdown above to see detailed performance metrics.
</Alert>

{/if}

---

## 📊 Category Trends by Year

```sql category_yearly
SELECT 
    year,
    emission_source as category,
    ROUND(SUM(calculated_emission), 2) as total_emissions
FROM duckdb_cl.full_emissions
WHERE (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
  AND (scope_label LIKE '${inputs.scope_filter.value}' OR '${inputs.scope_filter.value}' = '%')
GROUP BY year, emission_source
ORDER BY year, total_emissions DESC
```

<LineChart 
    data={category_yearly}
    x=year
    y=total_emissions
    series=category
    title="Category Emission Trends by Year"
    yFmt="#,##0"
    markers=true
/>

---

## 🗓️ Category Heatmap by Month

```sql category_monthly_heatmap
SELECT 
    emission_source as category,
    month,
    month_number,
    ROUND(SUM(calculated_emission), 2) as emissions
FROM duckdb_cl.full_emissions
WHERE (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
  AND (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
  AND (scope_label LIKE '${inputs.scope_filter.value}' OR '${inputs.scope_filter.value}' = '%')
GROUP BY emission_source, month, month_number
ORDER BY category, month_number
```

<Heatmap 
    data={category_monthly_heatmap}
    x=month
    y=category
    value=emissions
    valueFmt="#,##0"
    colorScale={['#f0f9ff', '#236aa4']}
    title="Category Emissions by Month"
    xSort=month_number
/>

---

## 📈 Category Distribution (Pie Chart)

```sql category_pie
SELECT 
    emission_source as name,
    ROUND(SUM(calculated_emission), 2) as value
FROM duckdb_cl.full_emissions
WHERE (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
  AND (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
  AND (scope_label LIKE '${inputs.scope_filter.value}' OR '${inputs.scope_filter.value}' = '%')
GROUP BY emission_source
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
        radius: ['35%', '65%'],
        center: ['35%', '50%'],
        avoidLabelOverlap: false,
        itemStyle: {
            borderRadius: 8,
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
        data: [...category_pie].slice(0, 15)
    }]
}}/>

