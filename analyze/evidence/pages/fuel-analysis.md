---
title: Fuel Analysis
---

# ⛽ Fuel Analysis

Deep dive into emissions by fuel type and combustion sources.

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

```sql fuel_emissions
SELECT 
    fuel_type,
    emission_source,
    ROUND(SUM(calculated_emission), 2) as total_emissions,
    ROUND(SUM(activity_value), 2) as total_activity,
    COUNT(*) as records
FROM duckdb_cl.full_emissions
WHERE fuel_type IS NOT NULL
  AND (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
  AND (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
GROUP BY fuel_type, emission_source
ORDER BY total_emissions DESC
```

```sql fuel_by_site
SELECT 
    site_name,
    fuel_type,
    ROUND(SUM(calculated_emission), 2) as total_emissions
FROM duckdb_cl.full_emissions
WHERE fuel_type IS NOT NULL
  AND (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
  AND (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
GROUP BY site_name, fuel_type
ORDER BY total_emissions DESC
```

```sql fuel_monthly
SELECT 
    fuel_type,
    month,
    month_number,
    ROUND(SUM(calculated_emission), 2) as total_emissions
FROM duckdb_cl.full_emissions
WHERE fuel_type IS NOT NULL
  AND (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
  AND (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
GROUP BY fuel_type, month, month_number
ORDER BY month_number
```

```sql emission_factors
SELECT 
    fuel_type,
    emission_source,
    ROUND(AVG(emission_factor), 4) as avg_emission_factor,
    emission_factor_unit,
    COUNT(*) as samples
FROM duckdb_cl.full_emissions
WHERE fuel_type IS NOT NULL
  AND emission_factor IS NOT NULL
  AND (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
  AND (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
GROUP BY fuel_type, emission_source, emission_factor_unit
ORDER BY avg_emission_factor DESC
```

---

## Emissions by Fuel Type

<BarChart 
    data={fuel_emissions}
    x=fuel_type
    y=total_emissions
    series=emission_source
    title="Emissions by Fuel Type (tCO₂e)"
    yFmt="#,##0"
    swapXY=true
    type=stacked
/>

<DataTable data={fuel_emissions} rows=15 search=true>
    <Column id=fuel_type title="Fuel Type"/>
    <Column id=emission_source title="Source"/>
    <Column id=total_emissions title="Emissions (tCO₂e)" fmt="#,##0.00"/>
    <Column id=total_activity title="Activity"/>
    <Column id=records title="Records"/>
</DataTable>

---

## Fuel Distribution

```sql fuel_pie_data
SELECT fuel_type as name, ROUND(SUM(calculated_emission), 2) as value 
FROM duckdb_cl.full_emissions
WHERE fuel_type IS NOT NULL
  AND (site_name LIKE '${inputs.selected_site.value}' OR '${inputs.selected_site.value}' = '%')
  AND (CAST(year AS VARCHAR) LIKE '${inputs.selected_year.value}' OR '${inputs.selected_year.value}' = '%')
GROUP BY fuel_type
```

<ECharts config={{
    tooltip: {
        formatter: '{b}: {c} ({d}%)'
    },
    series: [{
        type: 'pie',
        radius: ['40%', '70%'],
        data: [...fuel_pie_data]
    }]
}}/>

---

## Fuel Type by Site

<BarChart 
    data={fuel_by_site}
    x=site_name
    y=total_emissions
    series=fuel_type
    title="Fuel Consumption by Site"
    yFmt="#,##0"
    swapXY=true
    type=stacked
/>

---

## Monthly Fuel Consumption Trend

<AreaChart 
    data={fuel_monthly}
    x=month
    y=total_emissions
    series=fuel_type
    title="Monthly Emissions by Fuel Type"
    yFmt="#,##0"
    xSort=month_number
/>

---

## Emission Factors Analysis

Understanding the carbon intensity of different fuels.

<DataTable data={emission_factors} rows=15 search=true>
    <Column id=fuel_type title="Fuel Type"/>
    <Column id=emission_source title="Source"/>
    <Column id=avg_emission_factor title="Avg EF" fmt="#,##0.0000"/>
    <Column id=emission_factor_unit title="Unit"/>
    <Column id=samples title="Samples"/>
</DataTable>

<BarChart 
    data={emission_factors}
    x=fuel_type
    y=avg_emission_factor
    title="Average Emission Factor by Fuel Type"
    swapXY=true
/>

---

[← Site Analysis](/site-analysis) | [Dashboard →](/)
