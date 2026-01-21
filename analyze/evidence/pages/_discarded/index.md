---
title: Carbon Lens BI Dashboard
---

# 🌍 Carbon Lens - Emissions Analytics

Welcome to the Carbon Lens Business Intelligence Dashboard. This platform provides comprehensive analytics on greenhouse gas emissions across all scopes.

```sql summary
SELECT * FROM duckdb_cl.summary_kpis
```

```sql scope_totals
SELECT * FROM duckdb_cl.emissions_by_scope
```

```sql yearly_data
SELECT * FROM duckdb_cl.emissions_yearly_trend
```

```sql company_data
SELECT * FROM duckdb_cl.company_comparison
```

```sql main_table
select * from duckdb_cl.full_emissions
```

## Key Metrics

<BigValue 
  data={summary}
  value=total_emissions
  title="Total Emissions (tCO₂e)"
  fmt="#,##0"
/>

<BigValue 
  data={summary}
  value=scope1_total
  title="Scope 1"
  fmt="#,##0"
/>

<BigValue 
  data={summary}
  value=scope2_total
  title="Scope 2"
  fmt="#,##0"
/>

<BigValue 
  data={summary}
  value=scope3_total
  title="Scope 3"
  fmt="#,##0"
/>

<BigValue 
  data={summary}
  value=sites_count
  title="Active Sites"
/>

<BigValue 
  data={summary}
  value=companies_count
  title="Companies"
/>

---

## Emissions by Scope

<BarChart 
    data={scope_totals}
    x=scope_label
    y=total_emissions
    title="Total Emissions by Scope (tCO₂e)"
    yFmt="#,##0"
    colorPalette={['#236aa4', '#45a1bf', '#85c7c6', '#f4b548']}
/>

<DataTable data={scope_totals} rows=5>
    <Column id=scope_label title="Scope"/>
    <Column id=total_emissions title="Emissions (tCO₂e)" fmt="#,##0.00"/>
    <Column id=record_count title="Records"/>
</DataTable>

---

## Yearly Emissions Trend

<LineChart 
    data={yearly_data}
    x=year
    y=total_emissions
    series=scope_label
    title="Emissions Trend by Year and Scope"
    yFmt="#,##0"
    markers=true
/>

---

## Company Performance

<BarChart 
    data={company_data}
    x=company_name
    y={['scope1', 'scope2', 'scope3']}
    title="Emissions by Company (Stacked by Scope)"
    type=stacked
    yFmt="#,##0"
    swapXY=true
/>

---

## Quick Navigation

<Grid cols=4>
    <BigLink url="/scope-analysis">📊 Scope Analysis</BigLink>
    <BigLink url="/site-analysis">🏭 Site Analysis</BigLink>
    <BigLink url="/fuel-analysis">⛽ Fuel Analysis</BigLink>
    <BigLink url="/trends">📈 Trends & Insights</BigLink>
</Grid>

---

<!-- <Alert status="info">
    Data is sourced from MongoDB via Fivetran and reflects approved emission records only.
</Alert> -->
