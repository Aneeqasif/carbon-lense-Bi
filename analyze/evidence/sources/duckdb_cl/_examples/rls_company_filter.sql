-- =============================================================================
-- ROW LEVEL SECURITY (RLS) Examples for Company-Based Filtering
-- =============================================================================
-- These examples show how to mimic RLS by filtering data to specific companies.
-- In a real application, the company_id would come from the authenticated user's
-- session/JWT token.
--
-- Usage: Replace ':company_id' with the actual company_id value
-- =============================================================================


-- -----------------------------------------------------------------------------
-- EXAMPLE 1: Basic company filter
-- Get all emissions for a specific company
-- -----------------------------------------------------------------------------
SELECT *
FROM mongo_carbonlens.v_emissions_wide
WHERE company_id = '6891bb1896163182f4b2783e';  -- MidalCables


-- -----------------------------------------------------------------------------
-- EXAMPLE 2: Company filter with aggregation
-- Total emissions by scope for a specific company
-- -----------------------------------------------------------------------------
SELECT 
    company_name,
    scope_label,
    COUNT(*) as records,
    ROUND(SUM(calculated_emission), 2) as total_emissions
FROM mongo_carbonlens.v_emissions_wide
WHERE company_id = '68a5883965ea59a1f18b3486'  -- Harman_Finochem
GROUP BY company_name, scope_label
ORDER BY scope_label;


-- -----------------------------------------------------------------------------
-- EXAMPLE 3: Company filter with site breakdown
-- Emissions by site for a specific company
-- -----------------------------------------------------------------------------
SELECT 
    site_name,
    site_city,
    site_country,
    scope_label,
    ROUND(SUM(calculated_emission), 2) as total_emissions,
    COUNT(*) as records
FROM mongo_carbonlens.v_emissions_wide
WHERE company_id = '6891bb1896163182f4b2783e'  -- MidalCables
GROUP BY site_name, site_city, site_country, scope_label
ORDER BY total_emissions DESC;


-- -----------------------------------------------------------------------------
-- EXAMPLE 4: Parameterized query pattern (for application use)
-- Replace $1 or :company_id with bound parameter
-- -----------------------------------------------------------------------------
-- In your application code:
-- const companyId = getUserCompanyId(req.user);  // From JWT/session
-- const result = await db.query(sql, [companyId]);

-- Pattern for DuckDB with parameter binding:
PREPARE company_emissions AS
SELECT 
    year,
    month,
    scope_label,
    emission_source,
    ROUND(SUM(calculated_emission), 2) as total_emissions
FROM mongo_carbonlens.v_emissions_wide
WHERE company_id = $1
GROUP BY year, month, scope_label, emission_source
ORDER BY year DESC, month;

-- Execute with: EXECUTE company_emissions('6891bb1896163182f4b2783e');


-- -----------------------------------------------------------------------------
-- EXAMPLE 5: Create a company-scoped view (alternative approach)
-- This could be used if you want to create views per company
-- -----------------------------------------------------------------------------
-- CREATE VIEW mongo_carbonlens.v_emissions_midalcables AS
-- SELECT * FROM mongo_carbonlens.v_emissions_wide
-- WHERE company_id = '6891bb1896163182f4b2783e';


-- -----------------------------------------------------------------------------
-- EXAMPLE 6: Multi-company filter (for consultants/admins with access to multiple)
-- Use IN clause for users with access to multiple companies
-- -----------------------------------------------------------------------------
SELECT 
    company_name,
    year,
    scope_label,
    ROUND(SUM(calculated_emission), 2) as total_emissions
FROM mongo_carbonlens.v_emissions_wide
WHERE company_id IN (
    '6891bb1896163182f4b2783e',  -- MidalCables
    '68b9632065e87c4c92f6fc69'   -- Demo
)
GROUP BY company_name, year, scope_label
ORDER BY company_name, year, scope_label;


-- -----------------------------------------------------------------------------
-- EXAMPLE 7: Evidence BI - Using Dropdown for company filter
-- In your Evidence markdown pages, you can create a company dropdown:
-- -----------------------------------------------------------------------------
/*
-- In Evidence .md file:

```sql companies
SELECT DISTINCT company_id, company_name 
FROM duckdb_cl.full_emissions 
WHERE company_name IS NOT NULL
ORDER BY company_name
```

<Dropdown 
    name=selected_company
    data={companies}
    value=company_id
    label=company_name
    title="Select Company"
/>

```sql company_emissions
SELECT 
    site_name,
    scope_label,
    emission_source,
    ROUND(SUM(calculated_emission), 2) as total_emissions
FROM duckdb_cl.full_emissions
WHERE company_id = '${inputs.selected_company.value}'
GROUP BY site_name, scope_label, emission_source
ORDER BY total_emissions DESC
```

<BarChart 
    data={company_emissions}
    x=site_name
    y=total_emissions
    series=scope_label
    type=stacked
/>
*/


-- -----------------------------------------------------------------------------
-- EXAMPLE 8: Complete RLS pattern for API/Backend
-- Mimics what you'd do in a Node.js/Express backend
-- -----------------------------------------------------------------------------
/*
// In your Node.js backend:

const getCompanyEmissions = async (req, res) => {
    // Get company_id from authenticated user (JWT claim or session)
    const companyId = req.user.company_id;
    
    if (!companyId) {
        return res.status(403).json({ error: 'No company access' });
    }
    
    const query = `
        SELECT 
            site_name,
            year,
            month,
            scope_label,
            emission_source,
            SUM(calculated_emission) as total_emissions
        FROM mongo_carbonlens.v_emissions_wide
        WHERE company_id = $1
        GROUP BY site_name, year, month, scope_label, emission_source
        ORDER BY year DESC, month
    `;
    
    const result = await db.query(query, [companyId]);
    res.json(result.rows);
};
*/


-- -----------------------------------------------------------------------------
-- QUICK REFERENCE: Available Companies
-- -----------------------------------------------------------------------------
SELECT 
    company_id,
    company_name,
    COUNT(*) as emission_records,
    COUNT(DISTINCT site_id) as sites,
    ROUND(SUM(calculated_emission), 2) as total_emissions
FROM mongo_carbonlens.v_emissions_wide
GROUP BY company_id, company_name
ORDER BY total_emissions DESC;

/*
Results:
┌──────────────────────────┬─────────────────┬─────────┬─────────────────┐
│        company_id        │  company_name   │ records │ total_emissions │
├──────────────────────────┼─────────────────┼─────────┼─────────────────┤
│ 6891bb1896163182f4b2783e │ MidalCables     │     309 │       251431.93 │
│ 68a5883965ea59a1f18b3486 │ Harman_Finochem │     261 │      2781876.11 │
│ 68b9632065e87c4c92f6fc69 │ Demo            │      85 │       135915.03 │
│ 685b9a84a92ea4956a357241 │ company_demo    │      14 │        15416.28 │
│ 6895d837e4928843c7c33e29 │ Glochem         │       2 │           25.16 │
└──────────────────────────┴─────────────────┴─────────┴─────────────────┘
*/
