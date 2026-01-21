-- =============================================================================
-- Carbon Lens BI - Wide Emissions View
-- =============================================================================
-- Creates a denormalized view combining all emission sources with company/site
-- dimensions for BI analytics in Evidence.
--
-- Source: local_cl_bi.db (Fivetran-ingested from MongoDB carbonLens)
-- Schema: mongo_carbonlens
-- 
-- Usage: duckdb local_cl_bi.db < 02_create_wide_bi_view.sql
-- =============================================================================

-- Drop existing views if they exist
DROP VIEW IF EXISTS mongo_carbonlens.v_emissions_wide;
DROP VIEW IF EXISTS mongo_carbonlens.v_emissions_summary;

-- =============================================================================
-- MAIN WIDE VIEW: All Emissions Unified
-- =============================================================================
CREATE OR REPLACE VIEW mongo_carbonlens.v_emissions_wide AS
WITH 
-- -----------------------------------------------------------------------------
-- Reference Data CTEs
-- -----------------------------------------------------------------------------
companies_dim AS (
    SELECT 
        _id AS company_id,
        company_name,
        company_email,
        address AS company_address,
        phone_number AS company_phone
    FROM mongo_carbonlens.companies
    WHERE _fivetran_deleted = false
),

sites_dim AS (
    SELECT 
        _id AS site_id,
        admin_id AS company_id,  -- Links to companies._id
        site_name,
        facility_city AS site_city,
        country AS site_country,
        sector_type,
        year_type
    FROM mongo_carbonlens.sites
    WHERE _fivetran_deleted = false
),

-- -----------------------------------------------------------------------------
-- Emission Source CTEs (UNION ALL for each scope/source)
-- -----------------------------------------------------------------------------

-- Scope 1: Stationary Combustion
scope1_stationary AS (
    SELECT 
        _id AS emission_id,
        company_id,
        site_id,
        user_id,
        CAST(year AS INTEGER) AS year,
        month,
        frequency,
        1 AS scope,
        'Stationary Combustion' AS emission_source,
        'stationarycombustions' AS source_table,
        name,
        calculated_emission,
        emission_factor,
        emission_factor_unit,
        -- Source-specific fields
        fuel_type,
        fuel_state,
        equipment,
        NULL AS vehicle_type,
        NULL AS vehicle_spec,
        NULL AS refrigerant,
        NULL AS transport_mode,
        NULL AS waste_type,
        NULL AS disposal_method,
        NULL AS waste_category,
        NULL AS hotel_type,
        NULL AS hotel_rating,
        NULL AS departure_city,
        NULL AS destination_city,
        NULL AS travel_class,
        NULL AS haul,
        -- Activity metrics
        activity AS activity_value,
        unit AS activity_unit,
        NULL::DOUBLE AS distance,
        NULL AS distance_unit,
        NULL::DOUBLE AS quantity,
        NULL::DOUBLE AS weight,
        NULL::INTEGER AS passengers,
        NULL::INTEGER AS nights_stayed,
        NULL::DOUBLE AS consumption,
        -- Financial
        spend AS spend_amount,
        currency,
        -- Status
        CASE 
            WHEN approved = 'true' OR approved = 'approved' THEN 'approved'
            WHEN approved = 'false' OR approved = 'pending' THEN 'pending'
            WHEN approved = 'rejected' THEN 'rejected'
            ELSE approved
        END AS approval_status,
        approved_by,
        -- Timestamps
        created_at,
        updated_at,
        source AS data_source,
        notes,
        _fivetran_synced
    FROM mongo_carbonlens.stationarycombustions
    WHERE _fivetran_deleted = false
),

-- Scope 1: Mobile Combustion
scope1_mobile AS (
    SELECT 
        _id AS emission_id,
        company_id,
        site_id,
        user_id,
        CAST(year AS INTEGER) AS year,
        month,
        frequency,
        1 AS scope,
        'Mobile Combustion' AS emission_source,
        'mobilecombustions' AS source_table,
        name,
        calculated_emission,
        emission_factor,
        emission_factor_unit,
        fuel_type,
        NULL AS fuel_state,
        NULL AS equipment,
        vehicle_type,
        vehicle_spec,
        NULL AS refrigerant,
        NULL AS transport_mode,
        NULL AS waste_type,
        NULL AS disposal_method,
        NULL AS waste_category,
        NULL AS hotel_type,
        NULL AS hotel_rating,
        NULL AS departure_city,
        NULL AS destination_city,
        NULL AS travel_class,
        NULL AS haul,
        activity AS activity_value,
        unit AS activity_unit,
        NULL::DOUBLE AS distance,
        NULL AS distance_unit,
        NULL::DOUBLE AS quantity,
        NULL::DOUBLE AS weight,
        NULL::INTEGER AS passengers,
        NULL::INTEGER AS nights_stayed,
        NULL::DOUBLE AS consumption,
        spend AS spend_amount,
        currency,
        CASE 
            WHEN approved = 'true' OR approved = 'approved' THEN 'approved'
            WHEN approved = 'false' OR approved = 'pending' THEN 'pending'
            WHEN approved = 'rejected' THEN 'rejected'
            ELSE approved
        END AS approval_status,
        approved_by,
        created_at,
        updated_at,
        source AS data_source,
        notes,
        _fivetran_synced
    FROM mongo_carbonlens.mobilecombustions
    WHERE _fivetran_deleted = false
),

-- Scope 1: Fugitive Emissions
scope1_fugitive AS (
    SELECT 
        _id AS emission_id,
        company_id,
        site_id,
        user_id,
        CAST(year AS INTEGER) AS year,
        month,
        frequency,
        1 AS scope,
        'Fugitive Emissions' AS emission_source,
        'fugitiveemissions' AS source_table,
        name,
        calculated_emission,
        CAST(emission_factor AS DOUBLE) AS emission_factor,
        emission_factor_unit,
        NULL AS fuel_type,
        NULL AS fuel_state,
        equipment,
        NULL AS vehicle_type,
        NULL AS vehicle_spec,
        refrigerant,
        NULL AS transport_mode,
        NULL AS waste_type,
        NULL AS disposal_method,
        NULL AS waste_category,
        NULL AS hotel_type,
        NULL AS hotel_rating,
        NULL AS departure_city,
        NULL AS destination_city,
        NULL AS travel_class,
        NULL AS haul,
        refill AS activity_value,
        unit AS activity_unit,
        NULL::DOUBLE AS distance,
        NULL AS distance_unit,
        CAST(units_no AS DOUBLE) AS quantity,
        NULL::DOUBLE AS weight,
        NULL::INTEGER AS passengers,
        NULL::INTEGER AS nights_stayed,
        NULL::DOUBLE AS consumption,
        CAST(spend AS DOUBLE) AS spend_amount,
        currency,
        CASE 
            WHEN approved = 'true' OR approved = 'approved' THEN 'approved'
            WHEN approved = 'false' OR approved = 'pending' THEN 'pending'
            WHEN approved = 'rejected' THEN 'rejected'
            ELSE approved
        END AS approval_status,
        approved_by,
        created_at,
        updated_at,
        source AS data_source,
        notes,
        _fivetran_synced
    FROM mongo_carbonlens.fugitiveemissions
    WHERE _fivetran_deleted = false
),

-- Scope 1: Fire Extinguishers (CO2)
scope1_fire AS (
    SELECT 
        _id AS emission_id,
        company_id,
        site_id,
        user_id,
        CAST(year AS INTEGER) AS year,
        month,
        frequency,
        1 AS scope,
        'Fire Extinguishers' AS emission_source,
        'fire_extinguishers' AS source_table,
        name,
        calculated_emission,
        CAST(emission_factor AS DOUBLE) AS emission_factor,
        emission_factor_unit,
        fuel_type,
        fuel_state,
        equipment,
        NULL AS vehicle_type,
        NULL AS vehicle_spec,
        NULL AS refrigerant,
        NULL AS transport_mode,
        NULL AS waste_type,
        NULL AS disposal_method,
        NULL AS waste_category,
        NULL AS hotel_type,
        NULL AS hotel_rating,
        NULL AS departure_city,
        NULL AS destination_city,
        NULL AS travel_class,
        NULL AS haul,
        CAST(activity AS DOUBLE) AS activity_value,
        unit AS activity_unit,
        NULL::DOUBLE AS distance,
        NULL AS distance_unit,
        NULL::DOUBLE AS quantity,
        NULL::DOUBLE AS weight,
        NULL::INTEGER AS passengers,
        NULL::INTEGER AS nights_stayed,
        NULL::DOUBLE AS consumption,
        CAST(spend AS DOUBLE) AS spend_amount,
        currency,
        CASE 
            WHEN approved = 'true' OR approved = 'approved' THEN 'approved'
            WHEN approved = 'false' OR approved = 'pending' THEN 'pending'
            WHEN approved = 'rejected' THEN 'rejected'
            ELSE approved
        END AS approval_status,
        NULL AS approved_by,
        created_at,
        updated_at,
        NULL AS data_source,
        NULL AS notes,
        _fivetran_synced
    FROM mongo_carbonlens.fire_extinguishers
    WHERE _fivetran_deleted = false
),

-- Scope 2: Purchased Electricity
scope2_purchased AS (
    SELECT 
        _id AS emission_id,
        company_id,
        site_id,
        user_id,
        CAST(year AS INTEGER) AS year,
        month,
        frequency,
        2 AS scope,
        'Purchased Electricity' AS emission_source,
        'purchasedelectricities' AS source_table,
        name,
        calculated_emission,
        emission_factor,
        emission_factor_unit,
        NULL AS fuel_type,
        NULL AS fuel_state,
        NULL AS equipment,
        NULL AS vehicle_type,
        NULL AS vehicle_spec,
        NULL AS refrigerant,
        NULL AS transport_mode,
        NULL AS waste_type,
        NULL AS disposal_method,
        NULL AS waste_category,
        NULL AS hotel_type,
        NULL AS hotel_rating,
        NULL AS departure_city,
        NULL AS destination_city,
        NULL AS travel_class,
        NULL AS haul,
        NULL::DOUBLE AS activity_value,
        unit AS activity_unit,
        NULL::DOUBLE AS distance,
        NULL AS distance_unit,
        NULL::DOUBLE AS quantity,
        NULL::DOUBLE AS weight,
        NULL::INTEGER AS passengers,
        NULL::INTEGER AS nights_stayed,
        TRY_CAST(consumption AS DOUBLE) AS consumption,
        TRY_CAST(spend_amount AS DOUBLE) AS spend_amount,
        currency,
        CASE 
            WHEN approved = 'true' OR approved = 'approved' THEN 'approved'
            WHEN approved = 'false' OR approved = 'pending' THEN 'pending'
            WHEN approved = 'rejected' THEN 'rejected'
            ELSE approved
        END AS approval_status,
        approved_by,
        created_at,
        updated_at,
        source AS data_source,
        notes,
        _fivetran_synced
    FROM mongo_carbonlens.purchasedelectricities
    WHERE _fivetran_deleted = false
),

-- Scope Renewable: Renewable Electricity (special scope)
scope_renewable AS (
    SELECT 
        _id AS emission_id,
        company_id,
        site_id,
        user_id,
        CAST(year AS INTEGER) AS year,
        month,
        frequency,
        0 AS scope,  -- Special scope for renewables
        'Renewable Electricity' AS emission_source,
        'renewableelectricities' AS source_table,
        name,
        calculated_emission,
        emission_factor,
        emission_factor_unit,
        NULL AS fuel_type,
        NULL AS fuel_state,
        NULL AS equipment,
        NULL AS vehicle_type,
        NULL AS vehicle_spec,
        NULL AS refrigerant,
        NULL AS transport_mode,
        NULL AS waste_type,
        NULL AS disposal_method,
        NULL AS waste_category,
        NULL AS hotel_type,
        NULL AS hotel_rating,
        NULL AS departure_city,
        NULL AS destination_city,
        NULL AS travel_class,
        NULL AS haul,
        NULL::DOUBLE AS activity_value,
        unit AS activity_unit,
        NULL::DOUBLE AS distance,
        NULL AS distance_unit,
        NULL::DOUBLE AS quantity,
        NULL::DOUBLE AS weight,
        NULL::INTEGER AS passengers,
        NULL::INTEGER AS nights_stayed,
        consumption,
        NULL::DOUBLE AS spend_amount,
        currency,
        CASE 
            WHEN approved = true THEN 'approved'
            ELSE 'pending'
        END AS approval_status,
        approved_by,
        created_at,
        updated_at,
        source AS data_source,
        NULL AS notes,
        _fivetran_synced
    FROM mongo_carbonlens.renewableelectricities
    WHERE _fivetran_deleted = false
),

-- Scope 3: Flight Travel
scope3_flights AS (
    SELECT 
        _id AS emission_id,
        company_id,
        site_id,
        user_id,
        CAST(year AS INTEGER) AS year,
        month,
        frequency,
        3 AS scope,
        'Flight Travel' AS emission_source,
        'flighttravels' AS source_table,
        name,
        calculated_emission,
        emission_factor,
        emission_factor_unit,
        NULL AS fuel_type,
        NULL AS fuel_state,
        NULL AS equipment,
        NULL AS vehicle_type,
        NULL AS vehicle_spec,
        NULL AS refrigerant,
        travel_mode AS transport_mode,
        NULL AS waste_type,
        NULL AS disposal_method,
        NULL AS waste_category,
        NULL AS hotel_type,
        NULL AS hotel_rating,
        departure_city,
        destination_city,
        class AS travel_class,
        flight_haul AS haul,
        NULL::DOUBLE AS activity_value,
        unit AS activity_unit,
        CAST(distance AS DOUBLE) AS distance,
        unit AS distance_unit,
        NULL::DOUBLE AS quantity,
        NULL::DOUBLE AS weight,
        no_of_passengers AS passengers,
        NULL::INTEGER AS nights_stayed,
        NULL::DOUBLE AS consumption,
        CAST(user_spend AS DOUBLE) AS spend_amount,
        user_spend_unit AS currency,
        CASE 
            WHEN approved = true THEN 'approved'
            ELSE 'pending'
        END AS approval_status,
        approved_by,
        created_at,
        updated_at,
        source AS data_source,
        NULL AS notes,
        _fivetran_synced
    FROM mongo_carbonlens.flighttravels
    WHERE _fivetran_deleted = false
),

-- Scope 3: Ground Travel
scope3_ground AS (
    SELECT 
        _id AS emission_id,
        company_id,
        site_id,
        user_id,
        CAST(year AS INTEGER) AS year,
        month,
        frequency,
        3 AS scope,
        'Ground Travel' AS emission_source,
        'groundtravels' AS source_table,
        name,
        calculated_emission,
        emission_factor,
        emission_factor_unit,
        NULL AS fuel_type,
        NULL AS fuel_state,
        NULL AS equipment,
        vehicle_type,
        vehicle_specification AS vehicle_spec,
        NULL AS refrigerant,
        'Ground' AS transport_mode,
        NULL AS waste_type,
        NULL AS disposal_method,
        NULL AS waste_category,
        NULL AS hotel_type,
        NULL AS hotel_rating,
        NULL AS departure_city,
        NULL AS destination_city,
        NULL AS travel_class,
        NULL AS haul,
        CAST(fuel_consumption AS DOUBLE) AS activity_value,
        unit AS activity_unit,
        CAST(total_distance_travelled AS DOUBLE) AS distance,
        unit_distance AS distance_unit,
        NULL::DOUBLE AS quantity,
        NULL::DOUBLE AS weight,
        number_of_passangers AS passengers,
        NULL::INTEGER AS nights_stayed,
        NULL::DOUBLE AS consumption,
        CAST(user_spend AS DOUBLE) AS spend_amount,
        user_spend_unit AS currency,
        CASE 
            WHEN approved = true THEN 'approved'
            ELSE 'pending'
        END AS approval_status,
        approved_by,
        created_at,
        updated_at,
        source AS data_source,
        NULL AS notes,
        _fivetran_synced
    FROM mongo_carbonlens.groundtravels
    WHERE _fivetran_deleted = false
),

-- Scope 3: Sea Travel
scope3_sea AS (
    SELECT 
        _id AS emission_id,
        company_id,
        site_id,
        user_id,
        CAST(year AS INTEGER) AS year,
        month,
        frequency,
        3 AS scope,
        'Sea Travel' AS emission_source,
        'seatravels' AS source_table,
        name,
        calculated_emission,
        emission_factor,
        emission_factor_unit,
        NULL AS fuel_type,
        NULL AS fuel_state,
        NULL AS equipment,
        vehicle_type,
        NULL AS vehicle_spec,
        NULL AS refrigerant,
        'Sea' AS transport_mode,
        NULL AS waste_type,
        NULL AS disposal_method,
        NULL AS waste_category,
        NULL AS hotel_type,
        NULL AS hotel_rating,
        departure_city,
        destination_city,
        passanger_type AS travel_class,
        NULL AS haul,
        NULL::DOUBLE AS activity_value,
        unit AS activity_unit,
        CAST(distance AS DOUBLE) AS distance,
        unit AS distance_unit,
        NULL::DOUBLE AS quantity,
        NULL::DOUBLE AS weight,
        no_of_passengers AS passengers,
        NULL::INTEGER AS nights_stayed,
        NULL::DOUBLE AS consumption,
        CAST(user_spend AS DOUBLE) AS spend_amount,
        user_spend_unit AS currency,
        CASE 
            WHEN approved = true THEN 'approved'
            ELSE 'pending'
        END AS approval_status,
        approved_by,
        created_at,
        updated_at,
        source AS data_source,
        NULL AS notes,
        _fivetran_synced
    FROM mongo_carbonlens.seatravels
    WHERE _fivetran_deleted = false
),

-- Scope 3: Accommodations
scope3_accommodation AS (
    SELECT 
        _id AS emission_id,
        company_id,
        site_id,
        user_id,
        CAST(year AS INTEGER) AS year,
        month,
        frequency,
        3 AS scope,
        'Accommodation' AS emission_source,
        'accomodations' AS source_table,
        name,
        calculated_emission,
        emission_factor,
        emission_factor_unit,
        NULL AS fuel_type,
        NULL AS fuel_state,
        NULL AS equipment,
        NULL AS vehicle_type,
        NULL AS vehicle_spec,
        NULL AS refrigerant,
        NULL AS transport_mode,
        NULL AS waste_type,
        NULL AS disposal_method,
        NULL AS waste_category,
        hotel_type,
        hotel_rating,
        NULL AS departure_city,
        region_of_hotel AS destination_city,
        NULL AS travel_class,
        NULL AS haul,
        NULL::DOUBLE AS activity_value,
        units AS activity_unit,
        NULL::DOUBLE AS distance,
        NULL AS distance_unit,
        NULL::DOUBLE AS quantity,
        NULL::DOUBLE AS weight,
        number_of_passangers AS passengers,
        number_of_nights_stayed AS nights_stayed,
        NULL::DOUBLE AS consumption,
        CAST(user_spend AS DOUBLE) AS spend_amount,
        user_spend_unit AS currency,
        CASE 
            WHEN approved = true THEN 'approved'
            ELSE 'pending'
        END AS approval_status,
        approved_by,
        created_at,
        updated_at,
        source AS data_source,
        NULL AS notes,
        _fivetran_synced
    FROM mongo_carbonlens.accomodations
    WHERE _fivetran_deleted = false
),

-- Scope 3: Waste Generation
scope3_waste AS (
    SELECT 
        _id AS emission_id,
        company_id,
        site_id,
        user_id,
        CAST(year AS INTEGER) AS year,
        month,
        frequency,
        3 AS scope,
        'Waste Generation' AS emission_source,
        'wastegenerations' AS source_table,
        name,
        calculated_emission,
        emission_factor,
        emission_factor_unit,
        NULL AS fuel_type,
        NULL AS fuel_state,
        NULL AS equipment,
        NULL AS vehicle_type,
        NULL AS vehicle_spec,
        NULL AS refrigerant,
        NULL AS transport_mode,
        waste_type,
        disposal_method,
        waste_category,
        NULL AS hotel_type,
        NULL AS hotel_rating,
        NULL AS departure_city,
        NULL AS destination_city,
        NULL AS travel_class,
        NULL AS haul,
        NULL::DOUBLE AS activity_value,
        unit AS activity_unit,
        NULL::DOUBLE AS distance,
        NULL AS distance_unit,
        quantity,
        NULL::DOUBLE AS weight,
        NULL::INTEGER AS passengers,
        NULL::INTEGER AS nights_stayed,
        NULL::DOUBLE AS consumption,
        NULL::DOUBLE AS spend_amount,
        NULL AS currency,
        CASE 
            WHEN approved = true THEN 'approved'
            ELSE 'pending'
        END AS approval_status,
        approved_by,
        created_at,
        updated_at,
        source AS data_source,
        notes,
        _fivetran_synced
    FROM mongo_carbonlens.wastegenerations
    WHERE _fivetran_deleted = false
),

-- Scope 3: End of Life Treatment
scope3_eol AS (
    SELECT 
        _id AS emission_id,
        company_id,
        site_id,
        user_id,
        CAST(year AS INTEGER) AS year,
        month,
        frequency,
        3 AS scope,
        'End of Life Treatment' AS emission_source,
        'endoflifetreatments' AS source_table,
        name,
        calculated_emission,
        emission_factor,
        emission_factor_unit,
        NULL AS fuel_type,
        NULL AS fuel_state,
        NULL AS equipment,
        NULL AS vehicle_type,
        NULL AS vehicle_spec,
        NULL AS refrigerant,
        NULL AS transport_mode,
        waste_type,
        disposal_method,
        waste_category,
        NULL AS hotel_type,
        NULL AS hotel_rating,
        NULL AS departure_city,
        NULL AS destination_city,
        NULL AS travel_class,
        NULL AS haul,
        NULL::DOUBLE AS activity_value,
        unit AS activity_unit,
        NULL::DOUBLE AS distance,
        NULL AS distance_unit,
        CAST(quantity AS DOUBLE) AS quantity,
        NULL::DOUBLE AS weight,
        NULL::INTEGER AS passengers,
        NULL::INTEGER AS nights_stayed,
        NULL::DOUBLE AS consumption,
        NULL::DOUBLE AS spend_amount,
        NULL AS currency,
        CASE 
            WHEN approved = true THEN 'approved'
            ELSE 'pending'
        END AS approval_status,
        approved_by,
        created_at,
        updated_at,
        source AS data_source,
        NULL AS notes,
        _fivetran_synced
    FROM mongo_carbonlens.endoflifetreatments
    WHERE _fivetran_deleted = false
),

-- Scope 3: Upstream Transportation
scope3_upstream AS (
    SELECT 
        _id AS emission_id,
        company_id,
        site_id,
        user_id,
        CAST(year AS INTEGER) AS year,
        month,
        frequency,
        3 AS scope,
        'Upstream Transportation' AS emission_source,
        'upstreamtransportations' AS source_table,
        name,
        calculated_emission,
        emission_factor,
        emission_factor_unit,
        NULL AS fuel_type,
        NULL AS fuel_state,
        NULL AS equipment,
        vehicle_type,
        vehicle_specification AS vehicle_spec,
        NULL AS refrigerant,
        transport_mode,
        product_type AS waste_type,  -- Reusing for product info
        laden AS disposal_method,     -- Reusing for laden status
        NULL AS waste_category,
        NULL AS hotel_type,
        NULL AS hotel_rating,
        NULL AS departure_city,
        NULL AS destination_city,
        NULL AS travel_class,
        NULL AS haul,
        NULL::DOUBLE AS activity_value,
        unit AS activity_unit,
        CAST(distance AS DOUBLE) AS distance,
        unit_of_distance AS distance_unit,
        CAST(quantity AS DOUBLE) AS quantity,
        NULL::DOUBLE AS weight,
        NULL::INTEGER AS passengers,
        NULL::INTEGER AS nights_stayed,
        NULL::DOUBLE AS consumption,
        NULL::DOUBLE AS spend_amount,
        NULL AS currency,
        CASE 
            WHEN approved = true THEN 'approved'
            ELSE 'pending'
        END AS approval_status,
        approved_by,
        created_at,
        updated_at,
        source AS data_source,
        NULL AS notes,
        _fivetran_synced
    FROM mongo_carbonlens.upstreamtransportations
    WHERE _fivetran_deleted = false
),

-- Scope 3: Downstream Transportation (DTD)
scope3_downstream AS (
    SELECT 
        _id AS emission_id,
        company_id,
        site_id,
        user_id,
        CAST(year AS INTEGER) AS year,
        month,
        frequency,
        3 AS scope,
        'Downstream Transportation' AS emission_source,
        'dtds' AS source_table,
        name,
        calculated_emission,
        emission_factor,
        emission_factor_unit,
        NULL AS fuel_type,
        NULL AS fuel_state,
        NULL AS equipment,
        vehicle_type,
        vehicle_specification AS vehicle_spec,
        NULL AS refrigerant,
        transport_mode,
        product_type AS waste_type,
        NULL AS disposal_method,
        NULL AS waste_category,
        NULL AS hotel_type,
        NULL AS hotel_rating,
        NULL AS departure_city,
        NULL AS destination_city,
        NULL AS travel_class,
        NULL AS haul,
        NULL::DOUBLE AS activity_value,
        unit AS activity_unit,
        CAST(distance AS DOUBLE) AS distance,
        unit_of_distance AS distance_unit,
        CAST(quantity AS DOUBLE) AS quantity,
        NULL::DOUBLE AS weight,
        NULL::INTEGER AS passengers,
        NULL::INTEGER AS nights_stayed,
        NULL::DOUBLE AS consumption,
        NULL::DOUBLE AS spend_amount,
        NULL AS currency,
        CASE 
            WHEN approved = 'true' OR approved = 'approved' THEN 'approved'
            WHEN approved = 'false' OR approved = 'pending' THEN 'pending'
            WHEN approved = 'rejected' THEN 'rejected'
            ELSE approved
        END AS approval_status,
        approved_by,
        created_at,
        updated_at,
        source AS data_source,
        NULL AS notes,
        _fivetran_synced
    FROM mongo_carbonlens.dtds
    WHERE _fivetran_deleted = false
),

-- Scope 3: Capital Goods
scope3_capital AS (
    SELECT 
        _id AS emission_id,
        company_id,
        site_id,
        user_id,
        CAST(year AS INTEGER) AS year,
        month,
        frequency,
        3 AS scope,
        'Capital Goods' AS emission_source,
        'capitalgoods' AS source_table,
        name,
        calculated_emission,
        emission_factor,
        emission_factor_unit,
        NULL AS fuel_type,
        NULL AS fuel_state,
        NULL AS equipment,
        NULL AS vehicle_type,
        NULL AS vehicle_spec,
        NULL AS refrigerant,
        NULL AS transport_mode,
        product_name AS waste_type,
        production_type AS disposal_method,
        category AS waste_category,
        NULL AS hotel_type,
        NULL AS hotel_rating,
        NULL AS departure_city,
        NULL AS destination_city,
        NULL AS travel_class,
        NULL AS haul,
        NULL::DOUBLE AS activity_value,
        unit AS activity_unit,
        NULL::DOUBLE AS distance,
        NULL AS distance_unit,
        CAST(quantity AS DOUBLE) AS quantity,
        CAST(weight AS DOUBLE) AS weight,
        NULL::INTEGER AS passengers,
        NULL::INTEGER AS nights_stayed,
        NULL::DOUBLE AS consumption,
        CAST(user_spend AS DOUBLE) AS spend_amount,
        user_spend_unit AS currency,
        CASE 
            WHEN approved = true THEN 'approved'
            ELSE 'pending'
        END AS approval_status,
        approved_by,
        created_at,
        updated_at,
        source AS data_source,
        NULL AS notes,
        _fivetran_synced
    FROM mongo_carbonlens.capitalgoods
    WHERE _fivetran_deleted = false
),

-- Scope 3: Raw Materials
scope3_rawmaterials AS (
    SELECT 
        _id AS emission_id,
        company_id,
        site_id,
        user_id,
        CAST(year AS INTEGER) AS year,
        month,
        frequency,
        3 AS scope,
        'Raw Materials' AS emission_source,
        'rawmaterials' AS source_table,
        name,
        calculated_emission,
        emission_factor,
        emission_factor_unit,
        NULL AS fuel_type,
        NULL AS fuel_state,
        NULL AS equipment,
        NULL AS vehicle_type,
        NULL AS vehicle_spec,
        NULL AS refrigerant,
        NULL AS transport_mode,
        product_name AS waste_type,
        NULL AS disposal_method,
        category AS waste_category,
        NULL AS hotel_type,
        NULL AS hotel_rating,
        NULL AS departure_city,
        NULL AS destination_city,
        NULL AS travel_class,
        NULL AS haul,
        NULL::DOUBLE AS activity_value,
        unit AS activity_unit,
        NULL::DOUBLE AS distance,
        NULL AS distance_unit,
        CAST(quantity AS DOUBLE) AS quantity,
        CAST(weight AS DOUBLE) AS weight,
        NULL::INTEGER AS passengers,
        NULL::INTEGER AS nights_stayed,
        NULL::DOUBLE AS consumption,
        CAST(user_spend AS DOUBLE) AS spend_amount,
        user_spend_unit AS currency,
        CASE 
            WHEN approved = 'true' OR approved = 'approved' THEN 'approved'
            WHEN approved = 'false' OR approved = 'pending' THEN 'pending'
            WHEN approved = 'rejected' THEN 'rejected'
            ELSE approved
        END AS approval_status,
        approved_by,
        created_at,
        updated_at,
        source AS data_source,
        NULL AS notes,
        _fivetran_synced
    FROM mongo_carbonlens.rawmaterials
    WHERE _fivetran_deleted = false
),

-- Scope 3: FERA Stationary
scope3_fera_stationary AS (
    SELECT 
        _id AS emission_id,
        company_id,
        site_id,
        user_id,
        CAST(year AS INTEGER) AS year,
        month,
        frequency,
        3 AS scope,
        'FERA Stationary' AS emission_source,
        'ferastationaries' AS source_table,
        name,
        calculated_emission,
        emission_factor,
        emission_factor_unit,
        fuel_type,
        fuel_state,
        equipment,
        NULL AS vehicle_type,
        NULL AS vehicle_spec,
        NULL AS refrigerant,
        NULL AS transport_mode,
        NULL AS waste_type,
        NULL AS disposal_method,
        NULL AS waste_category,
        NULL AS hotel_type,
        NULL AS hotel_rating,
        NULL AS departure_city,
        NULL AS destination_city,
        NULL AS travel_class,
        NULL AS haul,
        CAST(activity AS DOUBLE) AS activity_value,
        unit AS activity_unit,
        NULL::DOUBLE AS distance,
        NULL AS distance_unit,
        NULL::DOUBLE AS quantity,
        NULL::DOUBLE AS weight,
        NULL::INTEGER AS passengers,
        NULL::INTEGER AS nights_stayed,
        NULL::DOUBLE AS consumption,
        CAST(spend AS DOUBLE) AS spend_amount,
        currency,
        CASE 
            WHEN approved = 'true' OR approved = 'approved' THEN 'approved'
            WHEN approved = 'false' OR approved = 'pending' THEN 'pending'
            WHEN approved = 'rejected' THEN 'rejected'
            ELSE approved
        END AS approval_status,
        approved_by,
        created_at,
        updated_at,
        NULL AS data_source,
        NULL AS notes,
        _fivetran_synced
    FROM mongo_carbonlens.ferastationaries
    WHERE _fivetran_deleted = false
),

-- Scope 3: FERA Mobile
scope3_fera_mobile AS (
    SELECT 
        _id AS emission_id,
        company_id,
        site_id,
        user_id,
        CAST(year AS INTEGER) AS year,
        month,
        frequency,
        3 AS scope,
        'FERA Mobile' AS emission_source,
        'feramobiles' AS source_table,
        name,
        calculated_emission,
        emission_factor,
        emission_factor_unit,
        fuel_type,
        NULL AS fuel_state,
        NULL AS equipment,
        vehicle_type,
        vehicle_spec,
        NULL AS refrigerant,
        NULL AS transport_mode,
        NULL AS waste_type,
        NULL AS disposal_method,
        NULL AS waste_category,
        NULL AS hotel_type,
        NULL AS hotel_rating,
        NULL AS departure_city,
        NULL AS destination_city,
        NULL AS travel_class,
        NULL AS haul,
        CAST(activity AS DOUBLE) AS activity_value,
        unit AS activity_unit,
        NULL::DOUBLE AS distance,
        NULL AS distance_unit,
        NULL::DOUBLE AS quantity,
        NULL::DOUBLE AS weight,
        NULL::INTEGER AS passengers,
        NULL::INTEGER AS nights_stayed,
        NULL::DOUBLE AS consumption,
        NULL::DOUBLE AS spend_amount,
        currency,
        CASE 
            WHEN approved = true THEN 'approved'
            ELSE 'pending'
        END AS approval_status,
        approved_by,
        created_at,
        updated_at,
        NULL AS data_source,
        NULL AS notes,
        _fivetran_synced
    FROM mongo_carbonlens.feramobiles
    WHERE _fivetran_deleted = false
),

-- Scope 3: FERA Electricity
scope3_fera_elec AS (
    SELECT 
        _id AS emission_id,
        company_id,
        site_id,
        user_id,
        CAST(year AS INTEGER) AS year,
        month,
        frequency,
        3 AS scope,
        'FERA Electricity' AS emission_source,
        'feraelectricities' AS source_table,
        name,
        calculated_emission,
        emission_factor,
        emission_factor_unit,
        NULL AS fuel_type,
        NULL AS fuel_state,
        NULL AS equipment,
        NULL AS vehicle_type,
        NULL AS vehicle_spec,
        NULL AS refrigerant,
        NULL AS transport_mode,
        NULL AS waste_type,
        NULL AS disposal_method,
        NULL AS waste_category,
        NULL AS hotel_type,
        NULL AS hotel_rating,
        NULL AS departure_city,
        NULL AS destination_city,
        NULL AS travel_class,
        NULL AS haul,
        NULL::DOUBLE AS activity_value,
        unit AS activity_unit,
        NULL::DOUBLE AS distance,
        NULL AS distance_unit,
        NULL::DOUBLE AS quantity,
        NULL::DOUBLE AS weight,
        NULL::INTEGER AS passengers,
        NULL::INTEGER AS nights_stayed,
        CAST(consumption AS DOUBLE) AS consumption,
        NULL::DOUBLE AS spend_amount,
        NULL AS currency,
        CASE 
            WHEN approved = true THEN 'approved'
            ELSE 'pending'
        END AS approval_status,
        approved_by,
        created_at,
        updated_at,
        source AS data_source,
        NULL AS notes,
        _fivetran_synced
    FROM mongo_carbonlens.feraelectricities
    WHERE _fivetran_deleted = false
),

-- -----------------------------------------------------------------------------
-- UNION ALL emission sources
-- -----------------------------------------------------------------------------
all_emissions AS (
    SELECT * FROM scope1_stationary
    UNION ALL SELECT * FROM scope1_mobile
    UNION ALL SELECT * FROM scope1_fugitive
    UNION ALL SELECT * FROM scope1_fire
    UNION ALL SELECT * FROM scope2_purchased
    UNION ALL SELECT * FROM scope_renewable
    UNION ALL SELECT * FROM scope3_flights
    UNION ALL SELECT * FROM scope3_ground
    UNION ALL SELECT * FROM scope3_sea
    UNION ALL SELECT * FROM scope3_accommodation
    UNION ALL SELECT * FROM scope3_waste
    UNION ALL SELECT * FROM scope3_eol
    UNION ALL SELECT * FROM scope3_upstream
    UNION ALL SELECT * FROM scope3_downstream
    UNION ALL SELECT * FROM scope3_capital
    UNION ALL SELECT * FROM scope3_rawmaterials
    UNION ALL SELECT * FROM scope3_fera_stationary
    UNION ALL SELECT * FROM scope3_fera_mobile
    UNION ALL SELECT * FROM scope3_fera_elec
)

-- -----------------------------------------------------------------------------
-- Final SELECT with dimension joins
-- -----------------------------------------------------------------------------
SELECT 
    -- Record ID
    e.emission_id,
    
    -- Company Dimension
    e.company_id,
    c.company_name,
    c.company_email,
    
    -- Site Dimension
    e.site_id,
    s.site_name,
    s.site_city,
    s.site_country,
    s.sector_type,
    
    -- Time Dimension
    e.year,
    -- Nullify invalid month values like 'Select Month'
    CASE 
        WHEN e.month IN ('January', 'February', 'March', 'April', 'May', 'June',
                         'July', 'August', 'September', 'October', 'November', 'December')
        THEN e.month
        ELSE NULL
    END AS month,
    -- Derive quarter from month name
    CASE 
        WHEN e.month IN ('January', 'February', 'March') THEN 1
        WHEN e.month IN ('April', 'May', 'June') THEN 2
        WHEN e.month IN ('July', 'August', 'September') THEN 3
        WHEN e.month IN ('October', 'November', 'December') THEN 4
        ELSE NULL
    END AS quarter,
    -- Month number for sorting
    CASE e.month
        WHEN 'January' THEN 1
        WHEN 'February' THEN 2
        WHEN 'March' THEN 3
        WHEN 'April' THEN 4
        WHEN 'May' THEN 5
        WHEN 'June' THEN 6
        WHEN 'July' THEN 7
        WHEN 'August' THEN 8
        WHEN 'September' THEN 9
        WHEN 'October' THEN 10
        WHEN 'November' THEN 11
        WHEN 'December' THEN 12
        ELSE NULL
    END AS month_number,
    e.frequency,
    
    -- Scope Classification
    e.scope,
    CASE e.scope 
        WHEN 0 THEN 'Renewables'
        WHEN 1 THEN 'Scope 1'
        WHEN 2 THEN 'Scope 2'
        WHEN 3 THEN 'Scope 3'
    END AS scope_label,
    e.emission_source,
    e.source_table,
    e.name AS entry_name,
    
    -- Primary Metrics
    e.calculated_emission,
    e.emission_factor,
    e.emission_factor_unit,
    
    -- Source-specific attributes
    e.fuel_type,
    e.fuel_state,
    e.equipment,
    e.vehicle_type,
    e.vehicle_spec,
    e.refrigerant,
    e.transport_mode,
    e.waste_type,
    e.disposal_method,
    e.waste_category,
    e.hotel_type,
    e.hotel_rating,
    e.departure_city,
    e.destination_city,
    e.travel_class,
    e.haul,
    
    -- Activity Metrics
    e.activity_value,
    e.activity_unit,
    e.distance,
    e.distance_unit,
    e.quantity,
    e.weight,
    e.passengers,
    e.nights_stayed,
    e.consumption,
    
    -- Financial
    e.spend_amount,
    e.currency,
    
    -- Status
    e.approval_status,
    e.approved_by,
    e.user_id,
    
    -- Metadata
    e.data_source,
    e.notes,
    e.created_at,
    e.updated_at,
    e._fivetran_synced AS synced_at

FROM all_emissions e
LEFT JOIN companies_dim c ON e.company_id = c.company_id
LEFT JOIN sites_dim s ON e.site_id = s.site_id
WHERE 
e.approval_status = 'approved' 
and s.site_name IS NOT NULL 
and c.company_name NOT ILIKE '%demo%';


-- =============================================================================
-- SUMMARY VIEW: Aggregated by Scope/Source
-- =============================================================================
CREATE VIEW mongo_carbonlens.v_emissions_summary AS
SELECT 
    company_id,
    company_name,
    site_id,
    site_name,
    year,
    quarter,
    scope,
    scope_label,
    emission_source,
    approval_status,
    COUNT(*) AS record_count,
    SUM(calculated_emission) AS total_emissions,
    AVG(calculated_emission) AS avg_emission,
    MIN(calculated_emission) AS min_emission,
    MAX(calculated_emission) AS max_emission,
    SUM(spend_amount) AS total_spend,
    MAX(synced_at) AS last_synced
FROM mongo_carbonlens.v_emissions_wide
GROUP BY 
    company_id, company_name, 
    site_id, site_name,
    year, quarter,
    scope, scope_label, 
    emission_source,
    approval_status;


-- =============================================================================
-- Verification Queries
-- =============================================================================
-- Quick check - record counts by scope
SELECT 
    scope,
    scope_label,
    COUNT(*) AS records,
    ROUND(SUM(calculated_emission), 2) AS total_co2e
FROM mongo_carbonlens.v_emissions_wide
GROUP BY scope, scope_label
ORDER BY scope;

-- Check source distribution
SELECT 
    scope,
    emission_source,
    scope_label,
    COUNT(*) AS records
FROM mongo_carbonlens.v_emissions_wide
GROUP BY scope, emission_source, scope_label
ORDER BY scope, emission_source;
