-- =============================================================================
-- Carbon Lens Data Warehouse - View Creation Script
-- Run this script ONCE after ingestion to create BI-ready views
-- =============================================================================
-- This script creates:
--   1. rawjd.v_* views - Parse JSON documents into flat columns with CDC dedup
--   2. rawmp.v_* views - Clean versions of mapped data with CDC dedup
--   3. All views filter out deleted records and show only current state
-- =============================================================================
-- CDC DEDUPLICATION STRATEGY:
--   - QUALIFY ROW_NUMBER() partitions by document ID, orders by timestamp DESC
--   - Filter _sdc_deleted_at IS NULL to exclude soft deletes
--   - This ensures only the latest version of each document is returned
-- =============================================================================

-- #############################################################################
-- PART 1: RAW JSON DOCUMENTS SCHEMA (rawjd)
-- Parse JSON documents into typed columns for easy querying
-- #############################################################################

-- =============================================================================
-- DIMENSION TABLES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- v_current_users: User master data
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS rawjd.v_current_users;
CREATE VIEW rawjd.v_current_users AS
SELECT 
    object_id as _id,
    document->>'name' as name,
    document->>'email' as email,
    document->>'role' as role,
    document->>'status' as status,
    CAST(document->>'isAdmin' AS BOOLEAN) as is_admin,
    document->>'admin' as admin_id,
    document->>'company' as company_id,
    document->>'createdAt' as created_at,
    document->>'updatedAt' as updated_at,
    -- CDC metadata for debugging
    cluster_time,
    operation_type,
    _sdc_extracted_at
FROM rawjd.users
WHERE _sdc_deleted_at IS NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY object_id 
    ORDER BY cluster_time DESC NULLS LAST, _sdc_extracted_at DESC
) = 1;

-- -----------------------------------------------------------------------------
-- v_current_companies: Company master data
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS rawjd.v_current_companies;
CREATE VIEW rawjd.v_current_companies AS
SELECT 
    object_id as _id,
    document->>'companyName' as company_name,
    document->>'companyEmail' as company_email,
    document->>'address' as address,
    document->>'admin' as admin_id,
    document->>'subscriptionStatus' as subscription_status,
    document->>'accountType' as account_type,
    document->>'createdAt' as created_at,
    document->>'updatedAt' as updated_at,
    cluster_time,
    operation_type,
    _sdc_extracted_at
FROM rawjd.companies
WHERE _sdc_deleted_at IS NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY object_id 
    ORDER BY cluster_time DESC NULLS LAST, _sdc_extracted_at DESC
) = 1;

-- -----------------------------------------------------------------------------
-- v_current_sites: Site/Facility master data
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS rawjd.v_current_sites;
CREATE VIEW rawjd.v_current_sites AS
SELECT 
    object_id as _id,
    document->>'siteName' as site_name,
    document->>'sectorType' as sector_type,
    document->>'facilityCity' as facility_city,
    document->>'country' as country,
    document->>'year' as year,
    document->>'yearType' as year_type,
    document->>'adminId' as admin_id,
    document->>'companyId' as company_id,
    document->>'createdAt' as created_at,
    document->>'updatedAt' as updated_at,
    cluster_time,
    operation_type,
    _sdc_extracted_at
FROM rawjd.sites
WHERE _sdc_deleted_at IS NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY object_id 
    ORDER BY cluster_time DESC NULLS LAST, _sdc_extracted_at DESC
) = 1;

-- =============================================================================
-- SCOPE 1 EMISSIONS - DIRECT EMISSIONS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- v_current_stationarycombustions: Stationary combustion (Scope 1)
-- Includes: boilers, furnaces, turbines, heaters, etc.
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS rawjd.v_current_stationarycombustions;
CREATE VIEW rawjd.v_current_stationarycombustions AS
SELECT 
    object_id as _id,
    -- Time dimensions
    document->>'year' as year,
    document->>'month' as month,
    document->>'frequency' as frequency,
    -- Hierarchy
    document->>'companyId' as company_id,
    document->>'siteId' as site_id,
    document->>'userId' as user_id,
    document->>'managerId' as manager_id,
    -- Emission details
    document->>'equipment' as equipment,
    document->>'fuelState' as fuel_state,
    document->>'fuelType' as fuel_type,
    document->>'unit' as unit,
    TRY_CAST(document->>'activity' AS DOUBLE) as activity,
    TRY_CAST(document->>'spend' AS DOUBLE) as spend,
    document->>'currency' as currency,
    TRY_CAST(document->>'emissionFactor' AS DOUBLE) as emission_factor,
    document->>'emissionFactorUnit' as emission_factor_unit,
    TRY_CAST(document->>'calculatedEmission' AS DOUBLE) as calculated_emission,
    document->>'source' as source,
    -- Workflow
    document->>'name' as submitter_name,
    document->>'approved' as approved,
    document->>'approvedBy' as approved_by,
    document->>'rejectedBy' as rejected_by,
    document->>'reason' as rejection_reason,
    document->>'notes' as notes,
    document->>'fileUrl' as file_url,
    -- Timestamps
    document->>'createdAt' as created_at,
    document->>'updatedAt' as updated_at,
    -- CDC metadata
    cluster_time,
    operation_type,
    _sdc_extracted_at
FROM rawjd.stationarycombustions
WHERE _sdc_deleted_at IS NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY object_id 
    ORDER BY cluster_time DESC NULLS LAST, _sdc_extracted_at DESC
) = 1;

-- -----------------------------------------------------------------------------
-- v_current_mobilecombustions: Mobile combustion (Scope 1)
-- Includes: company vehicles, forklifts, equipment
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS rawjd.v_current_mobilecombustions;
CREATE VIEW rawjd.v_current_mobilecombustions AS
SELECT 
    object_id as _id,
    document->>'year' as year,
    document->>'month' as month,
    document->>'frequency' as frequency,
    document->>'companyId' as company_id,
    document->>'siteId' as site_id,
    document->>'userId' as user_id,
    document->>'managerId' as manager_id,
    document->>'method' as method,
    document->>'vehicleType' as vehicle_type,
    document->>'vehicleSpec' as vehicle_spec,
    document->>'fuelType' as fuel_type,
    TRY_CAST(document->>'fuelAmount' AS DOUBLE) as fuel_amount,
    document->>'unit' as unit,
    TRY_CAST(document->>'activity' AS DOUBLE) as activity,
    TRY_CAST(document->>'spend' AS DOUBLE) as spend,
    document->>'currency' as currency,
    TRY_CAST(document->>'emissionFactor' AS DOUBLE) as emission_factor,
    TRY_CAST(document->>'calculatedEmission' AS DOUBLE) as calculated_emission,
    document->>'source' as source,
    document->>'name' as submitter_name,
    document->>'approved' as approved,
    document->>'approvedBy' as approved_by,
    document->>'notes' as notes,
    document->>'createdAt' as created_at,
    document->>'updatedAt' as updated_at,
    cluster_time,
    operation_type,
    _sdc_extracted_at
FROM rawjd.mobilecombustions
WHERE _sdc_deleted_at IS NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY object_id 
    ORDER BY cluster_time DESC NULLS LAST, _sdc_extracted_at DESC
) = 1;

-- -----------------------------------------------------------------------------
-- v_current_fugitiveemissions: Fugitive emissions (Scope 1)
-- Includes: refrigerants, AC systems, fire suppression
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS rawjd.v_current_fugitiveemissions;
CREATE VIEW rawjd.v_current_fugitiveemissions AS
SELECT 
    object_id as _id,
    document->>'year' as year,
    document->>'month' as month,
    document->>'frequency' as frequency,
    document->>'companyId' as company_id,
    document->>'siteId' as site_id,
    document->>'userId' as user_id,
    document->>'managerId' as manager_id,
    document->>'equipment' as equipment,
    document->>'refrigerant' as refrigerant,
    TRY_CAST(document->>'unitsNo' AS INTEGER) as units_no,
    TRY_CAST(document->>'refill' AS DOUBLE) as refill,
    document->>'unit' as unit,
    TRY_CAST(document->>'spend' AS DOUBLE) as spend,
    document->>'currency' as currency,
    TRY_CAST(document->>'emissionFactor' AS DOUBLE) as emission_factor,
    TRY_CAST(document->>'calculatedEmission' AS DOUBLE) as calculated_emission,
    document->>'source' as source,
    document->>'name' as submitter_name,
    document->>'approved' as approved,
    document->>'approvedBy' as approved_by,
    document->>'notes' as notes,
    document->>'createdAt' as created_at,
    document->>'updatedAt' as updated_at,
    cluster_time,
    operation_type,
    _sdc_extracted_at
FROM rawjd.fugitiveemissions
WHERE _sdc_deleted_at IS NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY object_id 
    ORDER BY cluster_time DESC NULLS LAST, _sdc_extracted_at DESC
) = 1;

-- =============================================================================
-- SCOPE 2 EMISSIONS - INDIRECT EMISSIONS FROM ENERGY
-- =============================================================================

-- -----------------------------------------------------------------------------
-- v_current_purchasedelectricities: Purchased electricity (Scope 2)
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS rawjd.v_current_purchasedelectricities;
CREATE VIEW rawjd.v_current_purchasedelectricities AS
SELECT 
    object_id as _id,
    document->>'year' as year,
    document->>'month' as month,
    document->>'frequency' as frequency,
    document->>'companyId' as company_id,
    document->>'siteId' as site_id,
    document->>'userId' as user_id,
    document->>'managerId' as manager_id,
    document->>'unit' as unit,
    TRY_CAST(document->>'activity' AS DOUBLE) as activity,
    TRY_CAST(document->>'spend' AS DOUBLE) as spend,
    document->>'currency' as currency,
    TRY_CAST(document->>'emissionFactor' AS DOUBLE) as emission_factor,
    TRY_CAST(document->>'calculatedEmission' AS DOUBLE) as calculated_emission,
    document->>'source' as source,
    document->>'name' as submitter_name,
    document->>'approved' as approved,
    document->>'approvedBy' as approved_by,
    document->>'notes' as notes,
    document->>'createdAt' as created_at,
    document->>'updatedAt' as updated_at,
    cluster_time,
    operation_type,
    _sdc_extracted_at
FROM rawjd.purchasedelectricities
WHERE _sdc_deleted_at IS NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY object_id 
    ORDER BY cluster_time DESC NULLS LAST, _sdc_extracted_at DESC
) = 1;

-- -----------------------------------------------------------------------------
-- v_current_renewableelectricities: Renewable electricity (Scope 2)
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS rawjd.v_current_renewableelectricities;
CREATE VIEW rawjd.v_current_renewableelectricities AS
SELECT 
    object_id as _id,
    document->>'year' as year,
    document->>'month' as month,
    document->>'frequency' as frequency,
    document->>'companyId' as company_id,
    document->>'siteId' as site_id,
    document->>'userId' as user_id,
    document->>'managerId' as manager_id,
    document->>'unit' as unit,
    TRY_CAST(document->>'activity' AS DOUBLE) as activity,
    TRY_CAST(document->>'calculatedEmission' AS DOUBLE) as calculated_emission,
    document->>'source' as source,
    document->>'name' as submitter_name,
    document->>'approved' as approved,
    document->>'approvedBy' as approved_by,
    document->>'notes' as notes,
    document->>'createdAt' as created_at,
    document->>'updatedAt' as updated_at,
    cluster_time,
    operation_type,
    _sdc_extracted_at
FROM rawjd.renewableelectricities
WHERE _sdc_deleted_at IS NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY object_id 
    ORDER BY cluster_time DESC NULLS LAST, _sdc_extracted_at DESC
) = 1;

-- =============================================================================
-- SCOPE 3 EMISSIONS - VALUE CHAIN EMISSIONS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- v_current_wastegenerations: Waste generation (Scope 3 Category 5)
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS rawjd.v_current_wastegenerations;
CREATE VIEW rawjd.v_current_wastegenerations AS
SELECT 
    object_id as _id,
    document->>'year' as year,
    document->>'month' as month,
    document->>'frequency' as frequency,
    document->>'companyId' as company_id,
    document->>'siteId' as site_id,
    document->>'userId' as user_id,
    document->>'managerId' as manager_id,
    document->>'wasteType' as waste_type,
    document->>'wasteCategory' as waste_category,
    document->>'disposalMethod' as disposal_method,
    document->>'treatmentType' as treatment_type,
    document->>'unit' as unit,
    TRY_CAST(document->>'activity' AS DOUBLE) as activity,
    TRY_CAST(document->>'emissionFactor' AS DOUBLE) as emission_factor,
    TRY_CAST(document->>'calculatedEmission' AS DOUBLE) as calculated_emission,
    document->>'source' as source,
    document->>'name' as submitter_name,
    document->>'approved' as approved,
    document->>'approvedBy' as approved_by,
    document->>'notes' as notes,
    document->>'createdAt' as created_at,
    document->>'updatedAt' as updated_at,
    cluster_time,
    operation_type,
    _sdc_extracted_at
FROM rawjd.wastegenerations
WHERE _sdc_deleted_at IS NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY object_id 
    ORDER BY cluster_time DESC NULLS LAST, _sdc_extracted_at DESC
) = 1;

-- -----------------------------------------------------------------------------
-- v_current_flighttravels: Business travel - flights (Scope 3 Category 6)
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS rawjd.v_current_flighttravels;
CREATE VIEW rawjd.v_current_flighttravels AS
SELECT 
    object_id as _id,
    document->>'year' as year,
    document->>'month' as month,
    document->>'frequency' as frequency,
    document->>'companyId' as company_id,
    document->>'siteId' as site_id,
    document->>'userId' as user_id,
    document->>'managerId' as manager_id,
    document->>'flightType' as flight_type,
    document->>'cabinClass' as cabin_class,
    document->>'unit' as unit,
    TRY_CAST(document->>'activity' AS DOUBLE) as activity,
    TRY_CAST(document->>'emissionFactor' AS DOUBLE) as emission_factor,
    TRY_CAST(document->>'calculatedEmission' AS DOUBLE) as calculated_emission,
    document->>'source' as source,
    document->>'name' as submitter_name,
    document->>'approved' as approved,
    document->>'approvedBy' as approved_by,
    document->>'notes' as notes,
    document->>'createdAt' as created_at,
    document->>'updatedAt' as updated_at,
    cluster_time,
    operation_type,
    _sdc_extracted_at
FROM rawjd.flighttravels
WHERE _sdc_deleted_at IS NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY object_id 
    ORDER BY cluster_time DESC NULLS LAST, _sdc_extracted_at DESC
) = 1;

-- -----------------------------------------------------------------------------
-- v_current_groundtravels: Business travel - ground (Scope 3 Category 6)
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS rawjd.v_current_groundtravels;
CREATE VIEW rawjd.v_current_groundtravels AS
SELECT 
    object_id as _id,
    document->>'year' as year,
    document->>'month' as month,
    document->>'frequency' as frequency,
    document->>'companyId' as company_id,
    document->>'siteId' as site_id,
    document->>'userId' as user_id,
    document->>'managerId' as manager_id,
    document->>'vehicleType' as vehicle_type,
    document->>'fuelType' as fuel_type,
    document->>'unit' as unit,
    TRY_CAST(document->>'activity' AS DOUBLE) as activity,
    TRY_CAST(document->>'emissionFactor' AS DOUBLE) as emission_factor,
    TRY_CAST(document->>'calculatedEmission' AS DOUBLE) as calculated_emission,
    document->>'source' as source,
    document->>'name' as submitter_name,
    document->>'approved' as approved,
    document->>'approvedBy' as approved_by,
    document->>'notes' as notes,
    document->>'createdAt' as created_at,
    document->>'updatedAt' as updated_at,
    cluster_time,
    operation_type,
    _sdc_extracted_at
FROM rawjd.groundtravels
WHERE _sdc_deleted_at IS NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY object_id 
    ORDER BY cluster_time DESC NULLS LAST, _sdc_extracted_at DESC
) = 1;

-- -----------------------------------------------------------------------------
-- v_current_seatravels: Business travel - sea (Scope 3 Category 6)
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS rawjd.v_current_seatravels;
CREATE VIEW rawjd.v_current_seatravels AS
SELECT 
    object_id as _id,
    document->>'year' as year,
    document->>'month' as month,
    document->>'frequency' as frequency,
    document->>'companyId' as company_id,
    document->>'siteId' as site_id,
    document->>'userId' as user_id,
    document->>'managerId' as manager_id,
    document->>'vesselType' as vessel_type,
    document->>'unit' as unit,
    TRY_CAST(document->>'activity' AS DOUBLE) as activity,
    TRY_CAST(document->>'emissionFactor' AS DOUBLE) as emission_factor,
    TRY_CAST(document->>'calculatedEmission' AS DOUBLE) as calculated_emission,
    document->>'source' as source,
    document->>'name' as submitter_name,
    document->>'approved' as approved,
    document->>'approvedBy' as approved_by,
    document->>'notes' as notes,
    document->>'createdAt' as created_at,
    document->>'updatedAt' as updated_at,
    cluster_time,
    operation_type,
    _sdc_extracted_at
FROM rawjd.seatravels
WHERE _sdc_deleted_at IS NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY object_id 
    ORDER BY cluster_time DESC NULLS LAST, _sdc_extracted_at DESC
) = 1;

-- -----------------------------------------------------------------------------
-- v_current_rawmaterials: Raw materials (Scope 3 Category 1)
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS rawjd.v_current_rawmaterials;
CREATE VIEW rawjd.v_current_rawmaterials AS
SELECT 
    object_id as _id,
    document->>'year' as year,
    document->>'month' as month,
    document->>'frequency' as frequency,
    document->>'companyId' as company_id,
    document->>'siteId' as site_id,
    document->>'userId' as user_id,
    document->>'managerId' as manager_id,
    document->>'materialType' as material_type,
    document->>'materialCategory' as material_category,
    document->>'unit' as unit,
    TRY_CAST(document->>'activity' AS DOUBLE) as activity,
    TRY_CAST(document->>'emissionFactor' AS DOUBLE) as emission_factor,
    TRY_CAST(document->>'calculatedEmission' AS DOUBLE) as calculated_emission,
    document->>'source' as source,
    document->>'name' as submitter_name,
    document->>'approved' as approved,
    document->>'approvedBy' as approved_by,
    document->>'notes' as notes,
    document->>'createdAt' as created_at,
    document->>'updatedAt' as updated_at,
    cluster_time,
    operation_type,
    _sdc_extracted_at
FROM rawjd.rawmaterials
WHERE _sdc_deleted_at IS NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY object_id 
    ORDER BY cluster_time DESC NULLS LAST, _sdc_extracted_at DESC
) = 1;

-- -----------------------------------------------------------------------------
-- v_current_capitalgoods: Capital goods (Scope 3 Category 2)
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS rawjd.v_current_capitalgoods;
CREATE VIEW rawjd.v_current_capitalgoods AS
SELECT 
    object_id as _id,
    document->>'year' as year,
    document->>'month' as month,
    document->>'frequency' as frequency,
    document->>'companyId' as company_id,
    document->>'siteId' as site_id,
    document->>'userId' as user_id,
    document->>'managerId' as manager_id,
    document->>'assetType' as asset_type,
    document->>'assetCategory' as asset_category,
    document->>'unit' as unit,
    TRY_CAST(document->>'activity' AS DOUBLE) as activity,
    TRY_CAST(document->>'spend' AS DOUBLE) as spend,
    document->>'currency' as currency,
    TRY_CAST(document->>'emissionFactor' AS DOUBLE) as emission_factor,
    TRY_CAST(document->>'calculatedEmission' AS DOUBLE) as calculated_emission,
    document->>'source' as source,
    document->>'name' as submitter_name,
    document->>'approved' as approved,
    document->>'approvedBy' as approved_by,
    document->>'notes' as notes,
    document->>'createdAt' as created_at,
    document->>'updatedAt' as updated_at,
    cluster_time,
    operation_type,
    _sdc_extracted_at
FROM rawjd.capitalgoods
WHERE _sdc_deleted_at IS NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY object_id 
    ORDER BY cluster_time DESC NULLS LAST, _sdc_extracted_at DESC
) = 1;

-- -----------------------------------------------------------------------------
-- v_current_upstreamtransportations: Upstream transport (Scope 3 Category 4)
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS rawjd.v_current_upstreamtransportations;
CREATE VIEW rawjd.v_current_upstreamtransportations AS
SELECT 
    object_id as _id,
    document->>'year' as year,
    document->>'month' as month,
    document->>'frequency' as frequency,
    document->>'companyId' as company_id,
    document->>'siteId' as site_id,
    document->>'userId' as user_id,
    document->>'managerId' as manager_id,
    document->>'transportMode' as transport_mode,
    document->>'vehicleType' as vehicle_type,
    document->>'unit' as unit,
    TRY_CAST(document->>'activity' AS DOUBLE) as activity,
    TRY_CAST(document->>'distance' AS DOUBLE) as distance,
    TRY_CAST(document->>'weight' AS DOUBLE) as weight,
    TRY_CAST(document->>'emissionFactor' AS DOUBLE) as emission_factor,
    TRY_CAST(document->>'calculatedEmission' AS DOUBLE) as calculated_emission,
    document->>'source' as source,
    document->>'name' as submitter_name,
    document->>'approved' as approved,
    document->>'approvedBy' as approved_by,
    document->>'notes' as notes,
    document->>'createdAt' as created_at,
    document->>'updatedAt' as updated_at,
    cluster_time,
    operation_type,
    _sdc_extracted_at
FROM rawjd.upstreamtransportations
WHERE _sdc_deleted_at IS NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY object_id 
    ORDER BY cluster_time DESC NULLS LAST, _sdc_extracted_at DESC
) = 1;

-- -----------------------------------------------------------------------------
-- v_current_employeecommutings: Employee commuting (Scope 3 Category 7)
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS rawjd.v_current_employeecommutings;
CREATE VIEW rawjd.v_current_employeecommutings AS
SELECT 
    object_id as _id,
    document->>'year' as year,
    document->>'month' as month,
    document->>'frequency' as frequency,
    document->>'companyId' as company_id,
    document->>'siteId' as site_id,
    document->>'userId' as user_id,
    document->>'managerId' as manager_id,
    document->>'transportMode' as transport_mode,
    document->>'vehicleType' as vehicle_type,
    document->>'fuelType' as fuel_type,
    document->>'unit' as unit,
    TRY_CAST(document->>'activity' AS DOUBLE) as activity,
    TRY_CAST(document->>'distance' AS DOUBLE) as distance,
    TRY_CAST(document->>'employees' AS INTEGER) as employees,
    TRY_CAST(document->>'emissionFactor' AS DOUBLE) as emission_factor,
    TRY_CAST(document->>'calculatedEmission' AS DOUBLE) as calculated_emission,
    document->>'source' as source,
    document->>'name' as submitter_name,
    document->>'approved' as approved,
    document->>'approvedBy' as approved_by,
    document->>'notes' as notes,
    document->>'createdAt' as created_at,
    document->>'updatedAt' as updated_at,
    cluster_time,
    operation_type,
    _sdc_extracted_at
FROM rawjd.employeecommutings
WHERE _sdc_deleted_at IS NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY object_id 
    ORDER BY cluster_time DESC NULLS LAST, _sdc_extracted_at DESC
) = 1;

-- -----------------------------------------------------------------------------
-- v_current_accomodations: Business travel accomodation (Scope 3 Category 6)
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS rawjd.v_current_accomodations;
CREATE VIEW rawjd.v_current_accomodations AS
SELECT 
    object_id as _id,
    document->>'year' as year,
    document->>'month' as month,
    document->>'frequency' as frequency,
    document->>'companyId' as company_id,
    document->>'siteId' as site_id,
    document->>'userId' as user_id,
    document->>'managerId' as manager_id,
    document->>'country' as country,
    document->>'unit' as unit,
    TRY_CAST(document->>'nights' AS INTEGER) as nights,
    TRY_CAST(document->>'activity' AS DOUBLE) as activity,
    TRY_CAST(document->>'emissionFactor' AS DOUBLE) as emission_factor,
    TRY_CAST(document->>'calculatedEmission' AS DOUBLE) as calculated_emission,
    document->>'source' as source,
    document->>'name' as submitter_name,
    document->>'approved' as approved,
    document->>'approvedBy' as approved_by,
    document->>'notes' as notes,
    document->>'createdAt' as created_at,
    document->>'updatedAt' as updated_at,
    cluster_time,
    operation_type,
    _sdc_extracted_at
FROM rawjd.accomodations
WHERE _sdc_deleted_at IS NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY object_id 
    ORDER BY cluster_time DESC NULLS LAST, _sdc_extracted_at DESC
) = 1;

-- -----------------------------------------------------------------------------
-- v_current_endoflifetreatments: End of life treatment (Scope 3 Category 12)
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS rawjd.v_current_endoflifetreatments;
CREATE VIEW rawjd.v_current_endoflifetreatments AS
SELECT 
    object_id as _id,
    document->>'year' as year,
    document->>'month' as month,
    document->>'frequency' as frequency,
    document->>'companyId' as company_id,
    document->>'siteId' as site_id,
    document->>'userId' as user_id,
    document->>'managerId' as manager_id,
    document->>'wasteType' as waste_type,
    document->>'treatmentType' as treatment_type,
    document->>'unit' as unit,
    TRY_CAST(document->>'activity' AS DOUBLE) as activity,
    TRY_CAST(document->>'emissionFactor' AS DOUBLE) as emission_factor,
    TRY_CAST(document->>'calculatedEmission' AS DOUBLE) as calculated_emission,
    document->>'source' as source,
    document->>'name' as submitter_name,
    document->>'approved' as approved,
    document->>'approvedBy' as approved_by,
    document->>'notes' as notes,
    document->>'createdAt' as created_at,
    document->>'updatedAt' as updated_at,
    cluster_time,
    operation_type,
    _sdc_extracted_at
FROM rawjd.endoflifetreatments
WHERE _sdc_deleted_at IS NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY object_id 
    ORDER BY cluster_time DESC NULLS LAST, _sdc_extracted_at DESC
) = 1;


-- #############################################################################
-- PART 2: MAPPED SCHEMA (rawmp)
-- Clean versions of pre-flattened data with CDC deduplication
-- Note: rawmp uses _sdc_extracted_at for dedup (no cluster_time available)
-- Note: rawmp column names may differ from rawjd JSON field names
-- #############################################################################

-- =============================================================================
-- DIMENSION TABLES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- v_current_users: User master data (mapped)
-- Note: users table has site_id not company_id in rawmp
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS rawmp.v_current_users;
CREATE VIEW rawmp.v_current_users AS
SELECT 
    _id,
    name,
    email,
    role,
    status,
    is_admin,
    admin_id,
    site_id,  -- Note: different from rawjd which has company_id
    created_at,
    updated_at,
    _sdc_extracted_at
FROM rawmp.carbonlens_users
WHERE _sdc_deleted_at IS NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY _id 
    ORDER BY _sdc_extracted_at DESC
) = 1;

-- -----------------------------------------------------------------------------
-- v_current_companies: Company master data (mapped)
-- Note: rawmp has baseline_year, city, country instead of company_email, address
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS rawmp.v_current_companies;
CREATE VIEW rawmp.v_current_companies AS
SELECT 
    _id,
    company_name,
    company_logo,
    admin_id,
    country,
    city,
    baseline_year,
    sector_type,
    currency,
    targets,
    created_at,
    updated_at,
    _sdc_extracted_at
FROM rawmp.carbonlens_companies
WHERE _sdc_deleted_at IS NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY _id 
    ORDER BY _sdc_extracted_at DESC
) = 1;

-- -----------------------------------------------------------------------------
-- v_current_sites: Site master data (mapped)
-- Note: rawmp sites do not have company_id column
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS rawmp.v_current_sites;
CREATE VIEW rawmp.v_current_sites AS
SELECT 
    _id,
    site_name,
    sector_type,
    facility_city,
    country,
    year,
    year_type,
    admin_id,
    _sdc_extracted_at
FROM rawmp.carbonlens_sites
WHERE _sdc_deleted_at IS NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY _id 
    ORDER BY _sdc_extracted_at DESC
) = 1;

-- =============================================================================
-- SCOPE 1 EMISSIONS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- v_current_stationarycombustions: Stationary combustion (mapped)
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS rawmp.v_current_stationarycombustions;
CREATE VIEW rawmp.v_current_stationarycombustions AS
SELECT 
    _id,
    year,
    month,
    frequency,
    company_id,
    site_id,
    user_id,
    manager_id,
    equipment,
    fuel_state,
    fuel_type,
    unit,
    activity,
    spend,
    currency,
    emission_factor,
    emission_factor_unit,
    calculated_emission,
    source,
    name as submitter_name,
    approved,
    approved_by,
    notes,
    created_at,
    updated_at,
    _sdc_extracted_at
FROM rawmp.carbonlens_stationarycombustions
WHERE _sdc_deleted_at IS NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY _id 
    ORDER BY _sdc_extracted_at DESC
) = 1;

-- =============================================================================
-- SCOPE 2 EMISSIONS  
-- =============================================================================

-- -----------------------------------------------------------------------------
-- v_current_purchasedelectricities: Purchased electricity (mapped)
-- Note: rawmp uses consumption not activity, spend_amount not spend
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS rawmp.v_current_purchasedelectricities;
CREATE VIEW rawmp.v_current_purchasedelectricities AS
SELECT 
    _id,
    year,
    month,
    frequency,
    company_id,
    site_id,
    user_id,
    manager_id,
    unit,
    consumption as activity,  -- Mapped to match rawjd naming
    spend_amount as spend,    -- Mapped to match rawjd naming
    currency,
    emission_factor,
    emission_factor_unit,
    emission_intensity,
    output_value,
    output_unit,
    calculated_emission,
    source,
    name as submitter_name,
    approved,
    approved_by,
    notes,
    created_at,
    updated_at,
    _sdc_extracted_at
FROM rawmp.carbonlens_purchasedelectricities
WHERE _sdc_deleted_at IS NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY _id 
    ORDER BY _sdc_extracted_at DESC
) = 1;

-- -----------------------------------------------------------------------------
-- v_current_renewableelectricities: Renewable electricity (mapped)
-- Note: rawmp uses consumption not activity, spend_amount not spend
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS rawmp.v_current_renewableelectricities;
CREATE VIEW rawmp.v_current_renewableelectricities AS
SELECT 
    _id,
    year,
    month,
    frequency,
    company_id,
    site_id,
    user_id,
    manager_id,
    unit,
    consumption as activity,  -- Mapped to match rawjd naming
    spend_amount as spend,    -- Mapped to match rawjd naming
    currency,
    emission_factor,
    emission_factor_unit,
    emission_intensity,
    output_value,
    output_unit,
    calculated_emission,
    source,
    name as submitter_name,
    approved,
    approved_by,
    notes,
    created_at,
    updated_at,
    _sdc_extracted_at
FROM rawmp.carbonlens_renewableelectricities
WHERE _sdc_deleted_at IS NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY _id 
    ORDER BY _sdc_extracted_at DESC
) = 1;


-- #############################################################################
-- PART 3: CONVENIENCE / AGGREGATE VIEWS
-- Pre-built views for common BI queries
-- #############################################################################

-- -----------------------------------------------------------------------------
-- v_emissions_summary_by_scope: High-level emissions by scope
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS rawjd.v_emissions_summary_by_scope;
CREATE VIEW rawjd.v_emissions_summary_by_scope AS
SELECT 
    year,
    company_id,
    site_id,
    'Scope 1 - Stationary' as emission_category,
    'Scope 1' as scope,
    SUM(calculated_emission) as total_emission_tco2e,
    COUNT(*) as record_count
FROM rawjd.v_current_stationarycombustions
WHERE approved = 'true'
GROUP BY year, company_id, site_id

UNION ALL

SELECT 
    year,
    company_id,
    site_id,
    'Scope 1 - Mobile' as emission_category,
    'Scope 1' as scope,
    SUM(calculated_emission) as total_emission_tco2e,
    COUNT(*) as record_count
FROM rawjd.v_current_mobilecombustions
WHERE approved = 'true'
GROUP BY year, company_id, site_id

UNION ALL

SELECT 
    year,
    company_id,
    site_id,
    'Scope 1 - Fugitive' as emission_category,
    'Scope 1' as scope,
    SUM(calculated_emission) as total_emission_tco2e,
    COUNT(*) as record_count
FROM rawjd.v_current_fugitiveemissions
WHERE approved = 'true'
GROUP BY year, company_id, site_id

UNION ALL

SELECT 
    year,
    company_id,
    site_id,
    'Scope 2 - Purchased Electricity' as emission_category,
    'Scope 2' as scope,
    SUM(calculated_emission) as total_emission_tco2e,
    COUNT(*) as record_count
FROM rawjd.v_current_purchasedelectricities
WHERE approved = 'true'
GROUP BY year, company_id, site_id

UNION ALL

SELECT 
    year,
    company_id,
    site_id,
    'Scope 2 - Renewable Electricity' as emission_category,
    'Scope 2' as scope,
    SUM(calculated_emission) as total_emission_tco2e,
    COUNT(*) as record_count
FROM rawjd.v_current_renewableelectricities
WHERE approved = 'true'
GROUP BY year, company_id, site_id

UNION ALL

SELECT 
    year,
    company_id,
    site_id,
    'Scope 3 - Waste' as emission_category,
    'Scope 3' as scope,
    SUM(calculated_emission) as total_emission_tco2e,
    COUNT(*) as record_count
FROM rawjd.v_current_wastegenerations
WHERE approved = 'true'
GROUP BY year, company_id, site_id

UNION ALL

SELECT 
    year,
    company_id,
    site_id,
    'Scope 3 - Business Travel (Flight)' as emission_category,
    'Scope 3' as scope,
    SUM(calculated_emission) as total_emission_tco2e,
    COUNT(*) as record_count
FROM rawjd.v_current_flighttravels
WHERE approved = 'true'
GROUP BY year, company_id, site_id

UNION ALL

SELECT 
    year,
    company_id,
    site_id,
    'Scope 3 - Business Travel (Ground)' as emission_category,
    'Scope 3' as scope,
    SUM(calculated_emission) as total_emission_tco2e,
    COUNT(*) as record_count
FROM rawjd.v_current_groundtravels
WHERE approved = 'true'
GROUP BY year, company_id, site_id;


-- =============================================================================
-- VERIFICATION: List all created views
-- =============================================================================
SELECT 
    table_schema as schema_name,
    table_name as view_name,
    'VIEW' as type
FROM information_schema.tables 
WHERE table_type = 'VIEW'
AND table_schema IN ('rawjd', 'rawmp')
ORDER BY table_schema, table_name;
