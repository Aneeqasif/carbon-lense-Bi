# Carbon Lens BI - Data Migration Summary

## Overview

This document summarizes all MongoDB collections and fields used by the Carbon Lens BI dashboards.

---

## Quick Reference - Collections for Analytics

### Core Emission Collections (22 Total)
these are the emittor sources that we have grouped in form of scopes.


| Scope       | Collection Name (MongoDB) | Primary Use                           |
|-------------|---------------------------|---------------------------------------|
| **Scope 1** | `stationarycombustions`   | Direct stationary fuel combustion     |
| **Scope 1** | `mobilecombustions`       | Direct mobile fuel combustion         |
| **Scope 1** | `fugitiveemissions`       | Refrigerant leakage                   |
| **Scope 2** | `purchasedelectricities`  | Grid electricity consumption          |
| **Scope 3** | `ferastationaries`        | Franchise/tenant stationary emissions |
| **Scope 3** | `feramobiles`             | Franchise/tenant mobile emissions     |
| **Scope 3** | `feraelectricities`       | Franchise/tenant electricity          |
| **Scope 3** | `flighttravels`           | Business air travel                   |
| **Scope 3** | `groundtravels`           | Business ground travel                |
| **Scope 3** | `seatravels`              | Business sea travel                   |
| **Scope 3** | `accomodations`           | Business hotel stays                  |
| **Scope 3** | `wastegenerations`        | Waste disposal emissions              |
| **Scope 3** | `endoflifetreatments`     | End-of-life product treatment         |
| **Scope 3** | `upstreamtransportations` | Upstream logistics (UTD)              |
| **Scope 3** | `dtds`                    | Downstream logistics (DTD)            |
| **Scope 3** | `capitalgoods`            | Purchased capital equipment           |
| **Scope 3** | `rawmaterials`            | Raw material procurement              |
| **Scope 3** | `fireextinguishers`       | CO₂ fire extinguisher emissions       |
| **Scope Renewables** | `renewableelectricities`  | Renewable energy (tracked separately) |

`renewableelectricities` do not belongs to a scope it has its own scope we can call it scope renewables or whatever.

### Reference Collections (3)

| Collection Name | Purpose |
|-----------------|---------|
| `companies` | Company master data |
| `sites` | Site/facility master data |
| `users` | User account data |

> [!NOTE]  
> in sites Table the admin_id corresponds to _id in companies table,
>
> but there can be a mismatch not sure though.

---

## Essential Fields for Analytics

### Common Fields (All Emission Collections)

These fields exist in ALL emission collections and are essential for analytics:


| Field                | Type          | Required    | Description                               |
| -------              | ------        | ----------  | -------------                             |
| `_id`                | ObjectId      | ✓          | MongoDB document ID                       |
| `companyId`          | ObjectId      | ✓          | Foreign key to companies                  |
| `siteId`             | ObjectId      | ✓          | Foreign key to sites                      |
| `userId`             | ObjectId      | ✓          | Foreign key to users (who entered)        |
| `year`               | String/Number | ✓          | Reporting year                            |
| `month`              | String        | ✓          | Reporting month                           |
| `frequency`          | String        | ✓          | "monthly" or "yearly"                     |
| `calculatedEmission` | Number        | ✓          | **PRIMARY METRIC** - CO₂e value           |
| `emissionFactor`     | Number        | ✓          | Emission factor used                      |
| `emissionFactorUnit` | String        |             | Unit of emission factor                   |
| `approved`           | String        | ✓          | Status: "pending", "approved", "rejected" |
| `approvedBy`         | String        |             | Approver identifier                       |
| `name`               | String        | ✓          | Entry name/description                    |
| `notes`              | String        |             | Additional notes                          |
| `source`             | String        |             | Data source reference                     |
| `createdAt`          | Date          | ✓          | Record creation timestamp                 |
| `updatedAt`          | Date          | ✓          | Last update timestamp                     |

---

## Collection-Specific Fields

### Scope 1: Stationary Combustion
```
stationarycombustions
├── equipment: String         # Equipment name
├── fuelState: String         # Solid, Liquid, Gaseous
├── fuelType: String          # Natural Gas, Coal, Diesel, etc.
├── unit: String              # Measurement unit (liters, kg, m³)
├── activity: Number          # Consumption amount
├── spend: Number             # Cost (optional)
└── currency: String          # Currency code
```

### Scope 1: Mobile Combustion
```
mobilecombustions
├── vehicleType: String       # Car, Truck, Van, etc.
├── vehicleSpec: String       # Vehicle specification
├── fuelType: String          # Diesel, Petrol, CNG, etc.
├── method: String            # Calculation method
├── activity: Number          # Distance or fuel consumed
├── unit: String              # km, liters, etc.
├── spend: Number             # Cost (optional)
└── currency: String          # Currency code
```

### Scope 1: Fugitive Emissions
```
fugitiveemissions
├── equipment: String         # Equipment type
├── refrigerant: String       # R410A, R22, CO2, etc.
├── unitsNo: Number           # Number of units
├── refill: Number            # Refill amount
├── unit: String              # kg, lbs
├── spend: Number             # Cost (optional)
└── currency: String          # Currency code
```


### Scope 2: Purchased Electricity
```
purchasedelectricities
├── consumption: String       # kWh consumed
├── unit: String              # kWh, MWh
├── spendAmount: String       # Cost
└── currency: String          # Currency code
```

### Scope 2: Renewable Electricity
```
renewableelectricities
├── consumption: String       # kWh consumed
├── unit: String              # kWh, MWh
├── spendAmount: String       # Cost
└── currency: String          # Currency code
```

### Scope 3: Flight Travel
```
flighttravels
├── tripID: String            # Trip identifier
├── no_of_Passengers: Number  # Passenger count
├── class: String             # Economy, Business, First
├── haul: String              # Short, Medium, Long
├── flight_Haul: String       # Domestic, International
├── departure_City: String    # Origin city
├── destination_City: String  # Destination city
├── travel_Mode: String       # Flight type
├── distance: Number          # Distance in km/miles
├── unit: String              # km, miles
├── userSpend: String         # Cost
└── userSpendUnit: String     # Currency
```

### Scope 3: Fire Extinguisher
```
fireextinguishers
├── equipment: String         # Equipment type
├── fuelState: String         # State
├── fuelType: String          # CO2, etc.
├── activity: Number          # Amount used
├── unit: String              # kg
├── spend: Number             # Cost (optional)
└── currency: String          # Currency code
```

### Scope 3: Ground Travel
```
groundtravels
├── vehicleType: String       # Car, Bus, Train, etc.
├── vehicleSpecification: String
├── numberOfPassangers: Number
├── fuelConsumption: Number   # Fuel used
├── totalDistanceTravelled: Number
├── unitDistance: String      # km, miles
├── userSpend: String         # Cost
└── userSpendUnit: String     # Currency
```

### Scope 3: Sea Travel
```
seatravels
├── tripID: String
├── no_of_Passengers: Number
├── passangerType: String
├── vehicleType: String       # Ferry, Cruise, etc.
├── departure_City: String
├── destination_City: String
├── distance: Number
├── unit: String
├── userSpend: String
└── userSpendUnit: String
```

### Scope 3: Accommodation
```
accomodations
├── hotelType: String         # Hotel category
├── regionOfHotel: String     # Geographic region
├── hotelRating: Number       # Star rating
├── numberOfPassangers: Number
├── numberOfNightsStayed: Number
├── userSpend: String         # Cost
├── userSpendUnit: String     # Currency
└── units: String             # Emission unit
```


### Scope 3: Waste Generation
```
wastegenerations
├── wasteType: String         # Organic, Plastic, etc.
├── wasteCategory: String     # Category classification
├── quantity: Number          # Amount
├── unit: String              # kg, tonnes
└── disposalMethod: String    # Landfill, Recycling, etc.
```

### Scope 3: End of Life Treatment
```
endoflifetreatments
├── wasteType: String
├── wasteCategory: String
├── quantity: String
├── unit: String
└── disposalMethod: String
```

### Scope 3: Upstream Transportation (UTD)
```
upstreamtransportations
├── shipmentId: String
├── productType: String
├── quantity: Number
├── unit: String
├── transportMode: String     # Road, Rail, Air, Sea
├── vehicleType: String
├── vehicleSpecification: String
├── distance: Number
├── unitOfDistance: String
└── laden: String             # Fully laden, Average, etc.
```

### Scope 3: Downstream Transportation (DTD)
```
dtds
├── shipmentId: String
├── productType: String
├── quantity: String
├── unit: String
├── transportMode: String
├── vehicleType: String
├── vehicleSpecification: String
├── distance: String
├── unitOfDistance: String
└── fuelType: String
```

### Scope 3: Capital Goods
```
capitalgoods
├── category: String          # Equipment category
├── productName: String
├── userSpend: Number         # Cost
├── userSpendUnit: String     # Currency
├── quantity: Number
├── weight: Number
├── unit: String
└── productionType: String
```

### Scope 3: Raw Materials
```
rawmaterials
├── category: String          # Material category
├── productName: String
├── userSpend: Number
├── userSpendUnit: String
├── quantity: Number
├── weight: Number
└── unit: String
```


### Scope 3: FERA Stationary
```
ferastationaries
├── equipment: String
├── fuelState: String
├── fuelType: String
├── activity: Number
├── unit: String
├── spend: Number
└── currency: String
```

### Scope 3: FERA Mobile
```
feramobiles
├── vehicleType: String
├── vehicleSpec: String
├── fuelType: String
├── method: String
├── activity: Number
├── unit: String
├── spend: Number
└── currency: String
```

### Scope 3: FERA Electricity
```
feraelectricities
├── consumption: String
├── unit: String
├── spendAmount: Number
└── currency: String
```

---

## Reference Data: Sites
```
sites
├── _id: ObjectId
├── siteName: String          # Facility name
├── facilityCity: String      # City
├── country: String           # Country
├── sectorType: String        # Industry sector
├── yearType: String          # Calendar/Financial
├── adminId: ObjectId         # Reference to company
└── siteAccess: [ObjectId]    # Users with access
```

## Reference Data: Companies
```
companies
├── _id: ObjectId
├── companyName: String
├── companyEmail: String
├── companyLogo: String
├── address: String
├── phoneNumber: String
├── scopes: [String]          # Enabled scopes
├── categories: [String]      # Enabled categories
├── units: [String]           # Available units
└── emissionFactorsByCategory: [Object]  # Custom emission factors
```

---


## Suggested Table Structure


### Denormalized Schema (Better for BI Tools)
basically combine union/joining properly all the relevant tables and create a wide table that is better for analysis easy of use.

```sql 
-- Single wide table for all emissions (easier for BI tools) for example i wanna have these roughly, 
    id SERIAL PRIMARY KEY,
    
    -- Identifiers
    company_id INTEGER,
    company_name VARCHAR(255),
    site_id INTEGER,
    site_name VARCHAR(255),
    site_city VARCHAR(100),
    site_country VARCHAR(100), -- facility_city
    
    -- Time
    year INTEGER,
    month VARCHAR(20),
    quarter INTEGER,  -- Derived: Q1, Q2, Q3, Q4
    frequency VARCHAR(20),
    
    -- Classification
    scope INTEGER,  -- 1, 2, or 3
    emissoin_source VARCHAR(50), -- name of emiter like stationarycombustions,Fugitive Emissions etc
    units
    
    -- Primary Metrics
    calculated_emission DECIMAL(18,6),
    emission_factor DECIMAL(18,6),
    
    -- Common attributes (nullable for non-applicable)
    fuel_type VARCHAR(100),
    fuel_state VARCHAR(50),
    equipment VARCHAR(100),
    vehicle_type VARCHAR(100),
    transport_mode VARCHAR(50),
    waste_type VARCHAR(100),
    disposal_method VARCHAR(100),
    
    -- Activity data
    activity_value DECIMAL(18,6),
    activity_unit VARCHAR(50),
    distance DECIMAL(18,6),
    distance_unit VARCHAR(20),
    quantity DECIMAL(18,6),
    weight DECIMAL(18,6),
    
    -- Financial
    spend_amount DECIMAL(18,2),
    currency VARCHAR(10),
    
    -- Status
    approved VARCHAR(20),
    
    -- Timestamps
    created_at TIMESTAMP,
    updated_at TIMESTAMP

    ..... other stuff that may come due combining
);

```
---

## Data Migration Checklist


## Key Metrics for BI Dashboards

we will create an extra script file for this.

| Metric | SQL Calculation |
|--------|-----------------|
| Total Emissions | `SUM(calculated_emission)` |
| Scope 1 Total   | `SUM(calculated_emission) WHERE scope = 'scope1'` |
| Scope 2 Total   | `SUM(calculated_emission) WHERE scope = 'scope2'` |
| Scope 3 Total   | `SUM(calculated_emission) WHERE scope = 'scope3'` |
| YoY Change %    | `((current_year - prev_year) / prev_year) * 100` |
| Emitter %       | `emitter_total / scope_total * 100` |
| Monthly Trend   | `GROUP BY year, month ORDER BY year, month` |
| Top 5 Sources   | `ORDER BY SUM(calculated_emission) DESC LIMIT 5` |

---

## Important Notes if applicable.

1. **Approved Data Only**: All dashboard queries filter `approved = true` - ensure this is replicated in analytics queries

2. **Renewable Electricity**: Tracked separately 

3. **Year Format**: Some tables may store year as String, others as Number - normalize to INTEGER

4. **Month Names**: Stored as full names ("January", "February", etc.) - consider adding month_number for sorting

5. **Emission Factor**: Each record stores its own emission factor - useful for audit trail but can be normalized

6. **Financial Year Support**: System supports both calendar and financial years - consider this in time dimension design
