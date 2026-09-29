USE SCHEMA silver;

CREATE OR REFRESH MATERIALIZED VIEW dim_county (
    state_zone STRING COMMENT 'NWS forecast zone code (e.g., NYZ072). Do NOT join directly to NYISO electricity zones.',
    zone_name STRING COMMENT 'Name of the NWS weather forecast zone.',
    county STRING COMMENT 'County name associated with the forecast zone.',
    fips STRING NOT NULL COMMENT 'Unique 5-digit Federal Information Processing Standard county code.',
    
    -- 1. Data Quality Expectations
    CONSTRAINT valid_fips EXPECT (fips IS NOT NULL) ON VIOLATION DROP ROW,

    -- 2. Primary Key Constraint for Unity Catalog / Databricks Genie
    CONSTRAINT pk_dim_county PRIMARY KEY (fips)
)
COMMENT "County dimension containing NWS forecast zones, counties, and FIPS codes"
AS
WITH stg_source AS (
    SELECT * FROM the_data_masons.bronze.bronze_weather_data
)
SELECT DISTINCT
    state_zone,
    zone_name,
    county,
    fips
FROM stg_source;