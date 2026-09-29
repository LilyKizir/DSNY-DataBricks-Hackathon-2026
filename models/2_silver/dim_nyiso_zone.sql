USE SCHEMA silver;

CREATE OR REFRESH MATERIALIZED VIEW dim_nyiso_zone (
    nyiso_zone STRING NOT NULL COMMENT 'Single-letter code representing NYISO load zone (A through K).',
    nyiso_zone_name STRING COMMENT 'Full descriptive name of the NYISO load zone.',
    
    -- 1. Data Quality Expectations
    CONSTRAINT valid_nyiso_zone EXPECT (nyiso_zone IS NOT NULL) ON VIOLATION DROP ROW,

    -- 2. Primary Key Constraint for Unity Catalog / Databricks Genie
    CONSTRAINT pk_dim_nyiso_zone PRIMARY KEY (nyiso_zone)
)
COMMENT "NYISO weather load zone dimension"
AS
WITH 
stg_source AS (
    select *
    FROM the_data_masons.bronze.bronze_weather_data
)
SELECT DISTINCT
    nyiso_zone,
    nyiso_zone_name
FROM stg_source;