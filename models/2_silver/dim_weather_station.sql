USE SCHEMA silver;

CREATE OR REFRESH MATERIALIZED VIEW dim_weather_station (
    station_id STRING NOT NULL COMMENT 'Unique identifier for the weather observation station.',
    station_name STRING COMMENT 'Human-readable name of the weather station.',
    
    -- 1. Data Quality Expectations
    CONSTRAINT valid_station_id EXPECT (station_id IS NOT NULL) ON VIOLATION DROP ROW,

    -- 2. Primary Key Constraint for Unity Catalog / Databricks Genie
    CONSTRAINT pk_dim_weather_station PRIMARY KEY (station_id)
)
COMMENT "Weather Station dimension containing station identifiers and names"
AS
WITH stg_source AS (
    SELECT * FROM the_data_masons.bronze.bronze_weather_data
)
SELECT DISTINCT
    station_id,
    station_name
FROM stg_source;