USE SCHEMA silver;

CREATE OR REFRESH MATERIALIZED VIEW fact_weather_data(
    -- 1. Data Quality Expectations
    -- CONSTRAINT valid_srd_id EXPECT (srd_id IS NOT NULL) ON VIOLATION DROP ROW,
    -- CONSTRAINT valid_period EXPECT (period_timestamp IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Cleaned and typed NY weather"
AS 
WITH bronze AS (
    SELECT 
        *
    FROM the_data_masons.bronze.bronze_weather_data
)

SELECT 
    state_zone,
    fips,
    nyiso_zone,
    station_id,
    distance_km,
    observed_at_utc,
    temperature,
    wind_speed,
    
    
    -- 4. Audit trailing
    ingested_at_utc AS bronze_processed_timestamp,
    current_timestamp() AS silver_processed_timestamp

FROM bronze;