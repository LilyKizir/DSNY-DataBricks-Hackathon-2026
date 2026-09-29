USE SCHEMA silver;

CREATE OR REFRESH MATERIALIZED VIEW fact_weather_data (
    state_zone COMMENT 'NWS weather forecast zone (e.g., NYZ072). Do NOT join this to NYISO grid electricity zones.',
    fips COMMENT 'County FIPS code.',
    nyiso_zone COMMENT 'NYISO load zone letter/identifier (e.g., A, B, C... J for NYC, K for Long Island).',
    station_id COMMENT 'Unique weather station identifier.',
    distance_km COMMENT 'Distance in km to station.',
    observed_at_utc COMMENT 'Timestamp of weather observation in UTC.',
    observation_hour COMMENT 'Timestamp of weather observation in UTC as string "yyyy-MM-ddTHH".',
    temperature COMMENT 'Air temperature in degrees Fahrenheit.',
    wind_speed COMMENT 'Wind speed in miles per hour (mph).',
    bronze_processed_timestamp COMMENT 'Audit timestamp from bronze layer.',
    silver_processed_timestamp COMMENT 'Audit timestamp for silver processing.',
    
    -- Data Quality Expectations
    CONSTRAINT valid_observed_at EXPECT (observed_at_utc IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_station EXPECT (station_id IS NOT NULL) ON VIOLATION DROP ROW,

    -- Foreign Key Constraints for Unity Catalog / Databricks Genie
    CONSTRAINT fk_fwd_station FOREIGN KEY (station_id) REFERENCES silver.dim_weather_station(station_id) NOT ENFORCED,
    CONSTRAINT fk_fwd_zone FOREIGN KEY (nyiso_zone) REFERENCES silver.dim_combined_zone(nyiso_zone) NOT ENFORCED,
    CONSTRAINT fk_fwd_county FOREIGN KEY (fips) REFERENCES silver.dim_county(fips) NOT ENFORCED
)
COMMENT "Cleaned and typed NY weather data with Unity Catalog relationships"
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
    -- Added + INTERVAL 30 MINUTES so observations at ~:51 past the hour round up to the correct grid hour
    date_format(observed_at_utc + INTERVAL 30 MINUTES, "yyyy-MM-dd'T'HH") AS observation_hour,
    temperature,
    wind_speed,
    
    -- Audit trailing
    ingested_at_utc AS bronze_processed_timestamp,
    current_timestamp() AS silver_processed_timestamp

FROM bronze;