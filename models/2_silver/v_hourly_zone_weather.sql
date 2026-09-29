-- USE SCHEMA silver;

CREATE OR REPLACE VIEW v_hourly_zone_weather AS
WITH station_hourly AS (
    -- Step 1: Deduplicate county-level repetitions per station per hour
    SELECT 
        observation_hour,
        nyiso_zone,
        station_id,
        AVG(temperature) AS temp_f,
        AVG(wind_speed) AS wind_mph
    FROM the_data_masons.silver.fact_weather_data
    GROUP BY 1, 2, 3
)
-- Step 2: Aggregate station readings to the zone level
SELECT 
    observation_hour,
    nyiso_zone,
    AVG(temp_f) AS avg_temperature_f,
    AVG(wind_mph) AS avg_wind_speed_mph
FROM station_hourly
GROUP BY 1, 2;