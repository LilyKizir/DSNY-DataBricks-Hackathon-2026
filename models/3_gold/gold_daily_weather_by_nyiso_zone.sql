USE SCHEMA gold;

CREATE OR REFRESH MATERIALIZED VIEW gold_daily_weather_by_nyiso_zone(
    -- 1. Data Quality Expectations
    CONSTRAINT valid_weather_date EXPECT (weather_date IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_nyiso_zone EXPECT (nyiso_zone IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Daily average temperature (F) and wind speed (mph) per NYISO zone, on the NY local (Eastern) calendar day"
AS
-- Average in steps so stations/hours with more readings don't dominate:
-- station-hour -> zone-hour -> zone-day
WITH station_hour_weather AS (
    SELECT
        nyiso_zone,
        station_id,
        -- NYISO reports on Eastern time, so bucket on the local hour
        DATE_TRUNC('HOUR', from_utc_timestamp(observed_at_utc, 'America/New_York') + INTERVAL 30 MINUTES) AS obs_hour_local,
        AVG(temperature) AS station_avg_temperature_f,
        AVG(wind_speed) AS station_avg_wind_speed_mph
    FROM the_data_masons.silver.fact_weather_data
    WHERE nyiso_zone IS NOT NULL
      AND observed_at_utc IS NOT NULL
    GROUP BY ALL
),

zone_hour_weather AS (
    SELECT
        nyiso_zone,
        obs_hour_local,
        AVG(station_avg_temperature_f) AS avg_temperature_f,
        AVG(station_avg_wind_speed_mph) AS avg_wind_speed_mph
    FROM station_hour_weather
    GROUP BY ALL
),

zone_day_weather AS (
    SELECT
        CAST(obs_hour_local AS DATE) AS weather_date,
        nyiso_zone,
        AVG(avg_temperature_f) AS daily_avg_temperature_f,
        AVG(avg_wind_speed_mph) AS daily_avg_wind_speed_mph,
        COUNT(*) AS hours_observed
    FROM zone_hour_weather
    GROUP BY ALL
)

SELECT
    d.weather_date,
    d.nyiso_zone,
    z.nyiso_zone_name,
    ROUND(d.daily_avg_temperature_f, 2) AS daily_avg_temperature_f,
    ROUND(d.daily_avg_wind_speed_mph, 2) AS daily_avg_wind_speed_mph,
    d.hours_observed,

    -- Audit trailing
    current_timestamp() AS gold_processed_timestamp

FROM zone_day_weather d
LEFT JOIN the_data_masons.silver.dim_nyiso_zone z
    ON d.nyiso_zone = z.nyiso_zone;
