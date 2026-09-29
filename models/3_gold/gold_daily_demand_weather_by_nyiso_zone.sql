USE SCHEMA gold;

CREATE OR REFRESH MATERIALIZED VIEW gold_daily_demand_weather_by_nyiso_zone(
    -- 1. Data Quality Expectations
    CONSTRAINT valid_demand_date EXPECT (demand_date IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_nyiso_zone EXPECT (nyiso_zone IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Daily electricity demand (MWh) with daily temperature, wind speed and degree days per NYISO zone, on the NY local (Eastern) calendar day, to show how weather drives demand"
AS
-- Weather is averaged in steps so stations/hours with more readings don't dominate:
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
        MIN(avg_temperature_f) AS daily_min_temperature_f,
        MAX(avg_temperature_f) AS daily_max_temperature_f,
        AVG(avg_wind_speed_mph) AS daily_avg_wind_speed_mph,
        COUNT(*) AS weather_hours_observed
    FROM zone_hour_weather
    GROUP BY ALL
),

hourly_demand AS (
    SELECT
        sub_ba_code AS nyiso_zone,
        -- EIA hourly periods are UTC and hour-ending, so step back an hour before taking the NY local day
        from_utc_timestamp(CAST(period_timestamp AS TIMESTAMP) - INTERVAL 1 HOUR, 'America/New_York') AS demand_hour_local,
        value_mwh
    FROM the_data_masons.silver.fact_NYISO_subregional_demand
    WHERE sub_ba_code IS NOT NULL
      AND period_timestamp IS NOT NULL
),

zone_day_demand AS (
    SELECT
        CAST(demand_hour_local AS DATE) AS demand_date,
        nyiso_zone,
        SUM(value_mwh) AS daily_demand_mwh,
        MAX(value_mwh) AS peak_hour_demand_mwh,
        COUNT(DISTINCT demand_hour_local) AS demand_hours_observed
    FROM hourly_demand
    GROUP BY ALL
),

-- dim_subregion uses EIA codes (ZONA ... ZONK), the demand fact uses the bare zone letter
zone_names AS (
    SELECT
        RIGHT(sub_ba_code, 1) AS nyiso_zone,
        sub_ba_name AS nyiso_zone_name
    FROM the_data_masons.silver.dim_subregion
),

zone_day AS (
    SELECT
        d.demand_date,
        d.nyiso_zone,
        z.nyiso_zone_name,
        d.daily_demand_mwh,
        d.peak_hour_demand_mwh,
        d.demand_hours_observed,
        -- The EIA feed has gaps; 23+ hours allows for the spring-forward DST day
        d.demand_hours_observed >= 23 AS is_complete_day,
        w.daily_avg_temperature_f,
        w.daily_min_temperature_f,
        w.daily_max_temperature_f,
        w.daily_avg_wind_speed_mph,
        w.weather_hours_observed
    FROM zone_day_demand d
    -- Zone H (Millwood) has no weather stations, so keep demand rows without weather
    LEFT JOIN zone_day_weather w
        ON d.nyiso_zone = w.nyiso_zone
       AND d.demand_date = w.weather_date
    LEFT JOIN zone_names z
        ON d.nyiso_zone = z.nyiso_zone
)

SELECT
    demand_date,
    nyiso_zone,
    nyiso_zone_name,
    ROUND(daily_demand_mwh, 2) AS daily_demand_mwh,
    ROUND(peak_hour_demand_mwh, 2) AS peak_hour_demand_mwh,
    demand_hours_observed,
    is_complete_day,
    ROUND(daily_avg_temperature_f, 2) AS daily_avg_temperature_f,
    ROUND(daily_min_temperature_f, 2) AS daily_min_temperature_f,
    ROUND(daily_max_temperature_f, 2) AS daily_max_temperature_f,
    ROUND(daily_avg_wind_speed_mph, 2) AS daily_avg_wind_speed_mph,
    weather_hours_observed,

    -- Degree days (65F base): the standard measure of cooling / heating need
    -- (GREATEST skips NULLs, so guard zones without weather)
    CASE WHEN daily_avg_temperature_f IS NOT NULL
         THEN ROUND(GREATEST(daily_avg_temperature_f - 65, 0), 2) END AS cooling_degree_days,
    CASE WHEN daily_avg_temperature_f IS NOT NULL
         THEN ROUND(GREATEST(65 - daily_avg_temperature_f, 0), 2) END AS heating_degree_days,

    -- How closely daily demand follows temperature in each zone, complete days only (-1 to 1)
    ROUND(CORR(
        CASE WHEN is_complete_day THEN daily_demand_mwh END,
        CASE WHEN is_complete_day THEN daily_avg_temperature_f END
    ) OVER (PARTITION BY nyiso_zone), 2) AS zone_demand_temperature_correlation,

    -- Audit trailing
    current_timestamp() AS gold_processed_timestamp

FROM zone_day;
