USE SCHEMA gold;

CREATE OR REFRESH MATERIALIZED VIEW gold_daily_weather_electricity_demand_by_nyiso_zone(
    -- 1. Data Quality Expectations
    CONSTRAINT valid_nyiso_zone EXPECT (nyiso_zone IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_demand_date EXPECT (demand_date IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Daily electricity demand (MWh) per NYISO zone alongside the zone's weather (temperature, wind) and heating/cooling degree days (65F base), on the NY local (Eastern) calendar, to analyse how weather drives electricity usage"
AS
-- Station-hour first so a station's repeated county rows count once per zone
WITH station_hourly AS (
    SELECT
        nyiso_zone,
        station_id,
        observation_hour,
        AVG(temperature) AS temperature_f,
        AVG(wind_speed) AS wind_speed_mph
    FROM the_data_masons.silver.fact_weather_data
    WHERE nyiso_zone IS NOT NULL
      AND observation_hour IS NOT NULL
    GROUP BY ALL
),

zone_hourly_weather AS (
    SELECT
        nyiso_zone,
        observation_hour,
        AVG(temperature_f) AS temperature_f,
        AVG(wind_speed_mph) AS wind_speed_mph
    FROM station_hourly
    GROUP BY ALL
),

-- Demand and weather share the same UTC grid hour key ("yyyy-MM-ddTHH"); join there, then move to NY local time
hourly_joined AS (
    SELECT
        to_date(from_utc_timestamp(to_timestamp(d.period_timestamp, "yyyy-MM-dd'T'HH"), 'America/New_York')) AS demand_date,
        d.sub_ba_code AS nyiso_zone,
        d.value_mwh AS demand_mwh,
        w.temperature_f,
        w.wind_speed_mph
    FROM the_data_masons.silver.fact_nyiso_subregional_demand AS d
    LEFT JOIN zone_hourly_weather AS w
        ON d.period_timestamp = w.observation_hour
       AND d.sub_ba_code = w.nyiso_zone
    WHERE d.period_timestamp IS NOT NULL
      AND d.sub_ba_code IS NOT NULL
),

zone_daily AS (
    SELECT
        demand_date,
        nyiso_zone,
        SUM(demand_mwh) AS total_demand_mwh,
        AVG(demand_mwh) AS avg_hourly_demand_mwh,
        MAX(demand_mwh) AS peak_hourly_demand_mwh,
        COUNT(demand_mwh) AS demand_hours,
        AVG(temperature_f) AS avg_temperature_f,
        MIN(temperature_f) AS min_temperature_f,
        MAX(temperature_f) AS max_temperature_f,
        AVG(wind_speed_mph) AS avg_wind_speed_mph,
        COUNT(temperature_f) AS weather_hours
    FROM hourly_joined
    GROUP BY ALL
)

SELECT
    zd.demand_date,
    zd.nyiso_zone,
    z.nyiso_zone_name,
    ROUND(zd.total_demand_mwh, 2) AS total_demand_mwh,
    ROUND(zd.avg_hourly_demand_mwh, 2) AS avg_hourly_demand_mwh,
    ROUND(zd.peak_hourly_demand_mwh, 2) AS peak_hourly_demand_mwh,
    zd.demand_hours,
    ROUND(zd.avg_temperature_f, 2) AS avg_temperature_f,
    ROUND(zd.min_temperature_f, 2) AS min_temperature_f,
    ROUND(zd.max_temperature_f, 2) AS max_temperature_f,
    ROUND(zd.avg_wind_speed_mph, 2) AS avg_wind_speed_mph,
    zd.weather_hours,
    -- Standard degree days from the daily mean temperature, 65F base;
    -- CASE keeps them NULL for zones without weather (GREATEST skips NULLs and would return 0)
    CASE WHEN zd.avg_temperature_f IS NOT NULL THEN ROUND(GREATEST(65 - zd.avg_temperature_f, 0), 2) END AS heating_degree_days,
    CASE WHEN zd.avg_temperature_f IS NOT NULL THEN ROUND(GREATEST(zd.avg_temperature_f - 65, 0), 2) END AS cooling_degree_days,
    -- Flags partial days (edges of the data, DST changes) so totals aren't compared unfairly
    (zd.demand_hours >= 23) AS is_complete_day,

    -- Audit trailing
    current_timestamp() AS gold_processed_timestamp

FROM zone_daily AS zd
LEFT JOIN the_data_masons.silver.dim_combined_zone AS z
    ON zd.nyiso_zone = z.nyiso_zone;
