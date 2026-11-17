USE SCHEMA gold;

CREATE OR REFRESH MATERIALIZED VIEW gold_nyc_daily_electricity_weather_past_year(
    -- 1. Data Quality Expectations
    CONSTRAINT valid_report_date EXPECT (report_date IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Daily electricity demand (MWh) for New York City (NYISO zone J) alongside NYC weather (temperature, wind) and heating/cooling degree days (65F base), on the NY local (Eastern) calendar, for the 365 days ending on the latest day with demand data"
AS
-- Hour keys are UTC "yyyy-MM-ddTHH"; NY local date is derived before bucketing into days
WITH demand_hourly AS (
    SELECT
        to_date(from_utc_timestamp(to_timestamp(period_timestamp, "yyyy-MM-dd'T'HH"), 'America/New_York')) AS report_date,
        value_mwh AS demand_mwh
    FROM the_data_masons.silver.fact_nyiso_subregional_demand
    WHERE sub_ba_code = 'J'
      AND period_timestamp IS NOT NULL
),

-- Window = 365 NY-local days ending on the latest day with NYC demand
window_bounds AS (
    SELECT
        date_sub(MAX(report_date), 364) AS start_date,
        MAX(report_date) AS end_date
    FROM demand_hourly
),

demand_daily AS (
    SELECT
        report_date,
        SUM(demand_mwh) AS total_demand_mwh,
        AVG(demand_mwh) AS avg_hourly_demand_mwh,
        MAX(demand_mwh) AS peak_hourly_demand_mwh,
        COUNT(demand_mwh) AS demand_hours
    FROM demand_hourly
    GROUP BY ALL
),

-- Station-hour first so a station's repeated county rows count once
station_hourly AS (
    SELECT
        station_id,
        observation_hour,
        AVG(temperature) AS temperature_f,
        AVG(wind_speed) AS wind_speed_mph
    FROM the_data_masons.silver.fact_weather_data
    WHERE nyiso_zone = 'J'
      AND observation_hour IS NOT NULL
    GROUP BY ALL
),

zone_hourly_weather AS (
    SELECT
        to_date(from_utc_timestamp(to_timestamp(observation_hour, "yyyy-MM-dd'T'HH"), 'America/New_York')) AS report_date,
        observation_hour,
        AVG(temperature_f) AS temperature_f,
        AVG(wind_speed_mph) AS wind_speed_mph
    FROM station_hourly
    GROUP BY ALL
),

weather_daily AS (
    SELECT
        report_date,
        AVG(temperature_f) AS avg_temperature_f,
        MIN(temperature_f) AS min_temperature_f,
        MAX(temperature_f) AS max_temperature_f,
        AVG(wind_speed_mph) AS avg_wind_speed_mph,
        COUNT(temperature_f) AS weather_hours
    FROM zone_hourly_weather
    GROUP BY ALL
),

-- Full join keeps weather-only days visible so gaps in the demand feed show up as NULL demand
daily AS (
    SELECT
        COALESCE(d.report_date, w.report_date) AS report_date,
        d.total_demand_mwh,
        d.avg_hourly_demand_mwh,
        d.peak_hourly_demand_mwh,
        COALESCE(d.demand_hours, 0) AS demand_hours,
        w.avg_temperature_f,
        w.min_temperature_f,
        w.max_temperature_f,
        w.avg_wind_speed_mph,
        COALESCE(w.weather_hours, 0) AS weather_hours
    FROM demand_daily AS d
    FULL OUTER JOIN weather_daily AS w
        ON d.report_date = w.report_date
)

SELECT
    dl.report_date,
    'J' AS nyiso_zone,
    z.nyiso_zone_name,
    ROUND(dl.total_demand_mwh, 2) AS total_demand_mwh,
    ROUND(dl.avg_hourly_demand_mwh, 2) AS avg_hourly_demand_mwh,
    ROUND(dl.peak_hourly_demand_mwh, 2) AS peak_hourly_demand_mwh,
    dl.demand_hours,
    ROUND(dl.avg_temperature_f, 2) AS avg_temperature_f,
    ROUND(dl.min_temperature_f, 2) AS min_temperature_f,
    ROUND(dl.max_temperature_f, 2) AS max_temperature_f,
    ROUND(dl.avg_wind_speed_mph, 2) AS avg_wind_speed_mph,
    dl.weather_hours,
    -- CASE keeps degree days NULL when there is no weather (GREATEST skips NULLs and would return 0)
    CASE WHEN dl.avg_temperature_f IS NOT NULL THEN ROUND(GREATEST(65 - dl.avg_temperature_f, 0), 2) END AS heating_degree_days,
    CASE WHEN dl.avg_temperature_f IS NOT NULL THEN ROUND(GREATEST(dl.avg_temperature_f - 65, 0), 2) END AS cooling_degree_days,
    -- Flags partial days (feed gaps, DST changes) so totals aren't compared unfairly
    (dl.demand_hours >= 23) AS is_complete_demand_day,

    -- Audit trailing
    current_timestamp() AS gold_processed_timestamp

FROM daily AS dl
CROSS JOIN window_bounds AS b
LEFT JOIN the_data_masons.silver.dim_combined_zone AS z
    ON z.nyiso_zone = 'J'
WHERE dl.report_date BETWEEN b.start_date AND b.end_date;
