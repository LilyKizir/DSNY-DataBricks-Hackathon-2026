USE SCHEMA gold;

CREATE OR REFRESH MATERIALIZED VIEW gold_nyc_demand_by_temperature_band_past_year(
    -- 1. Data Quality Expectations
    CONSTRAINT valid_temperature_band EXPECT (temperature_band_low_f IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "How weather drives New York City (NYISO zone J) electricity demand: complete NY-local days in the 365 days ending on the latest day with demand data, grouped into 5F bands of daily mean temperature, with average daily/peak demand, degree days and the uplift versus the lowest-demand band"
AS
-- Hour keys are UTC "yyyy-MM-ddTHH"; NY local date is derived before bucketing into days
WITH demand_hourly AS (
    SELECT
        to_date(from_utc_timestamp(to_timestamp(period_timestamp, "yyyy-MM-dd'T'HH"), 'America/New_York')) AS report_date,
        value_mwh AS demand_mwh
    FROM the_data_masons.silver.fact_nyiso_subregional_demand
    WHERE sub_ba_code = 'J'
      AND period_timestamp IS NOT NULL
      AND value_mwh IS NOT NULL
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
        AVG(wind_speed_mph) AS avg_wind_speed_mph
    FROM zone_hourly_weather
    GROUP BY ALL
),

-- Only complete demand days (23+ hours covers DST) so partial feed days don't drag bands down
daily AS (
    SELECT
        d.report_date,
        d.total_demand_mwh,
        d.peak_hourly_demand_mwh,
        w.avg_temperature_f,
        w.avg_wind_speed_mph,
        GREATEST(65 - w.avg_temperature_f, 0) AS heating_degree_days,
        GREATEST(w.avg_temperature_f - 65, 0) AS cooling_degree_days,
        CAST(FLOOR(w.avg_temperature_f / 5) * 5 AS INT) AS temperature_band_low_f
    FROM demand_daily AS d
    INNER JOIN weather_daily AS w
        ON d.report_date = w.report_date
    CROSS JOIN window_bounds AS b
    WHERE d.demand_hours >= 23
      AND w.avg_temperature_f IS NOT NULL
      AND d.report_date BETWEEN b.start_date AND b.end_date
),

bands AS (
    SELECT
        temperature_band_low_f,
        COUNT(*) AS day_count,
        MIN(report_date) AS first_date,
        MAX(report_date) AS last_date,
        AVG(total_demand_mwh) AS avg_daily_demand_mwh,
        MIN(total_demand_mwh) AS min_daily_demand_mwh,
        MAX(total_demand_mwh) AS max_daily_demand_mwh,
        AVG(peak_hourly_demand_mwh) AS avg_peak_hourly_demand_mwh,
        AVG(avg_temperature_f) AS avg_temperature_f,
        AVG(avg_wind_speed_mph) AS avg_wind_speed_mph,
        AVG(heating_degree_days) AS avg_heating_degree_days,
        AVG(cooling_degree_days) AS avg_cooling_degree_days
    FROM daily
    GROUP BY ALL
),

-- Baseline = the band with the lowest average daily demand (least weather-driven load)
baseline AS (
    SELECT
        temperature_band_low_f AS baseline_band_low_f,
        avg_daily_demand_mwh AS baseline_daily_demand_mwh
    FROM bands
    ORDER BY avg_daily_demand_mwh ASC
    LIMIT 1
)

SELECT
    bd.temperature_band_low_f,
    bd.temperature_band_low_f + 5 AS temperature_band_high_f,
    concat(bd.temperature_band_low_f, '-', bd.temperature_band_low_f + 5, 'F') AS temperature_band,
    bd.day_count,
    bd.first_date,
    bd.last_date,
    ROUND(bd.avg_daily_demand_mwh, 2) AS avg_daily_demand_mwh,
    ROUND(bd.min_daily_demand_mwh, 2) AS min_daily_demand_mwh,
    ROUND(bd.max_daily_demand_mwh, 2) AS max_daily_demand_mwh,
    ROUND(bd.avg_peak_hourly_demand_mwh, 2) AS avg_peak_hourly_demand_mwh,
    ROUND(bd.avg_temperature_f, 2) AS avg_temperature_f,
    ROUND(bd.avg_wind_speed_mph, 2) AS avg_wind_speed_mph,
    ROUND(bd.avg_heating_degree_days, 2) AS avg_heating_degree_days,
    ROUND(bd.avg_cooling_degree_days, 2) AS avg_cooling_degree_days,
    concat(bl.baseline_band_low_f, '-', bl.baseline_band_low_f + 5, 'F') AS baseline_temperature_band,
    ROUND(bd.avg_daily_demand_mwh - bl.baseline_daily_demand_mwh, 2) AS demand_uplift_vs_baseline_mwh,
    ROUND(try_divide(bd.avg_daily_demand_mwh - bl.baseline_daily_demand_mwh, bl.baseline_daily_demand_mwh) * 100, 2) AS demand_uplift_vs_baseline_pct,

    -- Audit trailing
    current_timestamp() AS gold_processed_timestamp

FROM bands AS bd
CROSS JOIN baseline AS bl;
