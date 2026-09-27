USE SCHEMA gold;

CREATE OR REFRESH MATERIALIZED VIEW gold_avg_temperature_jan_2026(
    -- 1. Data Quality Expectations
    CONSTRAINT valid_nyiso_zone EXPECT (nyiso_zone IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Average temperature (F) for January 2026 per NYISO zone, plus the statewide average, on the NY local (Eastern) calendar"
AS
-- Built on the daily zone table so it inherits its station-hour -> zone-hour -> zone-day averaging
WITH jan_2026_daily AS (
    SELECT
        weather_date,
        nyiso_zone,
        nyiso_zone_name,
        daily_avg_temperature_f
    FROM the_data_masons.gold.gold_daily_weather_by_nyiso_zone
    WHERE weather_date >= DATE'2026-01-01'
      AND weather_date <  DATE'2026-02-01'
),

zone_month AS (
    SELECT
        nyiso_zone,
        nyiso_zone_name,
        AVG(daily_avg_temperature_f) AS avg_temperature_f,
        MIN(daily_avg_temperature_f) AS min_daily_avg_temperature_f,
        MAX(daily_avg_temperature_f) AS max_daily_avg_temperature_f,
        COUNT(*) AS days_observed
    FROM jan_2026_daily
    GROUP BY ALL
)

SELECT
    DATE'2026-01-01' AS month_start_date,
    nyiso_zone,
    nyiso_zone_name,
    ROUND(avg_temperature_f, 2) AS avg_temperature_f,
    ROUND(min_daily_avg_temperature_f, 2) AS min_daily_avg_temperature_f,
    ROUND(max_daily_avg_temperature_f, 2) AS max_daily_avg_temperature_f,
    days_observed,
    -- Each zone weighted equally so dense station clusters (e.g. zone C) don't dominate
    ROUND(AVG(avg_temperature_f) OVER (), 2) AS statewide_avg_temperature_f,

    -- Audit trailing
    current_timestamp() AS gold_processed_timestamp

FROM zone_month;
