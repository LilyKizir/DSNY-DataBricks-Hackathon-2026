USE SCHEMA silver;

CREATE OR REFRESH MATERIALIZED VIEW fact_NYISO_subregional_demand (
    srd_id STRING COMMENT 'Unique row hash key for subregional demand.',
    period_timestamp STRING COMMENT 'Hourly truncated timestamp (UTC) for electricity demand as string "yyyy-MM-ddTHH".',
    ba_code STRING COMMENT 'Balancing authority code (e.g., NYIS).',
    sub_ba_code STRING COMMENT 'Sub-balancing authority zone code (e.g., ZONA, ZONJ). Joins to dim_combined_zone.sub_ba_code.',
    value_mwh DOUBLE COMMENT 'Electricity demand/load for the hour in Megawatt-hours (MWh).',
    bronze_processed_timestamp TIMESTAMP COMMENT 'Audit timestamp from bronze layer.',
    silver_processed_timestamp TIMESTAMP COMMENT 'Audit timestamp for silver processing.',
    
    -- 1. Data Quality Expectations
    CONSTRAINT valid_srd_id EXPECT (srd_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_period EXPECT (period_timestamp IS NOT NULL) ON VIOLATION DROP ROW,

    -- 2. Foreign Key Constraints for Unity Catalog / Databricks Genie
    CONSTRAINT fk_srd_ba FOREIGN KEY (ba_code) REFERENCES silver.dim_balancing_authority(ba_code) NOT ENFORCED,
    CONSTRAINT fk_srd_subba FOREIGN KEY (sub_ba_code) REFERENCES silver.dim_combined_zone(nyiso_zone) NOT ENFORCED
)
COMMENT "Cleaned and typed NYISO subregional demand"
AS 
WITH bronze AS (
    SELECT 
        *
    FROM the_data_masons.bronze.bronze_nyiso_subregional_demand
)

SELECT 
    srd_id,
    
    -- Keeping string format 'yyyy-MM-dd\'T\'HH' to directly match fact_weather_data.observation_hour
    period AS period_timestamp,
    
    ba_code,
    SPLIT(sub_ba_code, 'N')[1] AS sub_ba_code,
    value_mwh,
    
    -- Audit trailing
    processed_timestamp AS bronze_processed_timestamp,
    current_timestamp() AS silver_processed_timestamp

FROM bronze;