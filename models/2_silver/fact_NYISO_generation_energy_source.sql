USE SCHEMA silver;

CREATE OR REFRESH MATERIALIZED VIEW fact_NYISO_generation_energy_source (
    ges_id COMMENT 'Unique row hash key for generation by fuel source.',
    period_timestamp COMMENT 'Hourly truncated timestamp (UTC) for energy generation as string "yyyy-MM-ddTHH".',
    ba_code COMMENT 'Balancing authority code.',
    fuel_code COMMENT 'Fuel source code: NG (Natural Gas), NUC (Nuclear), WAT (Hydro), WND (Wind), SUN (Solar), OIL (Oil), COL (Coal), OTH (Other).',
    value_mwh COMMENT 'Net generation in Megawatt-hours (MWh).',
    bronze_processed_timestamp COMMENT 'Audit timestamp from bronze layer.',
    silver_processed_timestamp COMMENT 'Audit timestamp for silver processing.',
    
    -- 1. Data Quality Expectations
    CONSTRAINT valid_ges_id EXPECT (ges_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_period EXPECT (period_timestamp IS NOT NULL) ON VIOLATION DROP ROW,

    -- 2. Foreign Key Constraints for Unity Catalog / Databricks Genie
    CONSTRAINT fk_ges_ba FOREIGN KEY (ba_code) REFERENCES silver.dim_balancing_authority(ba_code) NOT ENFORCED,
    CONSTRAINT fk_ges_fuel FOREIGN KEY (fuel_code) REFERENCES silver.dim_energy_source(fuel_code) NOT ENFORCED
)
COMMENT "Cleaned and typed NYISO energy generation by fuel type"
AS 
WITH bronze AS (
    SELECT 
        *
    FROM the_data_masons.bronze.bronze_nyiso_generation_energy_source 
)

SELECT 
    ges_id,
    
    -- Keeping string format 'yyyy-MM-dd\'T\'HH' to directly match fact_weather_data.observation_hour
    period AS period_timestamp,
    
    ba_code,
    fuel_code,
    value_mwh,
    
    -- Audit trailing
    processed_timestamp AS bronze_processed_timestamp,
    current_timestamp() AS silver_processed_timestamp

FROM bronze;