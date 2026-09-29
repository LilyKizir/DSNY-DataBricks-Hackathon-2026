USE SCHEMA silver;

CREATE OR REFRESH MATERIALIZED VIEW fact_NYISO_operation_metrics (
    rom_id COMMENT 'Unique row hash key for operation metrics.',
    period_timestamp COMMENT 'Hourly truncated timestamp (UTC) as string "yyyy-MM-ddTHH".',
    ba_code COMMENT 'Balancing authority code.',
    type_code COMMENT 'Operational metric code: D = Demand, DF = Day-Ahead Demand Forecast, NG = Net Generation, TI = Total Interchange.',
    value_mwh COMMENT 'Metric value in Megawatt-hours (MWh).',
    bronze_processed_timestamp COMMENT 'Audit timestamp from bronze layer.',
    silver_processed_timestamp COMMENT 'Audit timestamp for silver processing.',
    
    -- 1. Data Quality Expectations
    CONSTRAINT valid_rom_id EXPECT (rom_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_period EXPECT (period_timestamp IS NOT NULL) ON VIOLATION DROP ROW,

    -- 2. Foreign Key Constraints for Unity Catalog / Databricks Genie
    CONSTRAINT fk_rom_ba FOREIGN KEY (ba_code) REFERENCES silver.dim_balancing_authority(ba_code) NOT ENFORCED,
    CONSTRAINT fk_rom_type FOREIGN KEY (type_code) REFERENCES silver.dim_metric_type(type_code) NOT ENFORCED
)
COMMENT "Cleaned and typed NYISO operating metrics"
AS 
WITH bronze AS (
    SELECT 
        *
    FROM the_data_masons.bronze.bronze_NYISO_operation_metrics 
)

SELECT 
    rom_id,
    
    -- Keeping string format 'yyyy-MM-dd\'T\'HH' to directly match fact_weather_data.observation_hour
    period AS period_timestamp,
    
    ba_code,
    type_code,
    value_mwh,
    
    -- Audit trailing
    processed_timestamp AS bronze_processed_timestamp,
    current_timestamp() AS silver_processed_timestamp

FROM bronze;