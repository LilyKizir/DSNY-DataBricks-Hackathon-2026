USE SCHEMA silver;

CREATE OR REFRESH MATERIALIZED VIEW fact_NYISO_operation_metrics(
    -- 1. Data Quality Expectations
    CONSTRAINT valid_rom_id EXPECT (rom_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_period EXPECT (period_timestamp IS NOT NULL) ON VIOLATION DROP ROW
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
    
    -- 3. Type casting (commented out in your original code)
    to_timestamp(period, "yyyy-MM-dd'T'HH") AS period_timestamp,
    
    ba_code,
    type_code,
    value_mwh,
    
    -- 4. Audit trailing
    processed_timestamp AS bronze_processed_timestamp,
    current_timestamp() AS silver_processed_timestamp

FROM bronze;