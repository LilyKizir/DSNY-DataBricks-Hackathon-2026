USE SCHEMA silver;

CREATE OR REFRESH MATERIALIZED VIEW fact_NYISO_generation_energy_source(
    -- 1. Data Quality Expectations
    CONSTRAINT valid_ges_id EXPECT (ges_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_period EXPECT (period_timestamp IS NOT NULL) ON VIOLATION DROP ROW
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
    
    -- 3. Type casting (commented out in your original code)
    to_timestamp(period, "yyyy-MM-dd'T'HH") AS period_timestamp,
    
    ba_code,
    fuel_code,
    value_mwh,
    
    -- 4. Audit trailing
    processed_timestamp AS bronze_processed_timestamp,
    current_timestamp() AS silver_processed_timestamp

FROM bronze;