USE SCHEMA silver;

CREATE OR REFRESH MATERIALIZED VIEW dim_energy_source (
    fuel_code STRING NOT NULL COMMENT 'Energy source short code: NG (Natural Gas), NUC (Nuclear), WAT (Hydro), WND (Wind), SUN (Solar), OIL (Oil), COL (Coal), OTH (Other).',
    fuel_name STRING COMMENT 'Full descriptive name of the fuel source or technology.',

    -- 1. Data Quality Expectations
    CONSTRAINT valid_fuel_code EXPECT (fuel_code IS NOT NULL) ON VIOLATION DROP ROW,

    -- 2. Primary Key Constraint for Unity Catalog / Databricks Genie
    CONSTRAINT pk_dim_energy_source PRIMARY KEY (fuel_code)
)
COMMENT "NYISO energy generation fuel source dimension"
AS
WITH stg_source AS (
    SELECT * FROM the_data_masons.bronze.bronze_nyiso_generation_energy_source
)
SELECT DISTINCT
    fuel_code,
    fuel_name
FROM stg_source;