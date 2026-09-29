USE SCHEMA silver;

CREATE OR REFRESH MATERIALIZED VIEW dim_subregion (
    sub_ba_code STRING NOT NULL COMMENT 'Sub-balancing authority zone code (e.g., ZONA..ZONK). Joins to dim_combined_zone.sub_ba_code.',
    sub_ba_name STRING COMMENT 'Full display name for the sub-balancing authority area.',
    
    -- 1. Data Quality Expectations
    CONSTRAINT valid_sub_ba_code EXPECT (sub_ba_code IS NOT NULL) ON VIOLATION DROP ROW,

    -- 2. Primary Key Constraint for Unity Catalog / Databricks Genie
    CONSTRAINT pk_dim_subregion PRIMARY KEY (sub_ba_code)
)
COMMENT "Subregion dimension containing NYISO sub-balancing authority codes and names"
AS
WITH stg_source AS (
    SELECT * FROM the_data_masons.bronze.bronze_nyiso_subregional_demand
)
SELECT DISTINCT
    SPLIT(sub_ba_code, 'N')[1] AS sub_ba_code,
    sub_ba_name
FROM stg_source;