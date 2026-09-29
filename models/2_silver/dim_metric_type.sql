USE SCHEMA silver;

CREATE OR REFRESH MATERIALIZED VIEW dim_metric_type (
    type_code STRING NOT NULL COMMENT 'Grid operation metric short code: D (Demand), DF (Day-ahead forecast), NG (Net generation), TI (Total interchange).',
    type_name STRING COMMENT 'Full descriptive name of the operating metric.',
    
    -- 1. Data Quality Expectations
    CONSTRAINT valid_type_code EXPECT (type_code IS NOT NULL) ON VIOLATION DROP ROW,

    -- 2. Primary Key Constraint for Unity Catalog / Databricks Genie
    CONSTRAINT pk_dim_metric_type PRIMARY KEY (type_code)
)
COMMENT "NYISO operating metric type dimension"
AS
WITH stg_source AS (
    SELECT * FROM the_data_masons.bronze.bronze_nyiso_operation_metrics
)
SELECT DISTINCT
    type_code,
    type_name
FROM stg_source;