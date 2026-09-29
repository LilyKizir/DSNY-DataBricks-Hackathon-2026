USE SCHEMA silver;

CREATE OR REFRESH MATERIALIZED VIEW dim_combined_zone (
    nyiso_zone STRING NOT NULL COMMENT 'NYISO load zone letter identifier (A..K).',
    nyiso_zone_name STRING COMMENT 'Full name of the NYISO weather load zone.',
    
    -- 1. Data Quality Expectations
    CONSTRAINT valid_sub_ba EXPECT (nyiso_zone IS NOT NULL) ON VIOLATION DROP ROW,

    -- 2. Primary Key Constraint for Unity Catalog / Databricks Genie
    CONSTRAINT pk_dim_combined_zone PRIMARY KEY (nyiso_zone)
)
COMMENT "Bridging dimension connecting NYISO demand subregions with weather zones"
AS
WITH stg_zones AS (
    SELECT * FROM the_data_masons.silver.dim_nyiso_zone
),
stg_subregion AS (
    SELECT 
        *
    FROM the_data_masons.silver.dim_subregion
)
SELECT 
    stg_subregion.sub_ba_code as nyiso_zone,
    stg_subregion.sub_ba_name as nyiso_zone_name
FROM stg_subregion
LEFT JOIN stg_zones
    ON stg_zones.nyiso_zone = stg_subregion.sub_ba_code;