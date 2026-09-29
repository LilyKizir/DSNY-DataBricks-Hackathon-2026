use schema silver;
CREATE OR REFRESH MATERIALIZED VIEW dim_balancing_authority(
    ba_code STRING NOT NULL COMMENT 'Balancing authority code (e.g., NYIS for New York ISO).',
    ba_name STRING COMMENT 'Full name of the balancing authority entity.',
    -- 1. Data Quality Expectations
    CONSTRAINT valid_ba_code EXPECT (ba_code IS NOT NULL) ON VIOLATION DROP ROW,

    -- Primary Key
    CONSTRAINT pk_dim_balancing_authority PRIMARY KEY (ba_code)
)
COMMENT "Balancing authority dim"
AS 
with  stg_region_source as (
    select distinct
         ba_code
        ,ba_name
    from the_data_masons.bronze.bronze_nyiso_operation_metrics
),
stg_subreg_source as (
    select distinct
         ba_code
        ,ba_name
    from the_data_masons.bronze.bronze_nyiso_subregional_demand
),
stg_gen_source as (
    select distinct
         ba_code
        ,ba_name 
    from the_data_masons.bronze.bronze_nyiso_generation_energy_source
),
unioned as (
    select * from stg_region_source
    union
    select * from stg_subreg_source
    union
    select * from stg_gen_source
)
select distinct *
from unioned
group by all