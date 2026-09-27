use schema silver;
CREATE OR REFRESH MATERIALIZED VIEW dim_balancing_authority(
        -- 1. Data Quality Expectations
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