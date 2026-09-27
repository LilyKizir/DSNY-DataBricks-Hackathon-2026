use schema silver;
CREATE OR REFRESH MATERIALIZED VIEW dim_energy_source(
        -- 1. Data Quality Expectations
)
COMMENT "Fuel Type dim"
AS
with stg_source as (
    select * from the_data_masons.bronze.bronze_nyiso_generation_energy_source
)
select distinct
     fuel_code
    ,fuel_name
from stg_source
group by all