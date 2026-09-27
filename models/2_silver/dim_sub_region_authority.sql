-- this must be unified with nyiso_subregion table

use schema silver;
CREATE OR REFRESH MATERIALIZED VIEW dim_subregion(
        -- 1. Data Quality Expectations
)
COMMENT "Subregion dim"
AS
with stg_source as (
    select * from the_data_masons.bronze.bronze_nyiso_subregional_demand
)
select distinct
    sub_ba_code
    ,sub_ba_name
from stg_source
group by all