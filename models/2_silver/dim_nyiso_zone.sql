-- this must be unified with subregion table

-- make from le's table

use schema silver;
CREATE OR REFRESH MATERIALIZED VIEW dim_nyiso_zone(
        -- 1. Data Quality Expectations
)
COMMENT "Nyiso Zone dim"
AS
with stg_source as (
    select * from the_data_masons.bronze.bronze_weather_data
)
select distinct
     nyiso_zone
     ,nyiso_zone_name
from stg_source
group by all