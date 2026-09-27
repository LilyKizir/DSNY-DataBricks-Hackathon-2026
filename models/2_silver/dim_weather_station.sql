use schema silver;
CREATE OR REFRESH MATERIALIZED VIEW dim_weather_station(
        -- 1. Data Quality Expectations
)
COMMENT "Weather Station dim"
AS
with stg_source as (
    select * from the_data_masons.bronze.bronze_weather_data
)
select distinct
     station_id
     ,station_name
from stg_source
group by all