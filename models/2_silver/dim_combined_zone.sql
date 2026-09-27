use schema silver;
CREATE OR REFRESH MATERIALIZED VIEW dim_combined_zone(
        -- 1. Data Quality Expectations
)
COMMENT "County dim"
AS
with stg_zones as (
    select * from the_data_masons.silver.dim_nyiso_zone
),
stg_subregion as (
    select *
        ,split(sub_ba_code, 'N')[1] as sub_ba_char
    from the_data_masons.silver.dim_subregion
)
select * except (sub_ba_char)
from stg_subregion
inner join stg_zones
    on stg_zones.nyiso_zone = stg_subregion.sub_ba_char;