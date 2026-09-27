use schema silver;
CREATE OR REFRESH MATERIALIZED VIEW dim_metric_type(
        -- 1. Data Quality Expectations
)
COMMENT "Metric Type dim"
AS
with stg_source as (
    select * from the_data_masons.bronze.bronze_nyiso_operation_metrics
)
select
    type_code
    ,type_name
from stg_source
group by all