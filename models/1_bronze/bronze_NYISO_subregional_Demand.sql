USE SCHEMA bronze;
CREATE OR REFRESH MATERIALIZED VIEW bronze_NYISO_subregional_demand
COMMENT "Parsed API data loaded from landing"
AS 
WITH exploded_data AS (
    SELECT explode(cast(raw_response:response.data AS ARRAY<VARIANT>)) AS d_value
    FROM the_data_masons.landing.raw_subregional_demand
)
SELECT
    md5(concat_ws('-', 
        coalesce(d_value:period::string, ''), 
        coalesce(d_value:subba::string, ''), 
        coalesce(d_value:parent::string, '')
    )) AS srd_id,
    
    d_value:period::string AS period,
    d_value:parent::string AS ba_code,
    d_value:`parent-name`::string AS ba_name,
    d_value:subba::string AS sub_ba_code,
    d_value:`subba-name`::string AS sub_ba_name,
    d_value:value::double AS value_mwh,
    d_value:`value-units`::string AS value_units,
    
    current_timestamp() AS processed_timestamp
FROM exploded_data;