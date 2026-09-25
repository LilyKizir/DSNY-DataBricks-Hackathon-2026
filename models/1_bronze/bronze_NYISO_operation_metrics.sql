USE SCHEMA bronze;
CREATE OR REFRESH MATERIALIZED VIEW bronze_NYISO_operation_metrics
COMMENT "Parsed API data loaded from landing"
AS 
WITH exploded_data AS (
    SELECT explode(cast(raw_response:response.data AS ARRAY<VARIANT>)) AS d_value
    FROM the_data_masons.landing.raw_regional_operating_metrics
)
SELECT
    md5(concat_ws('-', 
        coalesce(d_value:period::string, ''), 
        coalesce(d_value:respondent::string, ''), 
        coalesce(d_value:type::string, '')
    )) AS rom_id,
    
    d_value:period::string AS period,
    d_value:respondent::string AS ba_code,
    d_value:`respondent-name`::string AS ba_name,
    d_value:type::string AS type_code,
    d_value:`type-name`::string AS type_name,
    d_value:value::double AS value_mwh,
    d_value:`value-units`::string AS value_units,
    
    current_timestamp() AS processed_timestamp
FROM exploded_data;