USE SCHEMA bronze;
CREATE OR REFRESH MATERIALIZED VIEW bronze_NYISO_generation_energy_source
COMMENT "Parsed API data loaded from landing"
AS 
WITH exploded_data AS (
    SELECT explode(cast(raw_response:response.data AS ARRAY<VARIANT>)) AS d_value
    FROM the_data_masons.landing.raw_generation_energy_source
)
SELECT
    md5(concat_ws('-', 
        coalesce(d_value:period::string, ''), 
        coalesce(d_value:respondent::string, ''), 
        coalesce(d_value:fueltype::string, '')
    )) AS ges_id,
    
    d_value:period::string AS period,
    d_value:respondent::string AS ba_code,
    d_value:`respondent-name`::string AS ba_name,
    d_value:fueltype::string AS fuel_code,
    d_value:`type-name`::string AS fuel_name,
    d_value:value::double AS value_mwh,
    d_value:`value-units`::string AS value_units,
    
    current_timestamp() AS processed_timestamp
FROM exploded_data;