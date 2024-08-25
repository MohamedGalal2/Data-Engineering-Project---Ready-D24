
SELECT 
    m.model,
    e.min_mpg,
    m.model_year
FROM `ready-data-de24.dbt_mgalal_star.fact_table` f
JOIN `ready-data-de24.dbt_mgalal_star.engine_dim` e 
    ON f.engine_key = e.engine_id
JOIN `ready-data-de24.dbt_mgalal_star.model_dim` m 
    ON f.model_key = m.model_id
WHERE e.fuel_type = 'Gasoline' 
  AND m.model_year >= 2019
ORDER BY e.min_mpg DESC
LIMIT 10

