
SELECT 
    f.brand,
    m.model,
    f.price,
    f.mileage,
    m.model_year
FROM `ready-data-de24.dbt_mgalal_star.fact_table` f
JOIN `ready-data-de24.dbt_mgalal_star.engine_dim` e 
    ON f.engine_key = e.engine_id
JOIN `ready-data-de24.dbt_mgalal_star.model_dim` m 
    ON f.model_key = m.model_id
JOIN `ready-data-de24.dbt_mgalal_star.junk_dim` j 
    ON f.junk_key = j.junk_id
WHERE e.fuel_type = 'Gasoline'
  AND j.automatic_transmission = 1
  AND f.mileage < 50000
  AND m.model_year >= 2019
ORDER BY f.price ASC
LIMIT 10
