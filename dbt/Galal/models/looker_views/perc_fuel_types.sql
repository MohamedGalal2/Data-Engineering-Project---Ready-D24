

WITH fuel_cte AS (
    SELECT 
        fuel_type, 
        COUNT(*) * 1.0 / (SELECT COUNT(*) FROM `ready-data-de24.dbt_mgalal_star.engine_dim`) AS percentage
    FROM 
        `ready-data-de24.dbt_mgalal_star.engine_dim`
    GROUP BY 
        fuel_type
)
SELECT 
    fuel_type, 
ROUND(percentage * 100, 2) AS percentage
FROM 
    fuel_cte
WHERE 
    fuel_type IN ('Gasoline', 'Hybrid', 'Electric')
