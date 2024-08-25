
SELECT
    ROW_NUMBER() OVER (ORDER BY engine, engine_size, transmission, fuel_type,min_mpg,max_mpg) AS engine_id,
    engine,
    engine_size,
    transmission,
    fuel_type,
    min_mpg,
    max_mpg
FROM `ready-data-de24.landing_09.cars-com_dataset`
GROUP BY engine, engine_size, transmission, fuel_type,min_mpg,max_mpg
