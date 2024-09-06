with engine_dim_cte as(
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
)
SELECT 
    engine_id,
    IFNULL(engine, 'Unknown') AS engine,
    IFNULL(engine_size,-1) as engine_size ,
    IFNULL(transmission , 'Unknown') AS transmission,
    IFNULL(fuel_type , 'Unknown') AS fuel_type,
     IFNULL(min_mpg,-1) as min_mpg,
     IFNULL(max_mpg,-1) as max_mpg


FROM engine_dim_cte

