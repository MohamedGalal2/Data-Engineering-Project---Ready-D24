{{ config(
    materialized='table',
    
) }}
    SELECT
        -- Select and transform fields as necessary
    engine,
    engine_size,
    transmission,
    fuel_type,
    min_mpg,
    max_mpg

    FROM `ready-data-de24.landing_09.cars-com_dataset`  -- Reference your base table

