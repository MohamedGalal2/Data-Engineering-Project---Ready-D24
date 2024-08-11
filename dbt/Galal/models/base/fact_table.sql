{{ config(
    materialized='table',
    
) }}
    SELECT
        -- Select and transform fields as necessary
    brand,
    price,
    mileage

    FROM `ready-data-de24.landing_09.cars-com_dataset`  -- Reference your base table

