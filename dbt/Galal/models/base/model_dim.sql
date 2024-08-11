{{ config(
    materialized='table',
    
) }}
    SELECT
        -- Select and transform fields as necessary
    model,
    year,
    interior_color,
    exterior_color,
    drivetrain

    FROM `ready-data-de24.landing_09.cars-com_dataset`  -- Reference your base table

