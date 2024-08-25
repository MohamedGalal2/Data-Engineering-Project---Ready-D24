
SELECT 

    ROW_NUMBER() OVER (ORDER BY model, year, interior_color, exterior_color, drivetrain) AS model_id,
    model,
    cast(year as INT64) as model_year,
    interior_color,
    exterior_color,
    drivetrain
FROM `ready-data-de24.landing_09.cars-com_dataset`
GROUP BY model, year, interior_color, exterior_color, drivetrain
