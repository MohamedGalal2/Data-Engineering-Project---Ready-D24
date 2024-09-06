with model_dim_cte as (
SELECT 

    ROW_NUMBER() OVER (ORDER BY model, year, interior_color, exterior_color, drivetrain) AS model_id,
    model,
    cast(year as INT64) as model_year,
    interior_color,
    exterior_color,
    drivetrain
FROM `ready-data-de24.landing_09.cars-com_dataset`
GROUP BY model, year, interior_color, exterior_color, drivetrain
)
select model_id,
    IFNULL(model,'Unknown') as model,
    model_year,
    IFNULL(interior_color,'Unknown') as interior_color,
    IFNULL(exterior_color,'Unknown') as exterior_color,
    IFNULL(drivetrain,'Unknown') as drivetrain
    from model_dim_cte
