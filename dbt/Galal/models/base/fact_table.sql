with landing_09_staging as (
    select 
    brand,
    price,
    mileage,
    IFNULL(engine, 'Unknown') AS engine,
    IFNULL(engine_size,-1) as engine_size ,
    IFNULL(transmission , 'Unknown') AS transmission,
    IFNULL(fuel_type , 'Unknown') AS fuel_type,
    IFNULL(min_mpg,-1) as min_mpg,
    IFNULL(max_mpg,-1) as max_mpg,
    IFNULL(automatic_transmission, -1) AS automatic_transmission,
    IFNULL(damaged, -1) AS damaged,
    IFNULL(first_owner, -1) AS first_owner,
    IFNULL(personal_using, -1) AS personal_using,
    IFNULL(turbo, -1) AS turbo,
    IFNULL(alloy_wheels, -1) AS alloy_wheels,
    IFNULL(adaptive_cruise_control, -1) AS adaptive_cruise_control,
    IFNULL(navigation_system, -1) AS navigation_system,
    IFNULL(power_liftgate, -1) AS power_liftgate,
    IFNULL(backup_camera, -1) AS backup_camera,
    IFNULL(keyless_start, -1) AS keyless_start,
    IFNULL(remote_start, -1) AS remote_start,
    IFNULL(sunroof_or_moonroof, -1) AS sunroof_or_moonroof,
    IFNULL(automatic_emergency_braking, -1) AS automatic_emergency_braking,
    IFNULL(stability_control, -1) AS stability_control,
    IFNULL(leather_seats, -1) AS leather_seats,
    IFNULL(memory_seat, -1) AS memory_seat,
    IFNULL(third_row_seating, -1) AS third_row_seating,
    IFNULL(apple_car_play_or_android_auto, -1) AS apple_car_play_or_android_auto,
    IFNULL(bluetooth, -1) AS bluetooth,
    IFNULL(usb_port, -1) AS usb_port,
    IFNULL(heated_seats, -1) AS heated_seats,
    IFNULL(model,'Unknown') as model,
    year,
    IFNULL(interior_color,'Unknown') as interior_color,
    IFNULL(exterior_color,'Unknown') as exterior_color,
    IFNULL(drivetrain,'Unknown') as drivetrain
    from `ready-data-de24.landing_09.cars-com_dataset`

)
,

fact_table_cte as (
    SELECT
        -- Select and transform fields as necessary
    brand,
    price,
    mileage,
    e.engine_id as engine_key ,
    j.junk_id as junk_key ,
    m.model_id as model_key
FROM  landing_09_staging t
LEFT JOIN  {{ ref('engine_dim') }} e 
    ON t.engine = e.engine 
    AND t.engine_size = e.engine_size 
    AND t.transmission = e.transmission 
    AND t.fuel_type = e.fuel_type
    AND t.min_mpg = e.min_mpg
    AND t.max_mpg = e.max_mpg
LEFT JOIN  {{ ref('junk_dim') }} j 
    ON t.automatic_transmission = j.automatic_transmission 
    AND t.damaged = j.damaged 
    AND t.first_owner = j.first_owner
    AND t.personal_using = j.personal_using
    AND t.turbo = j.turbo
    AND t.alloy_wheels = j.alloy_wheels
    AND t.adaptive_cruise_control = j.adaptive_cruise_control
    AND t.navigation_system = j.navigation_system
    AND t.power_liftgate = j.power_liftgate
    AND t.backup_camera = j.backup_camera
    AND t.keyless_start = j.keyless_start
    AND t.remote_start = j.remote_start
    AND t.sunroof_or_moonroof = j.sunroof_or_moonroof
    AND t.automatic_emergency_braking = j.automatic_emergency_braking
    AND t.stability_control = j.stability_control
    AND t.leather_seats = j.leather_seats
    AND t.memory_seat = j.memory_seat
    AND t.third_row_seating = j.third_row_seating
    AND t.apple_car_play_or_android_auto = j.apple_car_play_or_android_auto
    AND t.bluetooth = j.bluetooth
    AND t.usb_port = j.usb_port
    AND t.heated_seats = j.heated_seats
LEFT JOIN  {{ ref('model_dim') }} m 
    ON t.model = m.model 
    AND t.year = m.model_year 
    AND t.interior_color = m.interior_color 
    AND t.exterior_color = m.exterior_color 
    AND t.drivetrain = m.drivetrain

)
select   brand,
    price,
    IFNULL(mileage,-1) as mileage ,
     engine_key ,
     junk_key ,
     model_key
    from fact_table_cte
    
