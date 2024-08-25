
    SELECT
        -- Select and transform fields as necessary
    brand,
    price,
    mileage,
    e.engine_id as engine_key ,
    j.junk_id as junk_key ,
    m.model_id as model_key
FROM `ready-data-de24.landing_09.cars-com_dataset` t
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


