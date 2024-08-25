
SELECT
    ROW_NUMBER() OVER () AS junk_id,
    automatic_transmission,
    damaged,
    first_owner,
    personal_using,
    turbo,
    alloy_wheels,
    adaptive_cruise_control,
    navigation_system,
    power_liftgate,
    backup_camera,
    keyless_start,
    remote_start,
    sunroof_or_moonroof,
    automatic_emergency_braking,
    stability_control,
    leather_seats,
    memory_seat,
    third_row_seating,
    apple_car_play_or_android_auto,
    bluetooth,
    usb_port,
    heated_seats
FROM `ready-data-de24.landing_09.cars-com_dataset`
group by automatic_transmission, damaged, first_owner, personal_using, turbo,
                       alloy_wheels, adaptive_cruise_control, navigation_system, power_liftgate,
                       backup_camera, keyless_start, remote_start, sunroof_or_moonroof,
                       automatic_emergency_braking, stability_control, leather_seats,
                       memory_seat, third_row_seating, apple_car_play_or_android_auto,
                       bluetooth, usb_port, heated_seats

