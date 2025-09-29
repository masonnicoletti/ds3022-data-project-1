{{ config(materialized='table') }}

-- Calculate CO2 (kg) per trip and add to yellow taxi data
SELECT 
    y.*,
    (y.trip_distance * v.co2_grams_per_mile) / 1000.0 AS trip_co2_kgs
FROM {{ source('trip_data', 'yellow_tripdata') }} y
JOIN {{ source('trip_data', 'vehicle_emissions') }} v
ON v.vehicle_type = 'yellow_taxi'