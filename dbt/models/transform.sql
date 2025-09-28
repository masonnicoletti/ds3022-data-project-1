{{ config(materialized='table') }}

# Calculate CO2 (kg) per trip
SELECT 
    y.*,
    (y.trip_distance * v.co2_grams_per_mile) / 1000.0 AS trip_co2_kgs
FROM {{ ref('yellow_tripdata') }} y
JOIN {{ ref('vehicle_emissions') }} v
ON v.vehicle_type = 'yellow_taxi'

