-- Transform green_tripdata table with dbt

{{ config(materialized='table', alias='green_tripdata') }}

WITH green_tripdata AS (
    SELECT * FROM {{ source('raw', 'green_tripdata') }}
),

green_tripdata_transformed AS (
    SELECT 
        g.*,

        -- Calculate CO2 (kg) per trip for green taxi
        (g.trip_distance * (
            SELECT co2_grams_per_mile FROM {{ source('raw', 'vehicle_emissions') }}
            WHERE vehicle_type = 'green_taxi'
        )) / 1000.0 AS trip_co2_kgs,
        
        -- Calculate average mph per trip
        g.trip_distance / (EXTRACT(EPOCH FROM (g.lpep_dropoff_datetime - g.lpep_pickup_datetime)) / 3600.0) AS avg_mph,

        -- Calculate trip hour
        EXTRACT(HOUR FROM g.lpep_pickup_datetime) AS hour_of_day,

        -- Calculate trip day of the week
        DAYOFWEEK(g.lpep_pickup_datetime) AS day_of_week,

        -- Calculate trip week of the year
        EXTRACT(WEEK FROM g.lpep_pickup_datetime) AS week_of_year,

        -- Calculate trip month of the year
        EXTRACT(MONTH FROM g.lpep_pickup_datetime) AS month_of_year

    FROM green_tripdata g
)

SELECT * FROM green_tripdata_transformed
