-- Transform yellow_tripdata table with dbt

{{ config(materialized='table', alias='yellow_tripdata') }}

WITH yellow_tripdata AS (
    SELECT * FROM {{ source('raw', 'yellow_tripdata') }}
),

yellow_tripdata_transformed AS (
    SELECT 
        y.*,

        -- Calculate CO2 (kg) per trip for yellow taxi
        (y.trip_distance * (
            SELECT co2_grams_per_mile FROM {{ source('raw', 'vehicle_emissions') }}
            WHERE vehicle_type = 'yellow_taxi'
        )) / 1000.0 AS trip_co2_kgs,
        
        -- Calculate average mph per trip
        y.trip_distance / (EXTRACT(EPOCH FROM (y.tpep_dropoff_datetime - y.tpep_pickup_datetime)) / 3600.0) AS avg_mph,

        -- Calculate trip hour
        EXTRACT(HOUR FROM y.tpep_pickup_datetime) AS hour_of_day,

        -- Calculate trip day of the week
        DAYOFWEEK(y.tpep_pickup_datetime) AS day_of_week,

        -- Calculate trip week of the year
        EXTRACT(WEEK FROM y.tpep_pickup_datetime) AS week_of_year,

        -- Calculate trip month of the year
        EXTRACT(MONTH FROM y.tpep_pickup_datetime) AS month_of_year

    FROM yellow_tripdata y
)

SELECT * FROM yellow_tripdata_transformed
