{% macro add_co2_column() %}
    {% set sql %}
        -- Add CO2 column if it doesn't exist
        ALTER TABLE yellow_tripdata ADD COLUMN IF NOT EXISTS trip_co2_kgs DOUBLE;
        
        -- Update the CO2 values (fixed syntax - no qualified column names)
        UPDATE yellow_tripdata 
        SET trip_co2_kgs = (trip_distance * v.co2_grams_per_mile) / 1000.0
        FROM vehicle_emissions v
        WHERE v.vehicle_type = 'yellow_taxi';
    {% endset %}
    
    {% do run_query(sql) %}
{% endmacro %}
