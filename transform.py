import duckdb
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='logs/transform.log',
    filemode='w'
)
logger = logging.getLogger(__name__)

def tripdata_transformations():
    
    logger.info("-- Data Transformation Started --")

    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        # Calculate CO2 (kg) per trip for yellow_tripdata
        con.execute("""
            ALTER TABLE yellow_tripdata 
            DROP COLUMN IF EXISTS trip_co2_kgs;
            
            ALTER TABLE yellow_tripdata
            ADD COLUMN trip_co2_kgs DOUBLE;
            
            UPDATE yellow_tripdata
            SET trip_co2_kgs = (trip_distance * (
                SELECT co2_grams_per_mile FROM vehicle_emissions
                WHERE vehicle_type = 'yellow_taxi')
            ) / 1000.0;
        """)
        logger.info("Calculated CO2 (kg) column for yellow_tripdata")

        # Calculate CO2 (kg) per trip for green_tripdata
        con.execute("""
            ALTER TABLE green_tripdata 
            DROP COLUMN IF EXISTS trip_co2_kgs;

            ALTER TABLE green_tripdata
            ADD COLUMN trip_co2_kgs DOUBLE;
            
            UPDATE green_tripdata
            SET trip_co2_kgs = (trip_distance * (
                SELECT co2_grams_per_mile FROM vehicle_emissions
                WHERE vehicle_type = 'green_taxi')
            ) / 1000.0;
        """)
        logger.info("Calculated CO2 (kg) column for green_tripdata")

        # Calculate average mph per trip for yellow_tripdata
        con.execute("""
            ALTER TABLE yellow_tripdata 
            DROP COLUMN IF EXISTS avg_mph;
            
            ALTER TABLE yellow_tripdata
            ADD COLUMN avg_mph DOUBLE;

            UPDATE yellow_tripdata
            SET avg_mph = trip_distance / (EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 3600.0);
        """)
        logger.info("Calculated average mph column for yellow_tripdata")

        # Calculate average mph per trip for green_tripdata
        con.execute("""
            ALTER TABLE green_tripdata 
            DROP COLUMN IF EXISTS avg_mph;
            
            ALTER TABLE green_tripdata
            ADD COLUMN avg_mph DOUBLE;

            UPDATE green_tripdata
            SET avg_mph = trip_distance / (EXTRACT(EPOCH FROM (lpep_dropoff_datetime - lpep_pickup_datetime)) / 3600.0);
        """)
        logger.info("Calculated average mph column for green_tripdata")

        # Calculate trip hour for yellow_tripdata
        con.execute("""
            ALTER TABLE yellow_tripdata
            DROP COLUMN IF EXISTS hour_of_day;

            ALTER TABLE yellow_tripdata
            ADD COLUMN hour_of_day INTEGER;

            UPDATE yellow_tripdata
            SET hour_of_day = EXTRACT(HOUR FROM tpep_pickup_datetime);
        """)
        logger.info("Calculated hour_of_day column for yellow_tripdata")

        # Calculate trip hour for green_tripdata
        con.execute("""
            ALTER TABLE green_tripdata
            DROP COLUMN IF EXISTS hour_of_day;

            ALTER TABLE green_tripdata
            ADD COLUMN hour_of_day INTEGER;

            UPDATE green_tripdata
            SET hour_of_day = EXTRACT(HOUR FROM lpep_pickup_datetime);
        """)
        logger.info("Calculated hour_of_day column for green_tripdata")

        # Calculate trip day of the week for yellow_tripdata
        con.execute("""
            ALTER TABLE yellow_tripdata
            DROP COLUMN IF EXISTS day_of_week;

            ALTER TABLE yellow_tripdata
            ADD COLUMN day_of_week INTEGER;

            UPDATE yellow_tripdata
            SET day_of_week = DAYOFWEEK(tpep_pickup_datetime);
        """)
        logger.info("Calculated day_of_week column for yellow_tripdata")

        # Calculate trip day of the week for green_tripdata
        con.execute("""
            ALTER TABLE green_tripdata
            DROP COLUMN IF EXISTS day_of_week;

            ALTER TABLE green_tripdata
            ADD COLUMN day_of_week INTEGER;

            UPDATE green_tripdata
            SET day_of_week = DAYOFWEEK(lpep_pickup_datetime);
        """)
        logger.info("Calculated day_of_week column for green_tripdata")

        # Calculate week number for yellow_tripdata
        con.execute("""
            ALTER TABLE yellow_tripdata
            DROP COLUMN IF EXISTS week_of_year;

            ALTER TABLE yellow_tripdata
            ADD COLUMN week_of_year INTEGER;

            UPDATE yellow_tripdata
            SET week_of_year = EXTRACT(WEEK FROM tpep_pickup_datetime);
        """)
        logger.info("Calculated week_of_year column for yellow_tripdata")

        # Calculate week number for green_tripdata
        con.execute("""
            ALTER TABLE green_tripdata
            DROP COLUMN IF EXISTS week_of_year;

            ALTER TABLE green_tripdata
            ADD COLUMN week_of_year INTEGER;

            UPDATE green_tripdata
            SET week_of_year = EXTRACT(WEEK FROM lpep_pickup_datetime);
        """)
        logger.info("Calculated week_of_year column for green_tripdata")

        # Calculate month for yellow_tripdata
        con.execute("""
            ALTER TABLE yellow_tripdata
            DROP COLUMN IF EXISTS month_of_year;
            
            ALTER TABLE yellow_tripdata
            ADD COLUMN month_of_year INTEGER;
            
            UPDATE yellow_tripdata
            SET month_of_year = EXTRACT(MONTH FROM tpep_pickup_datetime);
        """)
        logger.info("Calculated month_of_year column for yellow_tripdata")
        
        # Calculate month for green_tripdata
        con.execute("""
            ALTER TABLE green_tripdata
            DROP COLUMN IF EXISTS month_of_year;
            
            ALTER TABLE green_tripdata
            ADD COLUMN month_of_year INTEGER;
            
            UPDATE green_tripdata
            SET month_of_year = EXTRACT(MONTH FROM lpep_pickup_datetime);
        """)
        logger.info("Calculated month_of_year column for green_tripdata")
    
        # Close DuckDB connection
        con.close()
        logger.info("Closed DuckDB connection")

        logger.info("-- Data Transformations Complete --")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")


if __name__ == "__main__":
    tripdata_transformations()
