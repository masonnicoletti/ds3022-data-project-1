from re import T
import duckdb
import os
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='../logs/clean.log',
    filemode='w'
)
logger = logging.getLogger(__name__)

# Function for data cleaning
def clean_tripdata_tables():

    logger.info("-- Data Cleaning Started --")

    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='../emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        # Remove duplicate trips from yellow_tripdata
        con.execute("""
            CREATE TABLE yellow_tripdata_clean AS
            SELECT DISTINCT * FROM yellow_tripdata;

            DROP TABLE yellow_tripdata;
            ALTER TABLE yellow_tripdata_clean RENAME TO yellow_tripdata;
        """)
        logger.info("Removed duplicate trips from yellow_tripdata")

        # Remove duplicate trips from green_tripdata
        con.execute("""
            CREATE TABLE green_tripdata_clean AS
            SELECT DISTINCT * FROM green_tripdata;

            DROP TABLE green_tripdata;
            ALTER TABLE green_tripdata_clean RENAME TO green_tripdata;
        """)
        logger.info("Removed duplicate trips from green_tripdata")

        # Remove trips with 0 passengers from yellow_tripdata
        con.execute("""
            DELETE FROM yellow_tripdata
            WHERE passenger_count = 0
            OR passenger_count IS NULL;
        """)
        logger.info("Removed trips with 0 passengers from yellow_tripdata")

        # Remove trips with 0 passengers from green_tripdata
        con.execute("""
            DELETE FROM green_tripdata
            WHERE passenger_count = 0 
            OR passenger_count IS NULL;
        """)
        logger.info("Removed trips with 0 passengers from green_tripdata")

        # Remove trips with 0 miles from yellow_tripdata
        con.execute("""
            DELETE FROM yellow_tripdata
            WHERE trip_distance = 0;
        """)
        logger.info("Removed trips with 0 miles from yellow_tripdata")

        # Remove trips with 0 miles from green_tripdata
        con.execute("""
            DELETE FROM green_tripdata
            WHERE trip_distance = 0;
        """)
        logger.info("Removed trips with 0 miles from green_tripdata")

        # Remove trips greater than 100 miles from yellow_tripdata
        con.execute("""
            DELETE FROM yellow_tripdata
            WHERE trip_distance > 100;
        """)
        logger.info("Removed trips greater than 100 miles from yellow_tripdata")

        # Remove trips greater than 100 miles from green_tripdata
        con.execute("""
            DELETE FROM green_tripdata
            WHERE trip_distance > 100;
        """)
        logger.info("Removed trips greater than 100 miles from green_tripdata")

        # Remove trips longer than 24 hours from yellow_tripdata
        con.execute("""
            DELETE FROM yellow_tripdata
            WHERE (tpep_dropoff_datetime - tpep_pickup_datetime) > INTERVAL '24 hours';
        """)
        logger.info("Removed trips longer than 24 hours from yellow_tripdata")

        # Remove trips longer than 24 hours from green_tripdata
        con.execute("""
            DELETE FROM green_tripdata
            WHERE (lpep_dropoff_datetime - lpep_pickup_datetime) > INTERVAL '24 hours';
        """)
        logger.info("Removed trips longer than 24 hours from green_tripdata")

        # Remove trips a negative or zero duration from yellow_tripdata
        con.execute("""
            DELETE FROM yellow_tripdata
            WHERE EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) <= 0
        """)
        logger.info("Removed trips with no duration from yellow_tripdata")

        # Remove trips a negative or zero duration from green_tripdata
        con.execute("""
            DELETE FROM green_tripdata
            WHERE EXTRACT(EPOCH FROM (lpep_dropoff_datetime - lpep_pickup_datetime)) <= 0
        """)
        logger.info("Removed trips with no duration from green_tripdata")

        # Close DuckDB connection
        con.close()
        logger.info("Closed DuckDB connection")

        logger.info("-- Data Cleaning Complete --")
    
    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")


# Function to verify data cleaning results
def tripdata_cleaning_tests():
    
    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='../emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        # Verify no duplicate trips in yellow_tripdata
        yellow_duplicates = con.execute(f"""
            SELECT COUNT(*) - COUNT(DISTINCT ROW(
                VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, 
                RatecodeID, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, 
                tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Airport_fee
                )) AS duplicates
            FROM yellow_tripdata;
        """).fetchone()
        print(f"Number of duplicates in yellow_tripdata: {yellow_duplicates[0]}")
        logger.info(f"Number of duplicates in yellow_tripdata: {yellow_duplicates[0]}")

        # Verify no duplicate trips in green_tripdata
        green_duplicates = con.execute(f"""
            SELECT COUNT(*) - COUNT(DISTINCT ROW(
                VendorID, lpep_pickup_datetime, lpep_dropoff_datetime, store_and_fwd_flag,
                RatecodeID, PULocationID, DOLocationID, passenger_count, trip_distance, fare_amount, extra, mta_tax, tip_amount, 
                tolls_amount, ehail_fee, improvement_surcharge, total_amount, payment_type, trip_type, congestion_surcharge
                )) AS duplicates
            FROM green_tripdata;
        """).fetchone()
        print(f"Number of duplicates in green_tripdata: {green_duplicates[0]}")
        logger.info(f"Number of duplicates in green_tripdata: {green_duplicates[0]}")

        # Verify no trips with 0 passengers in yellow_tripdata
        yellow_zero_passengers = con.execute(f"""
            SELECT COUNT(*) FROM yellow_tripdata
            WHERE passenger_count = 0
            OR passenger_count IS NULL;
        """).fetchone()
        print(f"Number of trips with 0 passengers in yellow_tripdata: {yellow_zero_passengers[0]}")
        logger.info(f"Number of trips with 0 passengers in yellow_tripdata: {yellow_zero_passengers[0]}")

         # Verify no trips with 0 passengers in green_tripdata
        green_zero_passengers = con.execute(f"""
            SELECT COUNT(*) FROM green_tripdata
            WHERE passenger_count = 0
            OR passenger_count IS NULL;
        """).fetchone()
        print(f"Number of trips with 0 passengers in green_tripdata: {green_zero_passengers[0]}")
        logger.info(f"Number of trips with 0 passengers in green_tripdata: {green_zero_passengers[0]}")

        # Verify no trips with 0 miles in yellow_tripdata
        yellow_zero_miles = con.execute(f"""
            SELECT COUNT(*) FROM yellow_tripdata
            WHERE trip_distance = 0;
        """).fetchone()
        print(f"Number of trips with zero miles in yellow_tripdata: {yellow_zero_miles[0]}")
        logger.info(f"Number of trips with zero miles in yellow_tripdata: {yellow_zero_miles[0]}")

        # Verify no trips with 0 miles in green_tripdata
        green_zero_miles = con.execute(f"""
            SELECT COUNT(*) FROM green_tripdata
            WHERE trip_distance = 0;
        """).fetchone()
        print(f"Number of trips with zero miles in green_tripdata: {green_zero_miles[0]}")
        logger.info(f"Number of trips with zero miles in green_tripdata: {green_zero_miles[0]}")

        # Verify no trips longer than 100 miles in yellow_tripdata
        yellow_hundred_miles = con.execute(f"""
            SELECT COUNT(*) FROM yellow_tripdata
            WHERE trip_distance > 100;
        """).fetchone()
        print(f"Number of trips longer than 100 miles in yellow_tripdata: {yellow_hundred_miles[0]}")
        logger.info(f"Number of trips longer than 100 miles in yellow_tripdata: {yellow_hundred_miles[0]}")

        # Verify no trips longer than 100 miles in green_tripdata
        green_hundred_miles = con.execute(f"""
            SELECT COUNT(*) FROM green_tripdata
            WHERE trip_distance > 100;
        """).fetchone()
        print(f"Number of trips longer than 100 miles in green_tripdata: {green_hundred_miles[0]}")
        logger.info(f"Number of trips longer than 100 miles in green_tripdata: {green_hundred_miles[0]}")

        # Verify no trips longer than 24 hours in yellow_tripdata
        yellow_long_trips = con.execute(f"""
            SELECT COUNT(*) FROM yellow_tripdata
            WHERE (tpep_dropoff_datetime - tpep_pickup_datetime) > INTERVAL '24 hours'; 
        """).fetchone()
        print(f"Number of trips longer than 24 hours in yellow_tripdata: {yellow_long_trips[0]}")
        logger.info(f"Number of trips longer than 24 hours in yellow_tripdata: {yellow_long_trips[0]}")

        # Verify no trips longer than 24 hours in green_tripdata
        green_long_trips = con.execute(f"""
            SELECT COUNT(*) FROM green_tripdata
            WHERE (lpep_dropoff_datetime - lpep_pickup_datetime) > INTERVAL '24 hours'; 
        """).fetchone() 
        print(f"Number of trips longer than 24 hours in green_tripdata: {green_long_trips[0]}")
        logger.info(f"Number of trips longer than 24 hours in green_tripdata: {green_long_trips[0]}")

        # Close DuckDB connection
        con.close()
        logger.info("Closed DuckDB connection")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")
    
    logger.info("-- Data Cleaning Verification Complete --")


if __name__ == "__main__":
    clean_tripdata_tables()
    tripdata_cleaning_tests()
