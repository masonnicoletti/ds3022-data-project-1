import duckdb
import os
import logging
import time

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='logs/load.log'
)
logger = logging.getLogger(__name__)

month_list = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]

def load_parquet_files():

    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        # Drop tables if preexisting
        con.execute(f"""
            -- SQL goes here
            DROP TABLE IF EXISTS yellow_tripdata;
            DROP TABLE IF EXISTS green_tripdata;
            DROP TABLE IF EXISTS vehicle_emissions;
        """)
        logger.info("Dropped redundant tables")

        # Create empty yellow_tripdata and green_tripdata tables
        con.execute(f"""
            CREATE TABLE yellow_tripdata AS
            SELECT * FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet') LIMIT 0;

            CREATE TABLE green_tripdata AS
            SELECT * FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-01.parquet') LIMIT 0;
        """)
        logger.info("Created empty tripdata tables")

        # Initialize for loop to iterate through taxi data
        for month in month_list:
            yellow_tripdata_file = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-{month}.parquet"
            green_tripdata_file = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-{month}.parquet"
            
            # Append taxi data to yellow_tripdata table
            con.execute(f"""
                INSERT INTO yellow_tripdata
                SELECT * FROM read_parquet('{yellow_tripdata_file}');
            """)
            logger.info(f"Imported yellow_tripdata_2024-{month} data to db")
            
            # Append taxi data to green_tripdata table
            con.execute(f"""
                INSERT INTO green_tripdata
                SELECT * FROM read_parquet('{green_tripdata_file}');
            """)
            logger.info(f"Imported green_tripdata_2024-{month} data to db") 
            
            # Limit file loading rate
            time.sleep(60)

        # Add data to vehicle_emissions table
        con.execute(f"""
            CREATE TABLE vehicle_emissions AS
            SELECT * FROM read_csv('./data/vehicle_emissions.csv')
        """)
        logger.info("Added vehicle_emissions data to db")

        # Save yellow_tripdata to repo as a parquet file
        con.execute(f"""
            COPY yellow_tripdata TO 'data/yellow_tripdata.parquet' (FORMAT PARQUET);
        """)
        logger.info("Saved yellow_tripdata parquet file")

        # Save green_tripdata to repo as a parquet file
        con.execute(f"""
            COPY green_tripdata TO 'data/green_tripdata.parquet' (FORMAT PARQUET);
        """)
        logger.info("Saved green_tripdata parquet file")

        # Output basic data statistics for yellow_tripdata
        yellow_tripdata_stats = con.execute("""
            SELECT 
                COUNT(*) AS row_count,
                AVG(trip_distance) AS average_distance,
                AVG(fare_amount) AS average_fare
            FROM yellow_tripdata;
        """).fetchdf()
        print("Yellow Trip Data Stats:\n", yellow_tripdata_stats)
        logger.info(f"Calculated basic statistics for yellow_tripdata:\n{yellow_tripdata_stats}")

        # Output basic data statistics for green_tripdata
        green_tripdata_stats = con.execute("""
            SELECT 
                COUNT(*) AS row_count,
                AVG(trip_distance) AS average_distance,
                AVG(fare_amount) AS average_fare
            FROM green_tripdata;
        """).fetchdf()
        print("Green Trip Data Stats:\n", green_tripdata_stats)
        logger.info(f"Calculated basic statistics for green_tripdata:\n{green_tripdata_stats}")

        # Output basic data statistics fro vehicle_emissions
        vehicle_emissions_stats = con.execute("""
            SELECT
                COUNT(*) AS row_count,
                MIN(co2_grams_per_mile) AS min_co2_grams_per_mile,
                MAX(co2_grams_per_mile) AS max_co2_grams_per_mile
            FROM vehicle_emissions;
        """).fetchdf()
        print("Vehicle Emissions Data Stats:\n", vehicle_emissions_stats)
        logger.info(f"Calculated basic statistics for vehicle_emissions:\n{vehicle_emissions_stats}")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

    # Close DuckDB connection
    con.close()
    logger.info("Data Loading Complete")

if __name__ == "__main__":
    load_parquet_files()