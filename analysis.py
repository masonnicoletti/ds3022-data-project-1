import duckdb
import logging
import matplotlib.pyplot as plt

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='logs/analysis.log',
    filemode='w'
)
logger = logging.getLogger(__name__)

def taxi_analysis():

    logger.info("-- Data Analysis Started --")

    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        # Largest carbon producing yellow trip

        # Largest carbon producing green trip

        # Carbon heavy and carbon light hours of the day for yellow trips

        # Carbon heavy and carbon light hours of the day for green trips

        # Carbon heavy and carbon light days of the week for yellow trips

        # Carbon heavy and carbon light days of the week for green trips

        # Carbon heavy and carbon light weeks of the year for yellow trips

        # Carbon heavy and carbon light weeks of the year for green trips

        # Carbon heavy and carbon light months of the year for yellow trips

        # Carbon heavy and carbon light months of the year for yellow trips




        # Close DuckDB connection
        con.close()
        logger.info("Closed DuckDB connection")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

    logger.info("-- Data Analysis Complete --")

if __name__ == "__main__":
    taxi_analysis()