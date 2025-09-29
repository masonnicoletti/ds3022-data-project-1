import duckdb
import logging
import pandas as pd
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
        yellow_most_co2 = con.execute("""
            SELECT tpep_pickup_datetime, trip_co2_kgs FROM yellow_tripdata
            ORDER BY trip_co2_kgs DESC LIMIT 1;
        """).fetchone()
        print(f"Yellow Taxi - Largest carbon producing trip of the year: Date: {yellow_most_co2[0]}, CO2: {yellow_most_co2[1]} kg")
        logger.info(f"Yellow Taxi - Largest carbon producing trip of the year: Date: {yellow_most_co2[0]}, CO2: {yellow_most_co2[1]} kg")

        # Largest carbon producing green trip
        green_most_co2 = con.execute("""
            SELECT lpep_pickup_datetime, trip_co2_kgs FROM green_tripdata
            ORDER BY trip_co2_kgs DESC LIMIT 1;
        """).fetchone()
        print(f"Green Taxi - Largest carbon producing trip of the year: Date: {green_most_co2[0]}, CO2: {green_most_co2[1]} kg")
        logger.info(f"Green Taxi - Largest carbon producing trip of the year: Date: {green_most_co2[0]}, CO2: {green_most_co2[1]} kg")

        # Carbon heavy and carbon light hours of the day for yellow trips
        yellow_heavy_hours = con.execute("""
            SELECT hour_of_day, AVG(trip_co2_kgs) AS avg_co2_hour FROM yellow_tripdata
            GROUP BY hour_of_day
            ORDER BY avg_co2_hour DESC;
        """).df()
        print(f"Yellow Taxi - Hour with most carbon emissions: {int(yellow_heavy_hours.iloc[0]['hour_of_day']+1)}, CO2: {yellow_heavy_hours.iloc[0]['avg_co2_hour']:.2f} kg")
        logger.info(f"Yellow Taxi - Hour with most carbon emissions: {int(yellow_heavy_hours.iloc[0]['hour_of_day']+1)}, CO2: {yellow_heavy_hours.iloc[0]['avg_co2_hour']:.2f} kg")

        # Plot carbon per hour for the yellow taxi
        plt.bar(yellow_heavy_hours['hour_of_day']+1, height=yellow_heavy_hours['avg_co2_hour'], color='gold')
        plt.xlabel("Hour of Day")
        plt.ylabel("CO2 (kg)")
        plt.title("Average CO2 Emissions Per Hour (Yellow Taxi)")
        plt.xticks(range(1, 25))
        plt.savefig("plots/yellow_hours.png")
        plt.close()
        logger.info("Plotted yellow taxi carbon per hour")

        # Carbon heavy and carbon light hours of the day for green trips
        green_heavy_hours = con.execute("""
            SELECT hour_of_day, AVG(trip_co2_kgs) AS avg_co2_hour FROM green_tripdata
            GROUP BY hour_of_day
            ORDER BY avg_co2_hour DESC;
        """).df()
        print(f"Green Taxi - Hour with most carbon emissions: {int(green_heavy_hours.iloc[0]['hour_of_day']+1)}, CO2: {green_heavy_hours.iloc[0]['avg_co2_hour']:.2f} kg")
        logger.info(f"Green Taxi - Hour with most carbon emissions: {int(green_heavy_hours.iloc[0]['hour_of_day']+1)}, CO2: {green_heavy_hours.iloc[0]['avg_co2_hour']:.2f} kg")

        # Plot carbon per hour for the green taxi
        plt.bar(green_heavy_hours['hour_of_day']+1, height=green_heavy_hours['avg_co2_hour'], color='green')
        plt.xlabel("Hour of Day")
        plt.ylabel("CO2 (kg)")
        plt.title("Average CO2 Emissions Per Hour (Green Taxi)")
        plt.xticks(range(1, 25))
        plt.savefig("plots/green_hours.png")
        plt.close()
        logger.info("Plotted green taxi carbon per hour")

        # Carbon heavy and carbon light days of the week for yellow trips
        yellow_days = con.execute("""
            SELECT day_of_week, AVG(trip_co2_kgs) AS avg_co2_day FROM yellow_tripdata
            GROUP BY day_of_week
            ORDER BY avg_co2_day DESC;
        """).df()
        print(f"Yellow Taxi - Day with most carbon emissions: {int(yellow_days.iloc[0]['day_of_week']+1)}, CO2: {yellow_days.iloc[0]['avg_co2_day']:.2f} kg")
        logger.info(f"Yellow Taxi - Day with most carbon emissions: {int(yellow_days.iloc[0]['day_of_week']+1)}, CO2: {yellow_days.iloc[0]['avg_co2_day']:.2f} kg")

        # Plot carbon per dat for the yellow taxi
        plt.bar(yellow_days['day_of_week']+1, height=yellow_days['avg_co2_day'], color='gold')
        plt.xlabel("Day of Week")
        plt.ylabel("CO2 (kg)")
        plt.title("Average CO2 Emissions By Day (Yellow Taxi)")
        plt.savefig("plots/yellow_days.png")
        plt.close()
        logger.info("Plotted yellow taxi carbon by day")

        # Carbon heavy and carbon light days of the week for green trips
        green_days = con.execute("""
            SELECT day_of_week, AVG(trip_co2_kgs) AS avg_co2_day FROM green_tripdata
            GROUP BY day_of_week
            ORDER BY avg_co2_day DESC;
        """).df()
        print(f"Green Taxi - Day with most carbon emissions: {int(green_days.iloc[0]['day_of_week']+1)}, CO2: {green_days.iloc[0]['avg_co2_day']:.2f} kg")
        logger.info(f"Green Taxi - Day with most carbon emissions: {int(green_days.iloc[0]['day_of_week']+1)}, CO2: {green_days.iloc[0]['avg_co2_day']:.2f} kg")

        # Plot carbon per day for the green taxi
        plt.bar(green_days['day_of_week']+1, height=green_days['avg_co2_day'], color='green')
        plt.xlabel("Day of Week")
        plt.ylabel("CO2 (kg)")
        plt.title("Average CO2 Emissions By Day (Green Taxi)")
        plt.savefig("plots/green_days.png")
        plt.close()
        logger.info("Plotted green taxi carbon by day")

        # Carbon heavy and carbon light weeks of the year for yellow trips
        yellow_weeks = con.execute("""
            SELECT week_of_year, AVG(trip_co2_kgs) AS avg_co2_week FROM yellow_tripdata
            GROUP BY week_of_year
            ORDER BY avg_co2_week DESC;
        """).df()
        print(f"Yellow Taxi - Week with most carbon emissions: {int(yellow_weeks.iloc[0]['week_of_year']+1)}, CO2: {yellow_weeks.iloc[0]['avg_co2_week']:.2f} kg")
        logger.info(f"Yellow Taxi - Week with most carbon emissions: {int(yellow_weeks.iloc[0]['week_of_year']+1)}, CO2: {yellow_weeks.iloc[0]['avg_co2_week']:.2f} kg")

        # Plot carbon per week for the yellow taxi
        plt.bar(yellow_weeks['week_of_year']+1, height=yellow_weeks['avg_co2_week'], color='gold')
        plt.xlabel("Week of Year")
        plt.ylabel("CO2 (kg)")
        plt.title("Average CO2 Emissions Per Week (Yellow Taxi)")
        plt.savefig("plots/yellow_weeks.png")
        plt.close()
        logger.info("Plotted yellow taxi carbon by week")

        # Carbon heavy and carbon light weeks of the year for green trips
        green_weeks = con.execute("""
            SELECT week_of_year, AVG(trip_co2_kgs) AS avg_co2_week FROM green_tripdata
            GROUP BY week_of_year
            ORDER BY avg_co2_week DESC;
        """).df()
        print(f"Green Taxi - Week with most carbon emissions: {int(green_weeks.iloc[0]['week_of_year']+1)}, CO2: {green_weeks.iloc[0]['avg_co2_week']:.2f} kg")
        logger.info(f"Green Taxi - Week with most carbon emissions: {int(green_weeks.iloc[0]['week_of_year']+1)}, CO2: {green_weeks.iloc[0]['avg_co2_week']:.2f} kg")

        # Plot carbon per week for the green taxi
        plt.bar(green_weeks['week_of_year']+1, height=green_weeks['avg_co2_week'], color='green')
        plt.xlabel("Week of Year")
        plt.ylabel("CO2 (kg)")
        plt.title("Average CO2 Emissions Per Week (Green Taxi)")
        plt.savefig("plots/green_weeks.png")
        plt.close()
        logger.info("Plotted green taxi carbon by week")

        # Carbon heavy and carbon light months of the year for yellow trips
        yellow_months = con.execute("""
            SELECT month_of_year, AVG(trip_co2_kgs) AS avg_co2_month FROM yellow_tripdata
            GROUP BY month_of_year
            ORDER BY avg_co2_month DESC;
        """).df()
        print(f"Yellow Taxi - Month with most carbon emissions: {int(yellow_months.iloc[0]['month_of_year']+1)}, CO2: {yellow_months.iloc[0]['avg_co2_month']:.2f} kg")
        logger.info(f"Yellow Taxi - Month with most carbon emissions: {int(yellow_months.iloc[0]['month_of_year']+1)}, CO2: {yellow_months.iloc[0]['avg_co2_month']:.2f} kg")

        # Plot carbon per month for the yellow taxi
        plt.bar(yellow_months['month_of_year']+1, height=yellow_months['avg_co2_month'], color='gold')
        plt.xlabel("Month of Year")
        plt.ylabel("CO2 (kg)")
        plt.title("Average CO2 Emissions Per Month (Yellow Taxi)")
        plt.savefig("plots/yellow_months.png")
        plt.close()
        logger.info("Plotted yellow taxi carbon by month")

        # Carbon heavy and carbon light months of the year for green trips
        green_months = con.execute("""
            SELECT month_of_year, AVG(trip_co2_kgs) AS avg_co2_month FROM green_tripdata
            GROUP BY month_of_year
            ORDER BY avg_co2_month DESC;
        """).df()
        print(f"Green Taxi - Month with most carbon emissions: {int(green_months.iloc[0]['month_of_year']+1)}, CO2: {green_months.iloc[0]['avg_co2_month']:.2f} kg")
        logger.info(f"Green Taxi - Month with most carbon emissions: {int(green_months.iloc[0]['month_of_year']+1)}, CO2: {green_months.iloc[0]['avg_co2_month']:.2f} kg")

        # Plot carbon per month for the green taxi
        plt.bar(green_months['month_of_year']+1, height=green_months['avg_co2_month'], color='green')
        plt.xlabel("Month of Year")
        plt.ylabel("CO2 (kg)")
        plt.title("Average CO2 Emissions Per Month (Green Taxi)")
        plt.savefig("plots/green_months.png")
        plt.close()
        logger.info("Plotted green taxi carbon by month")

        # Extract monthly total for yellow and green data sets
        yellow_monthly_totals = con.execute("""
            SELECT month_of_year, SUM(trip_co2_kgs) AS total_co2 FROM yellow_tripdata
            GROUP BY month_of_year
            ORDER BY month_of_year;
        """).df()
        green_monthly_totals = con.execute("""
            SELECT month_of_year, SUM(trip_co2_kgs) AS total_co2 FROM green_tripdata
            GROUP BY month_of_year
            ORDER BY month_of_year;
        """).df()
        logger.info("Extracted total monthly carbon emissions")

        # Plot time series of carbon emissions per month
        fig, ax1 = plt.subplots(figsize=(12, 8))
        ax1.set_xlabel("Month")
        ax1.set_ylabel("Yellow Taxi CO2 Emissions (kg)")
        line1 = ax1.plot(yellow_monthly_totals['month_of_year'], yellow_monthly_totals['total_co2'], 
                        label="Yellow Taxi", color='gold', marker='o', linewidth=2)
        ax1.set_ylim(bottom=0, top=5000000)
        ax2 = ax1.twinx()
        ax2.set_ylabel("Green Taxi CO2 Emissions (kg)")
        line2 = ax2.plot(green_monthly_totals['month_of_year'], green_monthly_totals['total_co2'], 
                        label="Green Taxi", color='green', marker='o', linewidth=2)
        ax2.set_ylim(bottom=0, top=65000)
        
        plt.title("Total Monthly CO2 Emissions")
        month_list = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        plt.xticks(range(1,13), month_list, rotation=45)
        lines = line1 + line2
        labels = [l.get_label() for l in lines]
        ax1.legend(lines, labels, loc='upper left')
        
        plt.savefig("plots/total_monthly_emissions.png")
        plt.close()
        logger.info("Plotted total monthly carbon emissions")

        # Close DuckDB connection
        con.close()
        logger.info("Closed DuckDB connection")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

    logger.info("-- Data Analysis Complete --")

if __name__ == "__main__":
    taxi_analysis()