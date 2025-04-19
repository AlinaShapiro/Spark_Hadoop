# coding: utf-8
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, to_date, month, year, avg
import time
import logging
import os
import psutil
import matplotlib.pyplot as plt
import seaborn as sns
import argparse


def create_histogram(data, title, xlabel, ylabel, filename):
    plt.figure(figsize=(10, 6))
    sns.histplot(data, bins=30, kde=True, edgecolor='black', alpha=0.7)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid(True)
    filepath = os.path.join(OUTPUT_DIR, filename)
    plt.savefig(filepath)
    logger.info(f"Histogram '{filename}' saved to '{OUTPUT_DIR}'")
    plt.close()

def create_bar_plot(pandas_df, x_col, y_col, title, filename):
    plt.figure(figsize=(12, 6))
    sns.barplot(x=x_col, y=y_col, data=pandas_df)
    plt.title(title)
    plt.xlabel(x_col)
    plt.ylabel(y_col)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    filepath = os.path.join(OUTPUT_DIR, filename)
    plt.savefig(filepath)
    logger.info(f"Plot '{filename}' saved to '{OUTPUT_DIR}'")
    plt.close()

def get_ram_usage_mb():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)

if __name__ == "__main__":

    parser = argparse.ArgumentParser( description='SparkRetailAnalysis')
    parser.add_argument('-o', '--optimized', action='store_true')
    args = parser.parse_args()

    WITH_OPT=True if args.optimized else False
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    logging.getLogger("py4j").setLevel(logging.ERROR)

    HDFS_PATH = "hdfs://namenode:9000/data/retail_data/online_retail_II.csv" #Path to the dataset
    APP_NAME = "SparkRetailAnalysis"
    OUTPUT_DIR = "plots"
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Initializing Spark Session
    spark = SparkSession.builder.appName(APP_NAME).enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    # Performance metrics
    time_measurements = []
    ram_measurements = []

    # Measure overall start time
    app_start_time = time.time()
    logger.info(f"Starting Spark Application: {APP_NAME}")

    retail_df = None
    monthly_avg_spending_df = None

    try:
        # Reading data from HDFS 
        read_start_time = time.time()
        logger.info(f"Stage 1: Reading data from HDFS: {HDFS_PATH}")
        retail_df = spark.read.csv(HDFS_PATH, header=True, inferSchema=True)
        row_count = retail_df.count()
        read_duration = time.time() - read_start_time
        logger.info(f"Stage 1: Read {row_count} rows in {read_duration:.2f} seconds.")
        time_measurements.append(('Read HDFS', read_duration))
        ram_measurements.append(('Read HDFS', get_ram_usage_mb()))
        retail_df.printSchema()
        if WITH_OPT:
            retail_df.cache() # Caching the initial DataFrame 
            logger.info("Cached the initial DataFrame.")


        # Filtering the data
        clean_start_time = time.time()
        logger.info("Stage 2: Filtering the data...")
        cleaned_df = retail_df.filter((col("Quantity") > 0) & (col("Price") > 0))
        cleaned_df = cleaned_df.withColumn("InvoiceDate", to_date(col("InvoiceDate")))
        cleaned_df = cleaned_df.withColumn("Year", year(col("InvoiceDate")))
        cleaned_df = cleaned_df.withColumn("Month", month(col("InvoiceDate")))
        cleaned_df = cleaned_df.withColumn("TotalSpending", col("Quantity") * col("Price"))

        if WITH_OPT:
            cleaned_df.cache() # Caching the filtered DataFrame
            logger.info("Cached the cleaned DataFrame.")
        clean_duration = time.time() - clean_start_time
        logger.info(f"Stage 2: Data Filtering completed in {clean_duration:.2f} seconds.")
        time_measurements.append(('Data Filtering ', clean_duration))
        ram_measurements.append(('Data Filtering ', get_ram_usage_mb()))

        # Repetitive Aggregation
        logger.info("Stage 3: Performing repetitive aggregation of data...")
        num_repetitions = 3
        for i in range(num_repetitions):
            task_start_time = time.time()
            current_ram = get_ram_usage_mb()

            # Perform a Spark operation (e.g., a simple aggregation)
            country_counts_df = cleaned_df.groupBy("Country").agg(count("*"))
            country_counts_df.explain() # Log execution plan
            country_counts_df.collect() # Trigger evaluation

            task_duration = time.time() - task_start_time
            logger.info(f"Stage 3 - Iteration {i+1}: Aggregated country counts in {task_duration:.2f} seconds, RAM: {current_ram:.2f} MB")
            time_measurements.append((f'Aggregate Country Counts - Iteration {i+1}', task_duration))
            ram_measurements.append((f'Aggregate Country Counts - Iteration {i+1}', current_ram))

        # Calculating monthly average spending
        avg_start_time = time.time()
        logger.info("Stage 4: Calculating monthly average spending per country...")
        if WITH_OPT:
            monthly_avg_spending_df = cleaned_df.groupBy("Country", "Year", "Month") \
            .agg(avg("TotalSpending").alias("AverageMonthlySpending")) \
            .orderBy("Country", "Year", "Month").persist() # Using persist for potential further use
        else:
            monthly_avg_spending_df = cleaned_df.groupBy("Country", "Year", "Month") \
                .agg(avg("TotalSpending").alias("AverageMonthlySpending")) \
                .orderBy("Country", "Year", "Month")
        monthly_avg_spending_df.explain() # Log execution plan
        monthly_avg_spending_df.count() # Trigger evaluation
        avg_duration = time.time() - avg_start_time
        logger.info(f"Stage 4: Calculated monthly average spending in {avg_duration:.2f} seconds.")
        monthly_avg_spending_df.show(truncate=False)
        time_measurements.append(('Calculate Avg Spending', avg_duration))
        ram_measurements.append(('Calculate Avg Spending', get_ram_usage_mb()))

        plot_start_time = time.time()
        logger.info("Stage 5: Generating plots...")
        country_col_name = next((c for c in retail_df.columns if c.lower().strip() == 'country'), None)
        if country_col_name and retail_df is not None:
            country_counts_df = retail_df.groupBy(col(country_col_name)).agg(count("*").alias("transaction_count")).orderBy(col("transaction_count").desc()).limit(20)
            country_counts_pd = country_counts_df.toPandas()
            create_bar_plot(country_counts_pd, country_col_name, 'transaction_count', 'Top 20 Countries by Transaction Count', 'transactions_by_country.png')
           
        # Analyzing quantity distribution (Top 20)
        if retail_df is not None:
            quantity_counts_df = retail_df.groupBy("Quantity").agg(count("*").alias("count")).orderBy(col("count").desc()).limit(20)
            quantity_counts_pd = quantity_counts_df.toPandas()
            create_bar_plot(quantity_counts_pd, 'Quantity', 'count', 'Quantity Distribution (Top 20)', 'quantity_distribution.png')

        # Analyze and plot monthly average spending in the United Kingdom
        if monthly_avg_spending_df is not None:
            uk_monthly_spending_df = monthly_avg_spending_df.filter(col("Country") == "United Kingdom").toPandas()
            uk_monthly_spending_df['MonthYear'] = uk_monthly_spending_df['Year'].astype(str) + '-' + uk_monthly_spending_df['Month'].astype(str).str.zfill(2)
            create_bar_plot(uk_monthly_spending_df, 'MonthYear', 'AverageMonthlySpending', 'Monthly Average Spending in United Kingdom', 'uk_monthly_spending.png')

        plot_duration = time.time() - plot_start_time
        logger.info(f"Stage 5: Plot generation completed in {plot_duration:.2f} seconds.")
        time_measurements.append(('Plot Generation', plot_duration))
        ram_measurements.append(('Plot Generation', get_ram_usage_mb()))

        # Application end time
        app_end_time = time.time()
        total_execution_time = app_end_time - app_start_time
        logger.info(f"Spark Application total execution time: {total_execution_time:.2f} seconds")

        # Log final time and RAM measurements
        logger.info("--- Performance Measurements ---")
        for name, duration in time_measurements:
            logger.info(f"Time taken for '{name}': {duration:.2f} seconds")
        logger.info("--- RAM Usage Measurements (at the end of each stage) ---")
        total_ram = 0
        for i, (name, ram) in enumerate(ram_measurements):
            total_ram += ram
            logger.info(f"RAM usage after '{name}': {ram:.2f} MB")
        if ram_measurements:
            logger.info(f"Spark Application total RAM usage: {(total_ram / len(ram_measurements)):.2f} MB")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        spark.stop()
        logger.info("Spark Application finished.")
  