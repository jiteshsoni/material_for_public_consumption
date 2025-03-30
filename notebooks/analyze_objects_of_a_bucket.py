# Databricks notebook source
import inflect
import numpy as np

def analyze_and_display_parquet_stats(path, count_records=False):
    # Initialize inflect engine
    p = inflect.engine()
    
    # Read the Parquet file once
    parquet_df = spark.read.parquet(path)
    
    # Prepare a list to hold statistics
    stats_list = []
    
    if count_records:
        record_count = parquet_df.count()
        record_count_in_words = p.number_to_words(record_count)
        stats_list.append(("Record count", f"{record_count} ({record_count_in_words})"))
    
    # List the files in the specified path
    files = dbutils.fs.ls(path)
    
    # Calculate the total number of files
    total_files = len(files)
    stats_list.append(("Total files", total_files))
    
    # Calculate the total size in bytes and convert to megabytes (MB)
    total_size_bytes = sum(file.size for file in files)
    total_size_mb = float(total_size_bytes / (1024 ** 2))
    
    # Extract file sizes for statistics calculation
    file_sizes = [file.size for file in files]
    median_size_mb = float(np.median(file_sizes) / (1024 ** 2))
    p10_size_mb = float(np.percentile(file_sizes, 10) / (1024 ** 2))
    p90_size_mb = float(np.percentile(file_sizes, 90) / (1024 ** 2))
    p99_size_mb = float(np.percentile(file_sizes, 99) / (1024 ** 2))
    
    # Append the computed statistics to the stats list
    stats_list.extend([
        ("Total size (MB)", total_size_mb),
        ("Median file size (MB)", median_size_mb),
        ("P10 file size (MB)", p10_size_mb),
        ("P90 file size (MB)", p90_size_mb),
        ("P99 file size (MB)", p99_size_mb)
    ])
    
    # Create a Spark DataFrame from the statistics list
    stats_df = spark.createDataFrame(stats_list, schema=["Metric", "Value"])
    
    # Display the computed statistics and the original Parquet DataFrame
    display(stats_df)

# COMMAND ----------

analyze_and_display_parquet_stats(path="abfss://container@publicstorageforspark.dfs.core.windows.net/synthetic_orders_100mb/", count_records=True)

# COMMAND ----------

analyze_and_display_parquet_stats(path="abfss://container@publicstorageforspark.dfs.core.windows.net/synthetic_orders_10mb/", count_records=True)

# COMMAND ----------

display(dbutils.fs.ls(write_path))

# COMMAND ----------


