# Databricks notebook source
# MAGIC %md
# MAGIC # 📖 DLT Streaming Table Reader
# MAGIC
# MAGIC Simple script to read the 50 streaming tables using DLT create_streaming_table syntax.

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# Configuration
config = {
    "catalog_name": "soni",
    "database_name": "default",
    "table_prefix": "stream_table",
    "baseline_streams": 50,  # Testing with 4 tables
    "max_files_per_trigger": 1000
}

# COMMAND ----------

def create_dlt_streaming_table(table_name):
    source_table = f"{config['catalog_name']}.{config['database_name']}.{table_name}"

    @dlt.table(
        name=f"dlt_table_decorator_{table_name}",
        comment=f"DLT streaming table reading from {source_table}",
    )
    def read_streaming_table():
        return (spark.readStream
                .option("maxFilesPerTrigger", config["max_files_per_trigger"])
                .table(source_table)
                .withColumn("processing_timestamp", current_timestamp())
                .withColumn("_metadata", col("_metadata")))

# COMMAND ----------

# Create all streaming tables
for i in range(1, config['baseline_streams'] + 1):
    table_name = f"{config['table_prefix']}_{i:03d}"
    create_dlt_streaming_table(table_name)