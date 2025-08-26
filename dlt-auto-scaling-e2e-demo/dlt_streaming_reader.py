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
    "baseline_streams": 4,  # Testing with 4 tables
    "max_files_per_trigger": 100
}

# COMMAND ----------

def create_dlt_streaming_table(table_name):
    source_table = f"{config['catalog_name']}.{config['database_name']}.{table_name}"
    
    dlt.create_streaming_table(
        name=f"dlt_{table_name}",
        comment=f"DLT streaming table reading from {source_table}",
        cluster_by_auto=True
    )
    @dlt.append_flow(
        name=f"dlt_{table_name}_flow",
        once=False,
        target=f"dlt_{table_name}"
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
