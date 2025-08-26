# Databricks notebook source
# MAGIC %md
# MAGIC # 📖 DLT Streaming Table Reader
# MAGIC 
# MAGIC This script reads the 50 streaming tables created by the `delta_table_streaming_benchmark.py` script using Delta Live Tables (DLT).
# MAGIC 
# MAGIC ## 🎯 Purpose:
# MAGIC - Read all 50 baseline streaming tables
# MAGIC - Create consolidated views for analysis
# MAGIC - Demonstrate DLT streaming table creation and reading
# MAGIC 
# MAGIC ## 📊 Tables to Read:
# MAGIC - `soni.default.stream_table_001` through `soni.default.stream_table_050`
# MAGIC - Special focus on tables 001 and 002 (which have parallel scaling data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 DLT Setup and Configuration

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configuration
config = {
    "catalog_name": "soni",
    "database_name": "default",
    "table_prefix": "stream_table",
    "baseline_streams": 50,
    "output_database": "dlt_processed"  # Where to store processed tables
}

print("🚀 DLT Streaming Table Reader Configuration:")
print(f"   📁 Source Catalog: {config['catalog_name']}")
print(f"   🗄️ Source Database: {config['database_name']}")
print(f"   📊 Tables to Read: {config['baseline_streams']} tables")
print(f"   🎯 Output Database: {config['output_database']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Create Output Database

# COMMAND ----------

# Create output database for processed tables
spark.sql(f"CREATE DATABASE IF NOT EXISTS {config['catalog_name']}.{config['output_database']}")
print(f"✅ Created output database: {config['catalog_name']}.{config['output_database']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📖 Read Individual Streaming Tables

# COMMAND ----------

# Read all 50 baseline streaming tables
for i in range(1, config['baseline_streams'] + 1):
    table_name = f"{config['table_prefix']}_{i:03d}"
    source_table = f"{config['catalog_name']}.{config['database_name']}.{table_name}"
    
    @dlt.table(
        name=f"dlt_{table_name}",
        comment=f"DLT streaming table reading from {source_table}",
        table_properties={
            "quality": "bronze",
            "source_table": source_table
        }
    )
    def read_streaming_table():
        return spark.readStream.table(source_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Special Tables with Parallel Load

# COMMAND ----------

# Table 001 - Has baseline + 3x scaling data
@dlt.table(
    name="dlt_stream_table_001_enhanced",
    comment="Enhanced view of table 001 with baseline + 3x scaling data",
    table_properties={
        "quality": "silver",
        "source_table": f"{config['catalog_name']}.{config['database_name']}.stream_table_001",
        "data_type": "parallel_load"
    }
)
def read_table_001_enhanced():
    return (spark.readStream.table(f"{config['catalog_name']}.{config['database_name']}.stream_table_001")
            .withColumn("load_type", 
                       when(col("stream_type") == "3x", "scaling_load")
                       .when(col("stream_type") == "baseline", "baseline_load")
                       .otherwise("unknown"))
            .withColumn("processing_timestamp", current_timestamp()))

# Table 002 - Has baseline + 9x scaling data
@dlt.table(
    name="dlt_stream_table_002_enhanced",
    comment="Enhanced view of table 002 with baseline + 9x scaling data",
    table_properties={
        "quality": "silver",
        "source_table": f"{config['catalog_name']}.{config['database_name']}.stream_table_002",
        "data_type": "parallel_load"
    }
)
def read_table_002_enhanced():
    return (spark.readStream.table(f"{config['catalog_name']}.{config['database_name']}.stream_table_002")
            .withColumn("load_type", 
                       when(col("stream_type") == "9x", "scaling_load")
                       .when(col("stream_type") == "baseline", "baseline_load")
                       .otherwise("unknown"))
            .withColumn("processing_timestamp", current_timestamp()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Consolidated Views

# COMMAND ----------

# Consolidated view of all baseline data
@dlt.table(
    name="dlt_all_baseline_consolidated",
    comment="Consolidated view of all baseline streaming data",
    table_properties={
        "quality": "gold",
        "data_type": "consolidated_baseline"
    }
)
def consolidate_baseline_data():
    # Union all baseline tables (001-050)
    baseline_dfs = []
    for i in range(1, config['baseline_streams'] + 1):
        table_name = f"{config['table_prefix']}_{i:03d}"
        source_table = f"{config['catalog_name']}.{config['database_name']}.{table_name}"
        
        df = (spark.readStream.table(source_table)
              .filter(col("stream_type") == "baseline")
              .withColumn("source_table", lit(table_name))
              .withColumn("consolidation_timestamp", current_timestamp()))
        baseline_dfs.append(df)
    
    # Union all dataframes
    if baseline_dfs:
        return baseline_dfs[0].unionAll(*baseline_dfs[1:])
    else:
        return spark.createDataFrame([], StructType([]))

# Consolidated view of all scaling data
@dlt.table(
    name="dlt_all_scaling_consolidated",
    comment="Consolidated view of all scaling streaming data",
    table_properties={
        "quality": "gold",
        "data_type": "consolidated_scaling"
    }
)
def consolidate_scaling_data():
    # Union scaling data from tables 001 and 002
    scaling_dfs = []
    
    # 3x scaling from table 001
    df_3x = (spark.readStream.table(f"{config['catalog_name']}.{config['database_name']}.stream_table_001")
             .filter(col("stream_type") == "3x")
             .withColumn("scaling_type", lit("3x"))
             .withColumn("source_table", lit("stream_table_001"))
             .withColumn("consolidation_timestamp", current_timestamp()))
    scaling_dfs.append(df_3x)
    
    # 9x scaling from table 002
    df_9x = (spark.readStream.table(f"{config['catalog_name']}.{config['database_name']}.stream_table_002")
             .filter(col("stream_type") == "9x")
             .withColumn("scaling_type", lit("9x"))
             .withColumn("source_table", lit("stream_table_002"))
             .withColumn("consolidation_timestamp", current_timestamp()))
    scaling_dfs.append(df_9x)
    
    # Union scaling dataframes
    return scaling_dfs[0].unionAll(scaling_dfs[1])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Analytics Views

# MAGIC ## 📈 Analytics Views

# COMMAND ----------

# Real-time throughput analytics
@dlt.table(
    name="dlt_throughput_analytics",
    comment="Real-time throughput analytics from streaming tables",
    table_properties={
        "quality": "gold",
        "data_type": "analytics"
    }
)
def throughput_analytics():
    # Read from consolidated baseline and scaling data
    baseline_df = spark.readStream.table("dlt_all_baseline_consolidated")
    scaling_df = spark.readStream.table("dlt_all_scaling_consolidated")
    
    # Calculate throughput metrics
    baseline_metrics = (baseline_df
                       .groupBy(window(col("event_timestamp"), "1 minute"))
                       .agg(
                           count("*").alias("baseline_records_per_minute"),
                           avg("temperature").alias("avg_temperature"),
                           avg("humidity").alias("avg_humidity"),
                           avg("pressure").alias("avg_pressure")
                       )
                       .withColumn("data_type", lit("baseline")))
    
    scaling_metrics = (scaling_df
                      .groupBy(window(col("event_timestamp"), "1 minute"), col("scaling_type"))
                      .agg(
                          count("*").alias("scaling_records_per_minute"),
                          avg("temperature").alias("avg_temperature"),
                          avg("humidity").alias("avg_humidity"),
                          avg("pressure").alias("avg_pressure")
                      )
                      .withColumn("data_type", lit("scaling")))
    
    return baseline_metrics.unionAll(scaling_metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Summary

# MAGIC This DLT pipeline creates:
# MAGIC 
# MAGIC ### 📖 Individual Table Readers (50 tables):
# MAGIC - `dlt_stream_table_001` through `dlt_stream_table_050`
# MAGIC 
# MAGIC ### 🎯 Enhanced Views:
# MAGIC - `dlt_stream_table_001_enhanced` - Table 001 with load type classification
# MAGIC - `dlt_stream_table_002_enhanced` - Table 002 with load type classification
# MAGIC 
# MAGIC ### 📊 Consolidated Views:
# MAGIC - `dlt_all_baseline_consolidated` - All baseline data combined
# MAGIC - `dlt_all_scaling_consolidated` - All scaling data combined
# MAGIC 
# MAGIC ### 📈 Analytics:
# MAGIC - `dlt_throughput_analytics` - Real-time throughput metrics
# MAGIC 
# MAGIC ## 🚀 Usage:
# MAGIC 1. Run the `delta_table_streaming_benchmark.py` first to create source tables
# MAGIC 2. Run this DLT pipeline to read and process the streaming data
# MAGIC 3. Monitor the analytics views for real-time insights
