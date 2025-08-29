# Databricks notebook source
# MAGIC %md
# MAGIC # Steady-Rate Streaming Workload Generator 🚀
# MAGIC
# MAGIC This notebook shows how to simulate streaming data at a steady rate and write it into Delta tables using **dbldatagen**. It’s designed as a flexible harness for testing Delta Live Tables (DLT) or Spark Structured Streaming pipelines under controlled conditions.
# MAGIC
# MAGIC ### 💡 How to use this notebook
# MAGIC - **Baseline Mode** → Run the notebook as-is to generate a steady workload (e.g., 50 streams at a fixed rows-per-second rate). This helps validate ingestion stability and table writes.
# MAGIC - **Parallel Mode** → Launch multiple copies of this notebook in parallel to **increase or vary the overall data production rate**. This creates a variable workload useful for stress-testing downstream pipelines.
# MAGIC - **DLT Integration** → Point the generated tables as source to feed your DLT pipeline and observe how it responds. This setup is especially helpful for checking **auto-scaling behavior**.
# MAGIC - **Experiment Safely** → Because the workload is synthetic and configurable, you can adjust stream counts, rates, and checkpoints without affecting production data.
# MAGIC
# MAGIC 👉 Use this notebook whenever you need a **repeatable and tunable workload generator** to study how DLT scales and adapts in real-world scenarios.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %pip install dbldatagen

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎛️ Configuration

# COMMAND ----------

import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
from pyspark.sql.functions import expr, current_timestamp

# Configuration
config = {
    "baseline_streams": 50,
    "baseline_rate": 10000,  # rows per second per stream
    "catalog_name": "soni",
    "database_name": "consistent_input",
    "table_prefix": "stream_table",
    "checkpoint_path": "/Volumes/soni/default/checkpoints/",
    "partitions": 4,
}

# Generate timestamp for unique checkpoint path
timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
config["checkpoint_path"] = f"/Volumes/soni/default/checkpoints/{timestamp_str}/"


# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Setup Databricks Environment

# COMMAND ----------

# Get existing Spark session (already available on Databricks cluster)
spark = SparkSession.getActiveSession()
if spark is None:
    raise Exception("No active Spark session found. Please ensure this notebook is attached to a running cluster.")
print(f"🔗 Spark Version: {spark.version}")

# Verify Delta Lake support
try:
    spark.sql("SELECT 1").collect()
    print("✅ Spark SQL working")
except Exception as e:
    print(f"❌ Spark SQL issue: {e}")
    raise

# Import dbldatagen (already installed via %pip)
import dbldatagen as dg
print("✅ dbldatagen available")

# Create catalog and database
spark.sql(f"CREATE CATALOG IF NOT EXISTS {config['catalog_name']}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {config['catalog_name']}.{config['database_name']}")
print(f"✅ Catalog '{config['catalog_name']}' and Database '{config['database_name']}' ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Data Generator Setup

# COMMAND ----------

# Define schema
schema = StructType([
    StructField("device_id", StringType(), False),
    StructField("event_timestamp", TimestampType(), False),  # Renamed to avoid conflicts
    StructField("temperature", DoubleType(), False),
    StructField("humidity", DoubleType(), False),
    StructField("pressure", DoubleType(), False),
    StructField("stream_type", StringType(), False),  # "baseline", "3x", "9x"
    StructField("stream_id", IntegerType(), False)
])

def create_stream(stream_id, rate, stream_type):
    """Create a streaming DataFrame with specified rate"""
    dataspec = (
        dg.DataGenerator(spark, name=f"stream_{stream_id}", partitions=config['partitions'])
        .withSchema(schema)
        .withColumnSpec("device_id", minValue=1000, maxValue=9999, prefix=f"DEV_{stream_id}_", random=True)
        .withColumnSpec("event_timestamp", begin="2023-01-01 00:00:00", end="2023-12-31 23:59:59", random=True)
        .withColumnSpec("temperature", minValue=15.0, maxValue=35.0, random=True)
        .withColumnSpec("humidity", minValue=30.0, maxValue=80.0, random=True)
        .withColumnSpec("pressure", minValue=980.0, maxValue=1020.0, random=True)
        .withColumnSpec("stream_type", values=[stream_type])
        .withColumnSpec("stream_id", values=[stream_id])
    )
    
    return dataspec.build(withStreaming=True, options={'rowsPerSecond': rate})

print("✅ Data generator functions ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Start Baseline Streams (50 streams at 1000 rows/sec each)

# COMMAND ----------

print(f"🚀 Starting {config['baseline_streams']} baseline streams...")

baseline_queries = []
demo_start_time = time.time()

# Start all baseline streams
for i in range(1, config['baseline_streams'] + 1):
    stream_df = create_stream(i, config['baseline_rate'], "baseline")
    table_name = f"{config['catalog_name']}.{config['database_name']}.{config['table_prefix']}_{i:03d}"
    checkpoint = f"{config['checkpoint_path']}baseline_{i:03d}/"
    
    query = (
        stream_df
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime="10 seconds")
        .queryName(f"baseline_stream_{i}")
        .toTable(table_name)
    )
    
    baseline_queries.append(query)
    
    if i <= 5 or i % 10 == 0 or i == config['baseline_streams']:
        print(f"✅ Started baseline stream {i}/{config['baseline_streams']} → {table_name}")
    
    time.sleep(0.1)  # Small delay

active_baseline = sum(1 for q in baseline_queries if q.isActive)
print(f"\n🎉 Baseline startup complete!")
print(f"📊 Active baseline streams: {active_baseline}/{config['baseline_streams']}")
print(f"📈 Total baseline throughput: {active_baseline * config['baseline_rate']:,} rows/sec")
print(f"⏰ Demo started at: {datetime.fromtimestamp(demo_start_time).strftime('%H:%M:%S')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🛠️ Cleanup Functions

# COMMAND ----------

def stop_all_streams():
    """Stop all running streams"""
    print("⏹️ Stopping all streams...")
    
    stopped = 0
    # Stop baseline streams
    for i, query in enumerate(baseline_queries):
        try:
            if query.isActive:
                query.stop()
                stopped += 1
        except:
            pass
    
    # Stop scaling streams
    if scale_3x_query:
        try:
            if scale_3x_query.isActive:
                scale_3x_query.stop()
                stopped += 1
        except:
            pass
            
    if scale_9x_query:
        try:
            if scale_9x_query.isActive:
                scale_9x_query.stop()
                stopped += 1
        except:
            pass
    
    print(f"✅ Stopped {stopped} streams")
    return stopped

def check_stream_status():
    """Check status of all streams"""
    print("📊 Stream Status Check:")
    
    active_baseline = sum(1 for q in baseline_queries if q.isActive)
    print(f"   Baseline: {active_baseline}/{len(baseline_queries)} active")
    
    return active_baseline


print("💡 Available functions:")
print("   stop_all_streams() - Stop all running streams")  
print("   check_stream_status() - Check which streams are active")

# COMMAND ----------

while True:
    # Quick status check
    print(f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}   Active streams: {check_stream_status()}")
    time.sleep(60)

# COMMAND ----------

