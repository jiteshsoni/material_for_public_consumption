# Databricks notebook source
# MAGIC %md
# MAGIC # Synthetic IoT Data Generator with Delta Dimension Table
# MAGIC
# MAGIC **Based on:** https://www.canadiandataguy.com/p/how-to-generate-1tb-of-synthetic
# MAGIC
# MAGIC **Features:**
# MAGIC - Generates synthetic IoT data at 6 rows every 2 seconds (3 rows per second)
# MAGIC - Creates and continuously updates a DIM_DEVICE_TYPE dimension table
# MAGIC - Random power consumption values for realistic IoT device simulation

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Install Dependencies

# COMMAND ----------

# MAGIC %pip install dbldatagen

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📚 Imports

# COMMAND ----------

# Core imports
import dbldatagen as dg
import uuid
import time
import random
from datetime import datetime

# PySpark imports
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
from pyspark.sql.functions import expr, current_timestamp, lit

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Configuration

# COMMAND ----------

# Streaming configuration
PARTITIONS = 2          # Small number of partitions for slower generation
ROWS_PER_SECOND = 3     # 6 rows every 2 seconds
UPDATE_INTERVAL = 60    # Update dimension table every 60 seconds

# Table names
DIM_TABLE_NAME = "soni.default.DIM_DEVICE_TYPE"

print(f"📊 IoT Data Generation Config:")
print(f"   - Rows per second: {ROWS_PER_SECOND}")
print(f"   - Partitions: {PARTITIONS}")
print(f"   - Dimension update interval: {UPDATE_INTERVAL} seconds")
print(f"   - Dimension table: {DIM_TABLE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏗️ Schema Definitions

# COMMAND ----------

# IoT streaming data schema
iot_data_schema = StructType([
    StructField("device_id", StringType(), False),
    StructField("event_timestamp", TimestampType(), False),
    StructField("temperature", DoubleType(), False),
    StructField("humidity", DoubleType(), False),
    StructField("pressure", DoubleType(), False),
    StructField("battery_level", IntegerType(), False),
    StructField("device_type", StringType(), False),
    StructField("error_code", IntegerType(), True),
    StructField("signal_strength", IntegerType(), False)
])

# Device dimension table schema
device_dim_schema = StructType([
    StructField("device_type", StringType(), False),
    StructField("power_consumption_watts", IntegerType(), False),
    StructField("updated_at", TimestampType(), False)
])

print("✅ Schemas defined successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Function Definitions

# COMMAND ----------

def update_device_dim_table():
    """Update device dimension table with random power consumption values"""
    
    # Generate random power consumption data for each device type
    updated_data = [
        ("Sensor", random.randint(1, 5), current_timestamp()),          # 1-5W random
        ("Actuator", random.randint(10, 25), current_timestamp()),      # 10-25W random  
        ("Gateway", random.randint(20, 40), current_timestamp()),       # 20-40W random
        ("Controller", random.randint(8, 15), current_timestamp())      # 8-15W random
    ]
    
    # Create DataFrame
    updated_df = spark.createDataFrame(updated_data, device_dim_schema)
    
    # Overwrite the Delta table
    updated_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(DIM_TABLE_NAME)
    
    print(f"🔄 Overwritten {DIM_TABLE_NAME} at {datetime.now()}")
    
    # Show updated data
    updated_table = spark.table(DIM_TABLE_NAME)
    display(updated_table)
    
    return updated_table

print("✅ Functions defined successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 IoT Data Generation Setup

# COMMAND ----------

# Create data generator specification
dataspec = (
    dg.DataGenerator(spark, name="iot_data", partitions=PARTITIONS)
    .withSchema(iot_data_schema)
    .withColumnSpec("device_id", percentNulls=0, minValue=1000, maxValue=9999, prefix="DEV_", random=True)
    .withColumnSpec("temperature", minValue=-10.0, maxValue=40.0, random=True)
    .withColumnSpec("humidity", minValue=0.0, maxValue=100.0, random=True)
    .withColumnSpec("pressure", minValue=900.0, maxValue=1100.0, random=True)
    .withColumnSpec("battery_level", minValue=0, maxValue=100, random=True)
    .withColumnSpec("device_type", values=["Sensor", "Actuator", "Gateway", "Controller"], random=True)
    .withColumnSpec("error_code", minValue=0, maxValue=999, random=True, percentNulls=0.2)
    .withColumnSpec("signal_strength", minValue=-100, maxValue=0, random=True)
)

print("✅ Data generator specification created")

# COMMAND ----------

# Create streaming DataFrame
streaming_df = (
    dataspec.build(
        withStreaming=True,
        options={
            'rowsPerSecond': ROWS_PER_SECOND,
        }
    )
    .withColumn("event_timestamp", 
                expr("current_timestamp() - make_interval(0, 0, 0, 0, 0, 0, floor(rand() * 2))"))
    .withColumn(
        "firmware_version",
        expr(
            "concat('v', cast(floor(rand() * 10) as string), '.', "
            "cast(floor(rand() * 10) as string), '.', "
            "cast(floor(rand() * 10) as string))"
        )
    )
    .withColumn(
        "location",
        expr(
            "concat(cast(rand() * 180 - 90 as decimal(8,6)), ',', "
            "cast(rand() * 360 - 180 as decimal(9,6)))"
        )
    )
)

# Display schema
print("📋 IoT Streaming Data Schema:")
streaming_df.printSchema()

print("✅ Streaming DataFrame created successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Data Visualization & Execution

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize IoT Streaming Data
# MAGIC Run this cell to see the live IoT data stream

# COMMAND ----------

# Visualize the streaming IoT data
display(streaming_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Streaming Dimension Data with dbldatagen
# MAGIC Create exactly 4 rows streaming for the dimension table using dbldatagen

# COMMAND ----------

# Create dimension data generator with exactly 4 rows
dim_dataspec = (
    dg.DataGenerator(spark, name="device_dim_data", rows=4, partitions=1)
    .withSchema(device_dim_schema)
    .withColumnSpec("device_type", values=["Sensor", "Actuator", "Gateway", "Controller"], random=False)
    .withColumnSpec("power_consumption_watts", minValue=1, maxValue=50, random=True)
    .withColumnSpec("updated_at", expr="current_timestamp()", random=False)
)

# Generate the dimension data
print("🎯 Generating 4 rows of device dimension data...")
dim_fake_df = dim_dataspec.build(
        withStreaming=True,
        options={
            'rowsPerSecond': ROWS_PER_SECOND,
        }
    )

# Show the generated data
print("📊 Generated dimension data:")
display(dim_fake_df)

print("✅ Streaming dimension data generator created")

# COMMAND ----------

# Display the streaming dimension schema
print("📋 Dimension Streaming Data Schema:")
dim_fake_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Streaming Dimension Data to Delta Table
# MAGIC Stream the 4 rows to Delta table with 1 minute trigger

# COMMAND ----------

# Start streaming query to write dimension data every minute
print(f"🚀 Starting streaming dimension data to {DIM_TABLE_NAME}")
print(f"⏱️  Updates every 1 minute")
print("=" * 50)

try:
    dim_update_query = (
        minute_trigger_df.writeStream
        .queryName("dim_device_type_updater_v2")
        .outputMode("update")
        .foreachBatch(process_dim_updates_proper)
        .trigger(processingTime=f"{UPDATE_INTERVAL} seconds")  # Process every 60 seconds
        .option("checkpointLocation", "/Volumes/soni/default/checkpoints/dim_update_checkpoint/{uuid.uuid4()}")  # Add checkpoint for reliability
        .start()
    )
    
    print(f"✅ Streaming job started successfully!")
    print(f"📊 Stream Name: {dim_update_query.name}")
    print(f"🆔 Stream ID: {dim_update_query.id}")
    print(f"⚡ Status: {dim_update_query.status}")
    
except Exception as e:
    print(f"❌ Failed to start streaming job: {str(e)}")
    print("💡 If you get serialization errors, try the manual update approach below")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Manual Dimension Update (Alternative)
# MAGIC Generate sample dimension data manually for testing

# COMMAND ----------

# Generate sample dimension data for preview
sample_dim_df = dim_dataspec.build()

print("📊 Sample dimension data:")
display(sample_dim_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Monitoring & Management

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Current Dimension Table State

# COMMAND ----------

# Check current dimension table
try:
    current_dim = spark.table(DIM_TABLE_NAME)
    print(f"📊 Current {DIM_TABLE_NAME} state:")
    display(current_dim)
except Exception as e:
    print(f"❌ Table {DIM_TABLE_NAME} does not exist yet. Run the streaming update first.")
    print(f"Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Active Streams

# COMMAND ----------

# Check active streams
active_streams = spark.streams.active
print(f"📡 Active Streams: {len(active_streams)}")

if active_streams:
    for stream in active_streams:
        print(f"   - {stream.name}: {stream.id}")
        print(f"     Status: {stream.status}")
else:
    print("   No active streams found")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stop Dimension Update Stream

# COMMAND ----------

# Stop dimension update stream (uncomment when needed)
# print("🛑 Stopping dimension update stream...")
# dim_update_query.stop()
# print("✅ Dimension update stream stopped")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stop All Streams (if needed)

# COMMAND ----------

# Stop all streams (uncomment when needed)
# print("🛑 Stopping all active streams...")
# for stream in spark.streams.active:
#     print(f"   Stopping: {stream.name}")
#     stream.stop()
# print("✅ All streams stopped")

# COMMAND ----------


