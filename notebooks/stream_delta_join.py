# Databricks notebook source
# MAGIC %md
# MAGIC # Synthetic IoT Data Generator with Delta Dimension Table
# MAGIC
# MAGIC **Based on:** https://www.canadiandataguy.com/p/how-to-generate-1tb-of-synthetic
# MAGIC
# MAGIC **Features:**
# MAGIC - Generates synthetic IoT data at 6 rows every 2 seconds (3 rows per second)
# MAGIC - Creates and continuously updates a DIM_DEVICE_TYPE dimension table using dbldatagen
# MAGIC - Random power consumption values for realistic IoT device simulation
# MAGIC - Production-ready with proper error handling and monitoring

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Install Dependencies

# COMMAND ----------

# MAGIC %pip install dbldatagen

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📚 Imports and Dependencies

# COMMAND ----------

# Core imports
import dbldatagen as dg
import uuid
import time
import random
from datetime import datetime, timezone
import logging

# PySpark imports
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
from pyspark.sql.functions import expr, current_timestamp, lit, col

# Delta imports
from delta.tables import DeltaTable

print("✅ All dependencies imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Configuration and Constants

# COMMAND ----------

# =============================================================================
# CONFIGURATION SECTION - Centralized configuration management
# =============================================================================

class StreamingConfig:
    """Centralized configuration class for streaming parameters"""
    
    # IoT Data Configuration
    IOT_PARTITIONS = 2
    IOT_ROWS_PER_SECOND = 3
    
    # Dimension Data Configuration  
    DIM_ROWS_COUNT = 4
    DIM_ROWS_PER_SECOND = 1
    DIM_TRIGGER_INTERVAL = "10 seconds"
    
    # Table Configuration
    DIM_TABLE_NAME = "soni.default.DIM_DEVICE_TYPE"
    STREAM_DELTA_JOIN_TARGET_TABLE = "soni.default.STREAM_DELTA_JOIN_IOT"
    
    # Checkpoint Configuration
    CHECKPOINT_BASE_PATH = "/Volumes/soni/default/checkpoints"
    
    # Device Types (Static reference data)
    DEVICE_TYPES = ["Sensor", "Actuator", "Gateway", "Controller"]
    
    # Power Consumption Ranges (Watts)
    POWER_RANGES = {
        "Sensor": (1, 5),
        "Actuator": (10, 25), 
        "Gateway": (20, 40),
        "Controller": (8, 15)
    }

# Initialize configuration
config = StreamingConfig()

print(f"📊 Configuration Summary:")
print(f"   - IoT Rows per second: {config.IOT_ROWS_PER_SECOND}")
print(f"   - Dimension rows: {config.DIM_ROWS_COUNT}")
print(f"   - Dimension trigger: {config.DIM_TRIGGER_INTERVAL}")
print(f"   - Target table: {config.DIM_TABLE_NAME}")

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

# Device dimension table schema (Nullable for flexibility)
device_dim_schema = StructType([
    StructField("device_type", StringType(), False),
    StructField("power_consumption_watts", IntegerType(), False),
    StructField("updated_at", TimestampType(), False)
])

# Create dimension data generator with exactly 4 rows
dim_dataspec = (
    dg.DataGenerator(spark, name="device_dim_data", rows=4, partitions=1)
    .withSchema(device_dim_schema)
    .withColumnSpec("device_type", values=["Sensor", "Actuator", "Gateway", "Controller"], random=False)
    .withColumnSpec("power_consumption_watts", minValue=1, maxValue=50, random=True)
    .withColumnSpec("updated_at", expr="current_timestamp()", random=False)
)

# Generate the streaming dimension data
print("🎯 Generating 4 rows of device dimension data every minute...")
dim_fake_df = dim_dataspec.build(
    withStreaming=True,
    options={
        'rowsPerSecond': 1,  # Distribute 4 rows over 60 seconds
    }
)

# COMMAND ----------

#display(dim_fake_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Dimension Stream - Start Here!
# MAGIC
# MAGIC **This creates the dimension table that gets updated with 4 rows every 10 seconds**

# COMMAND ----------

def write_dimension_batch_to_delta(df, epoch_id: int):
    """
    Simple foreachBatch function for writing dimension data
    """
    
    try:
        # Deduplicate by device_type
        processed_df = df.dropDuplicates(["device_type"])
        
        # Write to Delta table
        processed_df.write \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable("soni.default.DIM_DEVICE_TYPE")
        
        print(f"✅ Batch {epoch_id}: Updated dimension table with {processed_df.count()} device types")
            
    except Exception as e:
        print(f"❌ Batch {epoch_id} failed: {str(e)}")
        raise

# Start the dimension streaming job
print("🚀 Starting DIMENSION streaming job (4 rows every 60 seconds)...")

checkpoint_path = f"/Volumes/soni/default/checkpoints/dim_checkpoint_{uuid.uuid4()}"

dimension_query = (
    dim_fake_df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .queryName("dim_device_type_updater")
    .trigger(processingTime="60 seconds")
    .foreachBatch(write_dimension_batch_to_delta)
    .start()
)

print(f"✅ Dimension streaming started!")
print(f"📊 Stream ID: {dimension_query.id}")
print(f"🎯 Checkpoint: {checkpoint_path}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 IoT Stream - Main Data Generation
# MAGIC
# MAGIC **This creates the main IoT data stream at 3 rows per second**

# COMMAND ----------

# Create IoT data generator specification
print("🚀 Setting up IOT data stream (3 rows per second)...")

iot_dataspec = (
    dg.DataGenerator(spark, name="iot_data", partitions=config.IOT_PARTITIONS)
    .withSchema(iot_data_schema)
    .withColumnSpec("device_id", percentNulls=0, minValue=1000, maxValue=9999, prefix="DEV_", random=True)
    .withColumnSpec("temperature", minValue=-10.0, maxValue=40.0, random=True)
    .withColumnSpec("humidity", minValue=0.0, maxValue=100.0, random=True)
    .withColumnSpec("pressure", minValue=900.0, maxValue=1100.0, random=True)
    .withColumnSpec("battery_level", minValue=0, maxValue=100, random=True)
    .withColumnSpec("device_type", values=config.DEVICE_TYPES, random=True)
    .withColumnSpec("error_code", minValue=0, maxValue=999, random=True, percentNulls=0.2)
    .withColumnSpec("signal_strength", minValue=-100, maxValue=0, random=True)
)

# Create IoT streaming DataFrame
iot_streaming_df = (
    iot_dataspec.build(
        withStreaming=True,
        options={
            'rowsPerSecond': config.IOT_ROWS_PER_SECOND,
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

print("✅ IoT data generator specification created")
print("📋 IoT Streaming Data Schema:")
iot_streaming_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔗 Stream-Static Join: IoT Data + Dimension Table
# MAGIC
# MAGIC **Enriching IoT streaming data with power consumption from dimension table**
# MAGIC
# MAGIC According to Databricks documentation, this is a stateless join that:
# MAGIC - Joins latest version of Delta table (dimension) with streaming data (IoT)
# MAGIC - No watermarking needed, low latency processing
# MAGIC - Perfect for joining facts with slowly-changing dimensions

# COMMAND ----------

# Create the stream-static join
print("🔗 Setting up stream-static join...")
print("   - Streaming source: IoT data (3 rows/sec)")
print("   - Static source: DIM_DEVICE_TYPE table (updates every 60 sec)")
print("   - Join key: device_type")
print("   - Result: All columns from both tables")

# Read the static dimension table
static_dim_df = spark.read.table(config.DIM_TABLE_NAME)

# Perform the stream-static join
# Join on device_type to enrich IoT data with power consumption info
enriched_iot_df = (
    iot_streaming_df
    .join(
        static_dim_df, 
        iot_streaming_df.device_type == static_dim_df.device_type, 
        "inner"
    )
    .select(
        # IoT data columns (with iot prefix to avoid conflicts)
        iot_streaming_df.device_id,
        # Dimension data columns (enrichment)
        static_dim_df.power_consumption_watts,
        static_dim_df.updated_at.alias("dim_updated_at"),
        iot_streaming_df.event_timestamp,
        iot_streaming_df.temperature,
        iot_streaming_df.humidity,
        iot_streaming_df.pressure,
        iot_streaming_df.battery_level,
        iot_streaming_df.device_type,
        iot_streaming_df.error_code,
        iot_streaming_df.signal_strength,
        iot_streaming_df.firmware_version,
        iot_streaming_df.location,
    )
)

print("✅ Stream-static join configured successfully!")
print("📊 Enriched IoT Data Schema (IoT + Dimension):")
enriched_iot_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Visualize the Enriched Streams
# MAGIC
# MAGIC **Run these cells to see the live data with enrichment**

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📊 Dimension Table (Updates every 60 seconds)

# COMMAND ----------

print("📊 Current Dimension Table:")
display(spark.read.table(config.DIM_TABLE_NAME))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🌊 Raw IoT Data Stream (3 rows per second)

# COMMAND ----------

# Visualize the raw streaming IoT data (before enrichment)
#display(iot_streaming_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ⚡ **ENRICHED IoT Data Stream** (IoT + Power Consumption)
# MAGIC
# MAGIC **This is the main result: IoT data enriched with dimension data!**

# COMMAND ----------

# Visualize the ENRICHED streaming data (IoT + Dimension)
print("⚡ Displaying ENRICHED IoT data with power consumption info...")
#display(enriched_iot_df)

# COMMAND ----------

( enriched_iot_df.writeStream
  .queryName("stream_delta_join")
  .trigger(processingTime="1 seconds")
  .option("checkpointLocation", f"{config.CHECKPOINT_BASE_PATH}/enriched_iot_{uuid.uuid4()}")
  .table(config.STREAM_DELTA_JOIN_TARGET_TABLE)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Monitoring & Management

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
# MAGIC ### Stop Streams (when needed)

# COMMAND ----------

# Stop dimension update stream (uncomment when needed)
# print("🛑 Stopping dimension update stream...")
# dimension_query.stop()
# print("✅ Dimension update stream stopped")

# COMMAND ----------

# Stop all streams (uncomment when needed)
def stop_all_active_streams():
    print("🛑 Stopping all active streams...")
    for stream in spark.streams.active:
        print(f"   Stopping: {stream.name}")
        stream.stop()
    print("✅ All streams stopped")



# COMMAND ----------

#stop_all_active_streams()

# COMMAND ----------


