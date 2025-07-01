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
# MAGIC ### Update Dimension Table with Proper foreachBatch
# MAGIC This implementation follows Spark Connect best practices to avoid serialization issues

# COMMAND ----------

# Create a streaming DataFrame that triggers every minute
minute_trigger_df = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 1)  # 1 row per second (will be controlled by trigger interval)
    .load()
    .select(lit("update_trigger").alias("trigger"))
)

# Define the streaming function following Spark Connect best practices
def process_dim_updates_proper(df, epoch_id):
    """
    Proper foreachBatch implementation for Spark Connect
    All dependencies are defined inside the function to avoid serialization issues
    """
    # Import all dependencies inside the function
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
    from pyspark.sql.functions import current_timestamp
    import random
    from datetime import datetime
    
    # Check if DataFrame is empty (recommended by Databricks)
    if df.isEmpty:
        print(f"⚠️  Empty DataFrame in epoch {epoch_id}, skipping...")
        return
    
    # Access Spark session from the DataFrame parameter (Spark Connect requirement)
    spark_session = df.sparkSession
    
    # Define all variables inside function (no external references)
    table_name = "soni.default.DIM_DEVICE_TYPE"  # Hardcoded to avoid external reference
    
    # Define schema inside function
    device_dim_schema = StructType([
        StructField("device_type", StringType(), False),
        StructField("power_consumption_watts", IntegerType(), False),
        StructField("updated_at", TimestampType(), False)
    ])
    
    print(f"🔄 Processing epoch {epoch_id} at {datetime.now()}")
    
    try:
        # Generate random power consumption data for each device type
        updated_data = [
            ("Sensor", random.randint(1, 5), current_timestamp()),          # 1-5W random
            ("Actuator", random.randint(10, 25), current_timestamp()),      # 10-25W random  
            ("Gateway", random.randint(20, 40), current_timestamp()),       # 20-40W random
            ("Controller", random.randint(8, 15), current_timestamp())      # 8-15W random
        ]
        
        # Create DataFrame using the session from df parameter
        updated_df = spark_session.createDataFrame(updated_data, device_dim_schema)
        
        # Overwrite the Delta table
        updated_df.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(table_name)
        
        print(f"✅ Successfully overwritten {table_name} at {datetime.now()}")
        
        # Show updated data using spark session from df
        updated_table = spark_session.table(table_name)
        row_count = updated_table.count()
        print(f"📊 Table now contains {row_count} rows")
        
        # Show sample data
        print("📋 Sample data:")
        updated_table.show(4, truncate=False)
        
    except Exception as e:
        print(f"❌ Error in epoch {epoch_id}: {str(e)}")
        # Re-raise to fail the streaming job for investigation
        raise e

# Start the streaming query with proper error handling
print(f"🚀 Starting {DIM_TABLE_NAME} streaming update job")
print(f"⏱️  Updates every {UPDATE_INTERVAL} seconds") 
print("⏹️  Use stream management section to stop")
print("=" * 50)

try:
    dim_update_query = (
        minute_trigger_df.writeStream
        .queryName("dim_device_type_updater_v2")
        .outputMode("update")
        .foreachBatch(process_dim_updates_proper)
        .trigger(processingTime=f"{UPDATE_INTERVAL} seconds")  # Process every 60 seconds
        .option("checkpointLocation", "/tmp/dim_update_checkpoint")  # Add checkpoint for reliability
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
# MAGIC ### Manual Dimension Table Update (Fallback)
# MAGIC Run this cell manually if the streaming approach has issues

# COMMAND ----------

# Simple function call without streaming complexity
print(f"🔄 Updating {DIM_TABLE_NAME} at {datetime.now()}")
update_device_dim_table()

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


