# Databricks notebook source
# MAGIC %md
# MAGIC # Synthetic IoT Data Generator
# MAGIC Based on: https://www.canadiandataguy.com/p/how-to-generate-1tb-of-synthetic
# MAGIC 
# MAGIC This notebook generates synthetic IoT data at a rate of 6 rows every 2 seconds (3 rows per second)

# COMMAND ----------

# MAGIC %pip install dbldatagen

# COMMAND ----------

# Setup and Imports
import dbldatagen as dg
import uuid

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
from pyspark.sql.functions import expr

# COMMAND ----------

# Parameters - Generate 6 rows every 2 seconds = 3 rows per second
PARTITIONS = 2  # Small number of partitions for slower generation
ROWS_PER_SECOND = 3  # 6 rows every 2 seconds

print(f"Configured to generate {ROWS_PER_SECOND} rows per second")
print(f"This equals {ROWS_PER_SECOND * 2} rows every 2 seconds")

# COMMAND ----------

# Schema Definition - IoT Data Schema
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

# COMMAND ----------

# Data Generator Specification
dataspec = (
    dg.DataGenerator(spark, name="iot_data", partitions=PARTITIONS)
    .withSchema(iot_data_schema)
    .withColumnSpec("device_id", percentNulls=0.1, minValue=1000, maxValue=9999, prefix="DEV_", random=True)
    .withColumnSpec("event_timestamp", begin="2023-01-01 00:00:00", end="2023-12-31 23:59:59", random=True)
    .withColumnSpec("temperature", minValue=-10.0, maxValue=40.0, random=True)
    .withColumnSpec("humidity", minValue=0.0, maxValue=100.0, random=True)
    .withColumnSpec("pressure", minValue=900.0, maxValue=1100.0, random=True)
    .withColumnSpec("battery_level", minValue=0, maxValue=100, random=True)
    .withColumnSpec("device_type", values=["Sensor", "Actuator", "Gateway", "Controller"], random=True)
    .withColumnSpec("error_code", minValue=0, maxValue=999, random=True, percentNulls=0.2)
    .withColumnSpec("signal_strength", minValue=-100, maxValue=0, random=True)
)

# COMMAND ----------

# Create Streaming DataFrame
streaming_df = (
    dataspec.build(
        withStreaming=True,
        options={
            'rowsPerSecond': ROWS_PER_SECOND,
        }
    )
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
    .withColumn(
        "data_payload",
        expr("repeat(uuid(), 22)")  # Add approx. 800 Bytes to construct 1 KB row
    )
)

# COMMAND ----------

# Display the schema
streaming_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 1: Write to Delta Table
# MAGIC Uncomment the code below to write streaming data to a Delta table

# COMMAND ----------

# # Write to Delta Table
# checkpoint_location = f"/tmp/synthetic_iot_data/checkpoint-{uuid.uuid4()}"
# table_name = "default.synthetic_iot_data_slow"

# query = (
#     streaming_df.writeStream
#     .queryName("synthetic_iot_data_stream")
#     .outputMode("append")
#     .option("checkpointLocation", checkpoint_location)
#     .toTable(table_name)
# )

# # Display stream information
# print(f"Stream started: {query.name}")
# print(f"Stream ID: {query.id}")
# print(f"Writing to table: {table_name}")
# print(f"Checkpoint location: {checkpoint_location}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 2: Display Stream to Console (for testing)
# MAGIC This will show the generated data in the console

# COMMAND ----------

# Display stream to console for testing
console_query = (
    streaming_df.writeStream
    .queryName("synthetic_iot_console_stream")
    .outputMode("append")
    .format("console")
    .option("truncate", False)
    .option("numRows", 10)
    .trigger(processingTime="2 seconds")  # Process every 2 seconds
)

# Start the stream
console_stream = console_query.start()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream Management
# MAGIC Use the commands below to manage your streams

# COMMAND ----------

# Check active streams
active_streams = spark.streams.active
print(f"Number of active streams: {len(active_streams)}")

for stream in active_streams:
    print(f"Stream: {stream.name}, ID: {stream.id}, Status: {stream.status}")

# COMMAND ----------

# Stop all streams (uncomment when needed)
# for stream in spark.streams.active:
#     print(f"Stopping stream: {stream.name}")
#     stream.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Data Preview
# MAGIC Generate a batch of sample data to preview the structure

# COMMAND ----------

# Generate sample batch data for preview
sample_df = dataspec.build()
sample_with_extras = (
    sample_df
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
    .withColumn(
        "data_payload",
        expr("repeat(uuid(), 22)")  # Add approx. 800 Bytes to construct 1 KB row
    )
)

display(sample_with_extras.limit(10))
