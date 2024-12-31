# Databricks notebook source
# MAGIC %pip install dbldatagen

# COMMAND ----------

import dbldatagen as dg
import uuid

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
from pyspark.sql.functions import expr


# COMMAND ----------

# Parameterize partitions and rows per second
PARTITIONS = 4 # Match with number of cores on your cluster
ROWS_PER_SECOND = 1 * 1000 * 1000 # 1 Million rows per second


# COMMAND ----------


# Define the schema for IoT data
iot_data_schema = StructType([
    StructField("device_id", StringType(), False),
    StructField("event_timestamp", TimestampType(), False),
    StructField("temperature", DoubleType(), False),
    StructField("humidity", DoubleType(), False),
    StructField("pressure", DoubleType(), False),
    StructField("battery_level", IntegerType(), False),
    StructField("device_type", StringType(), False),
    StructField("error_code", IntegerType(), True),  # Optional field
    StructField("signal_strength", IntegerType(), False)
])

# Create a data generator specification
# Define columns, their data ranges, and other properties
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

# Build the streaming DataFrame
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

# Uncomment to preview the streaming DataFrame
# display(streaming_df)

# COMMAND ----------



# COMMAND ----------

# Write the streaming data to a Delta table
(
    streaming_df.writeStream
        .queryName("iot_data_stream")  # Assign a name to the stream
        .outputMode("append")
        .option("checkpointLocation", f"/tmp/dbldatagen/streamingDemo/checkpoint-{uuid.uuid4()}")
        .toTable("soni.default.iot_data_1kb_rows")
)


# COMMAND ----------


