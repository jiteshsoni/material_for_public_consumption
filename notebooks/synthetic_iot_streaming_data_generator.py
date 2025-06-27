# Databricks notebook source
# MAGIC %pip install dbldatagen

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG  if not exists soni

# COMMAND ----------

import dbldatagen as dg
import uuid

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
from pyspark.sql.functions import expr


# COMMAND ----------



# COMMAND ----------

# Parameterize partitions and rows per second
PARTITIONS = 40 # Match with number of cores on your cluster
ROWS_PER_SECOND = 5 * 100 * 10000  # 1 Million rows per second


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
    .withColumnSpec("device_id", minValue=1000, maxValue=9999, prefix="DEV_", random=True)
    .withColumnSpec("event_timestamp", begin="2025-01-01 00:00:00", end="2025-06-01 23:59:59", random=True)
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
        "event_timestamp_string",
        expr(
            "CAST(event_timestamp as string)"
        )
    ).withColumn(
        "event_timestamp",
        expr("current_timestamp() - make_interval(0, 0, 0, 0, 0, 0, floor(rand() * 120))")
    )

)

# Uncomment to preview the streaming DataFrame
#display(streaming_df)

# COMMAND ----------



# COMMAND ----------

# Write the streaming data to a Delta table
(
    streaming_df.where("device_id is not null").writeStream
        .queryName("iot_data_stream")  # Assign a name to the stream
        .outputMode("append")
        .option("checkpointLocation", f"/Volumes/soni/default/checkpoints/checkpoint-{uuid.uuid4()}")
        .toTable("soni.default.iot_data_to_be_merge")
)


# COMMAND ----------

spark.read.table("soni.default.iot_data_to_be_merge").count()

# COMMAND ----------

1,481,130,000
