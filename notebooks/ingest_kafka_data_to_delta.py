# Databricks notebook source
https://confluent.cloud/environments/env-g9z3n3/clusters/lkc-mn35ow/connectors/sources/sample_data

# COMMAND ----------

from pyspark.sql.types import StructType, StringType, FloatType
from pyspark.sql.functions import *
# Define the schema based on the DataFrame structure you are writing to Kafka
schema = StructType() \
    .add("event_id", StringType()) \
    .add("vehicle_year_make_model", StringType()) \
    .add("vehicle_year_make_model_cat", StringType()) \
    .add("vehicle_make_model", StringType()) \
    .add("vehicle_make", StringType()) \
    .add("vehicle_model", StringType()) \
    .add("vehicle_year", StringType()) \
    .add("vehicle_category", StringType()) \
    .add("vehicle_object", StringType()) \
    .add("latitude", StringType()) \
    .add("longitude", StringType()) \
    .add("location_on_land", StringType()) \
    .add("local_latlng", StringType()) \
    .add("zipcode", StringType()) \
    .add("large_text_col_1", StringType()) \
    .add("large_text_col_2", StringType()) \
    .add("large_text_col_3", StringType()) \
    .add("large_text_col_4", StringType()) \
    .add("large_text_col_5", StringType()) \
    .add("large_text_col_6", StringType()) \
    .add("large_text_col_7", StringType()) \
    .add("large_text_col_8", StringType()) \
    .add("large_text_col_9", StringType())

def read_kafka_stream():
    kafka_stream = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka_bootstrap_servers_tls ) 
      .option("subscribe", topic )
      .option("failOnDataLoss","false")
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.mechanism", "PLAIN") 
      .option("kafka.sasl.jaas.config", f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_api_key}" password="{kafka_api_secret}";')
      .option("minPartitions",12)
      .load()
      .select(from_json(col("value").cast("string"), schema).alias("data"), "topic", "partition", "offset", "timestamp", "timestampType" )
      .select("topic", "partition", "offset", "timestamp", "timestampType", "data.*")
    )
    return kafka_stream

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


