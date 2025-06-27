# Databricks notebook source
# MAGIC %pip install dbldatagen

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
from pyspark.sql.functions import expr, when, col, concat, lit
import dbldatagen as dg
import uuid

# Configuration constants for high-throughput streaming
PARTITIONS = 12  # Increased partitions for 1M rows/sec
ROWS_PER_SECOND = 1000000

# IoT schema with motor/environmental monitoring columns
iot_data_schema = StructType([
    StructField("site_id", StringType(), False),
    StructField("temperature", DoubleType(), False),
    StructField("battery_level", IntegerType(), False),
    StructField("signal_strength", IntegerType(), False),
    StructField("angle_actual", DoubleType(), False),
    StructField("angle_target", DoubleType(), False),
    StructField("rpm", DoubleType(), False),
    StructField("torque", DoubleType(), False),
    StructField("motor_temp", DoubleType(), False),
    StructField("ambient_temp", DoubleType(), False),
    StructField("wind_speed", DoubleType(), False),
    StructField("irradiance", DoubleType(), False),
    StructField("fault_code", StringType(), False)
])

# Data generator with motor/environmental monitoring specs
dataspec = (
    dg.DataGenerator(spark, name="iot_data", partitions=PARTITIONS)
    .withSchema(iot_data_schema)
    .withColumnSpec("site_id", minValue=1000000, maxValue=4000000, prefix="DEV_", random=True)
    .withColumnSpec("temperature", minValue=-10.0, maxValue=40.0, random=True)
    .withColumnSpec("battery_level", minValue=0, maxValue=100, random=True)
    .withColumnSpec("signal_strength", minValue=-100, maxValue=0, random=True)
    .withColumnSpec("angle_actual", minValue=0.0, maxValue=180.0, random=True)
    .withColumnSpec("angle_target", expr="angle_actual + (rand()-0.5)*10")
    .withColumnSpec("rpm", minValue=0.0, maxValue=60.0, random=True)
    .withColumnSpec("torque", minValue=0.0, maxValue=500.0, random=True)
    .withColumnSpec("motor_temp", minValue=15.0, maxValue=85.0, random=True)
    .withColumnSpec("ambient_temp", minValue=-5.0, maxValue=45.0, random=True)
    .withColumnSpec("wind_speed", minValue=0.0, maxValue=25.0, random=True)
    .withColumnSpec("irradiance", minValue=0.0, maxValue=1200.0, random=True)
    .withColumnSpec("fault_code", 
                   values=["OK","E01_motor_overload","E02_sensor_fail","E03_comm_loss"],
                   weights=[0.85,0.05,0.05,0.05], random=True)
)

# Build high-throughput streaming DataFrame (3M rows/sec)
streaming_df = (
    dataspec.build(
        withStreaming=True,
        options={
            'rowsPerSecond': ROWS_PER_SECOND,
            'numPartitions': PARTITIONS,
        }
    )
    # Deterministic tracker_row based on site_id hash for 1000x compression
    .withColumn("tracker_row", 
                expr("abs(hash(site_id)) % 1000 + 1"))
    # More realistic timestamp
    .withColumn("event_timestamp", 
                expr("current_timestamp() - make_interval(0, 0, 0, 0, 0, 0, floor(rand() * 120))"))
    
    # Essential derived columns only (minimal overhead for performance)
    .withColumn("is_battery_low", when(col("battery_level") < 20, True).otherwise(False))
    .withColumn("angle_deviation", expr("abs(angle_actual - angle_target)"))
    .withColumn("has_fault", when(col("fault_code") != "OK", True).otherwise(False))
)

# Display the optimized streaming DataFrame
# display(streaming_df)

# COMMAND ----------

#display(streaming_df)

# COMMAND ----------

# Write the streaming data to a Parquet Table
(
    streaming_df.writeStream
        .queryName("iot_data_stream_1Million_events_per_second")  # Assign a name to the stream
        .outputMode("append")
        .format('parquet')
        .option("checkpointLocation", f"/Volumes/soni/default/checkpoints/iot_data_checkpoint_1million_events_per_second-{uuid.uuid4()}")
        .start("/Volumes/soni/default/streaming_writes/synthetic_data_1million_events_per_second/")
        )

# COMMAND ----------

#dbutils.fs.rm("/Volumes/soni/default/streaming_writes/synthetic_data_1million_events_per_second/", True)

# COMMAND ----------


