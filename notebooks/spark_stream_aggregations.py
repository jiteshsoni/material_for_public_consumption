# Databricks notebook source
# MAGIC %md
# MAGIC [You can enable RockDB-based state management by setting the following configuration in the SparkSession before starting the streaming query](https://docs.databricks.com/structured-streaming/rocksdb-state-store.html#configure-rocksdb-state-store-on-databricks)
# MAGIC
# MAGIC
# MAGIC [Why are altering spark.shuffle.service.enabled](https://kb.databricks.com/jobs/job-fails-with-spark-shuffle-fetchfailedexception-error)

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuring RocksDB state store
# MAGIC spark.conf.set(
# MAGIC     "spark.sql.streaming.stateStore.providerClass",
# MAGIC     "com.databricks.sql.streaming.state.RocksDBStateStoreProvider"
# MAGIC )
# MAGIC
# MAGIC #spark.conf.set("spark.shuffle.service.enabled", "false")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Libraries
# MAGIC Install Faker which is only needed for the purope of this demo.

# COMMAND ----------

!pip install faker_vehicle
!pip install faker

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from faker import Faker
from faker_vehicle import VehicleProvider
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import uuid
from utils import logger
from pyspark.sql.streaming import StreamingQuery

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# define schema name and where should the table be stored
schema_name = "test_streaming_joins"
schema_storage_location = "/tmp/CHOOSE_A_PERMANENT_LOCATION/"
target_table = f"{schema_name}.streaming_aggregation"
checkpoint_location= f"{schema_storage_location}{target_table}/_checkpoint/",
silver_target_table=  f"{schema_name}.silver_streaming"
silver_checkpoint_locattion = f"{schema_storage_location}{silver_target_table}/_checkpoint/",
column_to_watermark_on = "timestamp"
how_late_can_the_data_be = "3 minutes"

# COMMAND ----------

# MAGIC %md
# MAGIC # Create the Target Schema/Database
# MAGIC Create a Schema and set location. This way all tables would inherit the base location.

# COMMAND ----------

checkpoint_location

# COMMAND ----------

create_schema_sql = f"""
    CREATE SCHEMA IF NOT EXISTS {schema_name}
    COMMENT 'This is {schema_name} schema'
    LOCATION '{schema_storage_location}';
"""
logger.info(f"Executing SQL: {create_schema_sql}")

# COMMAND ----------

spark.sql(create_schema_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC # Generate Fake Data at Scale

# COMMAND ----------

# MAGIC %md #### Use Faker to define functions to help generate fake column values

# COMMAND ----------

# Using Faker to define functions for fake data generation
fake = Faker()
fake.add_provider(VehicleProvider)

event_id = F.udf(lambda: str(uuid.uuid4()), StringType())

vehicle_year_make_model = F.udf(fake.vehicle_year_make_model)
vehicle_year_make_model_cat = F.udf(fake.vehicle_year_make_model_cat)
vehicle_make_model = F.udf(fake.vehicle_make_model)
vehicle_make = F.udf(fake.vehicle_make)
vehicle_model = F.udf(fake.vehicle_model)
vehicle_year = F.udf(fake.vehicle_year)
vehicle_category = F.udf(fake.vehicle_category)
vehicle_object = F.udf(fake.vehicle_object)

latitude = F.udf(fake.latitude)
longitude = F.udf(fake.longitude)
location_on_land = F.udf(fake.location_on_land)
local_latlng = F.udf(fake.local_latlng)
zipcode = F.udf(fake.zipcode)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Streaming source data at your desired rate

# COMMAND ----------

# Generate streaming data
def generated_vehicle_and_geo_df(rowsPerSecond: int =100, numPartitions: int = 10):
    logger.info("Generating vehicle and geo data frame...")
    return (
        spark.readStream.format("rate")
        .option("numPartitions", numPartitions)
        .option("rowsPerSecond", rowsPerSecond)
        .load()
        .withColumn("event_id", event_id())
        .withColumn("vehicle_year_make_model", vehicle_year_make_model())
        .withColumn("vehicle_year_make_model_cat", vehicle_year_make_model_cat())
        .withColumn("vehicle_make_model", vehicle_make_model())
        .withColumn("vehicle_make", vehicle_make())
        .withColumn("vehicle_year", vehicle_year())
        .withColumn("vehicle_category", vehicle_category())
        .withColumn("vehicle_object", vehicle_object())
        .withColumn("latitude", latitude())
        .withColumn("longitude", longitude())
        .withColumn("location_on_land", location_on_land())
        .withColumn("local_latlng", local_latlng())
        .withColumn("zipcode", zipcode())
    )

# You can uncomment the below display command to check if the code in this cell works
display(generated_vehicle_and_geo_df())


# COMMAND ----------


def streaming_aggregation(rows_per_second: int = 100, 
                          num_partitions: int = 10,
                          how_late_can_the_data_be :str = "30 minutes",
                          window_duration: str = "1 minutes",
                          checkpoint_location: str = checkpoint_location,
                          output_table_name: str = target_table) -> StreamingQuery:
    """
    Aggregate streaming data and write to a Delta table.
    
    Parameters:
    - rows_per_second (int): Number of rows per second for generated data.
    - num_partitions (int): Number of partitions for the generated data.
    - window_duration (str): Window duration for aggregation.
    - checkpoint_location (str): Path for checkpointing.
    - output_table_name (str): Name of the output Delta table.

    Returns:
    - StreamingQuery: Spark StreamingQuery object.
    """
    
    logger.info("Starting streaming aggregation...")

    raw_stream = generated_vehicle_and_geo_df(rows_per_second, num_partitions)

    aggregated_data = (
        raw_stream
        .withWatermark(column_to_watermark_on, how_late_can_the_data_be)
        .groupBy(
            F.window(column_to_watermark_on, window_duration),
            "zipcode"
        )
        .agg(
            F.min("vehicle_year").alias("oldest_vehicle_year")
        )
    )

    query = (
        aggregated_data
        .writeStream
        .queryName(f"write_stream_to_delta_table: {output_table_name}")
        .format("delta")
        .option("checkpointLocation", checkpoint_location)
        .outputMode("append")
        .toTable(output_table_name)
          # This actually starts the streaming job
    )
    
    logger.info(f"Streaming query started with ID: {query.id}")
    logger.info(f"Current status of the query: {query.status}")
    logger.info(f"Recent progress updates: {query.recentProgress}")

    # If you want to programmatically stop the query after some condition or time, you can do so. 
    # For the sake of this example, I am NOT stopping it. But if you need to, you can invoke:
    # query.stop()
    # logger.info("Streaming query stopped.")

    return query



streaming_aggregation(
   checkpoint_location = checkpoint_location,
   output_table_name = target_table,
   how_late_can_the_data_be = "5 minutes"
)



# COMMAND ----------

df = spark.sql(f"""
               SELECT * 
               FROM {target_table}
               """)
print(df.count())
display(df)

# COMMAND ----------

display(
  spark.sql(f"""
select count( zipcode) 
 from {target_table}
 """))


# COMMAND ----------

display(
  spark.sql(f"""
DESCRIBE HISTORY {target_table}
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream from the Table we just aggregted to another Stream. In this scerario, we will do Delta to Delta Streaming

# COMMAND ----------


checkpoint_location
display(
  spark.readStream.table(target_table).select("zipcode","window")
   .writeStream
        .queryName(f"write_stream_to_delta_table: {silver_target_table}")
        .format("delta")
        .option("checkpointLocation", silver_checkpoint_locattion)
        .outputMode("append")
        .toTable(silver_target_table)
)

# COMMAND ----------

display(
  spark.readStream.table(silver_target_table)
)

# COMMAND ----------

display(
  spark.sql(f"""
DESCRIBE HISTORY {silver_target_table}
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean up the databse which was created

# COMMAND ----------

# Clean up
spark.sql(f"drop schema if exists {schema_name} CASCADE")
logger.info(f"Dropped schema: {schema_name}")

dbutils.fs.rm(schema_storage_location, True)
logger.info(f"Removed storage location: {schema_storage_location}")

# COMMAND ----------

dbutils.fs.rm(f"{checkpoint_location}", recurse=True)

# COMMAND ----------

dbutils.fs.rm(f"{silver_checkpoint_locattion}", recurse=True)

# COMMAND ----------


