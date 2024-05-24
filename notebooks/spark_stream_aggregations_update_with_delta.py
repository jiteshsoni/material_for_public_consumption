# Databricks notebook source
# MAGIC %md
# MAGIC [You can enable RockDB-based state management by setting the following configuration in the SparkSession before starting the streaming query](https://docs.databricks.com/structured-streaming/rocksdb-state-store.html#configure-rocksdb-state-store-on-databricks)
# MAGIC
# MAGIC
# MAGIC [Why are altering spark.shuffle.service.enabled](https://kb.databricks.com/jobs/job-fails-with-spark-shuffle-fetchfailedexception-error)

# COMMAND ----------


# Configuring RocksDB state store
spark.conf.set(
    "spark.sql.streaming.stateStore.providerClass",
    "com.databricks.sql.streaming.state.RocksDBStateStoreProvider"
)

#spark.conf.set("spark.shuffle.service.enabled", "false")

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
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql import SparkSession
from utils import logger

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# define schema name and where should the table be stored
catalog_name = "shared"
schema_name = "test_streaming_agg_update"
target_table = f"{catalog_name}.{schema_name}.streaming_agg_update_delta"

checkpoint_location= f"/tmp/{target_table}/_checkpoint/",

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
    CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}
    COMMENT 'This is {schema_name} schema'
"""
logger.info(f"Executing SQL: {create_schema_sql}")


# COMMAND ----------

spark.sql(create_schema_sql)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Create empty Delta table

# COMMAND ----------

create_table_sql = f"""
 CREATE TABLE IF NOT EXISTS {target_table}
  USING DELTA
  AS (
    SELECT
      named_struct('start', NULL, 'end', NULL) AS window,
      NULL AS zipcode,
      '' AS collected_vehicle_year
    WHERE 1 = 0
)
"""
logger.info(f"Executing SQL: {create_table_sql}")

# COMMAND ----------

spark.sql(create_table_sql)

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
def generated_vehicle_and_geo_df(rowsPerSecond: int =100, numPartitions: int = 200):
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
#display(generated_vehicle_and_geo_df())


# COMMAND ----------

class forEachBatchProcessor:
    def __init__(self, join_column: str, target_table:str):
        self.join_column = join_column
        self.target_table = target_table

    def print_attributes(self):
        attributes = vars(self)
        print(
            "\n".join([f"{attr}: {value}" for attr, value in attributes.items()])
        )

    def make_changes_using_the_micro_batch(self, microBatchOutputDF, batchId: int):
        self.print_attributes()
        print(f"Processing batchId: {batchId}")
        # Your processing logic using the parameter
        view_name = f"updates_for_batchId_{batchId}"
        microBatchOutputDF.dropDuplicates([self.join_column]).createOrReplaceTempView(view_name)
        sql_logic = f"""
                MERGE INTO {self.target_table} t
                USING {view_name} s
                ON s.{self.join_column} = t.{self.join_column}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """
        print(f"Processing sql_logic: {sql_logic}")
        to_be_written_df = microBatchOutputDF.sparkSession.sql(sql_logic)
        # to_be_written_df.write.mode("append").saveAsTable(target_table_name)

# COMMAND ----------

instantiateForEachBatchProcessor = forEachBatchProcessor(
            join_column = "zipcode",
            target_table = target_table,
        )

# COMMAND ----------


def streaming_aggregation(rows_per_second: int = 100, 
                          num_partitions: int = 20,
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
            F.concat_ws(",", F.collect_set("vehicle_year")).alias("collected_vehicle_year")
        )
    )
    query = (
            aggregated_data
            .writeStream
            .queryName(f"write_stream_to_delta_table: {output_table_name}")
            .option("checkpointLocation", checkpoint_location)
            .foreachBatch(instantiateForEachBatchProcessor.make_changes_using_the_micro_batch)
            .outputMode("update").start()
        )
        
    logger.info(f"Streaming query started with ID: {query.id}")
    logger.info(f"Current status of the query: {query.status}")
    logger.info(f"Recent progress updates: {query.recentProgress}")

    # return query



# COMMAND ----------


streaming_aggregation(
   checkpoint_location = checkpoint_location,
   output_table_name = target_table,
   how_late_can_the_data_be = "1 minutes"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Check the output table

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
