# Databricks notebook source
!pip install faker

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

# Import necessary libraries and modules for the entire script
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, current_timestamp, collect_set
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable
import logging
from faker import Faker

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure logging

# COMMAND ----------


logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s %(levelname)-4s %(name)s][%(funcName)s] %(message)s",
)
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("pyspark").setLevel(logging.INFO)
logger: logging.Logger = logging.getLogger(__name__)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# Initialize Faker for generating fake data
fake = Faker()

# Parameters
catalog_dot_database_name = "soni.default"
target_table = f"{catalog_dot_database_name}.streaming_agg_fake_events_with_merge"
source_table = f"{catalog_dot_database_name}.fake_events"
checkpoint_location = f"/tmp/{target_table}/_checkpoint/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reusable functions

# COMMAND ----------

# Function to validate table name
def is_valid_table_name(table_name: str) -> bool:
    """Check if the table name follows the 3-space naming convention."""
    return len(table_name.split('.')) == 3

# Function to check table existence
def does_table_exist(spark: SparkSession, table_name: str) -> bool:
    """Check if a table exists in the Spark session."""
    if not is_valid_table_name(table_name):
        raise ValueError(f"The table name '{table_name}' does not follow the 3-dot naming convention.")
    try:
        spark.read.table(table_name)
        return True
    except AnalysisException:
        return False


# COMMAND ----------

# MAGIC %md 
# MAGIC ## Function to generate fake data

# COMMAND ----------


def generate_fake_data(spark: SparkSession, num_records: int):
    """Generate fake data and append it to the source table."""
    schema = StructType([
        StructField("request_id", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("mac_id", StringType(), True),
    ])
    data = [(fake.word(), fake.ipv4(), fake.mac_address()) for _ in range(num_records)]
    df = spark.createDataFrame(data, schema=schema).withColumn("event_ts", current_timestamp())
    display(df)
    df.write.mode("append").saveAsTable(source_table)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Class to handle batch processing

# COMMAND ----------


class forEachBatchProcessor:
    def __init__(self, target_table: str):
        self.target_table = target_table

    def make_changes_using_the_micro_batch(self, microBatchOutputDF, batchId: int):
        """Process each micro-batch and perform aggregation and merge/upsert operations."""
        # Aggregate data
        agg_df = microBatchOutputDF.groupBy("request_id").agg(
            collect_set("event_ts").alias("collected_event_ts"),
            collect_set(F.struct(*microBatchOutputDF.columns)).alias("collected_events")
        ).withColumn("ingestion_timestamp", current_timestamp())

        # Merge or create table based on existence
        if does_table_exist(spark, self.target_table):
            delta_table = DeltaTable.forName(spark, self.target_table)
            delta_table.alias("delta").merge(
                agg_df.alias("updates"),
                "delta.request_id = updates.request_id"
            ).whenMatchedUpdate(set={
                "collected_events": F.concat(col("delta.collected_events"), col("updates.collected_events"))
            }).whenNotMatchedInsertAll().execute()
        else:
            agg_df.write.mode("append").saveAsTable(self.target_table)


# COMMAND ----------

instantiateForEachBatchProcessor = forEachBatchProcessor(target_table=target_table)

# COMMAND ----------


generate_fake_data(spark, num_records=5)
events_stream = spark.readStream.table(f"{catalog_dot_database_name}.fake_events")
(
  events_stream
  .writeStream
  .trigger(availableNow=True)
  .option("checkpointLocation", checkpoint_location)
  .option("queryName", f"StreamTo{target_table}")
  .foreachBatch(instantiateForEachBatchProcessor.make_changes_using_the_micro_batch)
  .start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM SONI.default.streaming_agg_fake_events_with_merge
# MAGIC where request_id = '<REPLACE_WITH_REQUEST_ID_WHICH_WE_JUST_INGESTED>'
