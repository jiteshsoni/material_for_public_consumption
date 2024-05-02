# Databricks notebook source
# MAGIC %md
# MAGIC # [Delta Lake - State of the Project ](https://delta.io/blog/state-of-the-project-pt1/?utm_source=bambu&utm_medium=social&blaid=5878484)
# MAGIC
# MAGIC ## [Enable Deletion Vectors](https://docs.databricks.com/en/delta/deletion-vectors.html)
# MAGIC ## [Enable Change Data Feed](https://docs.databricks.com/en/delta/delta-change-data-feed.html)

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
import logging, time

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

# MAGIC %sql
# MAGIC show tables in soni.default

# COMMAND ----------



# Parameters
catalog_dot_database_name = "soni.default"
target_table = f"{catalog_dot_database_name}.streaming_agg_events_with_merge"
source_table = f"{catalog_dot_database_name}.events"
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
# MAGIC ## Class to handle batch processing

# COMMAND ----------


class forEachBatchProcessor:
    def __init__(self, target_table: str):
        self.target_table = target_table

    def make_changes_using_the_micro_batch(self, microBatchOutputDF, batchId: int):
        """Process each micro-batch and perform aggregation and merge/upsert operations."""
        # Aggregate data
        agg_df = microBatchOutputDF.groupBy("request_id").agg(
            collect_set(F.struct(*microBatchOutputDF.columns)).alias("collected_events")
        ).withColumn("ingestion_timestamp", current_timestamp())

        print(f"Writing to target_table {self.target_table}")

        # Merge or create table based on existence
        if does_table_exist(spark, self.target_table):
            delta_table = DeltaTable.forName(spark, self.target_table)
            delta_table.alias("delta").merge(
                agg_df.alias("updates"),
                "delta.request_id = updates.request_id"
            ).whenMatchedUpdate(set={
                "collected_events": F.concat(col("delta.collected_events"), col("updates.collected_events")),
                "ingestion_timestamp": col("updates.ingestion_timestamp")
            }).whenNotMatchedInsertAll().execute()
        else:
            agg_df.write.mode("append").saveAsTable(self.target_table)


# COMMAND ----------

instantiateForEachBatchProcessor = forEachBatchProcessor(target_table=target_table)

# COMMAND ----------



events_stream = spark.readStream.table(source_table)
(
  events_stream
  .writeStream
  .trigger(processingTime="60 seconds")
  .option("checkpointLocation", checkpoint_location)
  .option("queryName", f"StreamTo{target_table}")
  .foreachBatch(instantiateForEachBatchProcessor.make_changes_using_the_micro_batch)
  .start()
)

# COMMAND ----------

display(
    spark.sql(f"""
      SELECT size(collected_events) as number_of_events_on_request_id ,  *
      FROM {target_table}
      order by size(collected_events) desc
""")
)

# COMMAND ----------

spark.sql(f"""
          ALTER TABLE {target_table} SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);
          """)

# COMMAND ----------

spark.sql(f"""
          ALTER TABLE {target_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
          """)

# COMMAND ----------


