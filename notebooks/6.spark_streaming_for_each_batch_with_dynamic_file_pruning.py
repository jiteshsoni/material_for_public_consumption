# Databricks notebook source
# MAGIC %md
# MAGIC # [Delta Lake - State of the Project ](https://delta.io/blog/state-of-the-project-pt1/?utm_source=bambu&utm_medium=social&blaid=5878484)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

# Import necessary libraries and modules for the entire script
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, current_timestamp, collect_set
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import min, max
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable
import logging


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

catalog_dot_database_name = "soni.default"
target_table = f"{catalog_dot_database_name}.events_left_join_requests_bids_dynamic_file_pruning"
events_table = f"{catalog_dot_database_name}.events"
requests_bids_table = f"{catalog_dot_database_name}.requests_bids"
checkpoint_location = f"/tmp/{target_table}/_checkpoint_appends/"
checkpoint_location_for_update =  f"/tmp/{target_table}/_checkpoint_v6/"

# COMMAND ----------

display(
  spark.sql(f"""
            SHOW TABLES IN {catalog_dot_database_name}
            """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Look at the source table

# COMMAND ----------

display(spark.sql(f"""
                  SELECT * 
                  FROM {events_table}
                  """))

# COMMAND ----------

display(spark.sql(f"""
                  SELECT * 
                  FROM {requests_bids_table}
                  """))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up Source Stream

# COMMAND ----------

events_with_watermark_df = spark.readStream.option("maxFilesPerTrigger",10).table(events_table).withWatermark("event_ts", "2 minutes")
#display(events_with_watermark_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read from agg_events and then Merge
# MAGIC

# COMMAND ----------

class forEachBatchProcessor:
    def __init__(self, target_table: str, table_to_join_with: str):
        self.target_table = target_table
        self.table_to_join_with = table_to_join_with

    def make_changes_using_the_micro_batch(self, microBatchOutputDF, batchId: int):
        """Process each micro-batch and perform operations."""
        timestamp_stats = microBatchOutputDF.select(min("event_ts").alias("min_timestamp"), max("event_ts").alias("max_timestamp"))
        # Step 2: Extract Values into Python Variables
        # To extract the min and max values into Python variables, you can use the collect() method:
        result = timestamp_stats.collect()
        min_timestamp = result[0]["min_timestamp"]
        max_timestamp = result[0]["max_timestamp"]
        print(f"Min timestamp: {min_timestamp}")
        print(f"Max timestamp: {max_timestamp}")

        print(f"Update collected_events into the target_table {self.target_table}")
        microBatchOutputDF.createOrReplaceTempView("updates")
        spark_session_for_this_micro_batch = microBatchOutputDF.sparkSession

        to_be_written_out_df = spark_session_for_this_micro_batch.sql(f"""
                                         SELECT 
                                            updates.*
                                            ,ref.request_id as requests_bids_request_id
                                         FROM updates
                                         LEFT JOIN {self.table_to_join_with} ref
                                         ON updates.request_id = ref.request_id 
                                         WHERE ref.event_ts BETWEEN '{min_timestamp}' AND '{max_timestamp}'
                                         """)
        to_be_written_out_df.write.mode("append").saveAsTable(target_table)


# COMMAND ----------

instantiateForEachBatchProcessor = forEachBatchProcessor(target_table=target_table,
                                                         table_to_join_with =requests_bids_table )

# COMMAND ----------

(
  events_with_watermark_df
  .writeStream
  .trigger(processingTime="1 minute")
  .option("checkpointLocation", checkpoint_location_for_update)
  .option("queryName", f"SparkStreamingForEachBatchWithAJoinDynamicFilePruning{target_table}")
  .foreachBatch(instantiateForEachBatchProcessor.make_changes_using_the_micro_batch)
  .start()
)

# COMMAND ----------

display(spark.sql(f"""
                  DESCRIBE HISTORY  {target_table}
                  """))

# COMMAND ----------

dbutils.fs.rm(checkpoint_location_for_update, True)

# COMMAND ----------


