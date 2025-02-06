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
from pyspark.sql.functions import col, when, lit, current_timestamp, date_format, hash, abs
from pyspark.sql.functions import min, max
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable
import logging
from typing import List
import uuid
from timeit import default_timer as timer

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "16")
spark.conf.set("spark.databricks.streaming.forEachBatch.optimized.enabled","true")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

catalog_dot_database_name = "soni.default"
target_table = f"{catalog_dot_database_name}.iot_data_liquid_merge"
keys_to_merge_on = ["device_id", "event_timestamp_string"]
# Ideally the checkpoint should have a fixed location, I have kept mine dynamic because I am doing experiments
checkpoint_location = f"/tmp/{target_table}/_checkpoint_{uuid.uuid4()}/"
source_table = f"{catalog_dot_database_name}.iot_data_to_be_merge"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enable Deletion Vectors because they make merges faster

# COMMAND ----------

# display(
#     spark.sql(
#         f"""
#       ALTER TABLE {target_table} SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);
# """
#     )
# )

# COMMAND ----------

# MAGIC %md ### Enable Liquid Clustering so that we can merge faster

# COMMAND ----------

# sql_to_enable_liquid_clustering = f"""
#       ALTER TABLE {target_table} CLUSTER BY ({", ".join(keys_to_merge_on)});
#       """
# print(sql_to_enable_liquid_clustering)

# COMMAND ----------

# display(spark.sql(sql_to_enable_liquid_clustering))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Verify if everything went into affect

# COMMAND ----------

display(
    spark.sql(
        f"""
            DESCRIBE DETAIL  {target_table}
            """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Streaming Source. It could be Kafka, Kinesis, S3, ADLS, Delta. For my case, I am considering Delta

# COMMAND ----------

stream_source_df = (
    spark.readStream.option("maxFilesPerTrigger", 8)
    .table(source_table)
)
# One-liner: Combine row selection and timestamp formatting directly in withColumn.
column_to_update = "event_timestamp_string"
# The reason we are doing this is to create a mix of updates and inserts to reflect real world scenario
stream_source_updated_df = stream_source_df.withColumn(
    column_to_update,
    when(
        abs(hash(col(column_to_update))) % 2 == 0,
        date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"),
    ).otherwise(col(column_to_update)),
)
# display(stream_source_updated_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Class so that we can pass parameters and do any custom processing

# COMMAND ----------

class forEachBatchProcessor:
    def __init__(self, target_table: int):
        self.target_table = target_table

    def print_attributes(self):
        attributes = vars(self)
        print("\n".join([f"{attr}: {value}" for attr, value in attributes.items()]))

    def make_changes_using_the_micro_batch(self, microBatchOutputDF, batchId: int):
        self.print_attributes()
        print(f"Processing batchId: {batchId}")
        spark_session_for_this_micro_batch =  microBatchOutputDF.sparkSession
        #spark_session_for_this_micro_batch.conf.set("spark.sql.adaptive.enabled", "true")

        # Your processing logic using the parameter
        view_name = f"updates_for_batchId_{batchId}"
        microBatchOutputDF.limit(1000).dropDuplicates(["device_id","event_timestamp_string"]).createOrReplaceTempView(view_name)

        sql_for_merge = f"""
          MERGE WITH SCHEMA EVOLUTION INTO {self.target_table} target
          USING {view_name} source
          ON source.device_id = target.device_id
            AND source.event_timestamp_string = target.event_timestamp_string
          WHEN MATCHED THEN
            UPDATE SET *
          WHEN NOT MATCHED THEN
            INSERT *
        """
        print(f"sql_for_merge: \n{sql_for_merge}")
        spark_session_for_this_micro_batch.sql(sql_for_merge)
        # microBatchOutputDF.sparkSession.sql(sql_logic).drop("dedupe")

        optimize_every_n_batches = 20
        # Define how often should optimize run? for example: at 20, it means that we will run the optimize command every 50 batches of stream data
        # This mean 1 out of 10 batches would be slow; aka 5% of the batches would be slower
        if batchId % optimize_every_n_batches == 0:
            self.optimize_and_liquid_cluster_table(table_name=self.target_table)

    @staticmethod
    def optimize_and_liquid_cluster_table(table_name: str) -> None:
        """
        Parameters:
            table_name: str
                    name of the table to be optimized
        """
        start = timer()
        print(f"Met condition to optimize table {table_name}")
        sql_query_optimize = f"OPTIMIZE {table_name}"
        spark.sql(sql_query_optimize)
        end = timer()
        time_elapsed_seconds = end - start
        print(
            f"Successfully optimized table {table_name} . Total time elapsed: {time_elapsed_seconds} seconds"
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create an instance of forEachBatchProcessor Class with the parameters

# COMMAND ----------

instantiateForEachBatchProcessor = forEachBatchProcessor(
    target_table=target_table,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orchestrate the job

# COMMAND ----------

(
    stream_source_updated_df.writeStream
    # .trigger(availableNow=True)
    .trigger(processingTime="5 seconds")
    .option("checkpointLocation", checkpoint_location)
    .option("queryName", "ParameterizeForEachBatch")
    .foreachBatch(instantiateForEachBatchProcessor.make_changes_using_the_micro_batch)
    .start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC --OPTIMIZE soni.default.iot_data_liquid_merge FULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC --OPTIMIZE soni.default.iot_data_liquid_merge

# COMMAND ----------


