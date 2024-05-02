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
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable
import logging
import time


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
target_table = f"{catalog_dot_database_name}.cre"
agg_events_table = f"{catalog_dot_database_name}.streaming_agg_events_with_merge"
requests_bids_table = f"{catalog_dot_database_name}.requests_bids"
checkpoint_location = f"/tmp/{target_table}/_checkpoint_appends/"
checkpoint_location_for_update =  f"/tmp/{target_table}/_checkpoint_update/"
delay_minutes_between_pipelines = 5

# COMMAND ----------

display(spark.sql(f"SHOW TABLES FROM {catalog_dot_database_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up Source Stream

# COMMAND ----------

agg_events_with_watermark_df = spark.readStream.option('skipChangeCommits' , 'true').table(agg_events_table).withWatermark("ingestion_timestamp", "15 minutes")
#display(agg_events_with_watermark_df)

# COMMAND ----------

requests_bids_with_watermark_df = spark.readStream.table(requests_bids_table).withWatermark("event_ts", "15 minutes")
#display(requests_bids_with_watermark_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream Stream Left Join

# COMMAND ----------

requests_bids_with_watermark_df.createOrReplaceTempView("requests_bids")
agg_events_with_watermark_df.createOrReplaceTempView("agg_events")



# COMMAND ----------

cre_df = spark.sql(f'''
                   SELECT 
                       agg_events.ingestion_timestamp as events_ingestion_timestamp
                      ,'insert' as insert_or_update
                      ,requests_bids.* 
                      ,collected_events
                   FROM requests_bids 
                   LEFT JOIN agg_events
                   ON requests_bids.request_id = agg_events.request_id 
                   AND requests_bids.event_ts <= agg_events.ingestion_timestamp + interval 15 minutes
                   AND requests_bids.event_ts >= agg_events.ingestion_timestamp
                   ''')
#display(cre_df)

# COMMAND ----------

(
    cre_df
     .writeStream
      #.trigger(availableNow=True)
        .trigger(processingTime='60 seconds')
        .queryName(f"write_to_delta_table: {target_table} after stream-stream left join just appends")
        .option("checkpointLocation", checkpoint_location)
        .format("delta")
        .outputMode("append")
        .toTable(target_table)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read from agg_events and then Merge
# MAGIC

# COMMAND ----------


spark.sql(f"""
          ALTER TABLE {target_table} SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);
          """)

# COMMAND ----------


class updateCre:
    def __init__(self, target_table: str, minutes_delay_between_update_and_insert_pipelines: int):
        self.target_table = target_table,
        self.minutes_delay_between_update_and_insert_pipelines = minutes_delay_between_update_and_insert_pipelines

    def make_changes_using_the_micro_batch(self, microBatchOutputDF, batchId: int):
        """Process each micro-batch and perform aggregation and merge/upsert operations."""
        count_of_updates = microBatchOutputDF.count()
        if count_of_updates >0:
            print (f"count_of_updates : {count_of_updates}" )
            ## Collect stats of the target table
            max_events_ingestion_timestamp_df = spark.read.table(target_table).where("insert_or_update = 'insert'").selectExpr(" max(events_ingestion_timestamp) as max_events_ingestion_timestamp")
            max_events_ingestion_timestamp_for_target = max_events_ingestion_timestamp_df.collect()[0]['max_events_ingestion_timestamp']
            print(f"Append pipeline has ingested as far as {str(max_events_ingestion_timestamp_for_target)}")
            
            ## Collect stats for the microBatchOutputDF
            timestamp_stats_for_updates = microBatchOutputDF.selectExpr("max(ingestion_timestamp) as max_events_ingestion_timestamp")
            # Step 2: Extract Values into Python Variables
            # To extract the min and max values into Python variables, you can use the collect() method:
            max_events_ingestion_timestamp_for_updates = timestamp_stats_for_updates.collect()[0]["max_events_ingestion_timestamp"]
            print(f"max_events_ingestion_timestamp_for_updates: {max_events_ingestion_timestamp_for_updates}")

            seconds_since_last_append_in_target = (max_events_ingestion_timestamp_for_target - max_events_ingestion_timestamp_for_updates).total_seconds()
            minutes_since_last_append_in_target = seconds_since_last_append_in_target/60
            print(f"minutes_since_last_append_in_target: {minutes_since_last_append_in_target}")

            if minutes_since_last_append_in_target < self.minutes_delay_between_update_and_insert_pipelines and seconds_since_last_append_in_target > 0:
                time.sleep(seconds_since_last_append_in_target)

            print(f"Given that minutes_since_last_append_in_target {minutes_since_last_append_in_target} is greater than the delay_minutes_between_update_and_insert_pipelines {self.minutes_delay_between_update_and_insert_pipelines} thus we will procceed with merge")    
            dedupe_updates_df = microBatchOutputDF.drop_duplicates(["request_id"])
            dedupe_updates_df.createOrReplaceTempView("deduped_updates")
            spark_session_for_this_micro_batch = microBatchOutputDF.sparkSession
            merge_sql = f"""
            MERGE INTO {target_table} AS target
            USING deduped_updates updates ON target.request_id = updates.request_id
            WHEN MATCHED THEN
                UPDATE SET 
                    target.collected_events = updates.collected_events,
                    target.insert_or_update = 'update'
            """
            spark_session_for_this_micro_batch.sql(merge_sql)

                      

# COMMAND ----------

target_table

# COMMAND ----------


class TableUpdater:
    def __init__(self, target_table: str, delay_minutes_between_pipelines: int):
        self.target_table = target_table
        self.delay_minutes_between_pipelines = delay_minutes_between_pipelines

    def process_micro_batch(self, microBatchOutputDF, batchId: int):
        """Process each micro-batch and perform aggregation and merge/upsert operations."""
        microBatchOutputDF.cache()
        update_count = microBatchOutputDF.count()
        if update_count > 0:
            print(f"Update count: {update_count}")

            # Collect stats of the target table
            spark_session = microBatchOutputDF.sparkSession
            max_ingestion_df = spark_session.read.table(self.target_table)\
                                             .where("insert_or_update = 'insert'")\
                                             .selectExpr("max(events_ingestion_timestamp) as max_ingestion_timestamp")
            max_ingestion_timestamp_target = max_ingestion_df.collect()[0]['max_ingestion_timestamp']
            print(f"Append pipeline ingested up to: {max_ingestion_timestamp_target}")

            # Collect stats for micro_batch_output_df
            timestamp_stats_df = microBatchOutputDF.selectExpr("max(ingestion_timestamp) as max_ingestion_timestamp")
            max_ingestion_timestamp_updates = timestamp_stats_df.collect()[0]["max_ingestion_timestamp"]
            print(f"Max ingestion timestamp for updates: {max_ingestion_timestamp_updates}")

            time_difference_seconds = (max_ingestion_timestamp_target - max_ingestion_timestamp_updates).total_seconds()
            time_difference_minutes = time_difference_seconds / 60
            print(f"Minutes since last append in target: {time_difference_minutes}")

            if time_difference_minutes < self.delay_minutes_between_pipelines and time_difference_seconds > 0:
                time.sleep(time_difference_seconds)
                print("Delay added to synchronize with the append pipeline.")

            print("Proceeding with merge operation.")
            deduped_updates_df = microBatchOutputDF.drop_duplicates(["request_id"])
            deduped_updates_df.createOrReplaceTempView("deduped_updates")

            merge_sql = f"""
            MERGE INTO {self.target_table} AS target
            USING deduped_updates AS updates ON target.request_id = updates.request_id
            WHEN MATCHED THEN
                UPDATE SET 
                    target.collected_events = updates.collected_events,
                    target.insert_or_update = 'update'
            """
            spark_session.sql(merge_sql)
            microBatchOutputDF.unpersist()

# COMMAND ----------

instantiateTableUpdater = TableUpdater(target_table=target_table,
                                 delay_minutes_between_pipelines=delay_minutes_between_pipelines)

# COMMAND ----------

agg_events_stream = spark.readStream.option("readChangeFeed", "true").option("maxFilesPerTrigger","10").table(agg_events_table).where("_change_type = 'update_postimage' ")
#display(agg_events_stream)

# COMMAND ----------

(
  agg_events_stream
  .writeStream
  .trigger(processingTime="1 minute")
  .option("checkpointLocation", checkpoint_location_for_update)
  .option("queryName", f"Update{target_table}")
  .foreachBatch(instantiateTableUpdater.process_micro_batch)
  .start()
)

# COMMAND ----------

display(spark.sql(f"""
                  DESCRIBE HISTORY  {target_table}
                  """))

# COMMAND ----------


