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
                      requests_bids.* 
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

# MAGIC %md
# MAGIC spark.sql(f"""
# MAGIC           ALTER TABLE {target_table} SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);
# MAGIC           """)

# COMMAND ----------


class updateCre:
    def __init__(self, target_table: str):
        self.target_table = target_table

    def make_changes_using_the_micro_batch(self, microBatchOutputDF, batchId: int):
        """Process each micro-batch and perform aggregation and merge/upsert operations."""
        print(f"Update collected_events into the target_table {self.target_table}")
        dedupe_updates_df = microBatchOutputDF.drop_duplicates(["request_id"])
        delta_table = DeltaTable.forName(spark, self.target_table)
        delta_table.alias("delta").merge(
            dedupe_updates_df.alias("updates"),
            "delta.request_id = updates.request_id"
        ).whenMatchedUpdate(set={
            "collected_events": col("updates.collected_events")
        }).execute()

# COMMAND ----------

instantiateUpdateCre = updateCre(target_table=target_table)

# COMMAND ----------

display(
  spark.readStream.option("readChangeFeed", "true").table(agg_events_table)
)

# COMMAND ----------

display(
  spark.readStream.option("readChangeFeed", "true").table("soni.default.streaming_agg_fake_events_with_merge").select("_change_type").distinct()
)

# COMMAND ----------

display(
  spark.readStream.option("readChangeFeed", "true").table(agg_events_table).select("_change_type").distinct()
)

# COMMAND ----------

agg_events_stream = spark.readStream.option("readChangeFeed", "true").table(agg_events_table).where("_change_type = 'update_postimage' ")
#display(agg_events_stream)

# COMMAND ----------

(
  agg_events_stream
  .writeStream
  .trigger(processingTime="1 minute")
  .option("checkpointLocation", checkpoint_location_for_update)
  .option("queryName", f"Update{target_table}")
  .foreachBatch(instantiateUpdateCre.make_changes_using_the_micro_batch)
  .start()
)

# COMMAND ----------

display(spark.sql(f"""
                  DESCRIBE HISTORY  {target_table}
                  """))

# COMMAND ----------


