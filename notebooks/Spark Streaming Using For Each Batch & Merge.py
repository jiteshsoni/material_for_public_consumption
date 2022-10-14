# Databricks notebook source
# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

target_table_name = "to_be_merged_into_table"
check_point_location = f"/tmp/delta/{target_table_name}/_checkpoints/"
join_column_name ="hash"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate data at your desired data

# COMMAND ----------

generated_df = (
     spark.readStream
        .format("rate")
        .option("numPartitions", 100)
        .option("rowsPerSecond", 10 * 1000)
        .load()
        .selectExpr(
          "md5( CAST (value AS STRING) ) as md5"
          ,"value"
          ,"value%1000000 as hash"
        )
)

#display(generated_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Empty Delta table so data could me merged into it

# COMMAND ----------

#spark.sql(f"""  DROP TABLE IF EXISTS {target_table_name};""")
(  
  generated_df.writeStream
  .format("delta")
  .outputMode("append").trigger(once=True)
  .option("checkpointLocation", check_point_location)
  .toTable(target_table_name)
)


# COMMAND ----------

display(spark.read.table(target_table_name))
#spark.read.table(target_table_name).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize/ Z-order a Delta table

# COMMAND ----------

from timeit import default_timer as timer



def optimize_and_zorder_table(table_name: str, zorder_by_col_name: str) -> None:
    """This runs an optimize and zorder command on a given table that being fed by a stream
        - These commands can't run in silo because it will require us to pause and then resume stream
        - Therefore, we need to call this function as a part of the upsert function. This enables us to optimize before the next batch of streaming data comes through.

    Parameters:
         table_name: str
                 name of the table to be optimized
         zorder_by_col_name: str
                 comma separated list of columns to zorder by. example "col_a, col_b, col_c"
    """
    start = timer()
    print(f"Met condition to optimize table {table_name}")
    sql_query_optimize = f"OPTIMIZE  {table_name} ZORDER BY ({zorder_by_col_name})"
    spark.sql(sql_query_optimize)
    end = timer()
    time_elapsed_seconds = end - start
    print(
        f"Successfully optimized table {table_name} . Total time elapsed: {time_elapsed_seconds} seconds"
    )

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Logic for the micro-batch

# COMMAND ----------

def make_changes_using_the_micro_batch(microBatchOutputDF, batchId: int):
    print(f"Processing batchId: {batchId}")
    microBatchOutputDF.createOrReplaceTempView("updates")
    spark_session_for_this_micro_batch = microBatchOutputDF._jdf.sparkSession()
    spark_session_for_this_micro_batch.sql(f"""
      SELECT * 
      FROM (
        select *
          ,rank() over(partition by {join_column_name} order by value desc) as dedupe
        from updates
        )
      WHERE 
          dedupe =1 
   """).drop("dedupe").createOrReplaceTempView("updates_which_need_to_be_merged")
    spark_session_for_this_micro_batch.sql(f"""
    MERGE INTO {target_table_name} target
    using updates_which_need_to_be_merged u
    on u.{join_column_name} = target.{join_column_name} 
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)
    optimize_every_n_batches = 20
    #Define how often should optimize run? for example: at 50, it means that we will run the optimize command every 50 batches of stream data
    if batchId % optimize_every_n_batches == 0:
        optimize_and_zorder_table(table_name = target_table_name,  zorder_by_col_name = join_column_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orchestrate from readStream -> Merge -> Optimize

# COMMAND ----------

(
  generated_df
 .writeStream.format('delta')
 #.trigger(availableNow=True) 
 .trigger(processingTime='10 seconds')
 .option("checkpointLocation", check_point_location)
 .foreachBatch(make_changes_using_the_micro_batch)
 .start()
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Use the target table as a source for the next streaming pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC #### Enable Change Data Feed 
# MAGIC Change data feed allows Databricks to track row-level changes between versions of a Delta table. When enabled on a Delta table, the runtime records change events for all the data written into the table. This includes the row data along with metadata indicating whether the specified row was inserted, deleted, or update
# MAGIC 
# MAGIC Reference: https://docs.databricks.com/delta/delta-change-data-feed.html#use-delta-lake-change-data-feed-on-databricks 

# COMMAND ----------

spark.sql(f'''
ALTER TABLE {target_table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed=true)
''')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading change data as a stream
# MAGIC https://docs.databricks.com/delta/delta-change-data-feed.html#read-changes-in-streaming-queries

# COMMAND ----------

# not providing a starting version/timestamp will result in the latest snapshot being fetched first
display(
   spark.readStream.format("delta") 
  .option("readChangeFeed", "true") 
  .table(target_table_name)
)
