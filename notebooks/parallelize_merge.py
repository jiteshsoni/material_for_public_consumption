# Databricks notebook source
from utils import logger
import asyncio

# COMMAND ----------

# MAGIC %md
# MAGIC ## Append to a single table

# COMMAND ----------

async def append_to_delta_table( index: int, loop_number: int= 0):
    target_table_name = "parallel_appends"
    task_name = f"{loop_number}_{index}"
    logger.info(f"Starting task {task_name} for table_name: {target_table_name}")
    # Create the data for the DataFrame
    data = [{'task_name': '{task_name}'}]
    df = spark.createDataFrame(data)
    (   
        df
        .write.format("delta")
        .mode("append")
        .saveAsTable(target_table_name)
    )
    logger.info(f"Write completed for table_name: {target_table_name}")

# COMMAND ----------

async def do_n_appends_to_a_delta_table(n: int = 10):
    task_list = list()
    for index in range(n):
        task_list.append(asyncio.create_task(append_to_delta_table(index)))
    logger.info(f"task_list:  {task_list}")
    await_completion_of_all_tasks = await asyncio.gather(*task_list)


await do_n_appends_to_a_delta_table()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate data at your desired data

# COMMAND ----------

generated_stream_df = (
    spark.readStream.format("rate")
    .option("numPartitions", 100)
    .option("rowsPerSecond", 10 * 1000)
    .load()
    .selectExpr(
        "md5( CAST (value AS STRING) ) as md5", "value", "value%1000000 as hash"
    )
)

# display(generated_stream_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Parallel write to multiple Delta Tables

# COMMAND ----------

target_table_name_prefix = "parallel_writes_"
check_point_base_location = "/tmp/_checkpoints/"
merge_column_name = "hash"

# COMMAND ----------

async def create_empty_delta_table(index: int):
    target_table_name = f"{target_table_name_prefix}{index}"
    logger.info(f"Starting task for table_name: {target_table_name}")
    (
        generated_stream_df.writeStream.format("delta")
        .outputMode("append")
        .trigger(once=True)
        .option(
            "checkpointLocation", f"{check_point_base_location}/{target_table_name}"
        )
        .option("queryName", f"write_stream_for_{target_table_name}")
        .toTable(target_table_name)
    )
    logger.info(f"Write completed for table_name: {target_table_name}")

# COMMAND ----------

async def create_n_tables(n: int = 10):
    task_list = list()
    for index in range(n):
        task_list.append(asyncio.create_task(create_empty_delta_table(index)))
    logger.info(f"task_list:  {task_list}")
    await_completion_of_all_tasks = await asyncio.gather(*task_list)


await create_n_tables()

# COMMAND ----------

# MAGIC %md
# MAGIC # Parallel Optimize/Zorder of multiple Delta Tables

# COMMAND ----------

from timeit import default_timer as timer


async def optimize_and_zorder_table(table_name: str, zorder_by_col_name: str) -> None:
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
    logger.info(f"Met condition to optimize table {table_name}")
    sql_query_optimize = f"OPTIMIZE  {table_name} ZORDER BY ({zorder_by_col_name})"
    spark.sql(sql_query_optimize)
    end = timer()
    time_elapsed_seconds = end - start
    logger.info(
        f"Successfully optimized table {table_name} . Total time elapsed: {time_elapsed_seconds} seconds"
    )

# COMMAND ----------

async def optimize_n_tables(n: int = 10):
    task_list = list()
    for index in range(n):
        target_table_name = f"{target_table_name_prefix}{index}"
        task_list.append(
            asyncio.create_task(
                optimize_and_zorder_table(
                    table_name=target_table_name, zorder_by_col_name="value"
                )
            )
        )
    await_completion_of_all_tasks = await asyncio.gather(*task_list)


await optimize_n_tables()

# COMMAND ----------

# MAGIC %md
# MAGIC # MERGE into multiple Delta Tables in parallel

# COMMAND ----------

# MAGIC %md 
# MAGIC ###  Merge Logic for the micro-batch

# COMMAND ----------

def make_changes_using_the_micro_batch(microBatchOutputDF, batchId: int):
    logger.info(f"Processing batchId: {batchId}")
    updates_view_name = f"updates_{batchId}"
    deduped_updates_view_name = f"{updates_view_name}_which_need_to_be_merged"
    microBatchOutputDF.createOrReplaceTempView(updates_view_name)
    spark_session_for_this_micro_batch = microBatchOutputDF._jdf.sparkSession()
    spark_session_for_this_micro_batch.sql(
        f"""
      SELECT * 
      FROM (
        select *
          ,rank() over(partition by {merge_column_name} order by value desc) as dedupe
        from {updates_view_name} updates
        )
      WHERE 
          dedupe =1 
   """
    ).drop("dedupe").createOrReplaceTempView(deduped_updates_view_name)
    spark_session_for_this_micro_batch.sql(
        f"""
    MERGE INTO {target_table_name} target
    using {deduped_updates_view_name} u
    on u.{merge_column_name} = target.{merge_column_name} 
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
    )
    optimize_every_n_batches = 20
    # Define how often should optimize run? for example: at 50, it means that we will run the optimize command every 50 batches of stream data
    if batchId % optimize_every_n_batches == 0:
        optimize_and_zorder_table(
            table_name=target_table_name, zorder_by_col_name=merge_column_name
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orchestrate from readStream -> Merge -> Optimize

# COMMAND ----------

async def merge_into_table(target_table_name: str):
    (
        generated_stream_df.writeStream.format("delta")
        .trigger(processingTime="10 seconds")
        .option(
            "checkpointLocation", f"{check_point_base_location}/{target_table_name}"
        )
        .option("queryName", f"write_stream_for_{target_table_name}")
        .foreachBatch(make_changes_using_the_micro_batch)
        .start()
    )


async def merge_into_n_tables(n: int = 10):
    task_list = list()
    for index in range(n):
        target_table_name = f"{target_table_name_prefix}{index}"
        task_list.append(
            asyncio.create_task(merge_into_table(target_table_name=target_table_name))
        )
        """
        task_list.append(
            optimize_and_zorder_table(
                table_name=target_table_name, zorder_by_col_name="value"
            )
        )
        """
    await_completion_of_all_tasks = await asyncio.gather(*task_list)


await merge_into_n_tables()

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

spark.sql(
    f"""
ALTER TABLE {target_table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed=true)
"""
)

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
