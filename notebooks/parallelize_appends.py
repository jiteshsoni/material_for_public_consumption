# Databricks notebook source
from utils import logger
import asyncio

# COMMAND ----------

# MAGIC %md
# MAGIC ## Append to a single table
# MAGIC

# COMMAND ----------

async def append_to_delta_table( index: int , loop_number: int= 0):
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

async def do_n_appends_to_a_delta_table(n: int = 100, loop_number: int = 0):
    task_list = list()
    for index in range(n):
        task_list.append(asyncio.create_task(append_to_delta_table(index, loop_number)))
    logger.info(f"task_list:  {task_list}")
    await_completion_of_all_tasks = await asyncio.gather(*task_list)


loop_number =0
while True:
    await do_n_appends_to_a_delta_table(n=1, loop_number=loop_number)
    loop_number+=1

# COMMAND ----------

# MAGIC %md
# MAGIC Started with version 15520 at 2023-05-16 17:50:15 with 1 write on 1 thread
# MAGIC Finished at 2023-05-16 18:17:23,605
# MAGIC
# MAGIC Time taken: 27 min = 1620 seconds
# MAGIC
# MAGIC Number of Version changes: 16993 - 15520 = 1473
# MAGIC
# MAGIC
# MAGIC ~ 1.09 second to commit each transaction or 0.9 commits per second
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY parallel_appends

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize parallel_appends

# COMMAND ----------


