# Databricks notebook source
# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

target_table_name = "for_each_batch_paramerterize"
check_point_location = f"/tmp/delta/{target_table_name}/_checkpoints/"
dedupe_colum_name ="hash"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate data at your desired data

# COMMAND ----------

generated_df = (
     spark.readStream
        .format("rate")
        .option("numPartitions", 4)
        .option("rowsPerSecond", 1 * 1000)
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
# MAGIC ## Logic for the micro-batch

# COMMAND ----------

class forEachBatchProcessor:
    def __init__(self, dedupe_column: str, filter_criteria:str, passed_value: int):
        self.dedupe_column = dedupe_column
        self.filter_criteria = filter_criteria
        self.passed_value = passed_value

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
        microBatchOutputDF.createOrReplaceTempView(view_name)
        sql_logic = f"""
            SELECT 
                * 
                ,{self.passed_value} as passed_value
                ,{batchId} as batch_id
            FROM (
              SELECT *
                ,rank() over(partition by {self.dedupe_column} order by value desc) as dedupe
              FROM {view_name}
              WHERE  
                {self.filter_criteria}
              )
            WHERE 
                dedupe =1 
        """
        print(f"Processing sql_logic: {sql_logic}")
        to_be_written_df = microBatchOutputDF.sparkSession.sql(sql_logic).drop("dedupe")
        to_be_written_df.write.mode("append").saveAsTable(target_table_name)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Create an instance of forEachBatchProcessor with the parameters. 
# MAGIC
# MAGIC You can add any number of parameters as you like

# COMMAND ----------


instantiateForEachBatchProcessor = forEachBatchProcessor(
            dedupe_column = dedupe_colum_name,
            filter_criteria = "1=1",
            passed_value = 3
        )



# COMMAND ----------

# MAGIC %md
# MAGIC ## Orchestrate from readStream -> forEachBatch

# COMMAND ----------

(
  generated_df
 .writeStream
 #.trigger(availableNow=True) 
 .trigger(processingTime='10 seconds')
 .option("checkpointLocation", check_point_location)
 .option("queryName", "ParameterizeForEachBatch")
 .foreachBatch(instantiateForEachBatchProcessor.make_changes_using_the_micro_batch)
 .start()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display the Target Table
# MAGIC

# COMMAND ----------

display(spark.read.table(target_table_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Up

# COMMAND ----------

spark.sql(f"""
          DROP TABLE IF EXISTS {target_table_name}
          """)

# COMMAND ----------

dbutils.fs.rm(check_point_location,True)

# COMMAND ----------


