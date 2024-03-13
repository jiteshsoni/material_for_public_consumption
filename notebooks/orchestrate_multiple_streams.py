# Databricks notebook source
# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

target_table_prefix = "orchestrate_multiple_streams_3second"
number_of_streams = 200
check_point_location_prefix= f"/tmp/delta/{target_table_prefix}/_checkpoints/"


# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the input streams for Canada & USA

# COMMAND ----------

generated_streaming_df = (
     spark.readStream
        .format("rate")
        .option("numPartitions", 8)
        .option("rowsPerSecond", 1 * 1000)
        .load()
        .selectExpr(
          "md5( CAST (value AS STRING) ) as md5"
          ,"value"
          ,f"value%{number_of_streams} as table_name_postfix"
          ,"current_timestamp() as ingestion_timestamp"  
        )
)

#display(generated_streaming_df)

# COMMAND ----------


for table_name_postfix in range(number_of_streams):
    output_table_name =f"{target_table_prefix}{table_name_postfix}"
    (
      generated_streaming_df
        .where(f"table_name_postfix= {table_name_postfix} ")
        .writeStream.format('delta')
        #.trigger(availableNow=True)
        .trigger(processingTime='3 seconds')
        .option("checkpointLocation", f"{check_point_location_prefix}/{table_name_postfix}")
        .queryName(f"write_stream_to_delta_table: {output_table_name}")
        .toTable(output_table_name)
    )

# COMMAND ----------


