# Databricks notebook source
import time
catalog_dot_database_name = "soni.default"
source_table = f"{catalog_dot_database_name}.events"

while True:
    # Read the Delta table
    df = spark.read.format("delta").table(source_table)

    # Sample 1000 rows without replacement
    # Adjust the fraction if necessary to get approximately 1000 rows
    # Note: The exact number of sampled rows can vary, especially with small datasets
    sampled_df = df.sample(withReplacement=False, fraction=0.001).limit(1000)

    # Append the sampled data back to the Delta table
    # Ensure that the schema of sampled_df matches the schema of the Delta table
    sampled_df.write.format("delta").mode("append").saveAsTable(source_table)
    time.sleep(60)

# COMMAND ----------


