# Databricks notebook source
import dlt
from pyspark.sql.functions import *

# COMMAND ----------

@dlt.view
def name():
    return spark.readStream.format("delta").table("soni.`2024_06_24`.human_name")

# COMMAND ----------

@dlt.view
def email():
    return spark.readStream.format("delta").table("soni.`2024_06_24`.human_email")

# COMMAND ----------

@dlt.view
def date_of_birth():
    return spark.readStream.format("delta").table(
        "soni.`2024_06_24`.human_date_of_birth"
    )

# COMMAND ----------

dlt.create_streaming_table(
      name = "ssn_data_with_cdf",
      table_properties={
        "pipelines.autoOptimize.zOrderCols": "ssn",
        "delta.enableChangeDataFeed": "true",
  }
)

# COMMAND ----------

dlt.apply_changes(
    flow_name="name_feed",
    target="ssn_data_with_cdf",
    source="name",
    keys=["ssn"],
    ignore_null_updates=True,
    stored_as_scd_type="2",
    sequence_by = "timestamp"
)

# COMMAND ----------

# APPLY CHANGES from different streams
dlt.apply_changes(
    flow_name="email_feed",
    target="ssn_data_with_cdf",
    source="email",
    keys=["ssn"],
    ignore_null_updates=True,
    stored_as_scd_type="2",
    sequence_by = "timestamp"
)

# COMMAND ----------

# APPLY CHANGES from different streams
dlt.apply_changes(
    flow_name="date_of_birth_feed",
    target="ssn_data_with_cdf",
    source="date_of_birth",
    keys=["ssn"],
    ignore_null_updates=True,
    stored_as_scd_type="2",
    sequence_by = "timestamp"
)
