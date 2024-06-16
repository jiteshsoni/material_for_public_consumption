# Databricks notebook source
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, expr



# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# Target Stareaming Table specification
bronze_source = "bronze_stream"
silver_target = "silver_apply_changes_v2"

# COMMAND ----------

import dlt

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream writes to a Delta Table with apply_changes

# COMMAND ----------

@dlt.view(
  name="streaming_users_v2"
)
def parse_streaming_users():
    result_df = (
          spark.readStream.format("delta")
          .option("skipChangeCommits", "true").table(f"soni.dlt_schema.{bronze_source}")
          .selectExpr(
              "fake_id as id"
              ,"fake_firstname as first_name"
              ,"json_data.email as email"
              ,"value"
            )
    )
    return result_df

# COMMAND ----------

dlt.create_streaming_table(silver_target)

dlt.apply_changes(
  target = silver_target,
  source = "streaming_users_v2",
  keys = ["id"],
  sequence_by = col("value"),
  stored_as_scd_type = 2
)

# COMMAND ----------


