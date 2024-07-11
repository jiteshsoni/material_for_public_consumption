# Databricks notebook source
import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

catalog_name = "soni"
database_name = "2024_06_25"

# List of tables to create views for
tables = ["human_name", "human_email", "human_date_of_birth"]
target_name = "DIM_SSN_SCD_TYP_2"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dynamically create DLT views

# COMMAND ----------

for table in tables:

    @dlt.view(name=f"dlt_view_{table}")
    def create_view(table=table):
        """
        Creates a DLT view for the given table from the Delta table.

        Args:
            table (str): Name of the table to create the view for.

        Returns:
            DataFrame: Streaming DataFrame containing the data from the specified table.
        """
        table_path = f"{catalog_name}.`{database_name}`.{table}"
        return spark.readStream.format("delta").table(table_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Streaming Target

# COMMAND ----------

dlt.create_streaming_table(
    name=target_name,
    table_properties={
        "pipelines.autoOptimize.zOrderCols": "ssn",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge Upsert into Delta table

# COMMAND ----------

dlt.apply_changes(
    flow_name=f"streaming_data_from_dlt_view_human_name_to_merge_into_{target_name}",
    target=target_name,
    source="dlt_view_human_name",
    keys=["ssn"],
    ignore_null_updates=True,
    stored_as_scd_type="2",
    sequence_by="timestamp",
)

# COMMAND ----------

dlt.apply_changes(
    flow_name=f"streaming_data_from_dlt_view_human_email_to_merge_into_{target_name}",
    target=target_name,
    source="dlt_view_human_email",
    keys=["ssn"],
    ignore_null_updates=True,
    stored_as_scd_type="2",
    sequence_by="timestamp",
)

# COMMAND ----------

dlt.apply_changes(
    flow_name=f"streaming_data_from_dlt_view_human_date_of_birth_to_merge_into_{target_name}",
    target=target_name,
    source="dlt_view_human_date_of_birth",
    keys=["ssn"],
    ignore_null_updates=True,
    stored_as_scd_type="2",
    sequence_by="timestamp",
)
