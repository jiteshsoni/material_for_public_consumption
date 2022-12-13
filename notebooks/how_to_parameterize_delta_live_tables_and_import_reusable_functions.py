# Databricks notebook source
# MAGIC %md
# MAGIC ## [This Notebook comes along with a Blog](https://medium.com/@canadiandataguy/how-to-parameterize-and-reusable-functions-work-with-delta-live-table-1994156db7fb)
# MAGIC [parameterize-pipelines](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-configuration.html#parameterize-pipelines)

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

# COMMAND ----------

# DBTITLE 1,The function is defined with try and except block so that it can work with Notebook as well, where we cannot pass the parameter value  from The DLT pipeline
def get_parameter_or_return_default(
    parameter_name: str = "pipeline.parameter_blah_blah",
    default_value: str = "default_value",
) -> str:
    try:
        parameter = spark.conf.get(parameter_name)
    except Exception:
        parameter = default_value
    return parameter

# COMMAND ----------

path_to_reusable_functions = get_parameter_or_return_default(
    parameter_name="pipeline.path_to_reusable_functions",
    default_value="/Workspace/Repos/jitesh.soni@databricks.com/material_for_public_consumption/",
)

# COMMAND ----------

parameter_abc = get_parameter_or_return_default(
    parameter_name="pipeline.parameter_abc", default_value="random_default_value"
)

# COMMAND ----------

print(
    f"parameter_which_specifies_path_for_file_with_functions_defined : {path_to_reusable_functions}"
)
print(f"parameter_abc : {parameter_abc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import the functions defined in the Python file

# COMMAND ----------

import sys

# Add the path so functions could be imported
sys.path.append(path_to_reusable_functions)

# Attempt the import
from reusable_functions import append_ingestion_columns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define function to return DataFrame

# COMMAND ----------

def static_dataframe():
    df_which_we_got_back_after_running_sql = spark.sql(
        f"""
            SELECT 
                '{path_to_reusable_functions}' as path_to_reusable_functions
                ,'{parameter_abc}' as parameter_abc
        """
    )
    return append_ingestion_columns(df_which_we_got_back_after_running_sql)


#display(static_dataframe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define a DLT Table

# COMMAND ----------

import dlt

# COMMAND ----------

@dlt.table(name="static_table", comment="Static table")
def dlt_static_table():
    return static_dataframe()

# COMMAND ----------


