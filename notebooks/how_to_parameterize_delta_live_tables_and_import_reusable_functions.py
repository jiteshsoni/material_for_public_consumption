# Databricks notebook source
# MAGIC %md
# MAGIC ## [This Notebook comes along with a Blog](https://medium.com/@canadiandataguy/how-to-parameterize-and-reusable-functions-work-with-delta-live-table-1994156db7fb)
# MAGIC [parameterize-pipelines](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-configuration.html#parameterize-pipelines)

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

# COMMAND ----------

# DBTITLE 1,The function is defined with try and except block so that it can work with Notebook as well, where we cannot pass the parameter value  from The DLT pipeline
def get_parameter_or_return_default(default_value: str) -> str:
    try:
        parameter = spark.conf.get("pipeline.parameter_name")
    except Exception:
        parameter = default_value
    return parameter

# COMMAND ----------

parameter_1 = get_parameter_or_return_default(default_value="test1")

# COMMAND ----------

parameter_1 = get_parameter_or_return_default(default_value="random_default_value")
