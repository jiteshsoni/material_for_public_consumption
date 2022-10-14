# Databricks notebook source
# DBTITLE 1,List files at Path
display(dbutils.fs.ls("dbfs:/databricks-datasets/airlines/"))

# COMMAND ----------

# DBTITLE 1,Reading README.md
with open("/dbfs/databricks-datasets/airlines/README.md") as f:
    x = ''.join(f.readlines())

print(x)

# COMMAND ----------

# DBTITLE 1,Read the CSV files, derive schema and repartitioning
from  pyspark.sql.functions import input_file_name
# Read the first file and derive schema from it
derived_schema = spark.read.load("dbfs:/databricks-datasets/airlines/part-00000", format="csv", header=True, inferschema=True).schema
# Read the files and apply the derived schema
csv_df = spark.read.format("csv").schema(derived_schema).load("dbfs:/databricks-datasets/airlines/")

# COMMAND ----------

# DBTITLE 1,Create Date Column
from pyspark.sql import functions as F
from pyspark.sql.types import *
csv_df = csv_df.withColumn("departure_date", F.expr(" CAST(Year || '-' || (CASE WHEN Month < 10 THEN 0 || Month ELSE Month END) || '-' || (CASE WHEN DayofMonth < 10 THEN 0 || DayofMonth ELSE DayofMonth END) AS DATE)"))
csv_df = csv_df.withColumn("model", F.expr("TailNum"))
csv_df = csv_df.withColumnRenamed("UniqueCarrier", "Airline")
csv_df.createOrReplaceTempView("csv_df")
# Repartition it 200 partitions
#csv_df.repartition(200).cache()

# COMMAND ----------

# DBTITLE 1,Sample the data
display(csv_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT
# MAGIC   Airline
# MAGIC FROM csv_df

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT TailNum, COUNT(1)
# MAGIC FROM csv_df
# MAGIC GROUP BY TailNum
# MAGIC order by 2 

# COMMAND ----------

# DBTITLE 1,Create Multiple Write Paths
base_path = "/Users/jitesh.soni@databricks.com/public_dataset/airlines/"
partition_by_date_zorder_by_model = base_path + "partition_by_airline_zorder_by_model"
partition_by_model_zorder_by_date = base_path + "partition_by_model_zorder_by_date"

# COMMAND ----------

# DBTITLE 1,Create Widget and get data back into a variable
dbutils.widgets.text("database_name", "flights_db", "1: Database name")
database_name =  dbutils.widgets.get("database_name").lower()

# COMMAND ----------

# DBTITLE 1,Create Database and Drop Table
# MAGIC %sql 
# MAGIC CREATE DATABASE IF NOT EXISTS $database_name ;
# MAGIC 
# MAGIC --DROP TABLE IF EXISTS $database_name.flights_partition_by_airline_and_date_zorder_by_model;
# MAGIC --DROP TABLE IF EXISTS $database_name.flights_partition_by_airline_and_model_zorder_by_date;

# COMMAND ----------

print(f"{database_name}.{partition_by_date_zorder_by_model}")

# COMMAND ----------

# DBTITLE 1,Write out 2 Delta table with different partitioning
csv_df.write.format("delta").mode("overwrite").partitionBy(['departure_date']).saveAsTable(f"{database_name}.partition_by_date_zorder_by_model}")

# COMMAND ----------

# DBTITLE 1,ZORDER By model
# MAGIC %sql
# MAGIC OPTIMIZE ${database_name}.${partition_by_date_zorder_by_model}
# MAGIC ZORDER BY (model)

# COMMAND ----------

csv_df.write.format("delta").mode("overwrite").partitionBy(['airline','model']).saveAsTable(f"{database_name}.flights_partition_by_airline_and_model_zorder_by_date")

# COMMAND ----------

# DBTITLE 1,ZORDER By departure_date
# MAGIC %sql
# MAGIC OPTIMIZE ${database_name}.flights_partition_by_airline_and_model_zorder_by_date
# MAGIC ZORDER BY (departure_date)

# COMMAND ----------


