# Databricks notebook source
# MAGIC %md
# MAGIC ## Install Libraries
# MAGIC Install Faker which is only needed for the purope of this demo.

# COMMAND ----------

!pip install faker

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from faker import Faker
from pyspark.sql.functions import *
from pyspark.sql.types import *
import uuid
from utils import logger
from datetime import date


# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

catalog_name ="soni"

# Generate a variable with today's date and  Format the date in YYYY_MM_DD
database_name = date.today().strftime('%Y_%m_%d')



# COMMAND ----------

# MAGIC %md
# MAGIC # Create the Target Schema/Database
# MAGIC Create a Schema

# COMMAND ----------

spark.sql(f"""
            CREATE DATABASE IF NOT EXISTS {catalog_name}.{database_name};
          """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Fake Data At Scale

# COMMAND ----------

# Initialize the Faker generator
fake = Faker()

# UDFs to generate fake data
@udf(StringType())
def ssn():
    return fake.ssn()

@udf(StringType())
def name():
    return fake.name()

@udf(StringType())
def address():
    return fake.address()

@udf(StringType())
def date_of_birth():
    return fake.date_of_birth().strftime('%Y-%m-%d')

@udf(StringType())
def email():
    return fake.email()

@udf(StringType())
def phone_number():
    return fake.phone_number()


# COMMAND ----------

def generated_human_data_df(rowsPerSecond: int= 1000, numPartitions: int= 4):
    return (
        spark.readStream.format("rate")
        .option("numPartitions", numPartitions)
        .option("rowsPerSecond", rowsPerSecond)
        .load()
        .withColumn("ssn", ssn())
        .withColumn("name", name())
        .withColumn("address", address())
        .withColumn("date_of_birth", date_of_birth())
        .withColumn("email", email())
        .withColumn("phone_number", phone_number())
    )

# COMMAND ----------

display(generated_human_data_df())


# COMMAND ----------

def stream_write_table(input_df : DataFrame, column_list: list, table_name:str):
    (
        input_df
        .selectExpr(*column_list)
        .writeStream
        .option("mergeSchema", "true")
        .trigger(processingTime='10 seconds')
        .queryName(f"write_to_delta_table: {table_name}")
        .option(
            "checkpointLocation",
            f"tmp/{table_name}/_checkpoint",
        )
        .format("delta")
        .toTable(table_name)
    )

#.trigger(availableNow=True)

# COMMAND ----------

stream_write_table(input_df= generated_human_data_df(), column_list=["ssn", "date_of_birth", "phone_number", "timestamp"] , table_name=f"{catalog_name}.{database_name}.human_date_of_birth")

# COMMAND ----------

stream_write_table(input_df= generated_human_data_df(), column_list=["ssn", "name", "address", "timestamp"] , table_name=f"{catalog_name}.{database_name}.human_name")

# COMMAND ----------

stream_write_table(input_df= generated_human_data_df(), column_list=["ssn", "email","timestamp"] , table_name=f"{catalog_name}.{database_name}.human_email")

# COMMAND ----------

display(
  spark.sql(f"""
          SHOW TABLES IN {catalog_name}.{database_name}
          """)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM soni.`2024_06_24`.human_name
# MAGIC WHERE address is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM soni.`2024_06_24`.human_date_of_birth

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM soni.`2024_06_24`.human_email

# COMMAND ----------

email_df = spark.readStream.table(f"{catalog_name}.{database_name}.human_email")
date_of_birth_df = spark.readStream.table(f"{catalog_name}.{database_name}.human_date_of_birth")
name_df = spark.readStream.table(f"{catalog_name}.{database_name}.human_name")

# COMMAND ----------

unioned_df = email_df.unionByName(date_of_birth_df, allowMissingColumns=True).unionByName(name_df, allowMissingColumns=True)

# COMMAND ----------

(
  unioned_df
    .writeStream
    .trigger(processingTime='10 seconds')
    .option(
            "checkpointLocation",
            f"tmp/unioned_v1/_checkpoint",
        ).table(f"{catalog_name}.{database_name}.unioned_v1")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM soni.`2024_06_24`.unioned_v1
# MAGIC where date_of_birth is not null

# COMMAND ----------

display(
  spark.readStream.format("delta")
  .option("readChangeFeed", "true").table("soni.dlt_2024.ssn_data_with_cdf")
)


# COMMAND ----------

display(
  spark.readStream.format("delta")
  .option("readChangeFeed", "true").table("soni.dlt_2024.silver_data")
)


# COMMAND ----------

display(
  spark.readStream.format("delta")
  .option("readChangeFeed", "true").table("soni.dlt_2024.silver_data")
  .groupBy(
            "_change_type"
        )
        .agg(
            F.count("_change_type").alias("number_of_changes")
        )
)


# COMMAND ----------


