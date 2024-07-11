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
from pyspark.sql.types import StringType
from utils import logger
from datetime import date


# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# Define catalog name and database name
CATALOG_NAME = "soni"
DATABASE_NAME = date.today().strftime('%Y_%m_%d')

DATABASE_NAME = '2024_06_25'

# COMMAND ----------

# MAGIC %md
# MAGIC # Create the Target Schema/Database
# MAGIC Create a Schema

# COMMAND ----------

def create_database(catalog_name: str, database_name: str):
    """Create a database schema if it does not exist."""
    spark.sql(f"""
        CREATE DATABASE IF NOT EXISTS {catalog_name}.{database_name};
    """)

create_database(CATALOG_NAME, DATABASE_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Fake Data At Scale

# COMMAND ----------

# Initialize the Faker generator
fake = Faker()

# UDFs to generate fake data
@udf(StringType())
def generate_ssn():
    return fake.ssn()

@udf(StringType())
def generate_name():
    return fake.name()

@udf(StringType())
def generate_address():
    return fake.address()

@udf(StringType())
def generate_date_of_birth():
    return fake.date_of_birth().strftime('%Y-%m-%d')

@udf(StringType())
def generate_email():
    return fake.email()

@udf(StringType())
def generate_phone_number():
    return fake.phone_number()

# COMMAND ----------

def generate_human_data_df(rows_per_second: int = 100, num_partitions: int = 4) -> DataFrame:
    """Generate a DataFrame with fake human data."""
    return (
        spark.readStream.format("rate")
        .option("numPartitions", num_partitions)
        .option("rowsPerSecond", rows_per_second)
        .load()
        .withColumn("ssn", generate_ssn())
        .withColumn("name", generate_name())
        .withColumn("address", generate_address())
        .withColumn("date_of_birth", generate_date_of_birth())
        .withColumn("email", generate_email())
        .withColumn("phone_number", generate_phone_number())
    )

# COMMAND ----------

#display(generate_human_data_df())


# COMMAND ----------

# MAGIC %md
# MAGIC ## Function to write Stream to Delta table

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

# Define tables and columns to be written
table_definitions = [
    {"columns": ["ssn", "date_of_birth", "phone_number", "timestamp"], "table_name": "human_date_of_birth"},
    {"columns": ["ssn", "name", "address", "timestamp"], "table_name": "human_name"},
    {"columns": ["ssn", "email", "timestamp"], "table_name": "human_email"},
]

for table_def in table_definitions:
    stream_write_table(
        input_df=generate_human_data_df(),
        column_list=table_def["columns"],
        table_name=f"{CATALOG_NAME}.{DATABASE_NAME}.{table_def['table_name']}"
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 
# MAGIC # Display the tables in the created database

# COMMAND ----------

display(
    spark.sql(f"""
        SHOW TABLES IN {CATALOG_NAME}.{DATABASE_NAME}
    """)
  )

# COMMAND ----------


