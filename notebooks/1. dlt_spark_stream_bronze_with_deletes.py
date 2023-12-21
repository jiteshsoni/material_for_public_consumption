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

from pyspark.sql import functions as F
from faker import Faker
import uuid, json
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, expr

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# Target Stareaming Table specification
bronze_target = "bronze_stream"

# COMMAND ----------

# MAGIC %md ## Use Faker to define functions to help generate fake column values

# COMMAND ----------

fake = Faker()
fake_id = F.udf(lambda: str(uuid.uuid4()))
fake_firstname = F.udf(fake.first_name)
fake_email = F.udf(fake.ascii_company_email)
# fake_date = F.udf(lambda:fake.date_time_this_month().strftime("%Y-%m-%d %H:%M:%S"))
fake_address = F.udf(fake.address)
fake_zipcode = F.udf(fake.zipcode)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Streaming source data at your desired rate

# COMMAND ----------

# Reading the stream and adding the fake data columns
streaming_df = (
    spark.readStream.format("rate")
    .option("numPartitions", 10)
    .option("rowsPerSecond", 1 * 100)
    .load()
    .withColumn("fake_id", fake_id())
    .withColumn("fake_firstname", fake_firstname())
    .withColumn("fake_email", fake_email())
    .withColumn("fake_address", fake_address())
    .withColumn("fake_zipcode", fake_zipcode())
    # Adding a struct column
    .withColumn("json_data", F.struct(
        F.col("fake_firstname").alias("firstname"), 
        F.col("fake_email").alias("email"), 
        F.col("fake_address").alias("address"), 
        F.col("fake_zipcode").alias("zipcode")
    ))
)

streaming_df.printSchema()
# You can uncomment the below display command to check if the code in this cell works
#display(streaming_df)

# COMMAND ----------

import dlt

# COMMAND ----------

@dlt.table(
    name = bronze_target
)
def dlt_bronze_target():
    return streaming_df
