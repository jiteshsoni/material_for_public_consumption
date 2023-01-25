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
import uuid

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# define schema name and where should the table be stored
schema_name = "test_streaming_joins"
schema_storage_location = "/tmp/CHOOSE_A_PERMANENT_LOCATION/"


# Please download this file from https://simplemaps.com/data/us-zips then download and place it at a location of your choice and then change the value for the variable below
static_table_csv_file = "/FileStore/jitesh.soni/data/us_zip_code_and_its_attributes.csv"

# Static table specification
static_table_name = "static_zip_codes"


# Target Stareaming Table specification
target_table_name = "joined_datasets"

# Recommend you to keep the checkpoint next to the Delta table so that you do have to notion about where the checkpoint is
checkpoint_location = f"{schema_storage_location}/{target_table_name}/_checkpoints/"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the Target Schema/Database

# COMMAND ----------

create_schema_sql = f"""
    CREATE SCHEMA IF NOT EXISTS {schema_name}
    COMMENT 'This is {schema_name} schema'
    LOCATION '{schema_storage_location}'
    WITH DBPROPERTIES ( Owner='Jitesh');
    """
print(f"create_schema_sql: {create_schema_sql}")

# COMMAND ----------

spark.sql(create_schema_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the static table

# COMMAND ----------

csv_df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(static_table_csv_file)
)
display(csv_df)

# COMMAND ----------

csv_df.write.saveAsTable(f"{schema_name}.{static_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Optimze and Zorder the table on key which would be used in joins

# COMMAND ----------

spark.sql(
    f"""
    OPTIMIZE {schema_name}.{static_table_name} ZORDER BY (zip);
    """
)

# COMMAND ----------

# MAGIC %md ## Use Faker to define functions to help generate fake column values

# COMMAND ----------

fake = Faker()
fake_id = F.udf(lambda: str(uuid.uuid4()))
fake_firstname = F.udf(fake.first_name)
fake_lastname = F.udf(fake.last_name)
fake_email = F.udf(fake.ascii_company_email)
# fake_date = F.udf(lambda:fake.date_time_this_month().strftime("%Y-%m-%d %H:%M:%S"))
fake_address = F.udf(fake.address)
fake_zipcode = F.udf(fake.zipcode)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Streaming source data at your desired rate

# COMMAND ----------

streaming_df = (
    spark.readStream.format("rate")
    .option("numPartitions", 10)
    .option("rowsPerSecond", 1 * 1000)
    .load()
    .withColumn("fake_id", fake_id())
    .withColumn("fake_firstname", fake_firstname())
    .withColumn("fake_lastname", fake_lastname())
    .withColumn("fake_email", fake_email())
    .withColumn("fake_address", fake_address())
    .withColumn("fake_zipcode", fake_zipcode())
)

# You can uncomment the below display command to check if the code in this cell works
# display(streaming_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream-Static Join 

# COMMAND ----------

lookup_delta_df = spark.read.table(static_table_name)


joined_streaming_df = streaming_df.join(
    lookup_delta_df,
    streaming_df["fake_zipcode"] == lookup_delta_df["zip"],
    "left_outer",
).drop("fake_zipcode")
# display(joined_streaming_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream writes to a Delta Table

# COMMAND ----------

(
    joined_streaming_df.writeStream
    # .trigger(availableNow=True)
    .queryName("do_a_stream_join_with_the_delta_table")
    .option("checkpointLocation", checkpoint_location)
    .format("delta")
    .toTable(f"{schema_name}.{target_table_name}")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check output

# COMMAND ----------

display(spark.read.table(f"{schema_name}.{target_table_name}"))

# COMMAND ----------

spark.read.table(f"{schema_name}.{target_table_name}").count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean up the databse which was created

# COMMAND ----------

spark.sql(
    f"""
    drop schema if exists {schema_name} CASCADE
"""
)

# COMMAND ----------

dbutils.fs.rm(checkpoint_location)

# COMMAND ----------


