# Databricks notebook source
# Create text widgets with default values (you can modify these as needed)
dbutils.widgets.text("input_path", "abfss://container@publicstorageforspark.dfs.core.windows.net/synthetic_orders_10mb/", "Input Path")
dbutils.widgets.text("target_table", "soni.default.25machine_rseries", "Target Table")

# COMMAND ----------

# Retrieve widget values
input_path = dbutils.widgets.get("input_path")
target_table = dbutils.widgets.get("target_table")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Files from Remote Bucket

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import explode, col

import uuid


# Initialize SparkSession
spark = SparkSession.builder.getOrCreate()

# Define the storage account and container details
storage_account_name = "publicstorageforspark"
container_name = "container"

sas_token = "SAS TOKEN HERE"


# Set Spark configuration with the SAS token
# Corrected configuration with dfs endpoint
spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account_name}.dfs.core.windows.net", 
              "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account_name}.dfs.core.windows.net", sas_token)

# COMMAND ----------

# # Read data from the Azure Blob Storage file using Spark with the defined schema
# stream_df = spark.readStream.format("cloudFiles") \
#   .option("cloudFiles.format", "parquet") \
#   .option("cloudFiles.schemaLocation", f"/tmp/{uuid.uuid4()}" )  \
#   .load(files_path)

# display(stream_df)

# COMMAND ----------

input_df = spark.read.parquet(input_path)

# COMMAND ----------


# Explode the lineItems array so that each element becomes its own row
df_exploded = input_df.withColumn("lineItem", explode("lineItems"))

# Flatten the DataFrame by selecting each nested field explicitly
flattened_df = df_exploded.select(
    col("orderId"),
    col("orderTimestamp"),
    col("paymentMethod"),
    col("lineItem.productId").alias("productId"),
    col("lineItem.quantity").alias("quantity"),
    col("lineItem.unitPrice").alias("unitPrice"),
    col("lineItem.lineTotal").alias("lineTotal"),
    col("customer.userId").alias("userId"),
    col("customer.firstName").alias("firstName"),
    col("customer.lastName").alias("lastName"),
    col("customer.email").alias("email"),
    col("customer.phone").alias("phone"),
    col("customer.address")
)


# COMMAND ----------

#display(flattened_df)

# COMMAND ----------


# Write the flattened DataFrame to a Delta table
flattened_df.where("orderTimestamp >= '2023-07-01 00:00:00' ").write.format("delta").partitionBy("paymentMethod").mode("overwrite").saveAsTable(f"{target_table}_partitioned")

# COMMAND ----------


# Write the flattened DataFrame to a Delta table
flattened_df.where("orderTimestamp >= '2023-07-01 00:00:00' ").write.format("delta").mode("overwrite").saveAsTable(target_table)

# COMMAND ----------


