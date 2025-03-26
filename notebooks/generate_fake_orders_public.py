# Databricks notebook source
# MAGIC %pip install dbldatagen

# COMMAND ----------

import dbldatagen as dg
import uuid
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType, LongType
from pyspark.sql.functions import expr, col, struct, array, concat, lit

# COMMAND ----------

PARTITIONS = 180  # Adjust based on your cluster size
ROWS_PER_SECOND = 10 * 1000* 1000  # Number of rows to generate

storage_account_name = "storage_account_name"
container_name = "container"  # Updated container name

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.YOURADLSBUCKETNAME.dfs.core.windows.net", "SharedKey")
spark.conf.set("fs.azure.account.key.YOURADLSBUCKETNAME.dfs.core.windows.net", "YourSharedKey")

# COMMAND ----------

# Define a flat schema with "orderId" as LongType (for large integer range) 
# and "orderTimestamp" to avoid ambiguity.
orders_schema = StructType([
    StructField("orderId", LongType(), False),            # Order ID as a large integer
    StructField("orderTimestamp", TimestampType(), False),  # Renamed field
    StructField("userId", IntegerType(), False),            # Pure integer user ID
    StructField("firstName", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("addressLine1", StringType(), True),
    StructField("addressLine2", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("postalCode", StringType(), True),
    StructField("country", StringType(), True),
    StructField("countryCode", StringType(), True),
    StructField("paymentMethod", StringType(), True),
    StructField("numLineItems", IntegerType(), True),
    # Line item 1
    StructField("lineItem1_productId", IntegerType(), True),
    StructField("lineItem1_quantity", IntegerType(), True),
    StructField("lineItem1_unitPrice", DoubleType(), True),
    # Line item 2
    StructField("lineItem2_productId", IntegerType(), True),
    StructField("lineItem2_quantity", IntegerType(), True),
    StructField("lineItem2_unitPrice", DoubleType(), True),
    # Line item 3
    StructField("lineItem3_productId", IntegerType(), True),
    StructField("lineItem3_quantity", IntegerType(), True),
    StructField("lineItem3_unitPrice", DoubleType(), True),
    # Line item 4
    StructField("lineItem4_productId", IntegerType(), True),
    StructField("lineItem4_quantity", IntegerType(), True),
    StructField("lineItem4_unitPrice", DoubleType(), True)
])

# COMMAND ----------

# Create a data generator specification using the updated schema.
orders_dataspec = (
    dg.DataGenerator(spark, name="orders_data", partitions=PARTITIONS)
    .withSchema(orders_schema)
    # Order-level fields:
    # Generate orderId as a random integer between 1 and 100,000,000,000.
    .withColumnSpec("orderId", minValue=1, maxValue=100000000000, random=True)
    .withColumnSpec("orderTimestamp", begin="2023-01-01 00:00:00", end="2023-12-31 23:59:59", random=True)
    # userId: pure integer in the range 1 to 100,000,000.
    .withColumnSpec("userId", minValue=1, maxValue=100000000, random=True)
    # Customer name fields
    .withColumnSpec("firstName", values=["John", "Jane", "Alice", "Bob"], random=True)
    .withColumnSpec("lastName", values=["Doe", "Smith", "Johnson", "Brown"], random=True)
    # Dummy email; will be recomputed later.
    .withColumnSpec("email", values=["dummy@example.com"], random=True)
    # Contact and address fields.
    .withColumnSpec("phone", values=["123-456-7890", "234-567-8901", "345-678-9012"], random=True)
    .withColumnSpec("addressLine1", values=["123 Main St", "456 Elm St", "789 Oak Ave"], random=True)
    .withColumnSpec("addressLine2", values=["Apt 4B", "", "Suite 100"], random=True)
    .withColumnSpec("city", values=["Anytown", "Otherville", "Cityplace"], random=True)
    .withColumnSpec("state", values=["CA", "NY", "TX"], random=True)
    .withColumnSpec("postalCode", values=["12345", "67890", "54321"], random=True)
    .withColumnSpec("country", values=["United States"], random=True)
    .withColumnSpec("countryCode", values=["US"], random=True)
    # Payment method
    .withColumnSpec("paymentMethod", values=["Credit Card", "Debit Card", "PayPal"], random=True)
    # Number of line items: random integer between 1 and 4.
    .withColumnSpec("numLineItems", minValue=1, maxValue=4, random=True)
    # Line item 1: product id in the range 1 to 1,000,000,000.
    .withColumnSpec("lineItem1_productId", minValue=1, maxValue=1000000000, random=True)
    .withColumnSpec("lineItem1_quantity", minValue=1, maxValue=5, random=True)
    .withColumnSpec("lineItem1_unitPrice", minValue=5.0, maxValue=50.0, random=True)
    # Line item 2:
    .withColumnSpec("lineItem2_productId", minValue=1, maxValue=1000000000, random=True)
    .withColumnSpec("lineItem2_quantity", minValue=1, maxValue=5, random=True)
    .withColumnSpec("lineItem2_unitPrice", minValue=5.0, maxValue=50.0, random=True)
    # Line item 3:
    .withColumnSpec("lineItem3_productId", minValue=1, maxValue=1000000000, random=True)
    .withColumnSpec("lineItem3_quantity", minValue=1, maxValue=5, random=True)
    .withColumnSpec("lineItem3_unitPrice", minValue=5.0, maxValue=50.0, random=True)
    # Line item 4:
    .withColumnSpec("lineItem4_productId", minValue=1, maxValue=1000000000, random=True)
    .withColumnSpec("lineItem4_quantity", minValue=1, maxValue=5, random=True)
    .withColumnSpec("lineItem4_unitPrice", minValue=5.0, maxValue=50.0, random=True)
)

# COMMAND ----------

# Build the streaming DataFrame with the data generator.
orders_df_flat = orders_dataspec.build(
    withStreaming=True,
    options={"rowsPerSecond": ROWS_PER_SECOND}
)

# Compute email from firstName and lastName (e.g., John.Doe@example.com)
orders_df_flat = orders_df_flat.withColumn(
    "email", concat(col("firstName"), lit("."), col("lastName"), lit("@example.com"))
)

# COMMAND ----------

# Assemble the nested structure.
orders_df = orders_df_flat.select(
    "orderId",
    "orderTimestamp",
    struct(
        col("userId"),
        col("firstName"),
        col("lastName"),
        col("email"),
        col("phone"),
        struct(
            col("addressLine1"),
            col("addressLine2"),
            col("city"),
            col("state"),
            col("postalCode"),
            col("country"),
            col("countryCode")
        ).alias("address")
    ).alias("customer"),
    "paymentMethod",
    expr("""
        filter(
            array(
                case when numLineItems >= 1 then struct(
                    lineItem1_productId as productId,
                    lineItem1_quantity as quantity,
                    lineItem1_unitPrice as unitPrice,
                    (lineItem1_quantity * lineItem1_unitPrice) as lineTotal
                ) end,
                case when numLineItems >= 2 then struct(
                    lineItem2_productId as productId,
                    lineItem2_quantity as quantity,
                    lineItem2_unitPrice as unitPrice,
                    (lineItem2_quantity * lineItem2_unitPrice) as lineTotal
                ) end,
                case when numLineItems >= 3 then struct(
                    lineItem3_productId as productId,
                    lineItem3_quantity as quantity,
                    lineItem3_unitPrice as unitPrice,
                    (lineItem3_quantity * lineItem3_unitPrice) as lineTotal
                ) end,
                case when numLineItems >= 4 then struct(
                    lineItem4_productId as productId,
                    lineItem4_quantity as quantity,
                    lineItem4_unitPrice as unitPrice,
                    (lineItem4_quantity * lineItem4_unitPrice) as lineTotal
                ) end
            ),
            x -> x is not null
        ) as lineItems
    """)
)


# COMMAND ----------

#display(orders_df)

# COMMAND ----------

write_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/synthetic_orders_100mb/"

# COMMAND ----------


(
  orders_df.repartition(30)
  .writeStream
    .format("parquet")
    .option("checkpointLocation", f"/tmp/dbldatagen/streamingDemo/checkpoint-{uuid.uuid4()}")
    .start(write_path)
)

# COMMAND ----------

import inflect

def number_to_words(number):
    p = inflect.engine()
    return p.number_to_words(number)

parquet_df = spark.read.parquet(write_path)

print(number_to_words(parquet_df.count()))

# COMMAND ----------

import inflect
import numpy as np

def analyze_and_display_parquet_stats(path, count_records=False):
    # Initialize inflect engine
    p = inflect.engine()
    
    # Read the Parquet file once
    parquet_df = spark.read.parquet(path)
    
    # Prepare a list to hold statistics
    stats_list = []
    
    if count_records:
        record_count = parquet_df.count()
        record_count_in_words = p.number_to_words(record_count)
        stats_list.append(("Record count", f"{record_count} ({record_count_in_words})"))
    
    # List the files in the specified path
    files = dbutils.fs.ls(path)
    
    # Calculate the total number of files
    total_files = len(files)
    stats_list.append(("Total files", total_files))
    
    # Calculate the total size in bytes and convert to megabytes (MB)
    total_size_bytes = sum(file.size for file in files)
    total_size_mb = float(total_size_bytes / (1024 ** 2))
    
    # Extract file sizes for statistics calculation
    file_sizes = [file.size for file in files]
    median_size_mb = float(np.median(file_sizes) / (1024 ** 2))
    p10_size_mb = float(np.percentile(file_sizes, 10) / (1024 ** 2))
    p90_size_mb = float(np.percentile(file_sizes, 90) / (1024 ** 2))
    p99_size_mb = float(np.percentile(file_sizes, 99) / (1024 ** 2))
    
    # Append the computed statistics to the stats list
    stats_list.extend([
        ("Total size (MB)", total_size_mb),
        ("Median file size (MB)", median_size_mb),
        ("P10 file size (MB)", p10_size_mb),
        ("P90 file size (MB)", p90_size_mb),
        ("P99 file size (MB)", p99_size_mb)
    ])
    
    # Create a Spark DataFrame from the statistics list
    stats_df = spark.createDataFrame(stats_list, schema=["Metric", "Value"])
    
    # Display the computed statistics and the original Parquet DataFrame
    display(stats_df)

# COMMAND ----------

#dbutils.fs.rm(write_path,True)

# COMMAND ----------

display(dbutils.fs.ls(write_path))

# COMMAND ----------


