# Databricks notebook source
# MAGIC %md
# MAGIC ## Install necessary packages
# MAGIC

# COMMAND ----------

# MAGIC %pip install Faker faker_vehicle

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import required libraries

# COMMAND ----------

from faker import Faker
from faker_vehicle import VehicleProvider
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import uuid
import logging
from pyspark.sql.streaming import StreamingQuery
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC

# COMMAND ----------

timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
checkpoint_location = f"/tmp/confluent_kafka_checkpoint_{timestamp}"

# Kafka configuration
topic = f"YOUR_KAFKA_TOPIC"
kafka_bootstrap_servers_tls = "YOUR_KAFKA_HOST.cloud:9092"
kafka_api_key = "YOUR_KAFKA_API_KEY"
kafka_api_secret = "YOUR_KAFKA_API_SECRET"

# Clean up the checkpoint location
#dbutils.fs.rm(checkpoint_location, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialization and  UDF

# COMMAND ----------

# Initialize Faker for data generation and add vehicle data provider
fake = Faker()
fake.add_provider(VehicleProvider)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)



# COMMAND ----------

# User-defined functions (UDFs) for generating fake data
event_id = F.udf(lambda: str(uuid.uuid4()), StringType())
vehicle_year_make_model = F.udf(fake.vehicle_year_make_model)
vehicle_year_make_model_cat = F.udf(fake.vehicle_year_make_model_cat)
vehicle_make_model = F.udf(fake.vehicle_make_model)
vehicle_make = F.udf(fake.vehicle_make)
vehicle_model = F.udf(fake.vehicle_model)
vehicle_year = F.udf(fake.vehicle_year)
vehicle_category = F.udf(fake.vehicle_category)
vehicle_object = F.udf(fake.vehicle_object)
latitude = F.udf(fake.latitude)
longitude = F.udf(fake.longitude)
location_on_land = F.udf(fake.location_on_land)
local_latlng = F.udf(fake.local_latlng)
zipcode = F.udf(fake.zipcode)

# COMMAND ----------

@F.udf(StringType())
def large_text_udf(size: int):
    """Generate large text data with a specified size."""
    return fake.text(max_nb_chars=size)

# Configuration for large text data
num_large_columns = 10  # Number of large text columns
size_per_large_column = (1024 * 1024) // num_large_columns  # Distribute 1MB across columns


# COMMAND ----------

# MAGIC %md
# MAGIC ## Function to generate a DataFrame simulating a 1MB row of data

# COMMAND ----------

def generate_1mb_row_df(rowsPerSecond=100, numPartitions=24):
    """Generate a DataFrame simulating streaming data, including vehicle and geographic data."""
    logger.info("Generating vehicle and geo data frame...")
    df = spark.readStream.format("rate") \
        .option("numPartitions", numPartitions) \
        .option("rowsPerSecond", rowsPerSecond) \
        .load() \
        .withColumn("event_id", event_id()) \
        .withColumn("vehicle_year_make_model", vehicle_year_make_model()) \
        .withColumn("vehicle_year_make_model_cat", vehicle_year_make_model_cat()) \
        .withColumn("vehicle_make_model", vehicle_make_model()) \
        .withColumn("vehicle_make", vehicle_make()) \
        .withColumn("vehicle_model", vehicle_model()) \
        .withColumn("vehicle_year", vehicle_year()) \
        .withColumn("vehicle_category", vehicle_category()) \
        .withColumn("vehicle_object", vehicle_object()) \
        .withColumn("latitude", latitude()) \
        .withColumn("longitude", longitude()) \
        .withColumn("location_on_land", location_on_land()) \
        .withColumn("local_latlng", local_latlng()) \
        .withColumn("zipcode", zipcode()) \
        .withColumn("large_text_col_1", large_text_udf(F.lit(size_per_large_column))) \
        .withColumn("large_text_col_2", large_text_udf(F.lit(size_per_large_column))) \
        .withColumn("large_text_col_3", large_text_udf(F.lit(size_per_large_column))) \
        .withColumn("large_text_col_4", large_text_udf(F.lit(size_per_large_column))) \
        .withColumn("large_text_col_5", large_text_udf(F.lit(size_per_large_column))) \
        .withColumn("large_text_col_6", large_text_udf(F.lit(size_per_large_column))) \
        .withColumn("large_text_col_7", large_text_udf(F.lit(size_per_large_column))) \
        .withColumn("large_text_col_8", large_text_udf(F.lit(size_per_large_column))) \
        .withColumn("large_text_col_9", large_text_udf(F.lit(size_per_large_column))) \
        .withColumn("large_text_col_10", large_text_udf(F.lit(size_per_large_column)))

    return df

# COMMAND ----------

#display(generate_1mb_row_df())

# COMMAND ----------

# MAGIC %md
# MAGIC # Start streaming data to Kafka

# COMMAND ----------


(generate_1mb_row_df(rowsPerSecond=100, numPartitions=12)
   .selectExpr("CAST(event_id AS STRING) AS key", "to_json(struct(*)) AS value")
   .writeStream
   .format("kafka")
   .option("kafka.bootstrap.servers", kafka_bootstrap_servers_tls)
   .option("kafka.security.protocol", "SASL_SSL")
   .option("kafka.sasl.mechanism", "PLAIN")
   .option("kafka.sasl.jaas.config", f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_api_key}" password="{kafka_api_secret}";')
   .option("checkpointLocation", checkpoint_location)
   .option("topic", topic)
   .option("kafka.max.request.size", "1100000")  # Setting new max request size to 1.1 MB
   .option("queryName", f"SendDataToKafka-{topic}")
   .start()
)


# COMMAND ----------


