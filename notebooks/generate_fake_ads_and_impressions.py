# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import uuid
import datetime
from faker import Faker
import random
import time, json
from datetime import timedelta

# COMMAND ----------

spark.conf.set("spark.databricks.io.directoryCommit.createSuccessFile","false")
spark.conf.set("parquet.enable.summary-metadata", "false")
sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
spark.conf.set("spark.sql.sources.commitProtocolClass","org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")

# COMMAND ----------

# Initialize Faker and Spark session
fake = Faker()

# COMMAND ----------


# Set the prefix path where the files will be stored
prefix_path = "abfss://storage-container@databrickstrainingadls.dfs.core.windows.net/fake_data/"

# COMMAND ----------


def generate_data(num_entries, output_dir):
    impressions = []
    clicks = []
    for _ in range(num_entries):
        event_id = fake.uuid4()
        ad_id = fake.uuid4()
        user_id = fake.uuid4()
        impression_id = fake.uuid4()
        timestamp = fake.date_time_this_month(before_now=True, after_now=False)
        ad_content = fake.sentence()
        device_type = random.choice(['mobile', 'tablet', 'desktop'])
        operating_system = random.choice(['Windows', 'macOS', 'Linux', 'Android', 'iOS'])
        browser = random.choice(['Chrome', 'Firefox', 'Safari', 'Edge', 'Opera'])
        impression_location = fake.city()
        time_of_day = timestamp.strftime('%H:%M:%S')
        referrer_url = fake.url()
        ad_size = random.choice(['300x250', '728x90', '160x600', '320x50'])
        connection_type = random.choice(['WiFi', '4G', '5G'])

        impressions.append({
            "impression_id": impression_id,
            "User_ID": user_id,
            "Timestamp": timestamp,
            "Ad_Content": ad_content,
            "Time_of_Day": time_of_day,
            "Referrer_URL": referrer_url,
            "Ad_Size": ad_size,
            "Connection_Type": connection_type
        })

        if random.random() < 0.1:  # 10% chance of click
            click_timestamp = timestamp + timedelta(seconds=random.randint(1, 3600))
            clicks.append({
                "Click_ID": fake.uuid4(),
                "impression_id": impression_id,
                "Click_Timestamp": click_timestamp,
                "Country": fake.country(),
                "City": fake.city(),
                "Latitude": fake.latitude(),
                "Longitude": fake.longitude(),
                "Device_Type": device_type,
                "Operating_System": operating_system,
                "Browser": browser,
                "Impression_Location": impression_location,
            })

    impressions_df = spark.createDataFrame(impressions)
    clicks_df = spark.createDataFrame(clicks)

    # Generate a single output file for each DataFrame
    impressions_df.coalesce(1).write.format('json').mode('append').save(output_dir + '/impressions')
    clicks_df.coalesce(1).write.format('json').mode('append').save(output_dir + '/clicks')


# COMMAND ----------

while True:
    start_time = time.time()  # Record the start time
    generate_data(num_entries=1000, output_dir=prefix_path)
    end_time = time.time()  # Record the end time after the function has executed
    elapsed_time = end_time - start_time  # Calculate the time taken to run the function
    time_to_sleep = max(60 - elapsed_time, 0)  # Ensure the sleep time never goes negative
    print(f"time_to_sleep: {time_to_sleep}")
    time.sleep(time_to_sleep)  # Sleep for the remainder of the minute

# COMMAND ----------

# clicks_df = spark.readStream \
#     .format("cloudFiles") \
#     .option("cloudFiles.format", "json") \
#     .option("cloudFiles.schemaLocation", "/tmp/jite/") \
#             .option("inferSchema", "true") \
#     .load("abfss://storage-container@databrickstrainingadls.dfs.core.windows.net/fake_data/clicks/")
# display(clicks_df)

# COMMAND ----------

# impressions_df = spark.readStream \
#     .format("cloudFiles") \
#     .option("cloudFiles.format", "json") \
#     .option("cloudFiles.schemaLocation", "/tmp/jite2/") \
#             .option("inferSchema", "true") \
#     .load("abfss://storage-container@databrickstrainingadls.dfs.core.windows.net/fake_data/impressions/")
# display(impressions_df)

# COMMAND ----------


