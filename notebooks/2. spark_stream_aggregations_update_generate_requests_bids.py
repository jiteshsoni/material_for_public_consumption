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

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import StructType, StringType, StructField, TimestampType
from datetime import datetime, timedelta
from faker import Faker
import random, time


fake = Faker()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# define schema name and where should the table be stored
catalog_dot_database_name = "soni.default"
target_table = "requests_bids"
check_point_location = f"/tmp/{catalog_dot_database_name}.{target_table}"

# COMMAND ----------

# MAGIC %md
# MAGIC # Generate Fake Data at Scale

# COMMAND ----------

generated_events_df = (
     spark.readStream
        .table(f"{catalog_dot_database_name}.events")
)
#display(generated_events_df)

# COMMAND ----------

class forEachBatchProcessor:
    def __init__(self, catalog_dot_database_name: str, target_table: str):
        self.catalog_dot_database_name = catalog_dot_database_name
        self.target_table = target_table

    def print_attributes(self):
        attributes = vars(self)
        print(
            "\n".join([f"{attr}: {value}" for attr, value in attributes.items()])
        )

    def make_changes_using_the_micro_batch(self, microBatchOutputDF, batchId: int):
        self.print_attributes()
        print(f"Processing batchId: {batchId}")
        # Your processing logic using the parameter
        view_name = f"updates_for_batchId_{batchId}"
        requests_bids_schema = StructType([
            StructField("event_ts", TimestampType(), True),
            StructField("delivery_receipt_id", StringType(), True),
            StructField("gesture_ad_id", StringType(), True),
            StructField("gesture_type", StringType(), True),
            StructField("ad_id", StringType(), True),
            StructField("ad_group_id", StringType(), True),
            StructField("ad_group_payment_type", StringType(), True),
            StructField("consumer_id", StringType(), True),
            StructField("request_type", StringType(), True),
            StructField("placement_id", StringType(), True),
            StructField("cpu", StringType(), True),
            StructField("conversion_id", StringType(), True),
            StructField("client_impression_tracking", StringType(), True),
            StructField("banner_id", StringType(), True),
            StructField("banner_details_id", StringType(), True),
            StructField("video_id", StringType(), True),
            StructField("datekey", StringType(), True),
            StructField("request_id", StringType(), True),
            StructField("allocation", StringType(), True),
            StructField("ip_addresses", StringType(), True),
        ])
        unique_request_ids = microBatchOutputDF.select("request_id").distinct()

        data = []
        for row in unique_request_ids.collect():
            request_id = row.request_id
            event_time = datetime.now()  # This returns a datetime object, not a string
            
            row = Row(
                event_ts=event_time,  # Directly use the datetime object
                delivery_receipt_id=fake.uuid4(),
                gesture_ad_id=fake.uuid4(),
                gesture_type=fake.word(),
                ad_id=fake.uuid4(),
                ad_group_id=fake.uuid4(),
                ad_group_payment_type=fake.word(),
                consumer_id=fake.uuid4(),
                request_type=fake.word(),
                placement_id=fake.uuid4(),
                cpu=fake.word(),
                conversion_id=fake.uuid4(),
                client_impression_tracking=fake.url(),
                banner_id=fake.uuid4(),
                banner_details_id=fake.uuid4(),
                video_id=fake.uuid4(),
                datekey=datetime.now().strftime("%Y%m%d"),
                request_id=request_id,
                allocation=fake.word(),
                ip_addresses=fake.ipv4(),
            )
            data.append(row)


        requests_bids_df = spark.createDataFrame(data, schema=requests_bids_schema)  # or your custom schema
        requests_bids_df.write.mode("append").saveAsTable(f"{self.catalog_dot_database_name}.{self.target_table}")

# COMMAND ----------

instantiateForEachBatchProcessor = forEachBatchProcessor(
            catalog_dot_database_name= catalog_dot_database_name,
            target_table= target_table
        )

# COMMAND ----------

i = 0
while True:
    (
    generated_events_df
      .writeStream
      .option("checkpointLocation", check_point_location)
      .option("queryName", f"CreateDataFor{catalog_dot_database_name}.{target_table}_{i}")
      .foreachBatch(instantiateForEachBatchProcessor.make_changes_using_the_micro_batch)
      .start()
    )
    time.sleep(180)
    i+=1

# COMMAND ----------

display(spark.sql(f"""
    select min(event_ts), max(event_ts)
    from {catalog_dot_database_name}.{target_table}
"""))

# COMMAND ----------

display(spark.sql(f"""
    select request_id,count(1) as number_of_records
    from {catalog_dot_database_name}.{target_table}
    group by all
    order by number_of_records desc
"""))

# COMMAND ----------

display(spark.sql(f"""
select *
from {catalog_dot_database_name}.{target_table}
where request_id = 'b522510e-a021-446a-b64e-dfbec79d0dc1'
"""))

# COMMAND ----------

display(spark.sql(f"""
    select *
    from {catalog_dot_database_name}.{target_table}
"""))

# COMMAND ----------

display(spark.sql(f"""
    select count(1)
    from {catalog_dot_database_name}.{target_table}
"""))

# COMMAND ----------


