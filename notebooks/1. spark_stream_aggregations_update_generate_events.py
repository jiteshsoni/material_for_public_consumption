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
from pyspark.sql.types import StructType
from datetime import datetime, timedelta
from faker import Faker
import random
import time


# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

catalog_dot_database_name = "soni.default"

# COMMAND ----------

# MAGIC %md
# MAGIC # Generate Fake Data at Scale

# COMMAND ----------

# MAGIC %md #### Use Faker to define functions to help generate fake column values

# COMMAND ----------

def generate_data(catalog_dot_database_name):
    fake = Faker()

    def random_time_within_6_hours(base_time):
        return base_time + timedelta(hours=random.randint(0, 6))

    base_time = datetime.now()

    # Number of unique request_ids
    num_unique_request_ids = 100
    request_ids = [fake.uuid4() for _ in range(num_unique_request_ids)]

    num_records = 600
    data = []

    for _ in range(num_records):
        chosen_request_id = random.choice(request_ids)
        event_time = random_time_within_6_hours(base_time)

        row = Row(
            event_ts=event_time,
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
            datekey=event_time.strftime("%Y%m%d"),
            request_id=chosen_request_id,
            allocation=fake.word(),
            rtb_bid_price=fake.random_number(digits=5),
            rtb_auction_price=fake.random_number(digits=5),
            rtb_auction_unit=fake.word(),
            rtb_bid_id=fake.uuid4(),
            rtb_bid_impression_id=fake.uuid4(),
            is_behavioral_targeted=fake.word(),
            assigned_hhid=fake.uuid4(),
            assigned_hhid_dma=fake.word(),
            parent_adgroup_id=fake.uuid4(),
            external_hhid=fake.uuid4(),
            matched_deal_id=fake.uuid4(),
            matched_datasource_segments=fake.word(),
            matched_device_segments=fake.word(),
            referer_url=fake.url(),
            viewability_tag_added=fake.word(),
            ip_addresses=fake.ipv4(),
        )
        data.append(row)

    event_schema = StructType().add("event_ts", "timestamp")\
        .add("delivery_receipt_id", "string")\
        .add("gesture_ad_id", "string")\
        .add("gesture_type", "string")\
        .add("ad_id", "string")\
        .add("ad_group_id", "string")\
        .add("ad_group_payment_type", "string")\
        .add("consumer_id", "string")\
        .add("request_type", "string")\
        .add("placement_id", "string")\
        .add("cpu", "string")\
        .add("conversion_id", "string")\
        .add("client_impression_tracking", "string")\
        .add("banner_id", "string")\
        .add("banner_details_id", "string")\
        .add("video_id", "string")\
        .add("datekey", "string")\
        .add("request_id", "string")\
        .add("allocation", "string")\
        .add("rtb_bid_price", "string")\
        .add("rtb_auction_price", "string")\
        .add("rtb_auction_unit", "string")\
        .add("rtb_bid_id", "string")\
        .add("rtb_bid_impression_id", "string")\
        .add("is_behavioral_targeted", "string")\
        .add("assigned_hhid", "string")\
        .add("assigned_hhid_dma", "string")\
        .add("parent_adgroup_id", "string")\
        .add("external_hhid", "string")\
        .add("matched_deal_id", "string")\
        .add("matched_datasource_segments", "string")\
        .add("matched_device_segments", "string")\
        .add("referer_url", "string")\
        .add("viewability_tag_added", "string")\
        .add("ip_addresses", "string")

    # Use the same schema as defined previously
    df = spark.createDataFrame(data, schema=event_schema)
    df.write.mode("append").saveAsTable(f"{catalog_dot_database_name}.events")

# COMMAND ----------

while True:
    generate_data(catalog_dot_database_name=catalog_dot_database_name)
    time.sleep(60)

# COMMAND ----------

display(
    spark.sql(f"""
        select min(event_ts), max(event_ts)
        from {catalog_dot_database_name}.events
""")
)

# COMMAND ----------

display(
    spark.sql(f"""
        select request_id,count(1) as number_of_records
        from {catalog_dot_database_name}.events
        group by all
        order by number_of_records desc
""")
)

# COMMAND ----------

display(
    spark.sql(f"""
        select *
        from {catalog_dot_database_name}.events
        where request_id = '47568c69-5959-4f8a-8738-62a2476a7c16'
""")
)

# COMMAND ----------

display(
    spark.sql(f"""
        select *
        from {catalog_dot_database_name}.events
""")
)

# COMMAND ----------

display(
    spark.sql(f"""
        select count(1)
        from {catalog_dot_database_name}.events
""")
)

# COMMAND ----------


