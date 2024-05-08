# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

import pandas as pd
from faker import Faker
import random
from datetime import timedelta
from pyarrow import parquet as pq
from pyarrow import Table
import uuid

# Initialize Faker
fake = Faker()

# COMMAND ----------


def generate_data(num_entries):
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
        
        # Create impression
        impressions.append({
            "Event_ID": event_id,
            "impression_id": impression_id,
            "User_ID": user_id,
            "Timestamp": timestamp,
            "Ad_Content": ad_content,
            "Device_Type": device_type,
            "Operating_System": operating_system,
            "Browser": browser,
            "Impression_Location": impression_location,
            "Time_of_Day": time_of_day,
            "Referrer_URL": referrer_url,
            "Ad_Size": ad_size,
            "Connection_Type": connection_type
        })
        
        # Randomly decide if this impression leads to a click
        if random.random() < 0.1:  # 10% chance of click
            click_timestamp = timestamp + timedelta(seconds=random.randint(1, 3600))
            clicks.append({
                "Click_ID": fake.uuid4(),
                "impression_id": impression_id,
                "Click_Timestamp": click_timestamp,
                "Country": fake.country(),
                "City": fake.city(),
                "Latitude": fake.latitude(),
                "Longitude": fake.longitude()
            })
    
    return pd.DataFrame(impressions), pd.DataFrame(clicks)

# Generate datasets
impressions_df, clicks_df = generate_data(1000)

# Example: Display the first few rows of the impressions dataset
print("Impressions Dataset Sample:")
print(impressions_df.head())


# Generate a random UUID for the impressions file name
impressions_file_name = str(uuid.uuid4()) + '.json'
# Write the impressions DataFrame to a JSON file with the random UUID as its name
impressions_df.to_json(impressions_file_name, orient='records', lines=True)

# Generate a random UUID for the clicks file name
clicks_file_name = str(uuid.uuid4()) + '.json'
# Write the clicks DataFrame to a JSON file with the random UUID as its name
clicks_df.to_json(clicks_file_name, orient='records', lines=True)

# Print the generated file names
print(f"Impressions file: {impressions_file_name}")
print(f"Clicks file: {clicks_file_name}")

# COMMAND ----------

impressions_df.head()

# COMMAND ----------

clicks_df.head()

# COMMAND ----------


