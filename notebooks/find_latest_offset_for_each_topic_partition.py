# Databricks notebook source
# MAGIC %md
# MAGIC ## Install necessary packages
# MAGIC

# COMMAND ----------

!pip install confluent-kafka

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import required libraries

# COMMAND ----------

import logging
from confluent_kafka import Consumer, KafkaError, TopicPartition, KafkaException
from confluent_kafka.admin import AdminClient
from collections import defaultdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC

# COMMAND ----------

# Kafka configuration
bootstrap_servers = "pkc.us-east-1.aws.confluent.cloud:9092"
username = ""
password = ""

# Clean up the checkpoint location
#dbutils.fs.rm(checkpoint_location, True)

# COMMAND ----------

from confluent_kafka import Consumer, TopicPartition, KafkaException
from confluent_kafka.admin import AdminClient
from collections import defaultdict
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

def get_all_topics_latest_offsets(config):
    try:
        # Create an AdminClient
        admin_client = AdminClient(config)

        # Create a Consumer (we won't actually consume, just use it to get metadata)
        consumer = Consumer(config)

        # Get the list of all topics
        cluster_metadata = admin_client.list_topics(timeout=30)
        all_topics = cluster_metadata.topics

        topics_latest_offsets = defaultdict(dict)

        for topic, topic_metadata in all_topics.items():
            # Skip internal topics (those starting with '__')
            if topic.startswith('__'):
                continue

            partitions = topic_metadata.partitions

            for partition_id in partitions:
                tp = TopicPartition(topic, partition_id)
                # Get the last offset (high watermark)
                low, high = consumer.get_watermark_offsets(tp, timeout=10)
                topics_latest_offsets[topic][partition_id] = high

        # Close the consumer
        consumer.close()

        return dict(topics_latest_offsets)

    except KafkaException as e:
        print(f"Error connecting to Kafka: {e}")
        return {}

# Kafka configuration
bootstrap_servers = 'pkc-p11xm.us-east-1.aws.confluent.cloud:9092'  # Replace with your Kafka broker(s)
config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'offset_checker',
    'auto.offset.reset': 'latest',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': username,  # Replace with your actual username
    'sasl.password': password,  # Replace with your actual password
    # Uncomment and set the following if needed:
    # 'ssl.ca.location': '/path/to/ca.pem'
}

# Get the latest offsets
all_topics_offsets = get_all_topics_latest_offsets(config)

# Create a Spark session (this is already available in Databricks, but included for completeness)
spark = SparkSession.builder.appName("KafkaOffsetsToDataFrame").getOrCreate()
if all_topics_offsets:
    # Convert the dictionary to a list of rows
    rows = []
    for topic, partitions in all_topics_offsets.items():
        for partition, offset in partitions.items():
            rows.append((bootstrap_servers, topic, partition, offset))

    # Define the schema for our DataFrame
    schema = StructType([
        StructField("bootstrap_servers", StringType(), False),
        StructField("topic", StringType(), False),
        StructField("partition", IntegerType(), False),
        StructField("offset", LongType(), False)
    ])

    # Create the DataFrame
    df = spark.createDataFrame(rows, schema)

    # Add a timestamp column
    df_with_timestamp = df.withColumn("timestamp", current_timestamp())

    # Show the DataFrame
    print("Latest offsets for all topics:")
    # Optionally, you can write this DataFrame to a Delta table or perform other operations
    df_with_timestamp.write.format("delta").mode("overwrite").saveAsTable("kafka_offsets")

else:
    print("No offset data retrieved. Check your Kafka connection settings.")

# Note: In Databricks, you typically don't need to stop the SparkSession
# spark.stop()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM kafka_offsets

# COMMAND ----------


