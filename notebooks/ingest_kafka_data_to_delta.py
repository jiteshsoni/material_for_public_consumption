# Databricks notebook source
# MAGIC %md
# MAGIC ## [Link to Kafka Cluster](https://confluent.cloud/environments/env-g9z3n3/clusters/lkc-mn35ow/connectors/sources/sample_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# Generate a unique scope name
scope_name = f'master_class_scope'
kafka_bootstrap_servers_tls = "pkc-mz3gw.westus3.azure.confluent.cloud:9092"
topic = "sample_data_stock_trades"
target_table = f"cdg_databricks_workspace_jan_2025.default.{topic}"
# Ideally do not write it temp but an actual location on s3 or blob stroage
checkpoint_location_prefix = f"/tmp/_checkpoint/{target_table}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handle Kafka secrets

# COMMAND ----------

# from databricks.sdk import WorkspaceClient

# # Create a WorkspaceClient
# w = WorkspaceClient()


# # Create the scope
# w.secrets.create_scope(scope=scope_name)

# # Add the secret to the scope
# w.secrets.put_secret(scope=scope_name, key="kafka_api_key", string_value="your api key here ")
# w.secrets.put_secret(scope=scope_name, key="kafka_api_secret", string_value="your api secrete here")

# COMMAND ----------

dbutils.secrets.list(scope=scope_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get back secrets from Databricks Secret

# COMMAND ----------

kafka_api_key = dbutils.secrets.get(scope=scope_name, key="kafka_api_key")
kafka_api_secret = dbutils.secrets.get(scope=scope_name, key="kafka_api_secret")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Pretty print JSON

# COMMAND ----------

import json

def print_pretty_json(data):
    """
    Parses and prints JSON fragments or JSON lines in a pretty format.

    Args:
        data (str): The input string containing JSON or JSON-like fragments.

    Returns:
        None
    """
    try:
        # Split the input by newlines to handle each fragment separately
        lines = data.split("\n")
        
        for line in lines:
            # Skip version or non-JSON lines
            if line.startswith("v"):
                print(f"Version metadata: {line}")
                continue
            
            # Attempt to parse and pretty-print each line as JSON
            try:
                json_obj = json.loads(line)
                pretty_json = json.dumps(json_obj, indent=4)
                print(pretty_json)
            except json.JSONDecodeError:
                print(f"Invalid JSON line: {line}")
    
    except Exception as e:
        print(f"An error occurred: {e}")


# COMMAND ----------

# MAGIC %md
# MAGIC # Start Ingestion from Kafka

# COMMAND ----------

from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC

# COMMAND ----------

def read_kafka_stream():
    # Read Kafka stream and process in a single step
    structured_stream = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers_tls)
        .option("failOnDataLoss", "false")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_api_key}" password="{kafka_api_secret}";')
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("minPartitions",6 ) # Match the number of Kafka Partitions 
        .option("maxOffsetsPerTrigger",100)
        .load()
        .withColumn("casted_value", expr("CAST(value AS STRING)"))
    )
    
    return structured_stream




# COMMAND ----------

# Display the structured stream
#display(read_kafka_stream())

# COMMAND ----------

# Write the streaming data to a Delta table
(
    read_kafka_stream()
    .writeStream
    .queryName(f"write_kafka_topic_{topic}_to_table_{target_table}")  # Assign a name to the stream
    .outputMode("append")
    .option("checkpointLocation", f"{checkpoint_location_prefix}")
    .trigger(availableNow=True) #.trigger(availableNow=True) #    \
    .toTable(target_table)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lets explore the checkpoint

# COMMAND ----------

checkpoint_location = f"{checkpoint_location_prefix}"
display(dbutils.fs.ls(checkpoint_location))

# COMMAND ----------

print_pretty_json(dbutils.fs.head(f"{checkpoint_location}/metadata/"))

# COMMAND ----------

dbutils.fs.ls(f"{checkpoint_location}/sources/")

# COMMAND ----------

dbutils.fs.ls(f"{checkpoint_location}/sources/0/0")

# COMMAND ----------

dbutils.fs.head(f"{checkpoint_location}/sources/0/0")

# COMMAND ----------

print_pretty_json(dbutils.fs.head(f"{checkpoint_location}/sources/0/0"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explore Offsets folder

# COMMAND ----------

display(dbutils.fs.ls(f"{checkpoint_location}/offsets/"))

# COMMAND ----------

display(dbutils.fs.ls(f"{checkpoint_location}/offsets/486"))

# COMMAND ----------

dbutils.fs.head(f"{checkpoint_location}/offsets/371")

# COMMAND ----------



# COMMAND ----------

print_pretty_json(dbutils.fs.head(f"{checkpoint_location}/offsets/599"))

# COMMAND ----------

print_pretty_json(dbutils.fs.head(f"{checkpoint_location}/offsets/600"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explore commits folder

# COMMAND ----------

display(dbutils.fs.ls(f"{checkpoint_location}/commits/"))

# COMMAND ----------

print_pretty_json(dbutils.fs.head(f"{checkpoint_location}/commits/600"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Beginner's stop here. This is enough information for you to build your first job

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explore metadata folder
# MAGIC
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls(f"{checkpoint_location}/metadata/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add the concept of state

# COMMAND ----------

def read_kafka_strea_and_apply_state():
    # Read Kafka stream and process in a single step
    structured_stream = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers_tls)
        .option("failOnDataLoss", "false")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_api_key}" password="{kafka_api_secret}";')
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("minPartitions",6)
        .option("maxOffsetsPerTrigger",100)
        .load()
        .withWatermark("timestamp","60 minutes")
        .dropDuplicatesWithinWatermark(["partition", "offset"])
        .withColumn("casted_value", expr("CAST(value AS STRING)"))
    )
    
    return structured_stream




# COMMAND ----------

#display(read_kafka_strea_and_apply_state())

# COMMAND ----------

dbutils.fs.rm(checkpoint_location_prefix_for_stateful_streaming, recurse=True)

# COMMAND ----------

target_table_for_stateful_streaming = f"{target_table}_for_stateful_streaming"
checkpoint_location_prefix_for_stateful_streaming = f"/tmp/_checkpoint/{target_table_for_stateful_streaming}"

# Write the streaming data to a Delta table
(
    read_kafka_strea_and_apply_state()
    .writeStream
    .queryName(f"write_kafka_topic_{topic}_to_table_{target_table_for_stateful_streaming}")  # Assign a name to the stream
    .outputMode("append")
    .option("checkpointLocation", f"{checkpoint_location_prefix_for_stateful_streaming}")
    .toTable(target_table_for_stateful_streaming)
)

# COMMAND ----------

display(dbutils.fs.ls(checkpoint_location_prefix_for_stateful_streaming))

# COMMAND ----------

display(dbutils.fs.ls(f"{checkpoint_location_prefix_for_stateful_streaming}/state"))

# COMMAND ----------

display(dbutils.fs.ls(f"{checkpoint_location_prefix_for_stateful_streaming}/state/0/"))

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Look at the change in the processing speed after we set spark shuffle partitions

# COMMAND ----------

# Dynamically set the shuffle partitions for the current job
spark.conf.set("spark.sql.shuffle.partitions", "6")  # Set to 6 partitions to match kafka
# Verify the change
print(f"Shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")


# COMMAND ----------

display(dbutils.fs.ls(f"{checkpoint_location_prefix_for_stateful_streaming}/state/0/"))

# COMMAND ----------

display(dbutils.fs.ls(f"{checkpoint_location_prefix_for_stateful_streaming}/state/0/1"))

# COMMAND ----------

display(dbutils.fs.ls(checkpoint_location_prefix_for_stateful_streaming))

# COMMAND ----------

# MAGIC %md
# MAGIC ## New API to read state
# MAGIC https://www.databricks.com/resources/demos/videos/data-streaming/querying-state-data-in-spark-structured-streaming-with-state-reader-api?itm_data=demo_center

# COMMAND ----------

# checkpoint_location
# checkpoint_location_prefix_for_stateful_streaming

# COMMAND ----------

statestore_df = spark.read.format("statestore").load(checkpoint_location_prefix_for_stateful_streaming)
display(statestore_df)

# COMMAND ----------

state_metadata_df = spark.read.format("state-metadata").load(checkpoint_location_prefix_for_stateful_streaming)
display(state_metadata_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Clean checkpoints and tables

# COMMAND ----------

dbutils.fs.rm(checkpoint_location_prefix_for_stateful_streaming,True)

# COMMAND ----------

dbutils.fs.rm(checkpoint_location_prefix,True)

# COMMAND ----------

spark.sql(f"Drop table if exists {target_table}")

# COMMAND ----------

spark.sql(f"Drop table if exists {target_table_for_stateful_streaming}")

# COMMAND ----------

 dbutils.fs.rm("/tmp/_checkpoint/", True)

# COMMAND ----------


