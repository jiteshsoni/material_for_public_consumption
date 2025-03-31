# Databricks notebook source
# MAGIC %md
# MAGIC # [Delta Lake - State of the Project ](https://delta.io/blog/state-of-the-project-pt1/?utm_source=bambu&utm_medium=social&blaid=5878484)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

# Import necessary libraries and modules for the entire script
import concurrent.futures
import uuid
from datetime import datetime
from pyspark.sql import DataFrame

# COMMAND ----------

#spark.conf.set("spark.databricks.streaming.forEachBatch.optimized.enabled","true")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

catalog_database = "soni.testing_parallel_writes"
source_table = f"soni.default.synthetic_iot_data"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Streaming Source. It could be Kafka, Kinesis, S3, ADLS, Delta. For my case, I am considering Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE soni.d

# COMMAND ----------

from pyspark.sql.functions import col, when, current_timestamp, rand

stream_source_df = (
    spark.readStream.option("maxFilesPerTrigger", 500)
    .option("startingVersion", "10000")
    .table(source_table)
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## The function which we will run in parallel

# COMMAND ----------

def process_table(device_type: str, batch_id: int, catalog_database: str, input_df: DataFrame) -> None:
    """
    Process a single device type table by filtering the DataFrame and saving it
    as a table in the specified catalog database.

    Args:
        device_type (str): The device type to filter by.
        batch_id (int): The batch identifier (currently unused; reserved for future use).
        catalog_database (str): The catalog database name where the table will be saved.
        input_df (DataFrame): The input Spark DataFrame containing all device data.
    """
    print_with_timestamp(f"Processing table for device type: {device_type}")
    
    # Filter the DataFrame for rows that match the given device type.
    filtered_df = input_df.filter(input_df.device_type == device_type)
    
    # Build the full table name.
    table_full_name = f"{catalog_database}.{device_type}"

    # Inflict an error code column to simulate a failure.
    if device_type == 'Sensor':
        table_full_name = f"{catalog_database}.$${device_type}"

    
    # Save the filtered DataFrame as a table with additional options.
    filtered_df.write.mode("append") \
        .option("txnVersion", batch_id) \
        .option("txnAppId", table_full_name) \
        .saveAsTable(table_full_name)
    
    print_with_timestamp(f"Finished writing table {table_full_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Class so that we can pass parameters and do any custom processing

# COMMAND ----------


def print_with_timestamp(message: str) -> None:
    """
    Print the given message prefixed with the current UTC timestamp.
    
    Args:
        message (str): The message to be printed.
    """
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"{timestamp} - {message}")



class ForEachBatchProcessor:
    """
    A processor for applying changes to micro-batch DataFrames. It filters the batch by device type
    and writes the results to separate tables concurrently.
    """
    def __init__(self, catalog_database: str):
        """
        Initialize the batch processor with the catalog database name.

        Args:
            catalog_database (str): The name of the catalog database.
        """
        self.catalog_database = catalog_database

    def print_attributes(self) -> None:
        """
        Print all instance attributes for debugging purposes.
        """
        for attribute, value in vars(self).items():
            print_with_timestamp(f"{attribute}: {value}")

    def process_micro_batch(self, micro_batch_df: DataFrame, batch_id: int) -> None:
        """
        Processes a micro-batch by caching the DataFrame and concurrently processing
        each device type. After processing, the DataFrame is unpersisted.
        
        Args:
            micro_batch_df (DataFrame): The micro-batch Spark DataFrame.
            batch_id (int): The unique identifier for the current batch.
        
        Raises:
            Exception: Aggregated exception if one or more device type tasks fail.
        """
        self.print_attributes()
        print_with_timestamp(f"Processing batch_id: {batch_id}")

        # Cache the DataFrame to optimize multiple accesses during processing.
        micro_batch_df.cache()

        # Define the list of device types to process.
        device_types = ["Controller", "Gateway", "Sensor", "Actuator"]
        exceptions = []  # To accumulate exceptions from threads.

        # Process each device type concurrently using a ThreadPoolExecutor.
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            # Submit tasks for each device type.
            future_to_device = {
                executor.submit(process_table, device_type, batch_id, self.catalog_database, micro_batch_df): device_type
                for device_type in device_types
            }
            
            # Retrieve results from futures as they complete.
            for future in concurrent.futures.as_completed(future_to_device):
                current_device = future_to_device[future]
                try:
                    future.result()
                except Exception as e:
                    print_with_timestamp(f"Exception occurred processing device type {current_device}: {e}")
                    exceptions.append((current_device, e))

        # Unpersist the DataFrame to free up memory.
        micro_batch_df.unpersist()

        # If there were any exceptions, raise an aggregated error.
        if exceptions:
            error_messages = ", ".join([f"{device}: {err}" for device, err in exceptions])
            raise Exception(f"Errors occurred in one or more threads: {error_messages}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Create an instance of forEachBatchProcessor Class with the parameters

# COMMAND ----------

instantiateForEachBatchProcessor = ForEachBatchProcessor(
    catalog_database=catalog_database,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orchestrate the job

# COMMAND ----------

(
    stream_source_df.writeStream
    .trigger(availableNow=True)
    .option("checkpointLocation", f"/tmp/_checkpoint_{uuid.uuid4()}/")
    .option("queryName", "ParameterizeForEachBatch")
    .foreachBatch(instantiateForEachBatchProcessor.process_micro_batch)
    .start()
)

# COMMAND ----------


