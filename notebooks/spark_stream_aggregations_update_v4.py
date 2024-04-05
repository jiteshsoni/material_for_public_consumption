# Databricks notebook source
# MAGIC %md
# MAGIC 1. Aggregate the data inside the for each batch approach
# MAGIC 2. No watermarks, no state store
# MAGIC 3. Merge could be replace with delete and insert. We need to accept the risk that update succeds and insert does not.
# MAGIC 4. Merge does not allow schema evolution

# COMMAND ----------

!pip install faker

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------


from pyspark.sql import functions as F
from pyspark.sql.functions import col, concat, current_timestamp, collect_set
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from utils import logger, does_table_exist
from pyspark.sql.streaming import StreamingQuery
from delta.tables import DeltaTable


from faker import Faker

# Initialize Faker and Spark session
fake = Faker()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# define schema name and where should the table be stored
catalog_dot_database_name = "soni.default"
target_table = f"{catalog_dot_database_name}.streaming_agg_fake_events_with_merge"
source_table = f"{catalog_dot_database_name}.fake_events"
checkpoint_location= f"/tmp/{target_table}/_checkpoint/",

# COMMAND ----------

def generate_fake_data(num_records:int):
    # Define the schema
    schema = StructType([
        StructField("request_id", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("mac_id", StringType(), True),
    ])

    # Generate fake data
    data = [(fake.word(), fake.ipv4(), fake.mac_address()) for _ in range(num_records)]

    # Create DataFrame with the defined schema
    df = spark.createDataFrame(data, schema=schema).withColumn("event_ts",current_timestamp())

    display(df)
    df.write.mode("append").saveAsTable(source_table)

# COMMAND ----------

generate_fake_data(num_records=500)

# COMMAND ----------

checkpoint_location

# COMMAND ----------

def cleanup():
    spark.sql(f"DROP TABLE IF EXISTS {target_table};")
    spark.sql(f"DROP TABLE IF EXISTS {source_table};")
    dbutils.fs.rm(f"{checkpoint_location}", True)

#cleanup()

# COMMAND ----------

display(    
    spark.sql(f"""
        show tables in {catalog_dot_database_name}
        """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Create the Target Schema/Database
# MAGIC Create a Schema and set location. This way all tables would inherit the base location.

# COMMAND ----------

checkpoint_location

# COMMAND ----------

events_stream = spark.readStream.table(f"{catalog_dot_database_name}.fake_events")

#display(events_stream)
events_stream.printSchema()

# COMMAND ----------

class forEachBatchProcessor:
    def __init__(self, target_table: str):
        self.target_table = target_table

    def print_attributes(self):
        attributes = vars(self)
        print(
            "\n".join([f"{attr}: {value}" for attr, value in attributes.items()])
        )

    def make_changes_using_the_micro_batch(self, microBatchOutputDF, batchId: int):
        self.print_attributes()
        print(f"Processing batchId: {batchId}")
        agg_df = aggregatedDF = microBatchOutputDF.groupBy("request_id").agg(
            F.collect_set("event_ts").alias("collected_event_ts"),
            F.collect_set(F.struct(*microBatchOutputDF.columns)).alias("collected_events")
        ).withColumn("ingestion_timestamp",current_timestamp())
        if does_table_exist(spark=spark, table_name=self.target_table):
            print (f"We can merge into {self.target_table} ")
            # Load the Delta table
            delta_table = DeltaTable.forName(spark,target_table)
            # Merge operation
            delta_table.alias("delta").merge(
                agg_df.alias("updates"),
                "delta.request_id = updates.request_id"
            ).whenMatchedUpdate(set={
                # Update logic: concatenate collected_events from both tables
                "collected_events": F.concat(col("delta.collected_events"), col("updates.collected_events"))
            }).whenNotMatchedInsertAll().execute()
        else:
            print (f"Lets create table: {self.target_table} ")
            agg_df.write.mode("append").saveAsTable(self.target_table)


# COMMAND ----------

instantiateForEachBatchProcessor = forEachBatchProcessor(
            target_table = target_table,
        )

# COMMAND ----------

generate_fake_data(num_records= 5)
(
  events_stream
    .writeStream
    .trigger(availableNow=True) 
    .option("checkpointLocation", checkpoint_location)
    .option("queryName", f"StreamTo{target_table}")
    .foreachBatch(instantiateForEachBatchProcessor.make_changes_using_the_micro_batch)
    .start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT request_id, size(collected_events) as number_of_Events
# MAGIC FROM SONI.default.streaming_agg_fake_events_with_merge
# MAGIC order by number_of_Events desc

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM SONI.default.streaming_agg_fake_events_with_merge
# MAGIC where request_id = 'film'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(1)
# MAGIC FROM SONI.default.streaming_agg_fake_events_with_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT request_id, SIZE() as duplicates
# MAGIC FROM SONI.default.streaming_agg_fake_events_with_merge
# MAGIC GROUP BY ALL 
# MAGIC order by duplicates desc

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT request_id, count(1) as duplicates
# MAGIC FROM SONI.default.streaming_agg_fake_events_with_merge
# MAGIC GROUP BY ALL 
# MAGIC order by duplicates desc

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1)
# MAGIC FROM SONI.default.fake_events

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM SONI.default.streaming_agg_fake_events_with_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT request_id, count(1) as number_of_duplicates
# MAGIC FROM SONI.default.streaming_agg_fake_events_with_merge
# MAGIC GROUP BY ALL
# MAGIC ORDER BY number_of_duplicates DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT request_id, SIZE(collected_events) AS num_events
# MAGIC FROM SONI.default.streaming_agg_events_with_merge
# MAGIC ORDER BY num_events DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) AS row_count
# MAGIC FROM SONI.default.streaming_agg_events_with_merge
