# Databricks notebook source
# MAGIC %md
# MAGIC [You can enable RockDB-based state management by setting the following configuration in the SparkSession before starting the streaming query](https://docs.databricks.com/structured-streaming/rocksdb-state-store.html#configure-rocksdb-state-store-on-databricks)

# COMMAND ----------

spark.conf.set(
  "spark.sql.streaming.stateStore.providerClass",
  "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Libraries
# MAGIC Install Faker which is only needed for the purope of this demo.

# COMMAND ----------



# COMMAND ----------

!pip install faker_vehicle
!pip install faker

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from faker import Faker
from faker_vehicle import VehicleProvider
from pyspark.sql import functions as F
import uuid
from utils import logger

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# define schema name and where should the table be stored
schema_name = "test_streaming_joins"
schema_storage_location = "/tmp/CHOOSE_A_PERMANENT_LOCATION/"


# COMMAND ----------

# MAGIC %md
# MAGIC # Create the Target Schema/Database
# MAGIC Create a Schema and set location. This way all tables would inherit the base location.

# COMMAND ----------

create_schema_sql = f"""
    CREATE SCHEMA IF NOT EXISTS {schema_name}
    COMMENT 'This is {schema_name} schema'
    LOCATION '{schema_storage_location}'
    WITH DBPROPERTIES ( Owner='Jitesh');
    """
print(f"create_schema_sql: {create_schema_sql}")

# COMMAND ----------

spark.sql(create_schema_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC # Generate Fake Data at Scale

# COMMAND ----------

# MAGIC %md #### Use Faker to define functions to help generate fake column values

# COMMAND ----------

fake = Faker()
fake.add_provider(VehicleProvider)

event_id = F.udf(lambda: str(uuid.uuid4()))

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

# MAGIC %md
# MAGIC ### Generate Streaming source data at your desired rate

# COMMAND ----------


def generated_vehicle_and_geo_df(rowsPerSecond: int, numPartitions: int):
    return (
        spark.readStream.format("rate")
        .option("numPartitions", numPartitions)
        .option("rowsPerSecond", rowsPerSecond)
        .load()
        .withColumn("event_id", event_id())
        .withColumn("vehicle_year_make_model", vehicle_year_make_model())
        .withColumn("vehicle_year_make_model_cat", vehicle_year_make_model_cat())
        .withColumn("vehicle_make_model", vehicle_make_model())
        .withColumn("vehicle_make", vehicle_make())
        .withColumn("vehicle_year", vehicle_year())
        .withColumn("vehicle_category", vehicle_category())
        .withColumn("vehicle_object", vehicle_object())
        .withColumn("latitude", latitude())
        .withColumn("longitude", longitude())
        .withColumn("location_on_land", location_on_land())
        .withColumn("local_latlng", local_latlng())
        .withColumn("zipcode", zipcode())
    )


# You can uncomment the below display command to check if the code in this cell works
# display(generated_vehicle_and_geo_df)


# COMMAND ----------

# MAGIC %md
# MAGIC # Create the base source table: Vehicle_Geo Table

# COMMAND ----------

table_name_vehicle_geo = "vehicle_geo"
def stream_write_to_vehicle_geo_table(
    rowsPerSecond: int = 1000, numPartitions: int = 10
):
    (
        generated_vehicle_and_geo_df(rowsPerSecond, numPartitions)
        .writeStream.queryName(f"write_to_delta_table: {table_name_vehicle_geo}")
        .option(
            "checkpointLocation",
            f"{schema_storage_location}/{table_name_vehicle_geo}/_checkpoint",
        )
        .format("delta")
        .toTable(f"{schema_name}.{table_name_vehicle_geo}")
    )


# COMMAND ----------

stream_write_to_vehicle_geo_table(rowsPerSecond=1000, numPartitions=10)

# COMMAND ----------

# MAGIC %md
# MAGIC **Once you have generated enough data, kill the above stream and get a base line for row count**

# COMMAND ----------

spark.read.table(f"{schema_name}.{table_name_vehicle_geo}").count()

# COMMAND ----------

display(
    spark.sql(
        f"""
    SELECT * 
    FROM {schema_name}.{table_name_vehicle_geo}
"""
    )
)

# COMMAND ----------

display(
    spark.sql(
        f"""
    SELECT 
         min(timestamp)
        ,max(timestamp)
        ,current_timestamp()
    FROM {schema_name}.{table_name_vehicle_geo}
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta to Delta Streaming

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create a Vehicle Table

# COMMAND ----------

vehicle_df = (
    spark.readStream.format("delta")
    .option("maxFilesPerTrigger", "100")
    .table(f"{schema_name}.vehicle_geo")
    .selectExpr(
        "event_id",
        "timestamp as vehicle_timestamp",
        "vehicle_year_make_model",
        "vehicle_year_make_model_cat",
        "vehicle_make_model",
        "vehicle_make",
        "vehicle_year",
        "vehicle_category",
        "vehicle_object",
    )
)
# display(vehicle_df)

# COMMAND ----------

table_name_vehicle = "vehicle"
def stream_write_to_vehicle_table():
    (
        vehicle_df.writeStream
        # .trigger(availableNow=True)
        .queryName(f"write_to_delta_table: {table_name_vehicle}")
        .option(
            "checkpointLocation",
            f"{schema_storage_location}/{table_name_vehicle}/_checkpoint",
        )
        .format("delta")
        .toTable(f"{schema_name}.{table_name_vehicle}")
    )


stream_write_to_vehicle_table()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create a Geo Table
# MAGIC We have added a filter when we write to this table. This would be useful when we emulate the left join scenario.
# MAGIC
# MAGIC Filter:  ```where("value like '1%' ")```

# COMMAND ----------

geo_df = (
    spark.readStream.format("delta")
    .option("maxFilesPerTrigger", "100")
    .table(f"{schema_name}.vehicle_geo")
    .selectExpr(
        "event_id",
        "value",
        "timestamp as geo_timestamp",
        "latitude",
        "longitude",
        "location_on_land",
        "local_latlng",
        "cast( zipcode as integer) as zipcode",
    )
    .where("value like '1%' ")
)
# geo_df.printSchema()
# display(geo_df)

# COMMAND ----------

table_name_geo = "geo"
def stream_write_to_geo_table():
    
    (
        geo_df.writeStream
        # .trigger(availableNow=True)
        .queryName(f"write_to_delta_table: {table_name_geo}")
        .option(
            "checkpointLocation",
            f"{schema_storage_location}/{table_name_geo}/_checkpoint",
        )
        .format("delta")
        .toTable(f"{schema_name}.{table_name_geo}")
    )


stream_write_to_geo_table()

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT *
        FROM {schema_name}.{table_name_geo}
    """
    )
)

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT count(1) as row_count
        FROM {schema_name}.{table_name_geo}
    """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Set a base line using traditional SQL
# MAGIC Before we do the actual streaming joins. Let's do a regular join and figure out the expected row count

# COMMAND ----------

# DBTITLE 1,Inner Join
sql_query_batch_inner_join = f"""
        SELECT count(vehicle.event_id) as row_count_for_inner_join
        FROM {schema_name}.{table_name_vehicle} vehicle
        JOIN {schema_name}.{table_name_geo} geo
        ON vehicle.event_id = geo.event_id
        AND vehicle_timestamp BETWEEN geo_timestamp  - INTERVAL 5 MINUTES AND geo_timestamp
        """
print(
    f""" Run SQL Query: 
          {sql_query_batch_inner_join}       
       """
)
display(spark.sql(sql_query_batch_inner_join))


# COMMAND ----------

# DBTITLE 1,Left Join
sql_query_batch_left_join = f"""
        SELECT count(vehicle.event_id) as row_count_for_left_join
        FROM {schema_name}.{table_name_vehicle} vehicle
        LEFT JOIN {schema_name}.{table_name_geo} geo
        ON vehicle.event_id = geo.event_id
            -- Assume there is a business logic that timestamp cannot be more than X minutes off
        AND vehicle_timestamp BETWEEN geo_timestamp  - INTERVAL 5 MINUTES AND geo_timestamp
        """
print(
    f""" Run SQL Query: 
          {sql_query_batch_left_join}       
       """
)
display(spark.sql(sql_query_batch_left_join))


# COMMAND ----------

# MAGIC %md
# MAGIC #### Summary so far:
# MAGIC 1. We created a Source Delta Table : vehicle_geo
# MAGIC 2. We took the previous table and divided it's column into 2 table: Vehicle and Geo
# MAGIC 3. Vehicle row count matches with vehicle_geo and it has a subset of those columns
# MAGIC 4. Geo row count is lesser then Vehicle because we added a filter when we wrote to Geo table
# MAGIC 5. We ran 2 SQL to identify what should the row count after we do stream-stream join

# COMMAND ----------

# MAGIC %md
# MAGIC # Stream-Stream Join

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a function to stream_from_delta_and_create_view

# COMMAND ----------


def stream_from_delta_and_create_view(
    schema_name: str,
    table_name: str,
    column_to_watermark_on: str,
    how_late_can_the_data_be: str = "2 minutes",
    maxFilesPerTrigger: int = 100,
):
    view_name = f"_streaming_vw_{schema_name}_{table_name}"
    print(
        f"Table {schema_name}.{table_name} is now streaming under a temporoary view called {view_name}"
    )
    (
        spark.readStream.format("delta")
        .option("maxFilesPerTrigger", f"{maxFilesPerTrigger}")
        .option("withEventTimeOrder", "true")
        .table(f"{schema_name}.{table_name}")
        .withWatermark(f"{column_to_watermark_on}", how_late_can_the_data_be)
        .createOrReplaceTempView(view_name)
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Streaming Views
# MAGIC Some people prefer to write logic in SQL. Thus, we are creating streaming views which could be manipulated with SQL

# COMMAND ----------

# MAGIC %md
# MAGIC #### Vehicle Stream

# COMMAND ----------

stream_from_delta_and_create_view(
    schema_name=schema_name,
    table_name="vehicle",
    column_to_watermark_on="vehicle_timestamp",
    how_late_can_the_data_be="1 minutes",
)


# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT COUNT(1)
        FROM _streaming_vw_test_streaming_joins_vehicle
    """
    )
)

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT *
        FROM _streaming_vw_test_streaming_joins_vehicle
    """
    )
)

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT 
            vehicle_make
            ,count(1) as row_count
        FROM _streaming_vw_test_streaming_joins_vehicle
        GROUP BY vehicle_make
        ORDER BY vehicle_make
    """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Geo Stream

# COMMAND ----------

stream_from_delta_and_create_view(
    schema_name=schema_name,
    table_name="geo",
    column_to_watermark_on="geo_timestamp",
    how_late_can_the_data_be="2 minutes",
)

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT *
        FROM _streaming_vw_test_streaming_joins_geo
    """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stream Stream Inner Join

# COMMAND ----------

sql_for_stream_stream_inner_join = f"""
    SELECT 
        vehicle.*
        ,geo.latitude
        ,geo.longitude
        ,geo.zipcode
    FROM _streaming_vw_test_streaming_joins_vehicle vehicle
    JOIN _streaming_vw_test_streaming_joins_geo geo
    ON vehicle.event_id = geo.event_id
    -- Assume there is a business logic that timestamp cannot be more than X minutes off
    AND vehicle_timestamp BETWEEN geo_timestamp  - INTERVAL 5 MINUTES AND geo_timestamp
"""
# display(spark.sql(sql_for_stream_stream_inner_join))

# COMMAND ----------


table_name_stream_stream_innner_join = "stream_stream_innner_join"

(
    spark.sql(sql_for_stream_stream_inner_join)
    .writeStream
    # .trigger(availableNow=True)
    .queryName(f"write_to_delta_table: {table_name_stream_stream_innner_join}")
    .option(
        "checkpointLocation",
        f"{schema_storage_location}/{table_name_stream_stream_innner_join}/_checkpoint",
    )
    .format("delta")
    .toTable(f"{schema_name}.{table_name_stream_stream_innner_join}")
)

# COMMAND ----------

# MAGIC %md
# MAGIC If the stream has finished then in the next step. You should find that row count should match up with the regular batch SQL Job

# COMMAND ----------

spark.read.table(f"{schema_name}.{table_name_stream_stream_innner_join}").count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stream Stream Left Join

# COMMAND ----------

sql_for_stream_stream_left_join = f"""
    SELECT 
        vehicle.*
        ,geo.latitude
        ,geo.longitude
        ,geo.zipcode
    FROM _streaming_vw_test_streaming_joins_vehicle vehicle
    LEFT JOIN _streaming_vw_test_streaming_joins_geo geo
    ON vehicle.event_id = geo.event_id
        AND vehicle_timestamp BETWEEN geo_timestamp  - INTERVAL 5 MINUTES AND geo_timestamp
"""
# display(spark.sql(sql_for_stream_stream_left_join))

# COMMAND ----------

table_name_stream_stream_left_join = "stream_stream_left_join"

(
    spark.sql(sql_for_stream_stream_left_join)
    .writeStream
    # .trigger(availableNow=True)
    .queryName(f"write_to_delta_table: {table_name_stream_stream_left_join}")
    .option(
        "checkpointLocation",
        f"{schema_storage_location}/{table_name_stream_stream_left_join}/_checkpoint",
    )
    .format("delta")
    .toTable(f"{schema_name}.{table_name_stream_stream_left_join}")
)

# COMMAND ----------

spark.read.table(f"{schema_name}.{table_name_stream_stream_left_join}").count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### You will find that some records could not match are not being released which is expected.
# MAGIC
# MAGIC [The outer NULL results will be generated with a delay that depends on the specified watermark delay and the time range condition. This is because the engine has to wait for that long to ensure there were no matches and there will be no more matches in future.](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#outer-joins-with-watermarking)
# MAGIC
# MAGIC **Watermark will advance once new data is pushed to it**

# COMMAND ----------

stream_write_to_vehicle_geo_table(rowsPerSecond=10, numPartitions=10)

# COMMAND ----------

# MAGIC %md
# MAGIC Send a batch of data is written and kill the above Stream. What to observe:
# MAGIC 1. Soon you should see the watermark moves ahead and number of records in 'Aggregation State' goes down.
# MAGIC 2. If you click on the running stream and click the raw data tab and look for "watermark". You would see it has advanced
# MAGIC 3. Once 0 records per seconds are being processed that means your stream has caught up and now your row count should match up with the traditional SQL left join

# COMMAND ----------

spark.read.table(f"{schema_name}.{table_name_stream_stream_left_join}").count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean up the databse which was created

# COMMAND ----------

spark.sql(
    f"""
    drop schema if exists {schema_name} CASCADE
"""
)

# COMMAND ----------

dbutils.fs.rm(schema_storage_location, True)

# COMMAND ----------

dbutils.fs.ls(schema_storage_location)

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in test_streaming_joins

# COMMAND ----------


