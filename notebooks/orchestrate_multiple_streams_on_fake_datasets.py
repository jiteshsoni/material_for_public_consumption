# Databricks notebook source
!pip install faker

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, FloatType, ArrayType, DateType, TimestampType
from faker import Faker
import random
from datetime import datetime
import logging

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters & Intialization

# COMMAND ----------

fake = Faker()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

check_point_location_prefix = "/tmp/checkpoints/healthcare_checkpoints/"
health_care_table = "soni.default.fake_health_care"
health_care_streaming_view = "health_care_streaming_view"
pt_cohort_table = "soni.default.fake_pt_cohort"
partition_column = "opportunity_type"


# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Fake Data

# COMMAND ----------

def generate_fake_data():
    """
    Generates a dictionary containing fake healthcare-related data.
    
    Returns:
        dict: A dictionary with fake healthcare-related data.
    """
    current_year = datetime.now().year
    data = {
        "pt_id": 1,
        "event_timestamp": fake.date_time_this_year(),
        "primary_prac_id": fake.random_int(min=100, max=200),
        "is_attribution_risk": fake.boolean(),
        "has_awv_appt": fake.boolean(),
        "in_map": random.choice(["IN_MAP", "NOT_IN_MAP"]),
        "in_map_years": [current_year, current_year - 1],
        "score_cacp": round(random.uniform(10, 15), 2),
        "score_kcm": round(random.uniform(10, 15), 2),
        "elig_awv_date": fake.date_this_year(),
        "last_awv_date": fake.date_this_year(before_today=True, after_today=False),
        "last_awv_is_medicare_age_in": fake.boolean(),
        "last_hospice_date": fake.date_this_year(before_today=True, after_today=False),
        "last_well_child_visit": fake.date_this_year(before_today=True, after_today=False),
        "insurer_line_of_business": random.choice(["Medicare FFS", "MA"]),
        "patient_insurer": fake.company(),
        "calculated_pdc": random.randint(80, 100),
        "latest_payer_pdc": random.randint(80, 100),
        "selected_pdc": random.randint(80, 100),
        "calculated_lid": fake.date_this_year(after_today=True),
        "latest_payer_lid": fake.date_this_year(after_today=True),
        "selected_lid": fake.date_this_year(after_today=True),
        "calculated_rdd": fake.date_this_year(before_today=True, after_today=False),
        "latest_payer_rdd": fake.date_this_year(before_today=True, after_today=False),
        "selected_rdd": fake.date_this_year(before_today=True, after_today=False),
        "has_exclusion_rx": fake.boolean(),
        "has_payer_year_outcome_exclusion": fake.boolean(),
        "recent_claims_id": fake.random_int(min=1, max=10),
        "recent_claims_days_supply": 30,
        "recent_claims_fill_date": fake.date_this_year(before_today=True, after_today=False),
        "recent_claims_quantity": 30,
        "recent_claims_ndc11": fake.ean(length=13),
        "recent_claims_med_class": "hypertension",
        "recent_claims_drug_name": "FixItAll",
        "recent_claims_refills_remaining": 1
    }
    return data

# Define schema for the DataFrame
schema = StructType([
    StructField("pt_id", IntegerType()),
    StructField("event_timestamp", TimestampType()),
    StructField("primary_prac_id", IntegerType()),
    StructField("is_attribution_risk", BooleanType()),
    StructField("has_awv_appt", BooleanType()),
    StructField("in_map", StringType()),
    StructField("in_map_years", ArrayType(IntegerType())),
    StructField("score_cacp", FloatType()),
    StructField("score_kcm", FloatType()),
    StructField("elig_awv_date", DateType()),
    StructField("last_awv_date", DateType()),
    StructField("last_awv_is_medicare_age_in", BooleanType()),
    StructField("last_hospice_date", DateType()),
    StructField("last_well_child_visit", DateType()),
    StructField("insurer_line_of_business", StringType()),
    StructField("patient_insurer", StringType()),
    StructField("calculated_pdc", IntegerType()),
    StructField("latest_payer_pdc", IntegerType()),
    StructField("selected_pdc", IntegerType()),
    StructField("calculated_lid", DateType()),
    StructField("latest_payer_lid", DateType()),
    StructField("selected_lid", DateType()),
    StructField("calculated_rdd", DateType()),
    StructField("latest_payer_rdd", DateType()),
    StructField("selected_rdd", DateType()),
    StructField("has_exclusion_rx", BooleanType()),
    StructField("has_payer_year_outcome_exclusion", BooleanType()),
    StructField("recent_claims_id", IntegerType()),
    StructField("recent_claims_days_supply", IntegerType()),
    StructField("recent_claims_fill_date", DateType()),
    StructField("recent_claims_quantity", IntegerType()),
    StructField("recent_claims_ndc11", StringType()),
    StructField("recent_claims_med_class", StringType()),
    StructField("recent_claims_drug_name", StringType()),
    StructField("recent_claims_refills_remaining", IntegerType())
])

# Register UDF
generate_data_udf = udf(generate_fake_data, schema)

def generate_fake_df(rowsPerSecond=400, numPartitions=4):
    """
    Generates a streaming DataFrame with fake healthcare data.

    Args:
        rowsPerSecond (int): Number of rows to generate per second. Default is 400.
        numPartitions (int): Number of partitions. Default is 4.

    Returns:
        DataFrame: A Spark DataFrame with fake healthcare data.
    """
    df = spark.readStream.format("rate") \
        .option("numPartitions", numPartitions) \
        .option("rowsPerSecond", rowsPerSecond) \
        .load() \
        .select(generate_data_udf().alias("data")) \
        .select("data.*")
    
    return df

# COMMAND ----------

#display(generate_fake_df())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Start streaming data to the table

# COMMAND ----------

generate_fake_df().writeStream.trigger(
    processingTime="10 seconds"
).option("queryName", "write_health_care").option(
    "checkpointLocation", f"{check_point_location_prefix}/{health_care_table}_1"
).toTable(health_care_table)

# COMMAND ----------

# MAGIC %md ## Display the generated table

# COMMAND ----------

display(spark.read.table(health_care_table))

# COMMAND ----------

# MAGIC %md
# MAGIC # Create a temporary view for the streaming data

# COMMAND ----------

spark.readStream \
    .option("maxFilesPerTrigger", "10") \
    .table(health_care_table) \
    .createOrReplaceTempView(health_care_streaming_view)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Generate wellness opportunities

# COMMAND ----------

wellness_opportunity = spark.sql(f"""
SELECT 
  "wellness" AS opportunity_type,
  *
FROM health_care_streaming_view
WHERE elig_awv_date <= date_add(current_date(), 30)
  AND in_map = "IN_MAP"
  AND NOT has_awv_appt
""")
#display(wellness_opportunity)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Write the wellness opportunities to a table

# COMMAND ----------

(
    wellness_opportunity
        .writeStream
        .partitionBy(partition_column)
        .option("queryName", "create_wellness_opportunity")
        #.trigger(availableNow=True)
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", f"{check_point_location_prefix}/{pt_cohort_table}_create_wellness_opportunity")
        .toTable(pt_cohort_table)
)
        

# COMMAND ----------

# MAGIC %md
# MAGIC # Generate medication adherence opportunities

# COMMAND ----------

med_adherence_extended_day_rx_df = spark.sql(f"""
SELECT
  "med_adherence_extended_day_rx" AS opportunity_type,
  *
FROM
  {health_care_streaming_view}
WHERE
  year(recent_claims_fill_date) = 2024
  AND NOT has_payer_year_outcome_exclusion
  AND NOT has_exclusion_rx
  AND recent_claims_days_supply = 30
  AND selected_pdc <= 97
  AND (
    datediff(current_date(), recent_claims_fill_date) > recent_claims_days_supply * (85 / 100)
  )
""")
#display(med_adherence_extended_day_rx_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Write the medication adherence opportunities to a table

# COMMAND ----------

(
  med_adherence_extended_day_rx_df
    .writeStream
    .partitionBy(partition_column)
    .option("queryName", "med_adherence_extended_day_rx")
    #.trigger(availableNow=True)
    .trigger(processingTime="10 seconds")
    .option("checkpointLocation", f"{check_point_location_prefix}/{pt_cohort_table}_med_adherence_extended_day_rx")
    .toTable(pt_cohort_table)
)

# COMMAND ----------


