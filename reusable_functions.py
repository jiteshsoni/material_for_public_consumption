from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, current_date

def append_ingestion_columns(_df: DataFrame):
    return _df.withColumn("ingestion_timestamp", current_timestamp()).withColumn(
        "ingestion_date", current_date()
    )