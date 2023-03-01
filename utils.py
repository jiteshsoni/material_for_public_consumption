# imports
import time
import logging

from typing import Tuple, List, Union
from timeit import default_timer as timer
import functools
from datetime import timedelta, datetime

from pyspark.sql.functions import current_timestamp, current_date
from pyspark.sql import DataFrame

from pyspark.sql import functions as F

from pyspark.sql.utils import AnalysisException
from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql.utils import *
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
import pyspark

spark = SparkSession.builder.getOrCreate()

dbutils = DBUtils(spark)


logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s %(levelname)-4s %(name)s][%(funcName)s] %(message)s",
)
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("pyspark").setLevel(logging.INFO)
logger: logging.Logger = logging.getLogger(__name__)


def append_ingestion_columns(_df: DataFrame):
    return _df.withColumn("ingestion_timestamp", current_timestamp()).withColumn(
        "ingestion_date", current_date()
    )

#  Check if the table or view with the specified name exists in the specified database.
def does_table_exist(database_name: str, table_name: str) -> bool:
    # Usage :doesTableExist(database_name='bronze', table_name ='synthetic_transactions')
    table_list = [
        table.name.lower() for table in spark.catalog.listTables(database_name)
    ]
    return table_name.lower() in table_list


# https://realpython.com/python-timer/
def custom_timer(func):
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        tic = time.perf_counter()
        value = func(*args, **kwargs)
        toc = time.perf_counter()
        elapsed_time = toc - tic
        print(f"Elapsed time: {elapsed_time:0.4f} seconds")
        return value

    return wrapper_timer





def get_max_of_a_column(table: str, column: str) -> DataFrame:
    logger.info(f"Trying to get_max_of_a_column table:{table} for column:{column} ")
    max_df = spark.sql(
        f"""
        SELECT MAX({column}) AS max_of_the_column_being_used_for_change_data_capture
        FROM {table}
        """
    )
    logger.info(f"max_value:{max_df.collect()[0][0]}")
    # display(max_df)
    return max_df


# get_max_of_a_column(table="mongodb_bronze.ext_subscription" , column ="recordTS")


@custom_timer
def optimize_and_zorder_table(table_name: str, zorder_by_col_name: str) -> None:
    """This function runs an optimize and zorder command on a given table that being fed by a stream
        - These commands can't run in silo because it will require us to pause and then resume stream
        - Therefore, we need to call this function as a part of the upsert function. This enables us to optimize before the next batch of streaming data comes through.
    Parameters:
         table_name: str
                 name of the table to be optimized
         zorder_by_col_name: str
                 comma separated list of columns to zorder by. example "col_a, col_b"
    """
    start = timer()
    logger.info(f"Met condition to optimize table {table_name}")
    sql_query_optimize = f"OPTIMIZE  {table_name} ZORDER BY ({zorder_by_col_name})"
    spark.sql(sql_query_optimize)
    end = timer()
    time_elapsed_seconds = end - start
    logger.info(
        f"Successfully optimized table {table_name} . Total time elapsed: {time_elapsed_seconds} seconds"
    )


def drop_all_tables_and_clean_path(database: str):
    spark.sql(f"show tables in {database}").createOrReplaceTempView("table_list")
    df = spark.sql(
        f"""
              SELECT  database || '.' || tableName  as database_name_table_name
              FROM table_list
              WHERE isTemporary = false
       """
    )
    display(df)

    list_of_tables = (
        df.select("database_name_table_name").rdd.map(lambda x: x[0]).collect()
    )
    print(list_of_tables)
    for table_name in list_of_tables:
        try:
            print(f"Processing database {table_name}")
            df = spark.sql(f"""DESCRIBE TABLE EXTENDED {table_name};""")
            location = (
                df.select(["data_type"]).where("col_name = 'Location'").collect()[0][0]
            )
            print(f"table location is : {location}")
            sql_to_drop_table = f""" DROP TABLE IF EXISTS {table_name};"""
            print(f"About to drop table using SQL statement: {sql_to_drop_table}")
            spark.sql(sql_to_drop_table)
            print("Table dropped and now about to clean location")
            dbutils.fs.rm(location, recurse=True)
            print(f"location cleaned: {location}")
        except AnalysisException as e:
            print(str(e))


def get_start_date_and_end_date_and_list_of_hourly_timestamps(
    end_date: str,
    look_back_days: str,
    return_date_objects: bool = False,
    minute_interval: int = 60,
) -> Tuple[str, str, List[Union[str, datetime]]]:
    """Returns the start_date and end_date and list of dates are string or datetime
        Parameters
        ----------
        :param end_date : end_date
            run_end_datedate: Parse the end_date from yyyy-mm-dd
        :param look_back_days : look_back_days
            run_date - look_back_days
        :param return_date_objects
    Usage:
    get_start_date_and_end_date_and_list_of_date(
        run_date="2021-08-19", look_back_days="2"
    )
    """
    list_of_dates = list()
    look_back_days_int = int(look_back_days)
    end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
    start_date_obj = end_date_obj - timedelta(days=look_back_days_int)
    print(f"start_date: {start_date_obj} and end_date: {end_date_obj}")
    run_date_obj = end_date_obj
    while run_date_obj >= start_date_obj:
        if return_date_objects is True:
            list_of_dates.append(run_date_obj)
        else:
            list_of_dates.append(run_date_obj.strftime("%Y-%m-%dT%H:%M:%S.%f"))
        run_date_obj -= timedelta(minutes=minute_interval)
    print(f"list_of_dates {list_of_dates}")
    return (
        start_date_obj.strftime("%Y-%m-%d"),
        end_date_obj.strftime("%Y-%m-%d"),
        list_of_dates,
    )


# get_start_date_and_end_date_and_list_of_hourly_timestamps(end_date= '2022-07-08',look_back_days =1,return_date_objects =False)


def cast_array_and_struct_as_string(_to_be_casted_df: DataFrame) -> DataFrame:
    for col_name, col_data_type in _to_be_casted_df.dtypes:
        # print (f"col_name {col_name}  col_data_type {col_data_type}")
        if col_data_type.startswith("struct") or col_data_type.startswith("array"):
            # logger.info(f"Casting {col_name} with {col_data_type} as string")
            _to_be_casted_df = _to_be_casted_df.withColumn(
                f"{col_name}", col(f"{col_name}").cast(StringType())
            )
    return _to_be_casted_df


def cast_array_as_string(
    _to_be_casted_df: DataFrame, list_of_columns_to_not_cast: list = []
) -> DataFrame:

    for col_name, col_data_type in _to_be_casted_df.dtypes:
        # print (f"col_name {col_name}  col_data_type {col_data_type}")
        if (
            col_data_type.startswith("array")
        ) and col_name not in list_of_columns_to_not_cast:
            # logger.info(f"Casting {col_name} with {col_data_type} as string")
            _to_be_casted_df = _to_be_casted_df.withColumn(
                f"{col_name}", col(f"{col_name}").cast(StringType())
            )
    return _to_be_casted_df


def drop_table_and_clean_path(table_name: str):
    try:
        print(f"Processing database {table_name}")
        df = spark.sql(f"""DESCRIBE TABLE EXTENDED {table_name};""")
        location = (
            df.select(["data_type"]).where("col_name = 'Location'").collect()[0][0]
        )
        print(f"table location is : {location}")
        sql_to_drop_table = f""" DROP TABLE IF EXISTS {table_name};"""
        print(f"About to drop table using SQL statement: {sql_to_drop_table}")
        spark.sql(sql_to_drop_table)
        print("Table dropped and now about to clean location")
        dbutils.fs.rm(location, recurse=True)
        print(f"location cleaned: {location}")
    except AnalysisException as e:
        print(str(e))


class CustomError(Exception):
    __module__ = Exception.__module__


# raise CustomError("Improved CustomError!")


def stream_delta_table(_database_name: str, _table_name: str) -> DataFrame:
    return (
        spark.readStream.format("delta")
        .table(f"{_database_name}.{_table_name}")
        .withColumn(
            "mocked_column_to_order_duplicate_records_with",
            F.expr(
                "CASE WHEN recordTS IS NULL THEN ingestion_timestamp ELSE recordTS END"
            ),
        )
    )


def convert_camel_case_to_snake_case(string: str) -> str:
    list_of_transformed_char = list()
    for index, char in enumerate(string):
        list_of_transformed_char.append(string[index].lower())
        if index < len(string) - 1:
            if string[index].islower() and string[index + 1].isupper():
                list_of_transformed_char.append("_")

    return "".join(list_of_transformed_char)


from functools import reduce


def camel_case_dataframe_columns(_to_be_camel_cased_df: DataFrame) -> DataFrame:
    # Get All column names from DataFrame
    _list_of_columns = _to_be_camel_cased_df.columns
    logger.info(f"Got _list_of_columns as {_list_of_columns}")
    _camel_cased_df = reduce(
        lambda to_be_renamed_df, col_name: to_be_renamed_df.withColumnRenamed(
            col_name, convert_camel_case_to_snake_case(col_name)
        ),
        _to_be_camel_cased_df.columns,
        _to_be_camel_cased_df,
    )
    logger.info(f"_camel_cased columns are {_camel_cased_df.columns}")
    return _camel_cased_df