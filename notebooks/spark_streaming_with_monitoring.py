# Databricks notebook source
from pyspark.sql.streaming import StreamingQueryListener

# This class will write data to event hub(kafka). However, you can make this class write to any place you want
class my_steaming_listener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"'{event.name}' [{event.id}] got started!")
    def onQueryProgress(self, event):
        print(f" (event.progress.json {str(event.progress.json)}")
    def onQueryTerminated(self, event):
        print(f"{event.id} got terminated!")


# Add my listener.
my_listener = my_steaming_listener()
spark.streams.addListener(my_listener)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

target_table_name = "write_to_this_table"
target_table_location = f"/tmp/to_be_deleted/{target_table_name}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate data at your desired data

# COMMAND ----------

generated_df = (
     spark.readStream
        .format("rate")
        .option("numPartitions", 10)
        .option("rowsPerSecond", 10 * 1000)
        .load()
        .selectExpr(
          "md5( CAST (value AS STRING) ) as md5"
          ,"value"
          ,"value%1000000 as hash"
        )
)

#display(generated_df)

# COMMAND ----------

# Documentation https://spark.apache.org/docs/3.3.1/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamWriter.start.html
(
generated_df
  .writeStream
  .trigger(processingTime='5 seconds')
  .option("checkpointLocation",f"{target_table_location}/_checkpoint_location_the_name_of_needs_to_start_with_underscore_" )
  .start(path=target_table_location
         ,outputMode="append"
         ,queryName="give_a_meaning_full_name_as_it_will_appear_on_strucuted_streaming_ui"
        )
)

# COMMAND ----------

# DBTITLE 1,Create Delta Table Pointing to the location specified in the earlier Streaming Write
# Documentation https://docs.databricks.com/sql/language-manual/sql-ref-syntax-ddl-create-table-using.html
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {target_table_name} USING DELTA LOCATION '{target_table_location}'
""")

# COMMAND ----------

# DBTITLE 1,Verify table location and other properties
display(
  spark.sql(f"""
    desc extended {target_table_name}
  """)
)

# COMMAND ----------



# COMMAND ----------



class your_personalized_listener(StreamingQueryListener):
    def onQueryStarted(self, event):
        """
        Called when a query is started.

        Parameters
        ----------
        event: :class:`pyspark.sql.streaming.listener.QueryStartedEvent`
            The properties are available as the same as Scala API.

        Notes
        -----
        This is called synchronously with
        meth:`pyspark.sql.streaming.DataStreamWriter.start`,
        that is, ``onQueryStart`` will be called on all listeners before
        ``DataStreamWriter.start()`` returns the corresponding
        :class:`pyspark.sql.streaming.StreamingQuery`.
        Do not block in this method as it will block your query.
        """
        pass

    def onQueryProgress(self, event):
        """
        Called when there is some status update (ingestion rate updated, etc.)

        Parameters
        ----------
        event: :class:`pyspark.sql.streaming.listener.QueryProgressEvent`
            The properties are available as the same as Scala API.

        Notes
        -----
        This method is asynchronous. The status in
        :class:`pyspark.sql.streaming.StreamingQuery` will always be
        latest no matter when this method is called. Therefore, the status
        of :class:`pyspark.sql.streaming.StreamingQuery`.
        may be changed before/when you process the event.
        For example, you may find :class:`StreamingQuery`
        is terminated when you are processing `QueryProgressEvent`.
        """
        event
        pass

    def onQueryTerminated(self, event):
        """
        Called when a query is stopped, with or without error.

        Parameters
        ----------
        event: :class:`pyspark.sql.streaming.listener.QueryTerminatedEvent`
            The properties are available as the same as Scala API.
        """
        pass


my_listener = MyListener()

# COMMAND ----------

#spark.streams.addListener(my_listener)

# COMMAND ----------

#spark.streams.removeListener(my_listener)

# COMMAND ----------


