# Databricks notebook source
# MAGIC %md
# MAGIC # Learning Spark Streaming Listeners
# MAGIC
# MAGIC This notebook teaches you how to monitor Spark Structured Streaming queries using `StreamingQueryListener` - a powerful feature for real-time streaming observability.
# MAGIC
# MAGIC ## What You'll Learn
# MAGIC
# MAGIC 1. **What is a StreamingQueryListener?** - Understanding the concept
# MAGIC 2. **Why use listeners?** - Benefits over other monitoring approaches
# MAGIC 3. **Event types** - What events are captured and when
# MAGIC 4. **Implementation** - Step-by-step listener creation
# MAGIC 5. **Real-world examples** - Practical monitoring scenarios
# MAGIC 6. **Best practices** - Production-ready patterns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Reference:** [Databricks Stream Monitoring Documentation](https://docs.databricks.com/aws/en/structured-streaming/stream-monitoring?language=Python)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. What is a StreamingQueryListener?
# MAGIC
# MAGIC A `StreamingQueryListener` is an **event-driven interface** in Spark that allows you to monitor streaming queries in real-time. 
# MAGIC
# MAGIC ### Key Concepts:
# MAGIC
# MAGIC - **Event-driven**: Triggered automatically when streaming events occur
# MAGIC - **Real-time**: No polling delays - immediate notification
# MAGIC - **Global**: Monitors ALL streaming queries in your Spark session
# MAGIC - **Comprehensive**: Captures detailed metrics about query performance
# MAGIC
# MAGIC ### How it works:
# MAGIC ```
# MAGIC Streaming Query → Event Occurs → Listener Triggered → Your Code Runs
# MAGIC     ↓              ↓               ↓                ↓
# MAGIC   Batch           Progress        onQueryProgress   Collect metrics
# MAGIC   Complete        Event           method called     Store in Delta
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Why Use Streaming Listeners?
# MAGIC
# MAGIC ### Traditional Monitoring (Polling-based):
# MAGIC ```python
# MAGIC # ❌ Inefficient approach
# MAGIC while True:
# MAGIC     for stream in spark.streams.active:
# MAGIC         progress = stream.lastProgress  # May be None
# MAGIC         # Process metrics...
# MAGIC     time.sleep(60)  # Fixed interval, may miss events
# MAGIC ```
# MAGIC
# MAGIC ### StreamingQueryListener (Event-driven):
# MAGIC ```python
# MAGIC # ✅ Efficient approach
# MAGIC class MyListener(StreamingQueryListener):
# MAGIC     def onQueryProgress(self, event):
# MAGIC         # Automatically called when batch completes
# MAGIC         # Always has fresh metrics
# MAGIC         # No polling overhead
# MAGIC ```
# MAGIC
# MAGIC ### Benefits:
# MAGIC - **Zero overhead** when no streaming activity
# MAGIC - **Immediate notification** of events
# MAGIC - **Complete data** - never miss a batch
# MAGIC - **Centralized monitoring** for all queries

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. StreamingQueryListener Event Types
# MAGIC
# MAGIC The listener captures three types of events during a streaming query's lifecycle:

# COMMAND ----------

# Let's start with imports for our examples
import time
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.streaming import StreamingQueryListener

print("✅ Imports ready - let's learn about streaming listeners!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Event 1: onQueryStarted
# MAGIC
# MAGIC **When:** A new streaming query begins execution
# MAGIC
# MAGIC **What you get:**
# MAGIC - Query ID and name
# MAGIC - Run ID (unique per restart)
# MAGIC - Start timestamp
# MAGIC
# MAGIC **Use cases:**
# MAGIC - Track query lifecycle
# MAGIC - Initialize monitoring dashboards
# MAGIC - Send alerts when critical streams start

# COMMAND ----------

# Example: Basic listener showing onQueryStarted
class LearningListener(StreamingQueryListener):
    
    def onQueryStarted(self, event):
        """This method is called when a streaming query starts"""
        print("🚀 QUERY STARTED EVENT")
        print(f"   Query ID: {event.id}")
        print(f"   Query Name: {event.name}")
        print(f"   Run ID: {event.runId}")
        print(f"   Started at: {event.timestamp}")
        print()
    
    def onQueryProgress(self, event):
        """This method is called after each batch is processed"""
        pass  # We'll implement this next
    
    def onQueryTerminated(self, event):
        """This method is called when a streaming query stops"""
        pass  # We'll implement this next

print("✅ Basic listener structure defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Event 2: onQueryProgress (Most Important!)
# MAGIC
# MAGIC **When:** After each batch is successfully processed
# MAGIC
# MAGIC **What you get:**
# MAGIC - **Performance metrics**: Input/output rows, processing rates
# MAGIC - **Timing metrics**: How long each phase took
# MAGIC - **Source details**: What data was read
# MAGIC - **Sink details**: Where data was written
# MAGIC - **State information**: Stateful operation metrics
# MAGIC - **Event time**: Watermark and event time processing info
# MAGIC
# MAGIC **Use cases:**
# MAGIC - Performance monitoring and alerting
# MAGIC - Capacity planning
# MAGIC - Debugging slow queries
# MAGIC - SLA monitoring

# COMMAND ----------

# Enhanced listener showing onQueryProgress details
class DetailedLearningListener(StreamingQueryListener):
    
    def onQueryStarted(self, event):
        print(f"🚀 Stream '{event.name}' started")
    
    def onQueryProgress(self, event):
        """This is where the magic happens - detailed batch metrics!"""
        progress = event.progress
        
        print("📊 BATCH COMPLETED")
        print(f"   Stream: {progress.name}")
        print(f"   Batch ID: {progress.batchId}")
        print(f"   Input rows: {progress.numInputRows}")
        print(f"   Input rate: {progress.inputRowsPerSecond:.2f} rows/sec")
        print(f"   Processing rate: {progress.processedRowsPerSecond:.2f} rows/sec")
        print(f"   Batch duration: {progress.batchDuration} ms")
        
        # Let's look at the detailed timing breakdown
        if hasattr(progress, 'durationMs') and progress.durationMs:
            durations = progress.durationMs
            print("   ⏱️  Timing breakdown:")
            for phase, duration in durations.items():
                print(f"      {phase}: {duration} ms")
        print()
    
    def onQueryTerminated(self, event):
        print(f"🛑 Stream terminated: {event.id}")

print("✅ Detailed listener defined - ready to see batch metrics!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Event 3: onQueryTerminated
# MAGIC
# MAGIC **When:** A streaming query stops (gracefully or due to error)
# MAGIC
# MAGIC **What you get:**
# MAGIC - Query ID and Run ID
# MAGIC - Exception information (if query failed)
# MAGIC - Termination timestamp
# MAGIC
# MAGIC **Use cases:**
# MAGIC - Error alerting and debugging
# MAGIC - Automatic restart logic
# MAGIC - Cleanup operations
# MAGIC - Audit logging

# COMMAND ----------

# Complete listener showing all three events
class CompleteLearningListener(StreamingQueryListener):
    
    def onQueryStarted(self, event):
        print("🚀 QUERY LIFECYCLE: Started")
        print(f"   Name: {event.name}")
        print(f"   ID: {event.id}")
        print()
    
    def onQueryProgress(self, event):
        progress = event.progress
        print(f"📊 QUERY LIFECYCLE: Batch {progress.batchId} completed")
        print(f"   Processed {progress.numInputRows} rows in {progress.batchDuration} ms")
        
        # Check for performance issues
        if progress.inputRowsPerSecond > progress.processedRowsPerSecond:
            print("   ⚠️  WARNING: Processing is lagging behind input rate!")
        print()
    
    def onQueryTerminated(self, event):
        print("🛑 QUERY LIFECYCLE: Terminated")
        print(f"   ID: {event.id}")
        if hasattr(event, 'exception') and event.exception:
            print(f"   ❌ Exception: {event.exception}")
        else:
            print("   ✅ Graceful shutdown")
        print()

print("✅ Complete lifecycle listener ready!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Hands-On: Implementing Your First Listener
# MAGIC
# MAGIC Let's build a practical streaming listener step by step.

# COMMAND ----------

# Step 1: Create a simple test stream to monitor
def create_demo_stream(name="learning_stream"):
    """Create a simple rate stream for learning purposes"""
    return (spark
        .readStream
        .format("rate")
        .option("rowsPerSecond", 2)  # Slow rate so we can observe
        .load()
        .writeStream
        .format("console")
        .option("numRows", 3)
        .queryName(name)
        .trigger(processingTime="5 seconds")  # Process every 5 seconds
        .start()
    )

print("✅ Demo stream creator ready")

# COMMAND ----------

# Step 2: Register our learning listener
learning_listener = CompleteLearningListener()
spark.streams.addListener(learning_listener)

print("✅ Learning listener registered!")
print("   Now any streaming query will trigger our listener events")

# COMMAND ----------

# Step 3: Start a demo stream and watch the events!
print("🎬 Starting demo stream - watch for listener events...")
demo_stream = create_demo_stream("my_first_monitored_stream")

print(f"✅ Demo stream started with ID: {demo_stream.id}")
print("   You should see listener events appearing below as batches process...")

# COMMAND ----------

# Let's wait and watch the listener events for a bit
print("⏳ Watching listener events for 20 seconds...")
time.sleep(20)

print("\n📝 What did you observe?")
print("   1. onQueryStarted event when stream began")
print("   2. onQueryProgress events every 5 seconds (our trigger interval)")
print("   3. Performance metrics in each progress event")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Production-Ready Listener: Storing Metrics
# MAGIC
# MAGIC Now let's build a realistic listener that stores metrics for analysis and alerting.

# COMMAND ----------

class ProductionMetricsListener(StreamingQueryListener):
    """
    Production-ready listener that stores comprehensive metrics
    """
    
    def __init__(self, spark, table_name="default.streaming_metrics"):
        self.spark = spark
        self.table_name = table_name
        self._ensure_table_exists()
        
    def onQueryStarted(self, event):
        """Log when streams start"""
        print(f"🚀 Production monitoring started for: {event.name}")
        
        metrics_data = {
            'event_type': 'query_started',
            'collection_time': datetime.now(),
            'stream_id': str(event.id),
            'stream_name': event.name,
            'run_id': str(event.runId),
            'batch_id': None,
            'num_input_rows': None,
            'input_rows_per_second': None,
            'processed_rows_per_second': None,
            'batch_duration_ms': None,
            'is_active': True,
            'raw_progress_json': json.dumps({
                'event': 'started',
                'id': str(event.id),
                'name': event.name,
                'timestamp': str(event.timestamp)
            })
        }
        
        self._write_metrics([metrics_data])
    
    def onQueryProgress(self, event):
        """Store detailed batch metrics"""
        progress = event.progress
        
        # Extract key metrics for easy querying
        metrics_data = {
            'event_type': 'query_progress',
            'collection_time': datetime.now(),
            'stream_id': str(progress.id),
            'stream_name': progress.name,
            'run_id': str(progress.runId),
            'batch_id': progress.batchId,
            'num_input_rows': progress.numInputRows,
            'input_rows_per_second': progress.inputRowsPerSecond,
            'processed_rows_per_second': progress.processedRowsPerSecond,
            'batch_duration_ms': progress.batchDuration,
            'is_active': True,
            'raw_progress_json': progress.json  # Complete metrics as JSON
        }
        
        self._write_metrics([metrics_data])
        
        # Real-time alerting logic
        self._check_for_alerts(progress)
    
    def onQueryTerminated(self, event):
        """Log termination events"""
        print(f"🛑 Production monitoring ended for: {event.id}")
        
        exception_info = {}
        if hasattr(event, 'exception') and event.exception:
            exception_info = {'exception': str(event.exception)}
            print(f"   ❌ Stream failed: {event.exception}")
        
        metrics_data = {
            'event_type': 'query_terminated',
            'collection_time': datetime.now(),
            'stream_id': str(event.id),
            'stream_name': None,
            'run_id': str(event.runId),
            'batch_id': None,
            'num_input_rows': None,
            'input_rows_per_second': None,
            'processed_rows_per_second': None,
            'batch_duration_ms': None,
            'is_active': False,
            'raw_progress_json': json.dumps({
                'event': 'terminated',
                'id': str(event.id),
                **exception_info
            })
        }
        
        self._write_metrics([metrics_data])
    
    def _write_metrics(self, metrics_data):
        """Store metrics in Delta table for analysis"""
        try:
            schema = StructType([
                StructField("event_type", StringType(), True),
                StructField("collection_time", TimestampType(), True),
                StructField("stream_id", StringType(), True),
                StructField("stream_name", StringType(), True),
                StructField("run_id", StringType(), True),
                StructField("batch_id", LongType(), True),
                StructField("num_input_rows", LongType(), True),
                StructField("input_rows_per_second", DoubleType(), True),
                StructField("processed_rows_per_second", DoubleType(), True),
                StructField("batch_duration_ms", LongType(), True),
                StructField("is_active", BooleanType(), True),
                StructField("raw_progress_json", StringType(), True)
            ])
            
            metrics_df = self.spark.createDataFrame(metrics_data, schema)
            
            metrics_df.write \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(self.table_name)
            
        except Exception as e:
            print(f"❌ Error storing metrics: {e}")
    
    def _check_for_alerts(self, progress):
        """Real-time alerting based on streaming metrics"""
        # Example alert conditions
        if progress.batchDuration > 30000:  # > 30 seconds
            print(f"🚨 ALERT: Slow batch detected in {progress.name} - {progress.batchDuration}ms")
        
        if progress.inputRowsPerSecond > progress.processedRowsPerSecond * 1.5:
            print(f"🚨 ALERT: {progress.name} is falling behind input rate!")
        
        if progress.numInputRows == 0:
            print(f"⚠️  WARNING: No input data for {progress.name}")
    
    def _ensure_table_exists(self):
        """Create metrics table if needed"""
        try:
            table_exists = spark.sql(f"SHOW TABLES LIKE '{self.table_name.split('.')[-1]}'").count() > 0
            
            if not table_exists:
                spark.sql(f"""
                    CREATE TABLE {self.table_name} (
                        event_type STRING,
                        collection_time TIMESTAMP,
                        stream_id STRING,
                        stream_name STRING,
                        run_id STRING,
                        batch_id BIGINT,
                        num_input_rows BIGINT,
                        input_rows_per_second DOUBLE,
                        processed_rows_per_second DOUBLE,
                        batch_duration_ms BIGINT,
                        is_active BOOLEAN,
                        raw_progress_json STRING
                    ) USING DELTA
                """)
                print(f"✅ Created metrics table: {self.table_name}")
        except Exception as e:
            print(f"⚠️  Table creation issue: {e}")

print("✅ Production-ready listener class defined!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Real-World Example: Comprehensive Monitoring

# COMMAND ----------

# Stop our learning listener first
spark.streams.removeListener(learning_listener)

# Start production monitoring
production_listener = ProductionMetricsListener(spark, "default.streaming_metrics")
spark.streams.addListener(production_listener)

print("✅ Production monitoring activated!")
print("   - Metrics stored in Delta table")
print("   - Real-time alerting enabled")
print("   - Comprehensive event tracking")

# COMMAND ----------

# Create multiple test streams to see monitoring in action
print("🎬 Creating multiple streams for monitoring demo...")

# Fast stream - should trigger processing alerts
fast_stream = (spark
    .readStream
    .format("rate")
    .option("rowsPerSecond", 100)  # High rate
    .load()
    .writeStream
    .format("console")
    .option("numRows", 2)
    .queryName("fast_data_stream")
    .trigger(processingTime="3 seconds")
    .start()
)

# Slow processing stream - should trigger duration alerts
slow_stream = (spark
    .readStream
    .format("rate")
    .option("rowsPerSecond", 5)
    .load()
    .select("*", expr("rand()"))  # Replace sleep with rand for artificial delay
    .writeStream
    .format("console")
    .option("numRows", 1)
    .queryName("slow_processing_stream")
    .trigger(processingTime="10 seconds")
    .start()
)

print("✅ Multiple streams created - watch for alerts and metrics!")

# COMMAND ----------

# Let's watch the production monitoring in action
print("⏳ Monitoring streams for 30 seconds - watch for alerts...")
time.sleep(30)

# Now let's look at the collected metrics
print("\n📊 Viewing collected metrics...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Analyzing Collected Metrics

# COMMAND ----------

# Show recent activity
spark.sql("""
    SELECT collection_time, event_type, stream_name, batch_id, 
           num_input_rows, input_rows_per_second, processed_rows_per_second,
           batch_duration_ms
    FROM default.streaming_metrics
    ORDER BY collection_time DESC
    LIMIT 20
""").display()

# COMMAND ----------

# Performance analysis query
print("📈 Performance Analysis:")
spark.sql("""
    SELECT 
        stream_name,
        COUNT(*) as total_batches,
        AVG(batch_duration_ms) as avg_batch_duration_ms,
        AVG(input_rows_per_second) as avg_input_rate,
        AVG(processed_rows_per_second) as avg_processing_rate,
        MAX(batch_duration_ms) as max_batch_duration_ms
    FROM default.streaming_metrics
    WHERE event_type = 'query_progress'
    GROUP BY stream_name
    ORDER BY avg_batch_duration_ms DESC
""").display()

# COMMAND ----------

# Extract detailed metrics from JSON
print("🔍 Detailed Timing Breakdown:")
spark.sql("""
    SELECT 
        stream_name,
        batch_id,
        get_json_object(raw_progress_json, '$.durationMs.triggerExecution') as trigger_time_ms,
        get_json_object(raw_progress_json, '$.durationMs.queryPlanning') as planning_time_ms,
        get_json_object(raw_progress_json, '$.durationMs.addBatch') as add_batch_time_ms,
        get_json_object(raw_progress_json, '$.sources[0].description') as source_type
    FROM default.streaming_metrics
    WHERE event_type = 'query_progress'
    ORDER BY collection_time DESC
    LIMIT 10
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Best Practices and Production Tips

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🎯 Best Practices for StreamingQueryListener
# MAGIC
# MAGIC #### 1. **Keep listener methods fast**
# MAGIC ```python
# MAGIC def onQueryProgress(self, event):
# MAGIC     # ✅ Good: Quick operations
# MAGIC     metrics = extract_key_metrics(event.progress)
# MAGIC     async_write_to_storage(metrics)
# MAGIC     
# MAGIC     # ❌ Bad: Slow operations
# MAGIC     complex_analysis(event.progress)  # Blocks other streams
# MAGIC     synchronous_api_call()           # Network delays
# MAGIC ```
# MAGIC
# MAGIC #### 2. **Handle errors gracefully**
# MAGIC ```python
# MAGIC def onQueryProgress(self, event):
# MAGIC     try:
# MAGIC         # Your monitoring logic
# MAGIC         store_metrics(event.progress)
# MAGIC     except Exception as e:
# MAGIC         # Don't let monitoring break your streams!
# MAGIC         log.error(f"Monitoring error: {e}")
# MAGIC ```
# MAGIC
# MAGIC #### 3. **Use efficient storage**
# MAGIC - Store key metrics as columns for fast querying
# MAGIC - Keep full JSON for detailed analysis
# MAGIC - Use Delta Lake for ACID guarantees
# MAGIC - Partition by date for efficient queries
# MAGIC
# MAGIC #### 4. **Monitor the monitor**
# MAGIC - Track listener performance
# MAGIC - Alert if metrics collection fails
# MAGIC - Have fallback monitoring strategies

# COMMAND ----------

# Example: Robust error handling in listener
class RobustProductionListener(StreamingQueryListener):
    
    def __init__(self, spark, table_name="default.streaming_metrics"):
        self.spark = spark
        self.table_name = table_name
        self.error_count = 0
        self.max_errors = 10
        
    def onQueryProgress(self, event):
        try:
            # Fast extraction of key metrics
            key_metrics = {
                'stream_name': event.progress.name,
                'batch_id': event.progress.batchId,
                'batch_duration': event.progress.batchDuration,
                'input_rate': event.progress.inputRowsPerSecond,
                'processing_rate': event.progress.processedRowsPerSecond
            }
            
            # Quick health check
            if key_metrics['batch_duration'] > 60000:  # > 1 minute
                self._send_alert("SLOW_BATCH", key_metrics)
            
            # Async storage (don't block)
            self._async_store_metrics(event.progress)
            
            # Reset error counter on success
            self.error_count = 0
            
        except Exception as e:
            self.error_count += 1
            print(f"⚠️  Listener error #{self.error_count}: {e}")
            
            if self.error_count >= self.max_errors:
                print(f"🚨 Listener disabled after {self.max_errors} errors")
                spark.streams.removeListener(self)
    
    def _send_alert(self, alert_type, metrics):
        """Send alerts without blocking"""
        print(f"🚨 {alert_type}: {metrics['stream_name']} - {metrics}")
        # In production: send to Slack, PagerDuty, etc.
    
    def _async_store_metrics(self, progress):
        """Store metrics asynchronously"""
        # In production: use message queue, async writes, etc.
        pass
    
    def onQueryStarted(self, event):
        print(f"✅ Monitoring: {event.name}")
    
    def onQueryTerminated(self, event):
        print(f"🛑 Stopped monitoring: {event.id}")

print("✅ Robust production listener pattern defined!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Cleanup and Summary

# COMMAND ----------

# Clean up our demo streams
print("🧹 Cleaning up demo streams...")

if 'demo_stream' in locals():
    demo_stream.stop()
    print("   ✅ Demo stream stopped")

if 'fast_stream' in locals():
    fast_stream.stop()
    print("   ✅ Fast stream stopped")

if 'slow_stream' in locals():
    slow_stream.stop()
    print("   ✅ Slow stream stopped")

# Remove our production listener
spark.streams.removeListener(production_listener)
print("   ✅ Production listener removed")

print("\n🎉 Demo completed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎓 What You Learned
# MAGIC
# MAGIC ### Concepts:
# MAGIC - ✅ **StreamingQueryListener fundamentals** - event-driven monitoring
# MAGIC - ✅ **Three event types** - started, progress, terminated
# MAGIC - ✅ **Real-time metrics** - performance, timing, throughput
# MAGIC - ✅ **Production patterns** - error handling, alerting, storage
# MAGIC
# MAGIC ### Practical Skills:
# MAGIC - ✅ **Implement custom listeners** for your monitoring needs
# MAGIC - ✅ **Store metrics efficiently** in Delta tables
# MAGIC - ✅ **Query streaming metrics** using JSON functions
# MAGIC - ✅ **Set up real-time alerts** based on performance thresholds
# MAGIC
# MAGIC ### Key Takeaways:
# MAGIC 1. **Event-driven > Polling** - More efficient and comprehensive
# MAGIC 2. **Keep listeners fast** - Don't block streaming execution
# MAGIC 3. **Store structured + raw data** - Best of both worlds
# MAGIC 4. **Monitor your monitoring** - Handle listener failures gracefully
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 Next Steps:
# MAGIC - Implement listeners for your production streams
# MAGIC - Build dashboards using the collected metrics
# MAGIC - Set up automated alerting for stream health
# MAGIC - Explore advanced patterns like listener composition
# MAGIC
# MAGIC ### 📚 Further Reading:
# MAGIC - [Databricks Stream Monitoring Guide](https://docs.databricks.com/aws/en/structured-streaming/stream-monitoring?language=Python)
# MAGIC - [Spark Structured Streaming Documentation](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
# MAGIC - [Delta Lake for Streaming Analytics](https://docs.delta.io/latest/delta-streaming.html)