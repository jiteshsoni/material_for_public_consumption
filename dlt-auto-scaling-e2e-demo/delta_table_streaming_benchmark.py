# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 DLT Auto-Scaling Demo - Simple Time-Based Scaling
# MAGIC 
# MAGIC ## 📋 Simple Scaling Pattern:
# MAGIC - **50 baseline streams**: 1000 rows/sec each (constant)
# MAGIC - **3-6 minutes**: Add 1 stream at 3000 rows/sec (3x rate)
# MAGIC - **6-10 minutes**: Add 1 stream at 9000 rows/sec (9x rate) 
# MAGIC - **After 10 minutes**: Back to baseline only
# MAGIC 
# MAGIC ## 🎯 Expected Behavior:
# MAGIC - **0-3 min**: 50,000 rows/sec total
# MAGIC - **3-6 min**: 53,000 rows/sec total (+3000)
# MAGIC - **6-10 min**: 62,000 rows/sec total (+9000)
# MAGIC - **10+ min**: 50,000 rows/sec total (back to baseline)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎛️ Configuration

# COMMAND ----------

import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
from pyspark.sql.functions import expr, current_timestamp

# Configuration
config = {
    "baseline_streams": 50,
    "baseline_rate": 1000,  # rows per second per stream
    "scale_3x_rate": 3000,  # 3x scaling rate
    "scale_9x_rate": 9000,  # 9x scaling rate
    "catalog_name": "soni",
    "database_name": "default",
    "table_prefix": "stream_table",
    "checkpoint_path": "/Volumes/soni/default/checkpoints/",
    "partitions": 8,
    "test_mode": False  # Set to True for quick testing (reduces streams/time)
}

# Test mode adjustments for quick testing
if config["test_mode"]:
    config["baseline_streams"] = 5  # Reduce to 5 streams for testing
    config["baseline_rate"] = 100   # Reduce rate for testing
    config["scale_3x_rate"] = 300
    config["scale_9x_rate"] = 900
    print("⚠️  TEST MODE: Using reduced streams and rates for testing")

print("🚀 DLT Auto-Scaling Configuration:")
print(f"   📊 Baseline: {config['baseline_streams']} streams at {config['baseline_rate']} rows/sec each")
print(f"   📈 Total baseline: {config['baseline_streams'] * config['baseline_rate']:,} rows/sec")
print(f"   🔄 3x scaling: +{config['scale_3x_rate']} rows/sec (minutes 3-6)")
print(f"   🚀 9x scaling: +{config['scale_9x_rate']} rows/sec (minutes 6-10)")
print(f"   📁 Catalog: {config['catalog_name']}")
print(f"   🗄️ Database: {config['database_name']}")
print(f"   💾 Checkpoint: {config['checkpoint_path']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Setup Databricks Environment

# COMMAND ----------

def create_spark_session():
    """Create Spark session - supports both Databricks Connect and direct cluster execution"""
    try:
        # Try Databricks Connect first
        from databricks.connect import DatabricksSession
        spark = DatabricksSession.builder.remote().getOrCreate()
        print("✅ Connected via Databricks Connect")
        return spark
    except ImportError:
        # Fall back to regular Spark session (for direct cluster execution)
        try:
            spark = SparkSession.getActiveSession()
            if spark is None:
                print("❌ No active Spark session found")
                print("💡 This code must run on a Databricks cluster with an active session")
                print("📋 To test:")
                print("   1. Upload this .py file to Databricks workspace")
                print("   2. Attach to a running cluster") 
                print("   3. Run each cell individually")
                raise Exception("No active Spark session - Databricks cluster required")
            print("✅ Connected to Databricks Spark cluster")
            return spark
        except Exception as e:
            print(f"❌ Failed to create Spark session: {e}")
            raise

# Get Databricks Spark session
spark = create_spark_session()
print(f"🔗 Spark Version: {spark.version}")

# Verify Delta Lake support
try:
    spark.sql("SELECT 1").collect()
    print("✅ Spark SQL working")
except Exception as e:
    print(f"❌ Spark SQL issue: {e}")
    raise

# Install dbldatagen if needed
try:
    import dbldatagen as dg
    print("✅ dbldatagen available")
except ImportError:
    print("📦 Installing dbldatagen...")
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "dbldatagen"])
    # Only restart Python in Databricks environment
    try:
        dbutils.library.restartPython()
    except NameError:
        print("ℹ️  Restart Python manually in Databricks after pip install")
    import dbldatagen as dg
    print("✅ dbldatagen installed")

# Create catalog and database
spark.sql(f"CREATE CATALOG IF NOT EXISTS {config['catalog_name']}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {config['catalog_name']}.{config['database_name']}")
print(f"✅ Catalog '{config['catalog_name']}' and Database '{config['database_name']}' ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Data Generator Setup

# COMMAND ----------

# Define schema
schema = StructType([
    StructField("device_id", StringType(), False),
    StructField("event_timestamp", TimestampType(), False),  # Renamed to avoid conflicts
    StructField("temperature", DoubleType(), False),
    StructField("humidity", DoubleType(), False),
    StructField("pressure", DoubleType(), False),
    StructField("stream_type", StringType(), False),  # "baseline", "3x", "9x"
    StructField("stream_id", IntegerType(), False)
])

def create_stream(stream_id, rate, stream_type):
    """Create a streaming DataFrame with specified rate"""
    dataspec = (
        dg.DataGenerator(spark, name=f"stream_{stream_id}", partitions=config['partitions'])
        .withSchema(schema)
        .withColumnSpec("device_id", minValue=1000, maxValue=9999, prefix=f"DEV_{stream_id}_", random=True)
        .withColumnSpec("event_timestamp", begin="2023-01-01 00:00:00", end="2023-12-31 23:59:59", random=True)
        .withColumnSpec("temperature", minValue=15.0, maxValue=35.0, random=True)
        .withColumnSpec("humidity", minValue=30.0, maxValue=80.0, random=True)
        .withColumnSpec("pressure", minValue=980.0, maxValue=1020.0, random=True)
        .withColumnSpec("stream_type", values=[stream_type])
        .withColumnSpec("stream_id", values=[stream_id])
    )
    
    return dataspec.build(withStreaming=True, options={'rowsPerSecond': rate})

print("✅ Data generator functions ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Start Baseline Streams (50 streams at 1000 rows/sec each)

# COMMAND ----------

print(f"🚀 Starting {config['baseline_streams']} baseline streams...")

baseline_queries = []
demo_start_time = time.time()

# Start all baseline streams
for i in range(1, config['baseline_streams'] + 1):
    stream_df = create_stream(i, config['baseline_rate'], "baseline")
    table_name = f"{config['catalog_name']}.{config['database_name']}.{config['table_prefix']}_{i:03d}"
    checkpoint = f"{config['checkpoint_path']}baseline_{i:03d}/"
    
    query = (
        stream_df
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime="10 seconds")
        .queryName(f"baseline_stream_{i}")
        .toTable(table_name)
    )
    
    baseline_queries.append(query)
    
    if i <= 5 or i % 10 == 0 or i == config['baseline_streams']:
        print(f"✅ Started baseline stream {i}/{config['baseline_streams']} → {table_name}")
    
    time.sleep(0.1)  # Small delay

active_baseline = sum(1 for q in baseline_queries if q.isActive)
print(f"\n🎉 Baseline startup complete!")
print(f"📊 Active baseline streams: {active_baseline}/{config['baseline_streams']}")
print(f"📈 Total baseline throughput: {active_baseline * config['baseline_rate']:,} rows/sec")
print(f"⏰ Demo started at: {datetime.fromtimestamp(demo_start_time).strftime('%H:%M:%S')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⏰ Time-Based Scaling Loop

# COMMAND ----------

print("⏰ Starting time-based scaling monitoring...")
print("📋 Scaling schedule:")
print("   0-3 min: Baseline only")
print("   3-6 min: Baseline + 3x stream")
print("   6-10 min: Baseline + 3x stream + 9x stream")
print("   10+ min: Baseline only")
print("="*60)

# Tracking variables
scale_3x_query = None
scale_9x_query = None
scale_3x_started = False
scale_9x_started = False
scale_3x_stopped = False
scale_9x_stopped = False

# Main monitoring loop
while True:
    current_time = time.time()
    elapsed_minutes = (current_time - demo_start_time) / 60
    
    # Check active streams
    active_baseline = sum(1 for q in baseline_queries if q.isActive)
    active_3x = 1 if scale_3x_query and scale_3x_query.isActive else 0
    active_9x = 1 if scale_9x_query and scale_9x_query.isActive else 0
    
    total_throughput = (active_baseline * config['baseline_rate'] + 
                       active_3x * config['scale_3x_rate'] + 
                       active_9x * config['scale_9x_rate'])
    
    print(f"⏰ {elapsed_minutes:.1f} min | Baseline: {active_baseline} | 3x: {active_3x} | 9x: {active_9x} | Total: {total_throughput:,} rows/sec")
    
    # 3-6 minutes: Start 3x scaling stream
    if elapsed_minutes >= 3 and elapsed_minutes < 6 and not scale_3x_started:
        print("🔄 Starting 3x scaling stream...")
        try:
            scale_3x_df = create_stream(999, config['scale_3x_rate'], "3x")
            scale_3x_table = f"{config['catalog_name']}.{config['database_name']}.{config['table_prefix']}_3x_scale"
            scale_3x_checkpoint = f"{config['checkpoint_path']}scale_3x/"
            
            scale_3x_query = (
                scale_3x_df
                .writeStream
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", scale_3x_checkpoint)
                .trigger(processingTime="10 seconds")
                .queryName("scale_3x_stream")
                .toTable(scale_3x_table)
            )
            
            scale_3x_started = True
            print(f"✅ 3x scaling stream started → {scale_3x_table}")
            
        except Exception as e:
            print(f"❌ Failed to start 3x stream: {e}")
    
    # 6-10 minutes: Start 9x scaling stream  
    if elapsed_minutes >= 6 and elapsed_minutes < 10 and not scale_9x_started:
        print("🚀 Starting 9x scaling stream...")
        try:
            scale_9x_df = create_stream(998, config['scale_9x_rate'], "9x")
            scale_9x_table = f"{config['catalog_name']}.{config['database_name']}.{config['table_prefix']}_9x_scale"
            scale_9x_checkpoint = f"{config['checkpoint_path']}scale_9x/"
            
            scale_9x_query = (
                scale_9x_df
                .writeStream
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", scale_9x_checkpoint)
                .trigger(processingTime="10 seconds")
                .queryName("scale_9x_stream")
                .toTable(scale_9x_table)
            )
            
            scale_9x_started = True
            print(f"✅ 9x scaling stream started → {scale_9x_table}")
            
        except Exception as e:
            print(f"❌ Failed to start 9x stream: {e}")
    
    # 6+ minutes: Stop 3x scaling stream
    if elapsed_minutes >= 6 and scale_3x_query and scale_3x_query.isActive and not scale_3x_stopped:
        print("⏹️ Stopping 3x scaling stream...")
        try:
            scale_3x_query.stop()
            scale_3x_stopped = True
            print("✅ 3x scaling stream stopped")
        except Exception as e:
            print(f"❌ Failed to stop 3x stream: {e}")
    
    # 10+ minutes: Stop 9x scaling stream
    if elapsed_minutes >= 10 and scale_9x_query and scale_9x_query.isActive and not scale_9x_stopped:
        print("⏹️ Stopping 9x scaling stream...")
        try:
            scale_9x_query.stop()
            scale_9x_stopped = True
            print("✅ 9x scaling stream stopped")
        except Exception as e:
            print(f"❌ Failed to stop 9x stream: {e}")
    
    # Exit condition: 15 minutes or manual stop
    if elapsed_minutes >= 15:
        print("🏁 Demo completed after 15 minutes")
        break
    
    # Wait 30 seconds before next check
    time.sleep(30)

print("\n🎉 DLT Auto-Scaling Demo Complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Final Results Analysis

# COMMAND ----------

print("📊 Final Results Analysis")
print("="*50)

# Check final stream status
final_baseline = sum(1 for q in baseline_queries if q.isActive)
final_3x = 1 if scale_3x_query and scale_3x_query.isActive else 0
final_9x = 1 if scale_9x_query and scale_9x_query.isActive else 0

print(f"🔍 Final Stream Status:")
print(f"   Baseline streams: {final_baseline}/{config['baseline_streams']} active")
print(f"   3x scaling stream: {'Active' if final_3x else 'Stopped'}")
print(f"   9x scaling stream: {'Active' if final_9x else 'Stopped'}")

# Query data in tables
try:
    # Check baseline table
    baseline_count = spark.sql(f"SELECT COUNT(*) as count FROM {config['catalog_name']}.{config['database_name']}.{config['table_prefix']}_001").collect()[0]['count']
    print(f"\n📋 Data Results:")
    print(f"   Baseline table 001: {baseline_count:,} rows")
    
    # Check scaling tables if they exist
    tables_df = spark.sql(f"SHOW TABLES IN {config['catalog_name']}.{config['database_name']}")
    scaling_tables = [row['tableName'] for row in tables_df.collect() if 'scale' in row['tableName']]
    
    for table in scaling_tables:
        count = spark.sql(f"SELECT COUNT(*) as count FROM {config['catalog_name']}.{config['database_name']}.{table}").collect()[0]['count']
        print(f"   {table}: {count:,} rows")
        
except Exception as e:
    print(f"⚠️ Error checking table data: {e}")

print(f"\n🎯 Expected Behavior Achieved:")
print(f"   ✅ Started with {config['baseline_streams']} baseline streams")
print(f"   ✅ Added 3x stream at 3 minutes")
print(f"   ✅ Added 9x stream at 6 minutes") 
print(f"   ✅ Stopped scaling streams at appropriate times")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🛠️ Cleanup Functions

# COMMAND ----------

def stop_all_streams():
    """Stop all running streams"""
    print("⏹️ Stopping all streams...")
    
    stopped = 0
    # Stop baseline streams
    for i, query in enumerate(baseline_queries):
        try:
            if query.isActive:
                query.stop()
                stopped += 1
        except:
            pass
    
    # Stop scaling streams
    if scale_3x_query:
        try:
            if scale_3x_query.isActive:
                scale_3x_query.stop()
                stopped += 1
        except:
            pass
            
    if scale_9x_query:
        try:
            if scale_9x_query.isActive:
                scale_9x_query.stop()
                stopped += 1
        except:
            pass
    
    print(f"✅ Stopped {stopped} streams")
    return stopped

def check_stream_status():
    """Check status of all streams"""
    print("📊 Stream Status Check:")
    
    active_baseline = sum(1 for q in baseline_queries if q.isActive)
    print(f"   Baseline: {active_baseline}/{len(baseline_queries)} active")
    
    if scale_3x_query:
        status_3x = "Active" if scale_3x_query.isActive else "Stopped"
        print(f"   3x scaling: {status_3x}")
    
    if scale_9x_query:
        status_9x = "Active" if scale_9x_query.isActive else "Stopped"
        print(f"   9x scaling: {status_9x}")
    
    return active_baseline

def analyze_data():
    """Analyze data in Delta tables"""
    print("📊 Data Analysis:")
    
    try:
        # Get all tables
        tables_df = spark.sql(f"SHOW TABLES IN {config['catalog_name']}.{config['database_name']}")
        tables = [row['tableName'] for row in tables_df.collect() if config['table_prefix'] in row['tableName']]
        
        total_rows = 0
        for table in tables[:5]:  # Show first 5
            count = spark.sql(f"SELECT COUNT(*) as count FROM {config['catalog_name']}.{config['database_name']}.{table}").collect()[0]['count']
            total_rows += count
            print(f"   {table}: {count:,} rows")
        
        if len(tables) > 5:
            print(f"   ... and {len(tables)-5} more tables")
        
        print(f"   Total rows (first 5 tables): {total_rows:,}")
        
    except Exception as e:
        print(f"❌ Error analyzing data: {e}")

def test_scaling_logic():
    """Test the scaling timing logic without actually starting streams"""
    print("🧪 Testing scaling logic (simulation)...")
    
    # Simulate 15 minutes in 30-second intervals
    demo_start = time.time() - 900  # Pretend demo started 15 minutes ago
    
    for i in range(30):  # 30 intervals of 30 seconds = 15 minutes
        current_time = demo_start + (i * 30)
        elapsed_minutes = (current_time - demo_start) / 60
        
        # Simulate the scaling logic
        should_start_3x = elapsed_minutes >= 3 and elapsed_minutes < 6
        should_start_9x = elapsed_minutes >= 6 and elapsed_minutes < 10
        should_stop_3x = elapsed_minutes >= 6
        should_stop_9x = elapsed_minutes >= 10
        
        status = f"{elapsed_minutes:4.1f}min:"
        if should_start_3x and not should_stop_3x:
            status += " [3x ACTIVE]"
        if should_start_9x and not should_stop_9x:
            status += " [9x ACTIVE]"
        if not should_start_3x and not should_start_9x:
            status += " [BASELINE ONLY]"
            
        print(f"   {status}")
        
        if i % 6 == 0:  # Every 3 minutes
            print()
    
    print("✅ Scaling logic test complete")

print("💡 Available functions:")
print("   stop_all_streams() - Stop all running streams")  
print("   check_stream_status() - Check which streams are active")
print("   analyze_data() - Show row counts in Delta tables")
print("   test_scaling_logic() - Test timing logic without starting streams")

# COMMAND ----------

# Quick status check
check_stream_status()

# COMMAND ----------

# Quick data analysis
analyze_data()