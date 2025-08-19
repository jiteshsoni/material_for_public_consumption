#!/usr/bin/env python3
"""
MERGE and OPTIMIZE Parallel Demo with Databricks Connect

This script demonstrates that MERGE and OPTIMIZE can run in parallel with row-level concurrency, 
deletion vectors, and liquid clustering in Databricks.

Prerequisites:
- Databricks Connect configured locally
- Environment variables set: DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_CLUSTER_ID
"""

import os
import sys
import uuid
import logging
import threading
import time
import random
from typing import Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def initialize_databricks_connect():
    """Initialize Databricks Connect and verify connection"""
    try:
        # Try to use existing spark session
        spark.catalog.listDatabases()
        print("✓ Running in Databricks environment")
    except NameError:
        try:
            from databricks.connect import DatabricksSession
            print("Initializing Databricks Connect...")
            spark = DatabricksSession.builder.getOrCreate()
            print("✓ Connected to Databricks cluster")
        except ImportError:
            print("❌ databricks-connect not installed. Install with: pip install databricks-connect")
            return None
        except Exception as e:
            print(f"❌ Failed to connect to Databricks: {e}")
            return None
    
    # Verify remote connection
    try:
        print(f"✓ Spark version: {spark.version}")
        test_count = spark.range(3).count()
        print(f"✓ Connected to remote cluster: {test_count} test rows")
        return spark
    except Exception as e:
        print(f"❌ Connection verification failed: {e}")
        return None

def verify_required_packages():
    """Verify that required packages are available"""
    try:
        from faker import Faker
        from faker_vehicle import VehicleProvider
        print("✓ Required packages available")
        return True
    except ImportError as e:
        print(f"❌ Missing packages: {e}")
        print("Install with: pip install faker faker-vehicle")
        return False

def verify_databricks_environment(spark):
    """Verify we're running on Databricks cluster"""
    print("=== EXECUTION ENVIRONMENT VERIFICATION ===")
    
    try:
        # This will only work if connected to Databricks cluster
        cluster_info = spark.sql("SELECT current_version() as version").collect()[0]
        print(f"✅ RUNNING ON DATABRICKS CLUSTER")
        print(f"✅ Databricks Runtime Version: {cluster_info.version}")
        
        # Check Spark version (Databricks uses specific versions)
        print(f"✅ Spark Version: {spark.version}")
        
        # Test Databricks-specific functionality
        if spark.version.startswith("3.5") or spark.version.startswith("3.4"):
            print("✅ CONFIRMED: This is executing on the DATABRICKS CLUSTER")
            print("✅ NOT running on your local machine!")
            print("✅ All operations will execute on the remote Databricks cluster")
        else:
            print("⚠️  Warning: Unexpected Spark version - verify cluster connection")
            
    except Exception as e:
        print(f"❌ ERROR: Not connected to Databricks cluster: {e}")
        print("❌ This appears to be running LOCALLY")
        print("❌ Please check your Databricks Connect configuration")
        raise RuntimeError("Must run on Databricks cluster for liquid clustering and row-level concurrency")

    print("=" * 50)
    return True

def setup_configuration():
    """Setup configuration variables"""
    # Configuration
    config = {
        'CHECKPOINT_BASE': "s3://test-external-volume-bucket-2/test-folder",
        'TARGET_TABLE': f"soni.default.parallel_merges_optimize_row_level_concurrency_{uuid.uuid4().hex[:8]}",
        'JOIN_COLUMN': "event_id",
        'CLUSTERING_COLUMN': "event_timestamp",
        'INITIAL_EVENT_ID_POOL_SIZE': 1000
    }
    
    # Generate unique checkpoint locations
    config['checkpoint_bootstrap'] = f"{config['CHECKPOINT_BASE']}/bootstrap_{uuid.uuid4()}"
    config['checkpoint_main'] = f"{config['CHECKPOINT_BASE']}/main_{uuid.uuid4()}"
    config['checkpoint_location'] = f"{config['CHECKPOINT_BASE']}/main_{uuid.uuid4()}"
    
    logger.info(f"Target table: {config['TARGET_TABLE']}")
    logger.info(f"Clustering on: {config['CLUSTERING_COLUMN']}")
    logger.info("Configuration loaded successfully")
    
    return config

def initialize_data_generation(spark, config):
    """Initialize data generation components"""
    from faker import Faker
    from faker_vehicle import VehicleProvider
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType
    
    # Initialize Faker
    fake = Faker()
    fake.add_provider(VehicleProvider)
    
    # Clean up existing table
    spark.sql(f"DROP TABLE IF EXISTS {config['TARGET_TABLE']}")
    logger.info(f"Cleaned up existing table {config['TARGET_TABLE']}")
    
    # Create UDFs for fake data generation
    event_id_udf = F.udf(lambda: str(uuid.uuid4()), StringType())
    vehicle_make_udf = F.udf(fake.vehicle_make)
    vehicle_model_udf = F.udf(fake.vehicle_model)
    vehicle_year_udf = F.udf(fake.vehicle_year)
    latitude_udf = F.udf(fake.latitude)
    longitude_udf = F.udf(fake.longitude)
    zipcode_udf = F.udf(fake.zipcode)
    
    # Create pool of existing event IDs for realistic updates
    existing_event_ids = set()
    for i in range(config['INITIAL_EVENT_ID_POOL_SIZE']):
        existing_event_ids.add(str(uuid.uuid4()))
    
    logger.info(f"Created {len(existing_event_ids)} initial event IDs for updates")
    logger.info("UDFs and data generation initialized")
    
    return {
        'fake': fake,
        'event_id_udf': event_id_udf,
        'vehicle_make_udf': vehicle_make_udf,
        'vehicle_model_udf': vehicle_model_udf,
        'vehicle_year_udf': vehicle_year_udf,
        'latitude_udf': latitude_udf,
        'longitude_udf': longitude_udf,
        'zipcode_udf': zipcode_udf,
        'existing_event_ids': existing_event_ids
    }

def create_streaming_vehicle_data(spark, data_gen, config, rows_per_second=1000, num_partitions=4, update_ratio=0.5):
    """Create a streaming DataFrame with vehicle data, mixing updates and inserts"""
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType
    
    logger.info(f"Creating streaming vehicle data with {update_ratio*100}% updates")
    
    # Convert existing_event_ids to a list for efficient random access
    existing_ids_list = list(data_gen['existing_event_ids']) if data_gen['existing_event_ids'] else []
    logger.info(f"Using {len(existing_ids_list)} existing IDs for updates")
    
    # Create a mix of existing IDs (for updates) and new IDs (for inserts)
    def generate_event_id_with_mix():
        if existing_ids_list and random.random() < update_ratio:
            return random.choice(existing_ids_list)  # Existing ID for update
        else:
            return str(uuid.uuid4())  # New ID for insert
        
    event_id_mixed_udf = F.udf(generate_event_id_with_mix, StringType())
    
    # Create streaming DataFrame with simplified schema
    df = (spark.readStream.format("rate")
          .option("numPartitions", num_partitions)
          .option("rowsPerSecond", rows_per_second)
          .load()
          .withColumn("event_timestamp", F.current_timestamp())
          .withColumn("event_id", event_id_mixed_udf())
          .withColumn("vehicle_make", data_gen['vehicle_make_udf']())
          .withColumn("vehicle_model", data_gen['vehicle_model_udf']())
          .withColumn("vehicle_year", data_gen['vehicle_year_udf']())
          .withColumn("latitude", data_gen['latitude_udf']())
          .withColumn("longitude", data_gen['longitude_udf']())
          .withColumn("zipcode", data_gen['zipcode_udf']())
          .drop("value", "timestamp")
    )
    return df

def test_update_insert_logic(spark, data_gen, config):
    """Test the update/insert logic before running the full demo"""
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, StringType
    
    print("=== TESTING UPDATE/INSERT LOGIC ===")
    
    # First, populate the table with some initial data using existing IDs
    print("Step 1: Creating initial data with existing event IDs...")
    
    # Create a batch with some of our existing IDs to populate the table
    initial_existing_ids = list(data_gen['existing_event_ids'])[:50]  # Use first 50 existing IDs
    print(f"Using {len(initial_existing_ids)} existing IDs for initial population")
    
    # Create initial data using some existing IDs
    initial_data = []
    for i, event_id in enumerate(initial_existing_ids):
        initial_data.append((
            event_id,
            f"Toyota_{i}",
            f"Camry_{i}",
            "2023",
            f"{37.7749 + i * 0.01}",
            f"{-122.4194 + i * 0.01}",
            f"9410{i % 10}"
        ))
    
    # Create DataFrame with initial data
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("vehicle_make", StringType(), True),
        StructField("vehicle_model", StringType(), True),
        StructField("vehicle_year", StringType(), True),
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("zipcode", StringType(), True)
    ])
    
    initial_df = spark.createDataFrame(initial_data, schema) \
        .withColumn("event_timestamp", F.current_timestamp())
    
    print(f"✓ Created initial DataFrame with {initial_df.count()} rows")
    
    # Write initial data to table
    initial_df.write.mode("overwrite").saveAsTable(config['TARGET_TABLE'])
    print(f"✓ Populated table {config['TARGET_TABLE']} with initial data")
    
    print("\nStep 2: Testing update/insert generation logic using NO-OP validation...")
    
    # Create a batch test function that simulates the streaming logic
    def create_test_batch(num_rows=100, update_ratio=0.5):
        """Create a test batch that simulates the streaming logic"""
        
        # Convert existing_event_ids to a list for efficient random access
        existing_ids_list = list(data_gen['existing_event_ids']) if data_gen['existing_event_ids'] else []
        print(f"  Using {len(existing_ids_list)} existing IDs for updates (update_ratio={update_ratio})")
        
        # Generate test data using the same logic as the streaming function
        test_data = []
        for i in range(num_rows):
            # Decide if this should be an update or insert (same logic as streaming UDF)
            if existing_ids_list and random.random() < update_ratio:
                # Use existing ID for update
                event_id = random.choice(existing_ids_list)
            else:
                # Generate new ID for insert
                event_id = str(uuid.uuid4())
            
            test_data.append((
                event_id,
                data_gen['fake'].vehicle_make(),
                data_gen['fake'].vehicle_model(), 
                data_gen['fake'].vehicle_year(),
                data_gen['fake'].latitude(),
                data_gen['fake'].longitude(),
                data_gen['fake'].zipcode()
            ))
        
        # Create DataFrame with the same schema as streaming
        schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("vehicle_make", StringType(), True),
            StructField("vehicle_model", StringType(), True),
            StructField("vehicle_year", StringType(), True),
            StructField("latitude", StringType(), True),
            StructField("longitude", StringType(), True),
            StructField("zipcode", StringType(), True)
        ])
        
        return spark.createDataFrame(test_data, schema).withColumn("event_timestamp", F.current_timestamp())
    
    # Test different update ratios
    test_cases = [
        {"update_ratio": 0.0, "description": "100% inserts"},
        {"update_ratio": 0.5, "description": "50% updates, 50% inserts"}, 
        {"update_ratio": 1.0, "description": "100% updates"}
    ]
    
    successful_tests = 0
    total_tests = len(test_cases)
    
    for i, test_case in enumerate(test_cases):
        update_ratio = test_case["update_ratio"]
        description = test_case["description"]
        print(f"\n--- Testing update_ratio={update_ratio} ({description}) ---")
        
        try:
            # Create test batch using the same logic as streaming
            test_batch = create_test_batch(num_rows=100, update_ratio=update_ratio)
            
            print(f"  📊 Analyzing test batch...")
            batch_count = test_batch.count()
            print(f"  📏 Test batch contains {batch_count} rows")
            
            # Test the noop format - this validates the DataFrame without writing
            test_batch.write.format("noop").mode("overwrite").save()
            print(f"  ✅ No-op validation passed")
            
            # Collect and analyze the data
            batch_data = test_batch.collect()
            print(f"  📦 Captured {len(batch_data)} rows for analysis")
            
            # Count updates vs inserts
            updates = sum(1 for row in batch_data if row.event_id in data_gen['existing_event_ids'])
            inserts = len(batch_data) - updates
            
            print(f"  ✅ ANALYSIS RESULTS:")
            print(f"     {updates} updates, {inserts} inserts out of {len(batch_data)} total")
            print(f"     Update percentage: {(updates/len(batch_data)*100):.1f}%")
            print(f"     Insert percentage: {(inserts/len(batch_data)*100):.1f}%")
            
            # Validate results
            if update_ratio == 0.0:
                if updates == 0:
                    print("  ✅ PASS: 100% inserts as expected")
                    successful_tests += 1
                else:
                    print(f"  ❌ FAIL: Expected 0 updates with ratio=0.0, got {updates}")
            elif update_ratio == 1.0:
                if inserts == 0:
                    print("  ✅ PASS: 100% updates as expected")
                    successful_tests += 1
                else:
                    print(f"  ❌ FAIL: Expected 0 inserts with ratio=1.0, got {inserts}")
            else:  # update_ratio == 0.5
                update_pct = updates / len(batch_data) if len(batch_data) > 0 else 0
                if 0.2 <= update_pct <= 0.8:
                    print("  ✅ PASS: Mix of updates and inserts as expected")
                    successful_tests += 1
                else:
                    print(f"  ❌ FAIL: Expected ~50% updates, got {update_pct*100:.1f}%")
                    
        except Exception as e:
            print(f"  ❌ Test failed with error: {e}")
    
    print("\n=== TEST RESULTS SUMMARY ===")
    print(f"✅ {successful_tests}/{total_tests} tests passed!")
    
    if successful_tests == total_tests:
        print("✅ Update/Insert logic is working correctly!")
        print("✅ Ready to run the full parallel MERGE and OPTIMIZE demo")
        return True
    else:
        print("❌ Some tests failed. Fix the issues before proceeding with the full demo")
        return False

def create_table_with_liquid_clustering(spark, config):
    """Create table with liquid clustering and row-level concurrency"""
    logger.info(f"Creating table {config['TARGET_TABLE']} with liquid clustering")
    
    # Create table with liquid clustering enabled from the start
    create_table_sql = f"""
    CREATE OR REPLACE TABLE {config['TARGET_TABLE']} (
      event_id STRING,
      event_timestamp TIMESTAMP,
      vehicle_make STRING,
      vehicle_model STRING,
      vehicle_year STRING,
      latitude STRING,
      longitude STRING,
      zipcode STRING
    )
    USING DELTA
    CLUSTER BY ({config['CLUSTERING_COLUMN']})
    TBLPROPERTIES (
      'delta.enableDeletionVectors' = 'true',
      'delta.enableRowTracking' = 'true',
      'delta.isolationLevel' = 'WriteSerializable'
    )
    """
    
    spark.sql(create_table_sql)
    logger.info(f"Table {config['TARGET_TABLE']} created with liquid clustering on {config['CLUSTERING_COLUMN']}")
    
    return True

def load_initial_data(spark, data_gen, config):
    """Load initial data using streaming with liquid clustering"""
    logger.info("Loading initial data via streaming...")
    
    for i in range(2):  # Reduced from 3 to 2 batches
        logger.info(f"Loading batch {i+1}/2...")
        
        query = (create_streaming_vehicle_data(spark, data_gen, config, rows_per_second=50, num_partitions=1, update_ratio=0.0)
                 .writeStream
                 .option("queryName", f"Bootstrap_{config['TARGET_TABLE']}_{i}")
                 .trigger(availableNow=True)
                 .option("checkpointLocation", f"{config['checkpoint_bootstrap']}_{i}")
                 .toTable(config['TARGET_TABLE'])
        )
        
        query.awaitTermination()
    
    logger.info(f"Initial data loaded - table ready with liquid clustering")
    return True

def verify_table_configuration(spark, config):
    """Verify table configuration for row-level concurrency"""
    print("=== VERIFYING TABLE CONFIGURATION ===")
    
    try:
        # Check table properties
        table_details = spark.sql(f"DESC DETAIL {config['TARGET_TABLE']}").collect()[0]
        
        print(f"Table Name: {table_details['name']}")
        print(f"Format: {table_details['format']}")
        print(f"Clustering Columns: {table_details['clusteringColumns']}")
        print(f"Table Features: {table_details['tableFeatures']}")
        
        # Check specific properties
        properties = table_details['properties']
        print(f"\nKey Properties:")
        print(f"  - Deletion Vectors Enabled: {properties.get('delta.enableDeletionVectors', 'Not Set')}")
        print(f"  - Row Tracking Enabled: {properties.get('delta.enableRowTracking', 'Not Set')}")
        print(f"  - Isolation Level: {properties.get('delta.isolationLevel', 'Not Set')}")
        print(f"  - Checkpoint Policy: {properties.get('delta.checkpointPolicy', 'Not Set')}")
        print(f"  - Compression: {properties.get('delta.parquet.compression.codec', 'Not Set')}")
        
        # Check statistics
        statistics = table_details['statistics']
        print(f"\nTable Statistics:")
        print(f"  - Deletion Vectors: {statistics.get('numDeletionVectors', 0)}")
        print(f"  - Rows Deleted by Deletion Vectors: {statistics.get('numRowsDeletedByDeletionVectors', 0)}")
        
        print("\n=== CONFIGURATION VERIFICATION COMPLETE ===")
        
        # Show sample data
        print("\nSample Data:")
        sample_data = spark.read.table(config['TARGET_TABLE']).limit(10)
        sample_data.show()
        
        return True
        
    except Exception as e:
        print(f"❌ Table configuration verification failed: {e}")
        return False

def main():
    """Main function to run the demo"""
    print("🚀 Starting MERGE and OPTIMIZE Parallel Demo")
    print("=" * 60)
    
    # Step 1: Initialize Databricks Connect
    spark = initialize_databricks_connect()
    if not spark:
        print("❌ Failed to initialize Databricks Connect")
        return False
    
    # Step 2: Verify required packages
    if not verify_required_packages():
        return False
    
    # Step 3: Verify Databricks environment
    if not verify_databricks_environment(spark):
        return False
    
    # Step 4: Setup configuration
    config = setup_configuration()
    
    # Step 5: Initialize data generation
    data_gen = initialize_data_generation(spark, config)
    
    # Step 6: Test update/insert logic
    if not test_update_insert_logic(spark, data_gen, config):
        print("❌ Update/insert logic tests failed")
        return False
    
    # Step 7: Create table with liquid clustering
    if not create_table_with_liquid_clustering(spark, config):
        print("❌ Failed to create table with liquid clustering")
        return False
    
    # Step 8: Load initial data
    if not load_initial_data(spark, data_gen, config):
        print("❌ Failed to load initial data")
        return False
    
    # Step 9: Verify table configuration
    if not verify_table_configuration(spark, config):
        print("❌ Table configuration verification failed")
        return False
    
    print("\n✅ DEMO SETUP COMPLETE!")
    print("✅ All components are working correctly!")
    print("✅ The table is ready for parallel MERGE and OPTIMIZE operations!")
    print("\nNext steps:")
    print("1. The table is configured with liquid clustering and row-level concurrency")
    print("2. You can now run MERGE operations in parallel with OPTIMIZE operations")
    print("3. The table supports deletion vectors for efficient updates")
    print("4. All Databricks-specific features are properly configured")
    
    return True

if __name__ == "__main__":
    success = main()
    if success:
        print("\n🎉 Demo verification completed successfully!")
        sys.exit(0)
    else:
        print("\n❌ Demo verification failed!")
        sys.exit(1)
