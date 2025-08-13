# MERGE and OPTIMIZE Parallel Demo

This demonstration shows how MERGE and OPTIMIZE operations can run in parallel with row-level concurrency, deletion vectors, and liquid clustering in Databricks.

## Overview

The demo creates a Delta table with the following features:
- **Row-level concurrency**: Multiple writers can modify different rows simultaneously
- **Deletion vectors**: Efficient deletion without rewriting entire files
- **Liquid clustering**: Automatic clustering on the merge column for optimal performance

## Files

1. **`merge_optimize_parallel_demo.py`** - Main demonstration notebook
2. **`test_merge_optimize_setup.py`** - Simple test script to verify functionality
3. **`README_MERGE_OPTIMIZE_DEMO.md`** - This documentation

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- Access to a catalog and schema (e.g., `soni.default`)
- S3 bucket access for checkpoint locations
- Cluster with Databricks Runtime 13.0 or later

## How to Run

### Option 1: Import as Databricks Notebook

1. In your Databricks workspace, create a new notebook
2. Copy the contents of `merge_optimize_parallel_demo.py` into the notebook
3. Run the cells sequentially

### Option 2: Use the Test Script First

1. Create a new notebook and copy `test_merge_optimize_setup.py`
2. Run it to verify basic functionality
3. Then run the main demo

### Option 3: Upload as Python File

1. Upload `merge_optimize_parallel_demo.py` to your Databricks workspace
2. Create a new notebook and use `%run` to execute it:
   ```python
   %run /path/to/merge_optimize_parallel_demo
   ```

## Configuration

Before running, update these variables in the script:

```python
# Update these to match your environment
target_table = "your_catalog.your_schema.parallel_merges_optimize_demo"
checkpoint_location_for_bootstrap = f"s3://your-bucket/checkpoints/{uuid.uuid4()}"
checkpoint_location = f"s3://your-bucket/checkpoints/{uuid.uuid4()}"
```

## What the Demo Does

1. **Creates a table** with row-level concurrency enabled
2. **Generates streaming data** using Faker for realistic vehicle and location data
3. **Runs MERGE operations** continuously using Structured Streaming
4. **Runs OPTIMIZE operations** in parallel using background threads
5. **Monitors both operations** to show they can run concurrently
6. **Verifies table configuration** to ensure proper setup

## Expected Output

You should see:
- Table creation with proper configuration
- MERGE batches processing every 10 seconds
- OPTIMIZE operations running every 15-30 seconds
- Table statistics updates every 60 seconds
- Both operations running without conflicts

## Key Features Demonstrated

### Row-Level Concurrency
- Multiple writers can modify different rows simultaneously
- No table-level locks during MERGE operations
- OPTIMIZE can run while MERGE is active

### Deletion Vectors
- Efficient deletion without rewriting entire files
- Better performance for frequent updates
- Reduced storage costs

### Liquid Clustering
- Automatic clustering on the merge column (`event_id`)
- Optimized for MERGE performance
- No manual clustering required

## Monitoring

The demo includes several monitoring features:
- Real-time table statistics
- Streaming job status
- Operation timing metrics
- Configuration verification

## Cleanup

To stop the demo and clean up resources:

1. Uncomment the cleanup code in the last cell
2. Run the cleanup cell
3. This will stop streaming jobs and drop the table

## Troubleshooting

### Common Issues

1. **Permission errors**: Ensure you have write access to the catalog and S3 bucket
2. **Package installation**: The script installs Faker and faker_vehicle automatically
3. **Streaming job failures**: Check the streaming job UI in Databricks
4. **Table already exists**: The script drops existing tables, but ensure you have permissions

### Debugging

- Check the Databricks streaming job UI for detailed logs
- Monitor the notebook output for error messages
- Verify table configuration with `DESC DETAIL` commands

## Performance Considerations

- Adjust `rowsPerSecond` and `numPartitions` based on your cluster size
- Monitor cluster resource usage during the demo
- Consider using larger clusters for production workloads

## References

- [Row-Level Concurrency Documentation](https://docs.databricks.com/aws/en/optimizations/isolation-level#write-conflicts-with-row-level-concurrency)
- [Delta Lake MERGE Documentation](https://docs.databricks.com/delta/delta-update.html)
- [Delta Lake OPTIMIZE Documentation](https://docs.databricks.com/delta/optimizations/file-mgmt.html)
- [Liquid Clustering Documentation](https://docs.databricks.com/delta/clustering/liquid-clustering.html)

## Support

If you encounter issues:
1. Check the Databricks documentation
2. Review the error messages in the notebook output
3. Verify your cluster configuration and permissions
4. Test with the simpler test script first

