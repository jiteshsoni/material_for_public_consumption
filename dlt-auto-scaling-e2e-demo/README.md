# Delta Table Streaming Benchmark - DLT Auto-Scaling Demo

This demo showcases a time-based scaling pattern for Delta table streaming to test and demonstrate auto-scaling capabilities in Databricks environments using Unity Catalog and Volumes.

## 📁 File Structure

```
dlt-auto-scaling-e2e-demo/
├── delta_table_streaming_benchmark.py      # Unified script (works both ways)
├── requirements.txt                        # Python dependencies
├── README.md                              # This documentation
├── databricks.yml                         # Databricks project configuration
└── pyproject.toml                         # Project metadata
```

## 🎯 Demo Overview

The `delta_table_streaming_benchmark.py` script creates a controlled streaming workload that simulates variable data volume patterns to test auto-scaling behavior. This **unified script** automatically detects whether it's running via Databricks Connect or directly on a Databricks cluster and adapts accordingly.

## 📊 Scaling Pattern

The demo follows a specific time-based scaling schedule:

### Phase 1: Baseline (0-3 minutes)
- **50 parallel streams** at **1000 rows/second** each
- **Total throughput**: 50,000 rows/second
- All streams write to separate Delta tables

### Phase 2: 3x Scaling (3-6 minutes)
- **50 baseline streams** continue at 1000 rows/second each
- **+1 additional stream** at **3000 rows/second** (3x rate)
- **Total throughput**: 53,000 rows/second

### Phase 3: 9x Scaling (6-10 minutes)
- **50 baseline streams** continue at 1000 rows/second each
- **+1 additional stream** at **9000 rows/second** (9x rate)
- **Total throughput**: 62,000 rows/second

### Phase 4: Return to Baseline (10+ minutes)
- **50 baseline streams** continue at 1000 rows/second each
- Scaling streams are stopped
- **Total throughput**: 50,000 rows/second

## 🚀 Expected Behavior

| Time Period | Baseline Streams | 3x Stream | 9x Stream | Total Throughput |
|-------------|------------------|-----------|-----------|------------------|
| 0-3 min     | 50 × 1000        | -         | -         | 50,000 rows/sec  |
| 3-6 min     | 50 × 1000        | 1 × 3000  | -         | 53,000 rows/sec  |
| 6-10 min    | 50 × 1000        | -         | 1 × 9000  | 62,000 rows/sec  |
| 10+ min     | 50 × 1000        | -         | -         | 50,000 rows/sec  |

## 🛠️ Configuration

The script includes configurable parameters:

```python
config = {
    "baseline_streams": 50,        # Number of baseline streams
    "baseline_rate": 1000,         # Rows per second per baseline stream
    "scale_3x_rate": 3000,         # 3x scaling rate
    "scale_9x_rate": 9000,         # 9x scaling rate
    "catalog_name": "soni",        # Unity Catalog name
    "database_name": "default",    # Database name
    "table_prefix": "stream_table",
    "checkpoint_path": "/Volumes/soni/default/checkpoints/",
    "partitions": 8,
    "test_mode": False,            # Set to True for reduced testing
    "demo_duration_minutes": 120   # Total demo duration in minutes (2 hours)
}
```

### 🔧 Key Features:
- **Unity Catalog**: Uses `soni.default` catalog and database
- **Volumes Checkpointing**: Unique timestamp-based checkpoint paths
- **Configurable Duration**: Default 120 minutes (2 hours), test mode 15 minutes
- **Automatic Dependencies**: Installs `dbldatagen` via `%pip install`

## 📁 Output Tables

The script creates the following Delta tables in Unity Catalog:

- **Baseline tables**: `soni.default.stream_table_001` through `soni.default.stream_table_050`
- **3x scaling table**: `soni.default.stream_table_3x_scale`
- **9x scaling table**: `soni.default.stream_table_9x_scale`

Each table contains streaming data with schema:
- `device_id` (String)
- `event_timestamp` (Timestamp)
- `temperature` (Double)
- `humidity` (Double)
- `pressure` (Double)
- `stream_type` (String) - "baseline", "3x", or "9x"
- `stream_id` (Integer)

## 🏃‍♂️ Running the Demo

### Prerequisites
- Databricks workspace with active cluster
- Unity Catalog enabled with `soni` catalog access
- Volumes access for checkpoint storage
- Python environment with PySpark and Delta Lake support

### Execution Options

#### Option 1: Databricks Notebook (Recommended)
1. Upload `delta_table_streaming_benchmark.py` to your Databricks workspace
2. Attach to a running cluster
3. Run the notebook cells sequentially
4. Monitor the scaling behavior in real-time

#### Option 2: Local Development with Databricks Connect
1. Ensure Databricks Connect is configured (`databricks-connect test`)
2. Run: `python delta_table_streaming_benchmark.py`
3. The script automatically detects Databricks Connect and connects to your remote cluster

#### How It Works
The script automatically detects the execution environment:
- **Databricks Connect available**: Uses remote cluster connection
- **Direct cluster execution**: Uses active Spark session
- **No connection**: Provides helpful error messages

### Test Mode
For quick testing, set `"test_mode": True` in the configuration:
- Reduces baseline streams from 50 to 5
- Reduces rates proportionally
- Reduces duration to 15 minutes
- Faster execution for development/testing

## 📈 Monitoring

The script provides real-time monitoring output:
- Stream status updates every 30 seconds
- Active stream counts and total throughput
- Scaling event notifications
- Final results analysis
- Duration and checkpoint path information

## 🧹 Cleanup Functions

The script includes utility functions:
- `stop_all_streams()` - Stop all running streams
- `check_stream_status()` - Check which streams are active
- `test_scaling_logic()` - Test timing logic without starting streams

## 🎯 Use Cases

This benchmark is useful for:
- **Auto-scaling testing** - Validate cluster scaling behavior
- **Performance benchmarking** - Measure throughput at different load levels
- **DLT pipeline testing** - Test Delta Live Tables with variable workloads
- **Monitoring validation** - Verify alerting and metrics collection
- **Capacity planning** - Understand resource requirements at different scales
- **Unity Catalog testing** - Validate catalog and database operations
- **Volumes testing** - Test checkpoint persistence in Volumes

## ⚠️ Important Notes

- The demo runs for **120 minutes (2 hours)** by default
- Test mode runs for **15 minutes**
- All streams use checkpointing for fault tolerance in Volumes
- Data is written to Delta tables in Unity Catalog for persistence
- The script requires an active Databricks Spark session
- Checkpoint paths include timestamps to prevent conflicts
- Test mode is recommended for initial validation

## 🔧 Customization

You can modify the configuration to test different scenarios:
- Change baseline rates and stream counts
- Adjust scaling factors and timing
- Modify demo duration (default: 120 minutes)
- Change Unity Catalog and database names
- Customize checkpoint paths
- Modify data schema and generation patterns
- Customize monitoring intervals and output formats