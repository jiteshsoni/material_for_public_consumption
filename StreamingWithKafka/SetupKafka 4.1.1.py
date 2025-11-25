# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Kafka 4.1.1 Setup on Databricks with KRaft Mode
# MAGIC 
# MAGIC ## 📋 Overview
# MAGIC 
# MAGIC This notebook provides a **complete, production-ready setup** of Apache Kafka 4.1.1 on a Databricks cluster using **KRaft mode** (no ZooKeeper required). It includes:
# MAGIC 
# MAGIC - ✅ Kafka installation and configuration
# MAGIC - ✅ KRaft-based cluster setup (controller + broker in one process)
# MAGIC - ✅ Topic creation and management
# MAGIC - ✅ Data ingestion from Databricks datasets
# MAGIC - ✅ Spark Structured Streaming integration
# MAGIC - ✅ Kafka-to-Kafka forwarding patterns
# MAGIC - ✅ Unity Catalog Volume to Kafka streaming
# MAGIC 
# MAGIC ## 🎯 Use Cases
# MAGIC 
# MAGIC - **Real-time data pipelines** on Databricks
# MAGIC - **Event streaming** for analytics workloads
# MAGIC - **Kafka integration** with Delta Lake
# MAGIC - **Stream processing** with Spark Structured Streaming
# MAGIC - **Development and testing** of Kafka-based applications
# MAGIC 
# MAGIC ## 🔧 Prerequisites
# MAGIC 
# MAGIC - Databricks cluster with **sudo access** (required for apt-get)
# MAGIC - **local_disk0** available (Kafka installation location)
# MAGIC - **Python 3.x** with PySpark
# MAGIC - **Network access** to Apache Kafka downloads
# MAGIC 
# MAGIC ## ⚠️ Important Notes
# MAGIC 
# MAGIC - This setup runs Kafka **on the driver node only**
# MAGIC - Suitable for **development, testing, and single-node streaming**
# MAGIC - For production multi-node clusters, consider managed Kafka services
# MAGIC - All Kafka data stored in `/local_disk0` (ephemeral storage)
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Download and Install Kafka 4.1.1
# MAGIC 
# MAGIC This cell:
# MAGIC - Downloads Kafka 4.1.1 (Scala 2.13 build) from Apache mirrors
# MAGIC - Extracts to `/local_disk0/kafka_2.13-4.1.1`
# MAGIC - Verifies installation by listing Kafka home contents
# MAGIC 
# MAGIC **Installation Location:** `/local_disk0` provides high-performance local storage on the driver node.

# COMMAND ----------

# MAGIC %sh
# MAGIC set -e
# MAGIC 
# MAGIC export KAFKA_VERSION="4.1.1"
# MAGIC export SCALA_VERSION="2.13"
# MAGIC export KAFKA_TGZ="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
# MAGIC export KAFKA_BASE_URL="https://downloads.apache.org/kafka/${KAFKA_VERSION}"
# MAGIC 
# MAGIC # extract location FIXED
# MAGIC export KAFKA_HOME="/local_disk0/kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
# MAGIC 
# MAGIC echo "Downloading Kafka ${KAFKA_VERSION}..."
# MAGIC wget -O "/tmp/${KAFKA_TGZ}" "${KAFKA_BASE_URL}/${KAFKA_TGZ}"
# MAGIC 
# MAGIC echo "Extracting to ${KAFKA_HOME} ..."
# MAGIC mkdir -p /local_disk0
# MAGIC tar -xzvf "/tmp/${KAFKA_TGZ}" -C /local_disk0 > /dev/null
# MAGIC 
# MAGIC echo "Kafka home contents:"
# MAGIC ls -l "${KAFKA_HOME}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Configure KRaft Server Properties
# MAGIC 
# MAGIC Kafka 4.x wants KRaft configs. We’ll:
# MAGIC 
# MAGIC Copy the sample kraft/server.properties
# MAGIC 
# MAGIC Patch minimal fields (listeners, log dirs, controller quorum, etc.)

# COMMAND ----------

# MAGIC %sh
# MAGIC set -e
# MAGIC 
# MAGIC export KAFKA_VERSION="4.1.1"
# MAGIC export SCALA_VERSION="2.13"
# MAGIC export KAFKA_HOME="/local_disk0/kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
# MAGIC export KAFKA_CONFIG_DIR="${KAFKA_HOME}/config/kraft"
# MAGIC export KAFKA_CONFIG_FILE="${KAFKA_CONFIG_DIR}/server.properties"
# MAGIC export KAFKA_LOG_ROOT="/local_disk0/kafka-logs"
# MAGIC export KAFKA_LOG_DIR="${KAFKA_LOG_ROOT}/data"
# MAGIC export KAFKA_META_DIR="${KAFKA_LOG_ROOT}/meta"
# MAGIC 
# MAGIC mkdir -p "${KAFKA_CONFIG_DIR}"
# MAGIC 
# MAGIC echo "Using config file: ${KAFKA_CONFIG_FILE}"
# MAGIC 
# MAGIC cat > "${KAFKA_CONFIG_FILE}" <<EOF
# MAGIC process.roles=broker,controller
# MAGIC node.id=1
# MAGIC 
# MAGIC controller.listener.names=CONTROLLER
# MAGIC controller.quorum.voters=1@localhost:9093
# MAGIC 
# MAGIC listeners=PLAINTEXT://:9092,CONTROLLER://:9093
# MAGIC listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
# MAGIC inter.broker.listener.name=PLAINTEXT
# MAGIC advertised.listeners=PLAINTEXT://localhost:9092
# MAGIC 
# MAGIC # 👇 Broker logs live here
# MAGIC log.dirs=${KAFKA_LOG_DIR}
# MAGIC # 👇 KRaft metadata logs live here (different directory)
# MAGIC metadata.log.dir=${KAFKA_META_DIR}
# MAGIC 
# MAGIC offsets.topic.replication.factor=1
# MAGIC transaction.state.log.replication.factor=1
# MAGIC transaction.state.log.min.isr=1
# MAGIC group.initial.rebalance.delay.ms=0
# MAGIC auto.create.topics.enable=true
# MAGIC EOF
# MAGIC 
# MAGIC echo "===== server.properties ====="
# MAGIC cat "${KAFKA_CONFIG_FILE}"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration Details
# MAGIC 
# MAGIC The generated `server.properties` includes:
# MAGIC 
# MAGIC **Key Configuration Parameters:**
# MAGIC 
# MAGIC | Parameter | Value | Purpose |
# MAGIC |-----------|-------|---------|
# MAGIC | `process.roles` | broker,controller | Combined mode (single node) |
# MAGIC | `node.id` | 1 | Unique node identifier |
# MAGIC | `listeners` | PLAINTEXT://:9092, CONTROLLER://:9093 | Client and controller ports |
# MAGIC | `log.dirs` | `/local_disk0/kafka-logs/data` | Kafka message data storage |
# MAGIC | `metadata.log.dir` | `/local_disk0/kafka-logs/meta` | KRaft metadata storage |
# MAGIC | `auto.create.topics.enable` | true | Auto-create topics on demand |
# MAGIC 
# MAGIC **Replication Settings** for single-node setup:
# MAGIC - `offsets.topic.replication.factor=1`
# MAGIC - `transaction.state.log.replication.factor=1`
# MAGIC 
# MAGIC **Production Note:** Increase replication factors for multi-broker setups.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Format the Storage Directory (KRaft Requirement)
# MAGIC 
# MAGIC **KRaft initialization** requires formatting storage directories before first use.
# MAGIC 
# MAGIC This cell:
# MAGIC 1. **Cleans previous Kafka data** (removes `/local_disk0/kafka-logs`)
# MAGIC 2. **Generates a unique cluster ID** using `kafka-storage.sh random-uuid`
# MAGIC 3. **Formats storage directories** with the cluster ID and metadata version
# MAGIC 
# MAGIC ### What Happens During Formatting
# MAGIC 
# MAGIC - Creates directory structure for data and metadata
# MAGIC - Initializes KRaft metadata logs
# MAGIC - Associates directories with the cluster ID
# MAGIC - Sets metadata version (4.1-IV1 for Kafka 4.1.1)
# MAGIC 
# MAGIC **⚠️ Warning:** This step deletes all existing Kafka data. Run carefully in production environments.

# COMMAND ----------

# MAGIC %sh
# MAGIC set -e
# MAGIC 
# MAGIC export KAFKA_VERSION="4.1.1"
# MAGIC export SCALA_VERSION="2.13"
# MAGIC export KAFKA_HOME="/local_disk0/kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
# MAGIC export KAFKA_CONFIG_FILE="${KAFKA_HOME}/config/kraft/server.properties"
# MAGIC export KAFKA_LOG_ROOT="/local_disk0/kafka-logs"
# MAGIC 
# MAGIC rm -rf "${KAFKA_LOG_ROOT}"
# MAGIC mkdir -p "${KAFKA_LOG_ROOT}"
# MAGIC 
# MAGIC CLUSTER_ID=$("${KAFKA_HOME}/bin/kafka-storage.sh" random-uuid)
# MAGIC echo "CLUSTER_ID=${CLUSTER_ID}"
# MAGIC 
# MAGIC "${KAFKA_HOME}/bin/kafka-storage.sh" format \
# MAGIC   -t "${CLUSTER_ID}" \
# MAGIC   -c "${KAFKA_CONFIG_FILE}"
# MAGIC 
# MAGIC echo "Storage formatted."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4️⃣ Start Kafka Server (Controller + Broker in One Process)
# MAGIC 
# MAGIC Launches the Kafka server in **background mode** using `nohup`.
# MAGIC 
# MAGIC ### Process Details
# MAGIC 
# MAGIC - **Startup script:** `kafka-server-start.sh`
# MAGIC - **Configuration:** Uses KRaft `server.properties` created in Step 2
# MAGIC - **Output location:** `/local_disk0/kafka.out` (stdout and stderr)
# MAGIC - **Startup delay:** 10-second sleep for initialization
# MAGIC - **Verification:** Checks if port 9092 is listening
# MAGIC 
# MAGIC ### Verification Steps
# MAGIC 
# MAGIC The cell checks for Kafka listening on port **9092** (broker port). If the check shows "Nothing on 9092", inspect `/local_disk0/kafka.out` for errors.
# MAGIC 
# MAGIC **Expected Output:** Should show Kafka process listening on `0.0.0.0:9092` after successful startup.

# COMMAND ----------

# MAGIC %sh
# MAGIC set -e
# MAGIC 
# MAGIC export KAFKA_VERSION="4.1.1"
# MAGIC export SCALA_VERSION="2.13"
# MAGIC export KAFKA_HOME="/local_disk0/kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
# MAGIC export KAFKA_CONFIG_FILE="${KAFKA_HOME}/config/kraft/server.properties"
# MAGIC 
# MAGIC # Kill existing Kafka process if running
# MAGIC pkill -f kafka.Kafka || true
# MAGIC sleep 2
# MAGIC 
# MAGIC echo "Starting Kafka 4.1.1..."
# MAGIC nohup "${KAFKA_HOME}/bin/kafka-server-start.sh" "${KAFKA_CONFIG_FILE}" \
# MAGIC   > /local_disk0/kafka.out 2>&1 &
# MAGIC 
# MAGIC # Wait for port 9092
# MAGIC echo "Waiting for Kafka to start..."
# MAGIC for i in {1..30}; do
# MAGIC   if netstat -plnt | grep -q 9092; then
# MAGIC     echo "Kafka started on port 9092."
# MAGIC     exit 0
# MAGIC   fi
# MAGIC   sleep 1
# MAGIC done
# MAGIC 
# MAGIC echo "Kafka failed to start after 30 seconds. Check /local_disk0/kafka.out"
# MAGIC exit 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### ⚠️ Troubleshooting
# MAGIC 
# MAGIC If port 9092 is not found, inspect the logs:

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /local_disk0/kafka.out

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5️⃣ Create a Kafka Topic (8 Partitions)
# MAGIC 
# MAGIC Creates a topic named `quickstart-event` with **8 partitions** for parallel processing.
# MAGIC 
# MAGIC ### Topic Configuration
# MAGIC 
# MAGIC | Parameter | Value | Purpose |
# MAGIC |-----------|-------|---------|
# MAGIC | `--topic` | quickstart-event | Topic name |
# MAGIC | `--partitions` | 8 | Number of partitions for parallelism |
# MAGIC | `--replication-factor` | 1 | Single replica (single-node setup) |
# MAGIC | `--bootstrap-server` | localhost:9092 | Kafka broker address |
# MAGIC 
# MAGIC ### Why 8 Partitions?
# MAGIC 
# MAGIC - **Parallelism:** Allows up to 8 concurrent consumers
# MAGIC - **Throughput:** Distributes load across multiple partitions
# MAGIC - **Scalability:** Matches common Spark cluster configurations
# MAGIC - **Performance:** Enables efficient data distribution
# MAGIC 
# MAGIC **Note:** Partitions cannot be decreased after creation, but can be increased later if needed.

# COMMAND ----------

# MAGIC %sh
# MAGIC export KAFKA_VERSION="4.1.1"
# MAGIC export SCALA_VERSION="2.13"
# MAGIC export KAFKA_HOME="/local_disk0/kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
# MAGIC 
# MAGIC "${KAFKA_HOME}/bin/kafka-topics.sh" \
# MAGIC   --create \
# MAGIC   --topic quickstart-event \
# MAGIC   --bootstrap-server localhost:9092 \
# MAGIC   --partitions 8 \
# MAGIC   --replication-factor 1 || echo "Topic already exists"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📊 Verify Topic Creation
# MAGIC 
# MAGIC Run the following command to describe the topic and verify its configuration.
# MAGIC 
# MAGIC **Expected Output:** Topic details showing 8 partitions, replication factor, and ISR (In-Sync Replicas) information.
# MAGIC 
# MAGIC **Note:** The next code cell contains the verification command.
# MAGIC 
# COMMAND ----------

# MAGIC %sh
# MAGIC export KAFKA_VERSION="4.1.1"
# MAGIC export SCALA_VERSION="2.13"
# MAGIC export KAFKA_HOME="/local_disk0/kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
# MAGIC 
# MAGIC "${KAFKA_HOME}/bin/kafka-topics.sh" --describe --topic quickstart-event --bootstrap-server localhost:9092

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6️⃣ Install kcat (kafkacat) - Kafka CLI Tool
# MAGIC 
# MAGIC Installs `kcat` (formerly `kafkacat`), a powerful command-line utility for Kafka operations.
# MAGIC 
# MAGIC ### About kcat
# MAGIC 
# MAGIC `kcat` is a non-JVM Kafka producer and consumer that provides:
# MAGIC 
# MAGIC - ✅ **Fast data ingestion** from files to Kafka topics
# MAGIC - ✅ **Quick data inspection** without writing code
# MAGIC - ✅ **Metadata queries** for topics, partitions, and offsets
# MAGIC - ✅ **Performance testing** for Kafka throughput
# MAGIC - ✅ **Debugging** Kafka connectivity issues
# MAGIC 
# MAGIC ### Installation Strategy
# MAGIC 
# MAGIC 1. Attempts to install `kcat` (modern name)
# MAGIC 2. Falls back to `kafkacat` if `kcat` unavailable
# MAGIC 3. Verifies installation with version check
# MAGIC 
# MAGIC **Why use kcat?** Much faster than `kafka-console-producer.sh` for bulk data ingestion.

# COMMAND ----------

# MAGIC %sh
# MAGIC set -e
# MAGIC 
# MAGIC if command -v kcat >/dev/null 2>&1 || command -v kafkacat >/dev/null 2>&1; then
# MAGIC   echo "kcat/kafkacat is already installed."
# MAGIC else
# MAGIC   sudo apt-get update -y
# MAGIC   if ! sudo apt-get install -y kcat; then
# MAGIC     echo "kcat not found; installing kafkacat instead"
# MAGIC     sudo apt-get install -y kafkacat
# MAGIC   fi
# MAGIC fi
# MAGIC 
# MAGIC echo "Installed version:"
# MAGIC kcat -V || kafkacat -V

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7️⃣ Produce Data from Databricks Datasets into Kafka
# MAGIC 
# MAGIC Ingests **Wikipedia clickstream data** from Databricks datasets into the `quickstart-event` topic.
# MAGIC 
# MAGIC ### Data Source
# MAGIC 
# MAGIC **Path:** `/dbfs/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/2015_2_clickstream.json`
# MAGIC 
# MAGIC **Content:** Wikipedia clickstream data showing:
# MAGIC - Page navigation patterns
# MAGIC - Referrer information (search engines, other Wikipedia pages, external sites)
# MAGIC - Click counts between pages
# MAGIC - Page IDs and titles
# MAGIC 
# MAGIC ### kcat Producer Options
# MAGIC 
# MAGIC | Option | Value | Purpose |
# MAGIC |--------|-------|---------|
# MAGIC | `-b` | localhost:9092 | Bootstrap server |
# MAGIC | `-t` | quickstart-event | Target topic |
# MAGIC | `-P` | (flag) | Producer mode |
# MAGIC | `-l` | file path | Read from file (each line = 1 message) |
# MAGIC 
# MAGIC ### Data Characteristics
# MAGIC 
# MAGIC - **Format:** JSON (one record per line)
# MAGIC - **Size:** Varies by dataset file
# MAGIC - **Schema:** `prev_id`, `curr_id`, `n`, `prev_title`, `curr_title`, `type`
# MAGIC 
# MAGIC **Performance Note:** `kcat` efficiently streams the entire file into Kafka with minimal overhead.

# COMMAND ----------

# MAGIC %sh
# MAGIC set -e
# MAGIC 
# MAGIC BROKER="localhost:9092"
# MAGIC TOPIC="quickstart-event"
# MAGIC INPUT_FILE="/dbfs/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/2015_2_clickstream.json"
# MAGIC 
# MAGIC echo "Producing data..."
# MAGIC 
# MAGIC if command -v kcat >/dev/null 2>&1; then
# MAGIC   kcat -b "${BROKER}" -t "${TOPIC}" -P -l "${INPUT_FILE}"
# MAGIC else
# MAGIC   kafkacat -b "${BROKER}" -t "${TOPIC}" -P -l "${INPUT_FILE}"
# MAGIC fi
# MAGIC 
# MAGIC echo "Done."

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Verify Data Ingestion
# MAGIC 
# MAGIC Consume and display the first 20 messages from the topic to verify successful ingestion.
# MAGIC 
# MAGIC **kcat Consumer Options:**
# MAGIC - `-C`: Consumer mode
# MAGIC - `-o beginning`: Start from the beginning of the topic
# MAGIC - `-e`: Exit after reaching the end
# MAGIC - `-q`: Quiet mode (no metadata output)

# COMMAND ----------

# MAGIC %sh
# MAGIC kcat -b localhost:9092 -t quickstart-event -C -o beginning -e -q | head -n 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8️⃣ Spark: Infer Schema from JSON File
# MAGIC 
# MAGIC Before consuming Kafka messages with Spark Structured Streaming, infer the JSON schema from the source file.
# MAGIC 
# MAGIC ### Why Infer Schema?
# MAGIC 
# MAGIC Kafka stores messages as **byte arrays** (binary). To parse JSON from Kafka:
# MAGIC 1. Read raw `value` column as string
# MAGIC 2. Apply `from_json()` with a schema
# MAGIC 3. Extract structured fields
# MAGIC 
# MAGIC ### Schema Inference Process
# MAGIC 
# MAGIC - Reads a sample of the JSON file using Spark
# MAGIC - Analyzes structure and infers data types
# MAGIC - Returns a StructType schema for use in `from_json()`
# MAGIC 
# MAGIC ### Expected Schema
# MAGIC 
# MAGIC ```
# MAGIC struct<
# MAGIC   curr_id:string,
# MAGIC   curr_title:string,
# MAGIC   n:string,
# MAGIC   prev_id:string,
# MAGIC   prev_title:string,
# MAGIC   type:string
# MAGIC >
# MAGIC ```
# MAGIC 
# MAGIC **Best Practice:** Cache the inferred schema and reuse it across streaming queries for consistency.

# COMMAND ----------

from pyspark.sql import functions as F

sample_path = "/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/2015_2_clickstream.json"
schema = spark.read.format("json").load(sample_path).schema
print(schema.simpleString())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9️⃣ Spark Structured Streaming: Read from Kafka
# MAGIC 
# MAGIC Demonstrates **real-time streaming** from Kafka using Spark Structured Streaming.
# MAGIC 
# MAGIC ### Streaming Pipeline Architecture
# MAGIC 
# MAGIC ```
# MAGIC Kafka Topic (quickstart-event)
# MAGIC     ↓
# MAGIC Spark readStream (Kafka source)
# MAGIC     ↓
# MAGIC JSON parsing with schema
# MAGIC     ↓
# MAGIC Structured DataFrame
# MAGIC     ↓
# MAGIC display() [real-time visualization]
# MAGIC ```
# MAGIC 
# MAGIC ### Key Configuration Options
# MAGIC 
# MAGIC | Option | Value | Purpose |
# MAGIC |--------|-------|---------|
# MAGIC | `format("kafka")` | - | Use Kafka source connector |
# MAGIC | `kafka.bootstrap.servers` | localhost:9092 | Broker address |
# MAGIC | `subscribe` | quickstart-event | Topic to consume |
# MAGIC | `startingOffsets` | earliest | Read all available data |
# MAGIC 
# MAGIC ### Data Transformation Steps
# MAGIC 
# MAGIC 1. **Read raw Kafka stream:** Contains `key`, `value`, `topic`, `partition`, `offset`, `timestamp`
# MAGIC 2. **Extract timestamp:** Rename to `event_ts` for clarity
# MAGIC 3. **Parse JSON value:** Use `from_json()` with inferred schema
# MAGIC 4. **Flatten structure:** Select nested fields to top level
# MAGIC 
# MAGIC ### Output Schema
# MAGIC 
# MAGIC After transformation:
# MAGIC - `event_ts`: Kafka message timestamp
# MAGIC - `curr_id`, `curr_title`, `n`, `prev_id`, `prev_title`, `type`: Wikipedia clickstream fields
# MAGIC 
# MAGIC **Usage Note:** The `display()` function creates a live-updating dashboard showing streaming data.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery

bootstrap_servers = "localhost:9092"
topic = "quickstart-event"

df_readstream = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", bootstrap_servers)
         .option("subscribe", topic)
         .option("startingOffsets", "earliest")
         .load()
         .select(
             F.col("timestamp").alias("event_ts"),
             F.from_json(F.col("value").cast("string"), schema).alias("data"),
         )
         .select("event_ts", "data.*")
)

display(df_readstream)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC ## 🔄 Advanced Pattern: Kafka-to-Kafka Forwarding
# MAGIC 
# MAGIC The following sections demonstrate **stream processing patterns** where Spark reads from one Kafka topic, transforms data, and writes to another topic.
# MAGIC 
# MAGIC ### Use Cases
# MAGIC 
# MAGIC - **Data enrichment:** Add computed fields or join with reference data
# MAGIC - **Filtering:** Route specific events to different topics
# MAGIC - **Format conversion:** Transform message formats
# MAGIC - **Aggregation:** Compute windowed aggregates and publish results
# MAGIC - **Multi-consumer scenarios:** Fan-out pattern for different downstream systems

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🏗️ Create Source and Sink Topics
# MAGIC 
# MAGIC Creates two topics for the Kafka-to-Kafka forwarding pattern:
# MAGIC 
# MAGIC 1. **quickstart-event** (source) - Already exists from Step 5
# MAGIC 2. **quickstart-event-processed** (sink) - New topic for processed data
# MAGIC 
# MAGIC Both topics configured with:
# MAGIC - **8 partitions** for parallel processing
# MAGIC - **Replication factor of 1** (single-node setup)
# MAGIC 
# MAGIC The command gracefully handles the case where `quickstart-event` already exists.

# COMMAND ----------

# MAGIC %sh
# MAGIC set -e
# MAGIC 
# MAGIC export KAFKA_VERSION="4.1.1"
# MAGIC export SCALA_VERSION="2.13"
# MAGIC export KAFKA_HOME="/local_disk0/kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
# MAGIC 
# MAGIC # Source topic (if needed)
# MAGIC "${KAFKA_HOME}/bin/kafka-topics.sh" \
# MAGIC   --create \
# MAGIC   --topic quickstart-event \
# MAGIC   --bootstrap-server localhost:9092 \
# MAGIC   --partitions 8 \
# MAGIC   --replication-factor 1 || echo "quickstart-event may already exist"
# MAGIC 
# MAGIC # Sink topic
# MAGIC "${KAFKA_HOME}/bin/kafka-topics.sh" \
# MAGIC   --create \
# MAGIC   --topic quickstart-event-processed \
# MAGIC   --bootstrap-server localhost:9092 \
# MAGIC   --partitions 8 \
# MAGIC   --replication-factor 1 || echo "quickstart-event-processed may already exist"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔄 Kafka-to-Kafka Streaming Query
# MAGIC 
# MAGIC Implements a **complete streaming pipeline** that reads from `quickstart-event` and writes to `quickstart-event-processed`.
# MAGIC 
# MAGIC ### Pipeline Architecture
# MAGIC 
# MAGIC ```
# MAGIC quickstart-event (source)
# MAGIC     ↓
# MAGIC Spark Structured Streaming
# MAGIC     ↓
# MAGIC Parse JSON → Structured DataFrame
# MAGIC     ↓
# MAGIC Prepare Kafka format (key/value binary)
# MAGIC     ↓
# MAGIC quickstart-event-processed (sink)
# MAGIC ```
# MAGIC 
# MAGIC ### Key Design Decisions
# MAGIC 
# MAGIC **1. Key Selection Strategy:**
# MAGIC - Uses `curr_id` as the message key
# MAGIC - Ensures messages for the same page go to the same partition
# MAGIC - Maintains ordering per `curr_id`
# MAGIC 
# MAGIC **2. Value Format:**
# MAGIC - Converts entire row back to JSON
# MAGIC - Preserves all original fields plus `event_ts`
# MAGIC - Binary encoding (UTF-8) for Kafka compatibility
# MAGIC 
# MAGIC **3. Checkpointing:**
# MAGIC - **Location:** `file:/tmp/kafka_forward/checkpoint`
# MAGIC - **Purpose:** Fault tolerance and exactly-once semantics
# MAGIC - **Recovery:** Allows restart from last committed offset
# MAGIC 
# MAGIC ### Exactly-Once Semantics
# MAGIC 
# MAGIC This pattern provides **end-to-end exactly-once** processing when:
# MAGIC - Source topic has replication and configured appropriately
# MAGIC - Checkpointing is enabled
# MAGIC - Idempotent producer is enabled (default in Kafka 3.0+)
# MAGIC 
# MAGIC ### Monitoring
# MAGIC 
# MAGIC The returned `StreamingQuery` object allows monitoring:
# MAGIC ```python
# MAGIC query.status           # Current status
# MAGIC query.recentProgress   # Recent batch statistics
# MAGIC query.lastProgress     # Last batch details
# MAGIC query.awaitTermination()  # Block until stopped
# MAGIC ```
# MAGIC 
# MAGIC **Production Note:** In production, store checkpoints in DBFS or cloud storage (S3, ADLS, GCS) for durability.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery

bootstrap_servers = "localhost:9092"
source_topic = "quickstart-event"
sink_topic = "quickstart-event-processed"

# Assume `schema` loaded from the JSON file as before
sample_path = "/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/2015_2_clickstream.json"
schema = spark.read.format("json").load(sample_path).schema

# ---- READ FROM SOURCE TOPIC ----
df_readstream = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", bootstrap_servers)
         .option("subscribe", source_topic)
         .option("startingOffsets", "earliest")
         .load()
         .select(
             F.col("timestamp").alias("event_ts"),
             F.from_json(F.col("value").cast("string"), schema).alias("data"),
         )
         .select("event_ts", "data.*")
)

# ---- PREPARE FOR KAFKA SINK ----
# use curr_id as key so partitions are stable; change if you like
df_for_kafka = (
    df_readstream
    .withColumn("key_str", F.col("curr_id").cast("string"))
    .withColumn("value_str", F.to_json(F.struct(*df_readstream.columns)))
    .select(
        F.col("key_str").cast("binary").alias("key"),
        F.col("value_str").cast("binary").alias("value"),
    )
)

checkpoint_path = "dbfs:/tmp/kafka_forward/checkpoint"

# Uncomment to reset checkpoint for a fresh start
# dbutils.fs.rm(checkpoint_path, True)

writer: DataStreamWriter = (
    df_for_kafka.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", sink_topic)
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
)

query: StreamingQuery = writer.start()
# query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC ## 🔗 Advanced Pattern: Unity Catalog Volume to Kafka
# MAGIC 
# MAGIC The following sections demonstrate streaming data from **Unity Catalog Volumes** (Parquet files) to Kafka using **Auto Loader**.
# MAGIC 
# MAGIC ### Architecture Overview
# MAGIC 
# MAGIC ```
# MAGIC Unity Catalog Volume (Parquet files)
# MAGIC     ↓
# MAGIC Auto Loader (cloudFiles)
# MAGIC     ↓
# MAGIC Schema inference & evolution
# MAGIC     ↓
# MAGIC Spark Structured Streaming
# MAGIC     ↓
# MAGIC Kafka Topic
# MAGIC ```
# MAGIC 
# MAGIC ### Use Case: Ethereum Blockchain Data
# MAGIC 
# MAGIC This example streams Ethereum block data from a Unity Catalog volume to a Kafka topic for real-time processing.
# MAGIC 
# MAGIC **Unity Catalog Path:**
# MAGIC - **Catalog:** blockchain
# MAGIC - **Schema:** ethereum  
# MAGIC - **Volume:** ethereum
# MAGIC - **Data:** blocks/ (Parquet files)
# MAGIC 
# MAGIC ### Why Auto Loader?
# MAGIC 
# MAGIC **Auto Loader** (`cloudFiles` format) provides:
# MAGIC - ✅ **Incremental processing:** Only reads new files
# MAGIC - ✅ **Schema inference:** Automatically detects Parquet schema
# MAGIC - ✅ **Schema evolution:** Handles schema changes gracefully
# MAGIC - ✅ **Scalability:** Efficiently processes millions of files
# MAGIC - ✅ **Exactly-once:** Built-in checkpointing for fault tolerance

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📊 Unity Catalog to Kafka Streaming Pipeline
# MAGIC 
# MAGIC Complete implementation streaming Ethereum blockchain data from Unity Catalog to Kafka.
# MAGIC 
# MAGIC **Configuration:**
# MAGIC - **Topic:** `ethereum-blocks` (create with 8 partitions before running)
# MAGIC - **Bootstrap servers:** `localhost:9092`
# MAGIC - **Schema location:** `dbfs:/tmp/autoloader/eth_blocks_schema`
# MAGIC - **Checkpoint:** `dbfs:/tmp/checkpoints/eth_blocks_to_kafka`
# MAGIC 
# MAGIC **Key Features:**
# MAGIC - Auto Loader for incremental file processing
# MAGIC - Schema inference with hints for BIGINT fields
# MAGIC - Block number as Kafka message key for ordering
# MAGIC - Full block data as JSON in message value
# MAGIC 
# MAGIC **Prerequisites:**
# MAGIC 1. Create `ethereum-blocks` topic with 8 partitions
# MAGIC 2. Ensure Unity Catalog volume contains Parquet files
# MAGIC 3. Verify access permissions to volume and DBFS paths

# COMMAND ----------

from pyspark.sql import functions as F

# === CONFIG ===
bootstrap_servers = "localhost:9092"
kafka_topic = "ethereum-blocks"  # make sure this topic exists with 8 partitions

# Auto Loader metadata/checkpoint locations (DBFS is fine)
schema_location = "dbfs:/tmp/autoloader/eth_blocks_schema"
checkpoint_location = "dbfs:/tmp/checkpoints/eth_blocks_to_kafka"

# Uncomment to reset checkpoint for a fresh start
# dbutils.fs.rm(checkpoint_location, True)

# Path to the Unity Catalog volume with Parquet blocks
# Catalog: blockchain, Schema: ethereum, Volume: ethereum
blocks_path = "/Volumes/blockchain/ethereum/ethereum/blocks/"

# === STREAMING READER FROM UC VOLUME (AUTO LOADER) ===
reader = (
    spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaHints", "number BIGINT, baseFeePerGas BIGINT")
        .load(blocks_path)
)

# Optional: peek at the stream
# display(reader)

# === PREPARE DATA FOR KAFKA (key/value) ===
# Use block `number` as key so messages for the same block go to same partition.
# Value is full row as JSON.
df_for_kafka = (
    reader
    .withColumn("key_str", F.col("number").cast("string"))  # change if your block id column is different
    .withColumn("value_str", F.to_json(F.struct(*reader.columns)))
    .select(
        F.col("key_str").cast("binary").alias("key"),
        F.col("value_str").cast("binary").alias("value"),
    )
)

# === STREAMING WRITE TO KAFKA ===
query = (
    df_for_kafka.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", kafka_topic)
        .option("checkpointLocation", checkpoint_location)
        .outputMode("append")
        .start()
)

# query.awaitTermination()  # uncomment if you want the cell to block

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Preview Ethereum Block Data Stream
# MAGIC 
# MAGIC Uses `display()` to visualize the streaming data from Unity Catalog before writing to Kafka.
# MAGIC 
# MAGIC **Purpose:**
# MAGIC - Verify Auto Loader is reading files correctly
# MAGIC - Inspect schema inference results
# MAGIC - Validate data quality before Kafka ingestion
# MAGIC - Debug schema hints and evolution
# MAGIC 
# MAGIC **Usage:** Run this cell to see a live-updating table of block data. Stop the display before running the full Kafka streaming pipeline.

# COMMAND ----------


from pyspark.sql import functions as F

# === CONFIG ===
bootstrap_servers = "localhost:9092"
kafka_topic = "ethereum-blocks"  # make sure this topic exists with 8 partitions

# Auto Loader metadata/checkpoint locations (DBFS is fine)
schema_location = "dbfs:/tmp/autoloader/eth_blocks_schema"
checkpoint_location = "dbfs:/tmp/checkpoints/eth_blocks_to_kafka"

# Uncomment to reset checkpoint for a fresh start
# dbutils.fs.rm(checkpoint_location, True)

# Path to the Unity Catalog volume with Parquet blocks
# Catalog: blockchain, Schema: ethereum, Volume: ethereum
blocks_path = "/Volumes/blockchain/ethereum/ethereum/blocks/"

# === STREAMING READER FROM UC VOLUME (AUTO LOADER) ===
reader = (
    spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaHints", "number BIGINT, baseFeePerGas BIGINT")
        .load(blocks_path)
)

# Optional: peek at the stream
display(reader)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC ## 📚 Additional Resources
# MAGIC 
# MAGIC ### Kafka Documentation
# MAGIC - [Apache Kafka 4.1.1 Documentation](https://kafka.apache.org/41/documentation.html)
# MAGIC - [KRaft Mode Overview](https://kafka.apache.org/documentation/#kraft)
# MAGIC - [Kafka Configuration Reference](https://kafka.apache.org/documentation/#configuration)
# MAGIC 
# MAGIC ### Spark Structured Streaming
# MAGIC - [Spark Kafka Integration Guide](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
# MAGIC - [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
# MAGIC 
# MAGIC ### Databricks Resources
# MAGIC - [Auto Loader Documentation](https://docs.databricks.com/ingestion/auto-loader/index.html)
# MAGIC - [Unity Catalog Volumes](https://docs.databricks.com/data-governance/unity-catalog/volumes.html)
# MAGIC - [Databricks Streaming Best Practices](https://docs.databricks.com/structured-streaming/production.html)
# MAGIC 
# MAGIC ### kcat (kafkacat)
# MAGIC - [kcat GitHub Repository](https://github.com/edenhill/kcat)
# MAGIC - [kcat Usage Examples](https://docs.confluent.io/kafka-clients/kcat/current/overview.html)
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## 🎯 Summary
# MAGIC 
# MAGIC This notebook provides a **complete Kafka 4.1.1 setup** on Databricks with three main streaming patterns:
# MAGIC 
# MAGIC 1. ✅ **Kafka → Spark:** Read and visualize Kafka topics in real-time
# MAGIC 2. ✅ **Kafka → Spark → Kafka:** Process and forward messages between topics  
# MAGIC 3. ✅ **Unity Catalog → Kafka:** Stream data lake files to Kafka for downstream processing
# MAGIC 
# MAGIC **Next Steps:**
# MAGIC - Explore Delta Lake integration with Kafka streaming
# MAGIC - Implement windowed aggregations on streaming data
# MAGIC - Set up multi-topic processing with Spark Structured Streaming
# MAGIC - Configure production-grade monitoring and alerting