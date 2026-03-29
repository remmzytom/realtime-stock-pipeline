# Databricks notebook source

# COMMAND ----------

# Databricks injects `dbutils` and `spark` as notebook-scoped globals at runtime.
# The annotation below declares them to static-analysis tools (Pylance / pyright)
# so they stop reporting "not defined" warnings — this has no effect at runtime.
from typing import Any

dbutils: Any  # noqa: F841

# COMMAND ----------
# MAGIC %md
# MAGIC # 01 · Bronze Ingestion — Raw Stock Data
# MAGIC
# MAGIC Reads live stock quotes from **Azure Event Hubs** (Kafka protocol) and
# MAGIC writes every raw message to the **bronze Delta Lake layer** in ADLS Gen2.
# MAGIC
# MAGIC No transformations are applied — bronze is a faithful, append-only record
# MAGIC of every message that arrived on the wire, enriched only with Kafka lineage
# MAGIC metadata so any row can be traced back to its exact source partition/offset.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Source** | Event Hub `stocks-raw` (`eventhub-stock-pipeline.servicebus.windows.net:9093`) |
# MAGIC | **Sink** | `abfss://bronze@stockstoragerm.dfs.core.windows.net/stocks` |
# MAGIC | **Format** | Delta Lake, append, partitioned by `event_date` |
# MAGIC | **Trigger** | 60 seconds |

# COMMAND ----------

# =============================================================================
# 1. ADLS Gen2 Authentication
#
# Pull the storage account key from the Databricks secret scope and hand it
# to Spark via the hadoop-azure configuration key.  All abfss:// paths for
# stockstoragerm will use this credential automatically.
# =============================================================================

STORAGE_ACCOUNT = "stockstoragerm"
SECRET_SCOPE    = "stock-pipeline"

storage_key = dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-account-key")

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    storage_key,
)

print(f"ADLS Gen2 authenticated — account: {STORAGE_ACCOUNT}")

# COMMAND ----------

# =============================================================================
# 2. Path Constants
#
# Define every storage path in one place so they are easy to update and
# consistent across all cells in this notebook.
# =============================================================================

BRONZE_PATH     = "abfss://bronze@stockstoragerm.dfs.core.windows.net/stocks"
CHECKPOINT_PATH = "abfss://bronze@stockstoragerm.dfs.core.windows.net/_checkpoints/bronze"

print(f"Bronze path : {BRONZE_PATH}")
print(f"Checkpoint  : {CHECKPOINT_PATH}")

# COMMAND ----------

# =============================================================================
# 3. Connection Test — List the Bronze Container
#
# Verify that the credential is valid and the container exists before
# starting the stream.  Failing here is much easier to debug than a
# cryptic write error mid-stream.
# =============================================================================

try:
    items = dbutils.fs.ls("abfss://bronze@stockstoragerm.dfs.core.windows.net/")
    print(f"Bronze container accessible — {len(items)} item(s) found:")
    for item in items:
        print(f"  {item.path}")
except Exception as exc:
    print(f"ERROR: cannot access bronze container — {exc}")
    raise

# COMMAND ----------

# =============================================================================
# 4. Event Hubs / Kafka Configuration
#
# Azure Event Hubs exposes a Kafka-compatible endpoint on port 9093.
# Authentication uses SASL/PLAIN over TLS:
#   - username  : the literal string "$ConnectionString"
#   - password  : the full Event Hubs connection string
#
# The connection string is stored in the Databricks secret scope so it is
# never visible in notebook source or logs.
# =============================================================================

eventhub_connection_string = dbutils.secrets.get(
    scope=SECRET_SCOPE,
    key="eventhub-connection-string",
)

EVENTHUB_NAMESPACE = "eventhub-stock-pipeline.servicebus.windows.net:9093"
KAFKA_TOPIC        = "stocks-raw"

# JAAS config string required by the Kafka client for SASL/PLAIN auth
SASL_JAAS_CONFIG = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
    'username="$ConnectionString" '
    f'password="{eventhub_connection_string}";'
)

kafka_options = {
    # Broker endpoint
    "kafka.bootstrap.servers":                        EVENTHUB_NAMESPACE,
    # TLS + SASL
    "kafka.security.protocol":                        "SASL_SSL",
    "kafka.sasl.mechanism":                           "PLAIN",
    "kafka.sasl.jaas.config":                         SASL_JAAS_CONFIG,
    # Tune timeouts for Event Hubs latency
    "kafka.session.timeout.ms":                       "60000",
    "kafka.request.timeout.ms":                       "60000",
    # Topic & offset behaviour
    "subscribe":                                      KAFKA_TOPIC,
    "startingOffsets":                                "earliest",   # replay from the beginning on first run
    "maxOffsetsPerTrigger":                           "10000",      # cap batch size per trigger interval
    "failOnDataLoss":                                 "false",      # tolerate offset gaps (e.g. after retention expiry)
}

print(f"Event Hubs configured — namespace: {EVENTHUB_NAMESPACE}, topic: {KAFKA_TOPIC}")

# COMMAND ----------

# =============================================================================
# 5. Stock Message Schema
#
# Declaring the schema explicitly is faster than schema inference (no extra
# Spark job) and ensures columns are typed correctly even when a field is
# null.  The shape must match the JSON produced by producer/producer.py.
# =============================================================================

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

STOCK_SCHEMA = StructType([
    StructField("symbol",             StringType(), nullable=False),
    StructField("open",               DoubleType(), nullable=True),
    StructField("high",               DoubleType(), nullable=True),
    StructField("low",                DoubleType(), nullable=True),
    StructField("close",              DoubleType(), nullable=True),
    StructField("volume",             LongType(),   nullable=True),
    StructField("latest_trading_day", StringType(), nullable=True),
    StructField("change_percent",     StringType(), nullable=True),
    StructField("ingested_at",        StringType(), nullable=True),  # ISO-8601 string from producer
])

print("Stock schema defined")

# COMMAND ----------

# =============================================================================
# 6. Read Raw Stream from Event Hubs
#
# Each Kafka message arrives with binary key and value columns plus envelope
# metadata (topic, partition, offset, timestamp).  We load everything and
# parse the value in the next step.
# =============================================================================

from pyspark.sql import functions as F

raw_stream = (
    spark.readStream
    .format("kafka")
    .options(**kafka_options)
    .load()
)

print("Kafka readStream created")

# COMMAND ----------

# =============================================================================
# 7. Parse JSON Payload and Add Bronze Metadata Columns
#
# Bronze = raw payload + provenance.  We:
#   1. Decode the binary value column as UTF-8 JSON.
#   2. Parse the JSON using the declared schema (bad rows → null symbol).
#   3. Flatten the struct fields into top-level columns.
#   4. Add lineage columns (kafka_partition, kafka_offset) so any row can be
#      traced back to the exact message on the Event Hub.
#   5. Add audit columns (bronze_loaded_at, event_date) for downstream use.
# =============================================================================

bronze_stream = (
    raw_stream
    .select(
        F.from_json(
            F.col("value").cast(StringType()),
            STOCK_SCHEMA,
        ).alias("payload"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    )
    .select(
        # ── Payload fields ──────────────────────────────────────────────────
        F.col("payload.symbol"),
        F.col("payload.open"),
        F.col("payload.high"),
        F.col("payload.low"),
        F.col("payload.close"),
        F.col("payload.volume"),
        F.col("payload.latest_trading_day"),
        F.col("payload.change_percent"),
        # Convert the ISO-8601 string from the producer into a proper timestamp
        F.to_timestamp(F.col("payload.ingested_at")).alias("ingested_at"),
        # ── Kafka lineage ────────────────────────────────────────────────────
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.col("kafka_timestamp"),
        # ── Bronze audit ─────────────────────────────────────────────────────
        # When Spark wrote this row to Delta (useful for late-arrival debugging)
        F.current_timestamp().alias("bronze_loaded_at"),
        # Partition column — date the message arrived on the Event Hub
        F.to_date(F.col("kafka_timestamp")).alias("event_date"),
    )
    # Drop rows where JSON parsing failed (symbol will be null)
    .filter(F.col("symbol").isNotNull())
)

print("Bronze transformation defined")

# COMMAND ----------

# =============================================================================
# 8. Write to Bronze Delta Lake
#
# - outputMode("append")  : bronze is immutable; we never update or delete rows
# - partitionBy("event_date") : keeps daily file sizes manageable and lets the
#                               Silver notebook prune with WHERE event_date = …
# - trigger(processingTime) : micro-batch every 60 seconds; balance between
#                              latency and overhead (tune for your SLA)
# - checkpointLocation      : stores Kafka offsets + Delta log metadata so the
#                              stream can resume exactly where it left off after
#                              a cluster restart
# =============================================================================

bronze_query = (
    bronze_stream.writeStream
    .format("delta")
    .outputMode("append")
    .partitionBy("event_date")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="60 seconds")
    .start(BRONZE_PATH)
)

print("Streaming query started")
print(f"  Query ID   : {bronze_query.id}")
print(f"  Sink path  : {BRONZE_PATH}")
print(f"  Checkpoint : {CHECKPOINT_PATH}")

# COMMAND ----------

# =============================================================================
# 9. Register Delta Table in the Metastore (run once)
#
# Creates a permanent name for the Delta table so downstream notebooks can
# query it with  SELECT * FROM stocks_bronze  instead of the raw abfss path.
# Using CREATE TABLE IF NOT EXISTS makes this cell idempotent — safe to re-run.
# =============================================================================

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS stocks_bronze
    USING DELTA
    LOCATION '{BRONZE_PATH}'
""")

print("Table 'stocks_bronze' registered in metastore")

# COMMAND ----------

# =============================================================================
# 10. Stream Health Monitor (interactive use)
#
# Uncomment to print progress metrics every 30 seconds while the notebook
# is running interactively.  Remove or leave commented for scheduled jobs —
# the query will keep running until the cluster is terminated.
# =============================================================================

# import time
#
# while bronze_query.isActive:
#     progress = bronze_query.lastProgress
#     if progress:
#         print(
#             f"[{progress['timestamp']}] "
#             f"batch={progress['batchId']}  "
#             f"input_rows={progress['numInputRows']}  "
#             f"rows/s={progress['processedRowsPerSecond']:.1f}"
#         )
#     time.sleep(30)

# COMMAND ----------

# =============================================================================
# 11. Spot-Check Query (interactive use)
#
# Uncomment after data has been flowing for at least one trigger interval to
# verify that rows are landing correctly in the bronze table.
# =============================================================================

# display(
#     spark.read.format("delta").load(BRONZE_PATH)
#     .orderBy(F.col("bronze_loaded_at").desc())
#     .limit(20)
# )
