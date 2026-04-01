# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 02 · Silver Transformation — Cleaned & Typed Stock Data
# MAGIC
# MAGIC Reads the raw bronze stream, applies type casting, validation, and
# MAGIC deduplication, then writes a clean, analyst-ready record set to the
# MAGIC **silver Delta Lake layer**.
# MAGIC
# MAGIC Silver is the first layer where data is *trusted*: every row has been
# MAGIC type-checked, validated, and deduplicated.  No aggregations are applied
# MAGIC here — those happen in the gold layer.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Source** | `abfss://bronze@stockstoragerm.dfs.core.windows.net/stocks` |
# MAGIC | **Sink**   | `abfss://silver@stockstoragerm.dfs.core.windows.net/stocks` |
# MAGIC | **Format** | Delta Lake, append, partitioned by `event_date` |
# MAGIC | **Trigger** | 60 seconds |

# COMMAND ----------

# =============================================================================
# 1. ADLS Gen2 Authentication
#
# Same pattern as the bronze notebook: fetch the storage account key from the
# Databricks secret scope and hand it to Spark's hadoop-azure connector.
# All abfss:// paths for stockstoragerm will use this credential automatically.
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
# =============================================================================

BRONZE_PATH     = "abfss://bronze@stockstoragerm.dfs.core.windows.net/stocks"
SILVER_PATH     = "abfss://silver@stockstoragerm.dfs.core.windows.net/stocks"
CHECKPOINT_PATH = "abfss://silver@stockstoragerm.dfs.core.windows.net/_checkpoints/silver"

print(f"Source (bronze) : {BRONZE_PATH}")
print(f"Sink   (silver) : {SILVER_PATH}")
print(f"Checkpoint      : {CHECKPOINT_PATH}")

# COMMAND ----------

# =============================================================================
# 3. Connection Test — Verify Both Containers Are Accessible
#
# Fail early with a clear message rather than discovering auth problems
# mid-stream when they are harder to diagnose.
# =============================================================================

for container, label in [
    ("abfss://bronze@stockstoragerm.dfs.core.windows.net/", "bronze"),
    ("abfss://silver@stockstoragerm.dfs.core.windows.net/", "silver"),
]:
    try:
        items = dbutils.fs.ls(container)
        print(f"{label} container accessible — {len(items)} item(s)")
    except Exception as exc:
        print(f"ERROR: cannot access {label} container — {exc}")
        raise

# COMMAND ----------

# =============================================================================
# 4. Read Bronze Stream (Delta format)
#
# We read bronze as a Delta streaming source so the silver notebook stays in
# sync automatically.  Delta tracks which files have already been processed via
# its transaction log, so we never reprocess or miss data across restarts.
#
# "maxFilesPerTrigger" caps how many Delta log files are processed per batch,
# preventing a single oversized batch after a long cluster outage.
# =============================================================================

bronze_stream = (
    spark.readStream
    .format("delta")
    .option("maxFilesPerTrigger", 10)
    .load(BRONZE_PATH)
)

print("Bronze Delta readStream created")

# COMMAND ----------

# =============================================================================
# 5. Type Casting
#
# Bronze stores everything with the types produced by from_json() against the
# declared schema, but some fields arrive as strings and need explicit casting
# before calculations can be performed downstream.
#
# - open / high / low / close → DoubleType  : enables arithmetic in gold layer
# - volume                    → LongType    : can exceed Int range for active stocks
# - latest_trading_day        → DateType    : enables date arithmetic & partitioning
# - ingested_at               → TimestampType : already cast in bronze, but re-cast
#                                               here for safety in case bronze schema
#                                               ever changes
# =============================================================================

from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, LongType, TimestampType

typed_stream = bronze_stream.select(
    # Symbol — keep as string, already correct type
    F.col("symbol"),

    # Price fields — cast to Double for arithmetic precision
    F.col("open").cast(DoubleType()).alias("open"),
    F.col("high").cast(DoubleType()).alias("high"),
    F.col("low").cast(DoubleType()).alias("low"),
    F.col("close").cast(DoubleType()).alias("close"),

    # Volume — cast to Long; stock volumes can exceed 2^31 for large-cap names
    F.col("volume").cast(LongType()).alias("volume"),

    # Parse "YYYY-MM-DD" string → proper DateType
    F.to_date(F.col("latest_trading_day"), "yyyy-MM-dd").alias("latest_trading_day"),

    # change_percent arrives as "X.XX%" string — strip the % and cast to Double
    # so gold aggregations can average or sum it without string manipulation
    F.regexp_replace(F.col("change_percent"), "%", "")
     .cast(DoubleType())
     .alias("change_percent"),

    # Ensure ingested_at is a proper TimestampType (was already parsed in bronze,
    # but explicit casting guards against any future schema drift)
    F.col("ingested_at").cast(TimestampType()).alias("ingested_at"),

    # Pass through all metadata columns unchanged
    F.col("kafka_partition"),
    F.col("kafka_offset"),
    F.col("kafka_timestamp"),
    F.col("bronze_loaded_at"),
    F.col("event_date"),
)

print("Type casting applied")

# COMMAND ----------

# =============================================================================
# 6. Data Quality Filters
#
# These filters enforce the minimum bar for a row to be considered usable.
# Rejected rows are silently dropped here; a dead-letter table can be added
# later if row-level rejection tracking is required.
#
# Rules applied:
#   a) symbol IS NOT NULL    — every row must be identifiable
#   b) close IS NOT NULL     — a quote with no close price is meaningless
#   c) close > 0             — zero or negative close prices are data errors;
#                              no listed equity has a zero or negative close
# =============================================================================

filtered_stream = (
    typed_stream
    # (a) Drop rows where the symbol could not be parsed from JSON
    .filter(F.col("symbol").isNotNull())
    # (b) Drop rows with a missing close price
    .filter(F.col("close").isNotNull())
    # (c) Drop rows with a zero or negative close price
    .filter(F.col("close") > 0)
)

print("Data quality filters applied")

# COMMAND ----------

# =============================================================================
# 7. Deduplication
#
# The Alpha Vantage GLOBAL_QUOTE endpoint can return the same trading-day
# quote multiple times if the producer polls faster than prices update (which
# is common outside market hours).  We deduplicate within each micro-batch on
# (symbol, latest_trading_day) keeping the row with the latest ingested_at,
# so we always keep the most recently fetched version of a quote.
#
# Note: dropDuplicates() in Structured Streaming deduplicates within the
# current micro-batch, not across all historical data.  The Delta MERGE in
# the gold layer handles cross-batch idempotency if needed.
# =============================================================================

from pyspark.sql.window import Window

def deduplicate_batch(batch_df, batch_id):
    """
    Runs inside foreachBatch so we have access to full batch-level operations
    (window functions, joins) that are not available in pure streaming mode.
    """
    if batch_df.isEmpty():
        return

    # Rank rows within each (symbol, latest_trading_day) group by ingested_at
    # descending — rank 1 is the most recently fetched quote for that symbol/day
    window = (
        Window
        .partitionBy("symbol", "latest_trading_day")
        .orderBy(F.col("ingested_at").desc())
    )

    deduped = (
        batch_df
        .withColumn("_rank", F.rank().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )

    # ── Silver audit columns ─────────────────────────────────────────────────

    # is_valid: true only when ALL four price fields are positive.
    # Marks rows that are safe for gold aggregations; rows with null/negative
    # sub-fields can still exist in silver for auditing but are flagged.
    is_valid = (
        (F.col("open")  > 0) &
        (F.col("high")  > 0) &
        (F.col("low")   > 0) &
        (F.col("close") > 0)
    )

    enriched = (
        deduped
        .withColumn("is_valid", is_valid)
        # Timestamp recording when this row was written to the silver layer
        .withColumn("silver_loaded_at", F.current_timestamp())
    )

    # Write deduplicated, enriched batch to silver Delta table
    (
        enriched.write
        .format("delta")
        .mode("append")
        .partitionBy("event_date")
        .save(SILVER_PATH)
    )

    print(
        f"Batch {batch_id}: "
        f"input={batch_df.count()}, "
        f"after dedup/filter={enriched.count()} rows written to silver"
    )

print("Deduplication + silver audit columns defined")

# COMMAND ----------

# =============================================================================
# 8. Write to Silver Delta Lake via foreachBatch
#
# foreachBatch gives us full DataFrame API inside each micro-batch, which is
# required for window-based deduplication (rank functions need a bounded
# dataset, not an unbounded stream).
#
# The checkpoint stores the bronze Delta log offsets processed so far, so
# the silver stream resumes exactly where it left off after any restart.
# =============================================================================

silver_query = (
    filtered_stream.writeStream
    .foreachBatch(deduplicate_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="60 seconds")
    .start()
)

print("Silver streaming query started")
print(f"  Query ID   : {silver_query.id}")
print(f"  Sink path  : {SILVER_PATH}")
print(f"  Checkpoint : {CHECKPOINT_PATH}")

# COMMAND ----------

# =============================================================================
# 9. Register Delta Table in the Metastore (run once)
#
# Idempotent — safe to re-run.  Lets downstream notebooks and SQL queries
# reference  stocks_silver  by name instead of the raw abfss path.
# =============================================================================

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS stocks_silver
    USING DELTA
    LOCATION '{SILVER_PATH}'
""")

print("Table 'stocks_silver' registered in metastore")

# COMMAND ----------

# =============================================================================
# 10. Stream Health Monitor (interactive use)
#
# Uncomment to tail streaming progress while the notebook is open
# interactively.  Leave commented for scheduled / always-on jobs.
# =============================================================================

# import time
#
# while silver_query.isActive:
#     progress = silver_query.lastProgress
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
# Uncomment after at least one trigger interval to verify the silver table
# looks correct.  Check that is_valid is True for all rows and that
# change_percent is a numeric Double (not a "X.XX%" string).
# =============================================================================

# display(
#     spark.read.format("delta").load(SILVER_PATH)
#     .orderBy(F.col("silver_loaded_at").desc())
#     .limit(20)
# )
