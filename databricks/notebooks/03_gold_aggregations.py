# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 03 · Gold Aggregations — Business-Ready Stock Metrics
# MAGIC
# MAGIC Reads the validated silver stream and produces three purpose-built
# MAGIC aggregation tables in the **gold Delta Lake layer**.
# MAGIC
# MAGIC Gold is the reporting layer — every table here answers a specific
# MAGIC business question and is safe to query directly from dashboards or
# MAGIC notebooks without further transformation.
# MAGIC
# MAGIC | Table | Business question |
# MAGIC |---|---|
# MAGIC | `stocks_gold_daily_ohlcv` | What was each symbol's price & volume summary for a given trading day? |
# MAGIC | `stocks_gold_top_performers` | Which symbols gained / lost the most on a given day? |
# MAGIC | `stocks_gold_volume_analysis` | How does today's volume compare to recent history for each symbol? |
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Source** | `abfss://silver@stockstoragerm.dfs.core.windows.net/stocks` |
# MAGIC | **Sink**   | `abfss://gold@stockstoragerm.dfs.core.windows.net/stocks` |
# MAGIC | **Format** | Delta Lake, append + mergeSchema, partitioned by `latest_trading_day` |
# MAGIC | **Trigger** | 60 seconds |

# COMMAND ----------

# =============================================================================
# 1. ADLS Gen2 Authentication
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

SILVER_PATH     = "abfss://silver@stockstoragerm.dfs.core.windows.net/stocks"

# Three separate gold sub-paths — one per aggregation table
GOLD_ROOT       = "abfss://gold@stockstoragerm.dfs.core.windows.net/stocks"
GOLD_OHLCV      = f"{GOLD_ROOT}/daily_ohlcv"
GOLD_PERFORMERS = f"{GOLD_ROOT}/top_performers"
GOLD_VOLUME     = f"{GOLD_ROOT}/volume_analysis"

CHECKPOINT_PATH = "abfss://gold@stockstoragerm.dfs.core.windows.net/_checkpoints/gold"

print(f"Source  (silver)         : {SILVER_PATH}")
print(f"Sink    daily_ohlcv      : {GOLD_OHLCV}")
print(f"Sink    top_performers   : {GOLD_PERFORMERS}")
print(f"Sink    volume_analysis  : {GOLD_VOLUME}")

# COMMAND ----------

# =============================================================================
# 3. Connection Test
# =============================================================================

for path, label in [
    ("abfss://silver@stockstoragerm.dfs.core.windows.net/", "silver"),
    ("abfss://gold@stockstoragerm.dfs.core.windows.net/",   "gold"),
]:
    try:
        items = dbutils.fs.ls(path)
        print(f"{label} container accessible — {len(items)} item(s)")
    except Exception as exc:
        print(f"ERROR: cannot access {label} container — {exc}")
        raise

# COMMAND ----------

# =============================================================================
# 4. Read Silver Stream (Delta format)
#
# We only use rows where is_valid = True — these have all four price fields
# confirmed positive by the silver layer.  Filtering here avoids propagating
# data-quality issues into business-facing gold tables.
# =============================================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

silver_stream = (
    spark.readStream
    .format("delta")
    .option("maxFilesPerTrigger", 10)
    .load(SILVER_PATH)
    .filter(F.col("is_valid") == True)   # only use quality-verified rows
)

print("Silver Delta readStream created (is_valid = True only)")

# COMMAND ----------

# =============================================================================
# 5. foreachBatch Handler — All Three Gold Aggregations
#
# foreachBatch gives us the full DataFrame API inside each micro-batch, which
# is required for window functions (rank, lag) that are not available in pure
# streaming mode.
#
# All three aggregations are computed from the same batch DataFrame to avoid
# reading silver multiple times per trigger.
# =============================================================================

def compute_gold(batch_df, batch_id):
    """
    Called once per trigger interval with the new silver rows as batch_df.
    Computes and appends three gold aggregation tables.
    """
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: no new rows, skipping")
        return

    # Cache the batch so we can reuse it for three different aggregations
    # without re-reading from storage on each operation.
    batch_df.cache()
    input_count = batch_df.count()

    gold_loaded_at = F.lit(F.current_timestamp().alias("gold_loaded_at"))

    # =========================================================================
    # 5a. Daily OHLCV Summary
    #
    # Aggregates all quotes received for a symbol on a given trading day into
    # a single summary row.  Because the producer polls intraday and Alpha
    # Vantage returns the current-day quote, multiple rows for the same
    # symbol/day will exist in silver — we average prices and sum volumes.
    #
    # Columns:
    #   avg_open / avg_high / avg_low / avg_close  — mean of all intraday polls
    #   total_volume   — sum across all polls (use carefully; reflects poll count)
    #   avg_change_pct — mean change_percent for the day
    #   price_range    — avg_high - avg_low: intraday volatility indicator
    # =========================================================================

    daily_ohlcv = (
        batch_df
        .groupBy("symbol", "latest_trading_day", "event_date")
        .agg(
            F.round(F.avg("open"),  4).alias("avg_open"),
            F.round(F.avg("high"),  4).alias("avg_high"),
            F.round(F.avg("low"),   4).alias("avg_low"),
            F.round(F.avg("close"), 4).alias("avg_close"),
            F.sum("volume").alias("total_volume"),
            F.round(F.avg("change_percent"), 4).alias("avg_change_pct"),
            # price_range captures intraday spread — useful for volatility analysis
            F.round(F.avg("high") - F.avg("low"), 4).alias("price_range"),
            F.count("*").alias("poll_count"),   # how many times the producer polled this symbol
        )
        .withColumn("gold_loaded_at", F.current_timestamp())
    )

    (
        daily_ohlcv.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("latest_trading_day")
        .save(GOLD_OHLCV)
    )

    ohlcv_count = daily_ohlcv.count()
    print(f"Batch {batch_id}: daily_ohlcv — {ohlcv_count} rows written")

    # =========================================================================
    # 5b. Top Performers — Gainers & Losers by Day
    #
    # Ranks all symbols by change_percent for each trading day, then keeps the
    # top 10 gainers (rank ascending by change_percent descending) and top 10
    # losers (rank ascending by change_percent ascending).
    #
    # Dashboard use case: "Which stocks moved the most today?"
    #
    # Columns:
    #   gainer_rank   — 1 = biggest gainer on that day
    #   loser_rank    — 1 = biggest loser on that day
    #   direction     — "GAINER" or "LOSER" label for easy filtering
    # =========================================================================

    # Compute the average change_percent per symbol/day first (same as OHLCV)
    # so each symbol has a single score to rank.
    symbol_day_perf = (
        batch_df
        .groupBy("symbol", "latest_trading_day")
        .agg(F.round(F.avg("change_percent"), 4).alias("avg_change_pct"))
    )

    day_window      = Window.partitionBy("latest_trading_day")
    gainer_window   = day_window.orderBy(F.col("avg_change_pct").desc())
    loser_window    = day_window.orderBy(F.col("avg_change_pct").asc())

    ranked = (
        symbol_day_perf
        .withColumn("gainer_rank", F.rank().over(gainer_window))
        .withColumn("loser_rank",  F.rank().over(loser_window))
    )

    # Keep top 10 gainers and top 10 losers; label each row for easy filtering
    gainers = (
        ranked.filter(F.col("gainer_rank") <= 10)
        .withColumn("direction", F.lit("GAINER"))
        .withColumn("rank", F.col("gainer_rank"))
        .drop("gainer_rank", "loser_rank")
    )
    losers = (
        ranked.filter(F.col("loser_rank") <= 10)
        .withColumn("direction", F.lit("LOSER"))
        .withColumn("rank", F.col("loser_rank"))
        .drop("gainer_rank", "loser_rank")
    )

    top_performers = (
        gainers.unionByName(losers)
        .withColumn("gold_loaded_at", F.current_timestamp())
    )

    (
        top_performers.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("latest_trading_day")
        .save(GOLD_PERFORMERS)
    )

    print(f"Batch {batch_id}: top_performers — {top_performers.count()} rows written")

    # =========================================================================
    # 5c. Volume Analysis — Per-Symbol Daily Volume vs. Historical Average
    #
    # Computes each symbol's total volume for the current trading day and
    # compares it to the rolling average of the previous 7 days already
    # stored in the gold volume table.  This lets analysts spot unusual
    # volume spikes that often precede significant price moves.
    #
    # Columns:
    #   daily_volume       — total volume for the symbol on latest_trading_day
    #   prev_7d_avg_volume — rolling 7-day average from historical gold data
    #   volume_ratio       — daily_volume / prev_7d_avg_volume
    #                        > 1.0 means above-average volume
    #                        > 2.0 typically considered a significant spike
    #   volume_vs_avg_pct  — percentage change vs. 7-day average
    # =========================================================================

    # Step 1: summarise the current batch to one row per symbol/day
    current_volume = (
        batch_df
        .groupBy("symbol", "latest_trading_day")
        .agg(F.sum("volume").alias("daily_volume"))
    )

    # Step 2: load historical volume from the gold table (if it exists) to
    # compute the 7-day rolling average.  We use a try/except because the table
    # won't exist on the very first run.
    try:
        historical = (
            spark.read.format("delta").load(GOLD_VOLUME)
            .select("symbol", "latest_trading_day", "daily_volume")
        )

        # Join current batch with the last 7 days of history per symbol
        # to produce the rolling average.
        seven_days_ago = F.date_sub(F.current_date(), 7)

        historical_7d = (
            historical
            .filter(F.col("latest_trading_day") >= seven_days_ago)
            .groupBy("symbol")
            .agg(F.round(F.avg("daily_volume"), 0).alias("prev_7d_avg_volume"))
        )

        volume_analysis = (
            current_volume
            .join(historical_7d, on="symbol", how="left")
            .withColumn(
                "volume_ratio",
                F.when(F.col("prev_7d_avg_volume").isNotNull() & (F.col("prev_7d_avg_volume") > 0),
                       F.round(F.col("daily_volume") / F.col("prev_7d_avg_volume"), 4))
                .otherwise(F.lit(None))
            )
            .withColumn(
                "volume_vs_avg_pct",
                F.when(F.col("prev_7d_avg_volume").isNotNull() & (F.col("prev_7d_avg_volume") > 0),
                       F.round((F.col("daily_volume") - F.col("prev_7d_avg_volume"))
                               / F.col("prev_7d_avg_volume") * 100, 2))
                .otherwise(F.lit(None))
            )
        )

    except Exception:
        # First run — no historical data yet; write current volume without ratios
        volume_analysis = (
            current_volume
            .withColumn("prev_7d_avg_volume", F.lit(None).cast("double"))
            .withColumn("volume_ratio",        F.lit(None).cast("double"))
            .withColumn("volume_vs_avg_pct",   F.lit(None).cast("double"))
        )

    volume_analysis = volume_analysis.withColumn("gold_loaded_at", F.current_timestamp())

    (
        volume_analysis.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("latest_trading_day")
        .save(GOLD_VOLUME)
    )

    print(f"Batch {batch_id}: volume_analysis — {volume_analysis.count()} rows written")

    # Release the cached batch from memory
    batch_df.unpersist()
    print(f"Batch {batch_id}: complete (input={input_count} silver rows)")


print("Gold foreachBatch handler defined")

# COMMAND ----------

# =============================================================================
# 6. Start the Gold Streaming Query
#
# A single writeStream processes the silver stream and fans out into the
# three gold tables inside compute_gold().  One checkpoint covers all three
# writes — if the job restarts, all three tables pick up from the same
# silver offset.
# =============================================================================

gold_query = (
    silver_stream.writeStream
    .foreachBatch(compute_gold)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="60 seconds")
    .start()
)

print("Gold streaming query started")
print(f"  Query ID  : {gold_query.id}")
print(f"  OHLCV     : {GOLD_OHLCV}")
print(f"  Performers: {GOLD_PERFORMERS}")
print(f"  Volume    : {GOLD_VOLUME}")

# COMMAND ----------

# =============================================================================
# 7. Register Gold Tables in the Metastore (run once, idempotent)
#
# Registers all three gold paths as named tables so SQL notebooks, BI tools,
# and Databricks SQL can query them by name.
# =============================================================================

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS stocks_gold_daily_ohlcv
    USING DELTA
    LOCATION '{GOLD_OHLCV}'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS stocks_gold_top_performers
    USING DELTA
    LOCATION '{GOLD_PERFORMERS}'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS stocks_gold_volume_analysis
    USING DELTA
    LOCATION '{GOLD_VOLUME}'
""")

print("Gold tables registered in metastore:")
print("  stocks_gold_daily_ohlcv")
print("  stocks_gold_top_performers")
print("  stocks_gold_volume_analysis")

# COMMAND ----------

# =============================================================================
# 8. Stream Health Monitor (interactive use)
# =============================================================================

# import time
#
# while gold_query.isActive:
#     progress = gold_query.lastProgress
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
# 9. Spot-Check Queries (interactive use)
#
# Uncomment the relevant block to validate each gold table after data flows.
# =============================================================================

# -- Daily OHLCV
# display(
#     spark.read.format("delta").load(GOLD_OHLCV)
#     .orderBy(F.col("latest_trading_day").desc(), F.col("symbol"))
#     .limit(20)
# )

# -- Top Performers (today's gainers)
# display(
#     spark.read.format("delta").load(GOLD_PERFORMERS)
#     .filter(F.col("direction") == "GAINER")
#     .orderBy(F.col("latest_trading_day").desc(), F.col("rank"))
#     .limit(10)
# )

# -- Volume Analysis (symbols with ratio > 2 = significant spike)
# display(
#     spark.read.format("delta").load(GOLD_VOLUME)
#     .filter(F.col("volume_ratio") > 2)
#     .orderBy(F.col("latest_trading_day").desc(), F.col("volume_ratio").desc())
#     .limit(10)
# )
