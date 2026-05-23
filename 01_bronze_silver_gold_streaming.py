# ============================================================
# Notebook 01: Rate-Based Streaming Pipeline (Learning Version)
# ============================================================
# Architecture: Rate Source -> Bronze -> Silver -> Gold
# ============================================================

from pyspark.sql.functions import expr, col, current_timestamp, when, avg, count, window, round
from pyspark.sql.types import *

# ==================================
# STEP 1: Generate Streaming Data
# ==================================
raw_df = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 5)
    .load()
)

# ==================================
# STEP 2: BRONZE LAYER (Raw + Metadata)
# ==================================
bronze_df = (
    raw_df
    .withColumn("temperature", expr("CAST(rand() * 45 AS INT)"))
    .withColumn("humidity", expr("CAST(rand() * 100 AS INT)"))
    .withColumn("city", expr(
        "CASE WHEN value % 3 = 0 THEN 'Bengaluru' "
        "WHEN value % 3 = 1 THEN 'Mumbai' "
        "ELSE 'Delhi' END"
    ))
    .withColumn("ingestion_time", current_timestamp())
)

bronze_query = (
    bronze_df.writeStream
    .format("delta")
    .option("checkpointLocation", "Files/checkpoints/bronze_v2")
    .outputMode("append")
    .toTable("weather_bronze")
)
print("Bronze stream started!")

# ==================================
# STEP 3: SILVER LAYER (Cleaned)
# ==================================
silver_input = (
    spark.readStream
    .format("delta")
    .table("weather_bronze")
)

silver_df = (
    silver_input
    .filter(col("temperature").between(0, 50))
    .filter(col("humidity").between(0, 100))
    .select(
        col("timestamp").alias("event_time"),
        col("city"),
        col("temperature"),
        col("humidity"),
        col("ingestion_time"),
        current_timestamp().alias("processed_time")
    )
)

silver_query = (
    silver_df.writeStream
    .format("delta")
    .option("checkpointLocation", "Files/checkpoints/silver_v2")
    .outputMode("append")
    .toTable("weather_silver")
)
print("Silver stream started!")

# ==================================
# STEP 4: GOLD LAYER (Aggregated)
# ==================================
gold_input = (
    spark.readStream
    .format("delta")
    .table("weather_silver")
)

gold_df = (
    gold_input
    .withWatermark("event_time", "2 minutes")
    .groupBy(
        window("event_time", "1 minute"),
        "city"
    )
    .agg(
        round(avg("temperature"), 2).alias("avg_temperature"),
        round(avg("humidity"), 2).alias("avg_humidity"),
        count("*").alias("total_readings")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("city"),
        col("avg_temperature"),
        col("avg_humidity"),
        col("total_readings")
    )
)

gold_query = (
    gold_df.writeStream
    .format("delta")
    .option("checkpointLocation", "Files/checkpoints/gold_v2")
    .outputMode("complete")
    .toTable("weather_gold")
)
print("Gold stream started!")

# ==================================
# STEP 5: Monitor Streams
# ==================================
print(f"Bronze active: {bronze_query.isActive}")
print(f"Silver active: {silver_query.isActive}")
print(f"Gold active:   {gold_query.isActive}")

# ==================================
# STEP 6: Stop All Streams (run when done)
# ==================================
# bronze_query.stop()
# silver_query.stop()
# gold_query.stop()
# print("All streams stopped!")
