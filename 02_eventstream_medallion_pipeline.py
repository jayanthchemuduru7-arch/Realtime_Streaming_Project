# ============================================================
# Notebook 02: Eventstream Medallion Pipeline (Production)
# ============================================================
# Reads from Fabric Eventstream (Bicycles sample data)
# and applies Medallion Architecture transformations.
#
# Schema:
# |-- BikepointID: string
# |-- Street: string
# |-- Neighbourhood: string
# |-- Latitude: double
# |-- Longitude: double
# |-- No_Bikes: long
# |-- No_Empty_Docks: long
# ============================================================

from pyspark.sql.functions import (
    col, current_timestamp, round, when, avg, sum, count, min, max
)

# ==================================
# STEP 1: Read Bronze Layer
# ==================================
bronze_stream = (
    spark.readStream
    .format("delta")
    .table("events_bronze")
)

bronze_stream.printSchema()
print("Bronze stream connected!")

# ==================================
# STEP 2: SILVER LAYER (Clean + Enrich)
# ==================================
silver_df = (
    bronze_stream
    .filter(col("BikepointID").isNotNull())
    .filter(col("Latitude").isNotNull())
    .filter(col("No_Bikes") >= 0)
    .filter(col("No_Empty_Docks") >= 0)
    .withColumn("Total_Docks", col("No_Bikes") + col("No_Empty_Docks"))
    .withColumn("Availability_Pct",
        round(
            when(col("No_Bikes") + col("No_Empty_Docks") > 0,
                (col("No_Bikes") / (col("No_Bikes") + col("No_Empty_Docks"))) * 100
            ).otherwise(0), 2
        )
    )
    .withColumn("Station_Status",
        when(col("No_Bikes") == 0, "EMPTY")
        .when(col("No_Empty_Docks") == 0, "FULL")
        .when(col("Availability_Pct") < 20, "LOW")
        .otherwise("NORMAL")
    )
    .withColumn("processed_time", current_timestamp())
    .select(
        col("BikepointID"),
        col("Street"),
        col("Neighbourhood"),
        col("Latitude"),
        col("Longitude"),
        col("No_Bikes"),
        col("No_Empty_Docks"),
        col("Total_Docks"),
        col("Availability_Pct"),
        col("Station_Status"),
        col("processed_time")
    )
)

silver_query = (
    silver_df.writeStream
    .format("delta")
    .option("checkpointLocation", "Files/checkpoints/bikes_silver")
    .outputMode("append")
    .toTable("bikes_silver")
)
print("Silver stream started!")

# ==================================
# STEP 3: GOLD LAYER (Aggregated)
# ==================================
gold_input = (
    spark.readStream
    .format("delta")
    .table("bikes_silver")
)

gold_df = (
    gold_input
    .groupBy("Neighbourhood")
    .agg(
        round(avg("No_Bikes"), 2).alias("avg_bikes_available"),
        round(avg("No_Empty_Docks"), 2).alias("avg_empty_docks"),
        round(avg("Availability_Pct"), 2).alias("avg_availability_pct"),
        sum("Total_Docks").alias("total_capacity"),
        count("*").alias("total_stations"),
        min("No_Bikes").alias("min_bikes"),
        max("No_Bikes").alias("max_bikes")
    )
)

gold_query = (
    gold_df.writeStream
    .format("delta")
    .option("checkpointLocation", "Files/checkpoints/bikes_gold")
    .outputMode("complete")
    .toTable("bikes_gold")
)
print("Gold stream started!")

# ==================================
# STEP 4: Monitor Streams
# ==================================
print(f"Silver active: {silver_query.isActive}")
print(f"Gold active:   {gold_query.isActive}")

# ==================================
# STEP 5: Stop All Streams (run when done)
# ==================================
# silver_query.stop()
# gold_query.stop()
# print("All streams stopped!")
