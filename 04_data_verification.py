# ============================================================
# Notebook 04: Data Verification Queries
# ============================================================

# ==================================
# CELL 1: Check all table schemas
# ==================================
print("=" * 60)
print("BRONZE SCHEMA")
print("=" * 60)
spark.table("events_bronze").printSchema()

print("=" * 60)
print("SILVER SCHEMA")
print("=" * 60)
spark.table("bikes_silver").printSchema()

print("=" * 60)
print("GOLD SCHEMA")
print("=" * 60)
spark.table("bikes_gold").printSchema()


# ==================================
# CELL 2: Row counts across layers
# ==================================
bronze = spark.sql("SELECT COUNT(*) FROM events_bronze").collect()[0][0]
silver = spark.sql("SELECT COUNT(*) FROM bikes_silver").collect()[0][0]
gold = spark.sql("SELECT COUNT(*) FROM bikes_gold").collect()[0][0]

print(f"Bronze rows (raw):         {bronze}")
print(f"Silver rows (cleaned):      {silver}")
print(f"Gold rows (aggregated):     {gold}")
print(f"Rows filtered out:          {bronze - silver}")
print(f"Neighbourhoods summarized:  {gold}")


# ==================================
# CELL 3: Sample data from each layer
# ==================================
print("BRONZE (raw from Eventstream):")
spark.sql("SELECT * FROM events_bronze LIMIT 5").show(truncate=False)

print("SILVER (cleaned + enriched):")
spark.sql("SELECT * FROM bikes_silver LIMIT 5").show(truncate=False)

print("GOLD (aggregated by neighbourhood):")
spark.sql("SELECT * FROM bikes_gold ORDER BY avg_availability_pct").show(truncate=False)


# ==================================
# CELL 4: Data quality checks
# ==================================
print("Station Status Distribution (Silver):")
spark.sql("""
    SELECT Station_Status, COUNT(*) as count
    FROM bikes_silver
    GROUP BY Station_Status
    ORDER BY count DESC
""").show()

print("Neighbourhood Summary (Gold):")
spark.sql("""
    SELECT
        Neighbourhood,
        avg_bikes_available,
        avg_availability_pct,
        total_stations
    FROM bikes_gold
    ORDER BY avg_availability_pct ASC
""").show(truncate=False)
