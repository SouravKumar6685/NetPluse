# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "80dcf58c-5264-4461-8fd6-8abb278d8e85",
# META       "default_lakehouse_name": "lh_netpulse_gold",
# META       "default_lakehouse_workspace_id": "ee55eafc-ce50-459a-8e28-ce1fedc0187f",
# META       "known_lakehouses": [
# META         {
# META           "id": "80dcf58c-5264-4461-8fd6-8abb278d8e85"
# META         },
# META         {
# META           "id": "cec1bc9b-9520-409f-8953-d3b0ce82ba40"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    explode, sequence, to_timestamp, col, year, month, date_format, 
    dayofweek, dayofmonth, hour, quarter, when, lit, current_timestamp
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.parquet.vorder.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.microsoft.delta.autoCompact.enabled", "true")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

GOLD_CATALOG = "lh_netpulse_gold"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

start_time = "2024-01-01 00:00:00"
end_time = "2027-12-31 23:00:00"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_sequence = spark.sql(f"""
    SELECT sequence(
        to_timestamp('{start_time}'), 
        to_timestamp('{end_time}'), 
        interval 1 hour
    ) as hour_array
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_hourly_base = df_sequence.select(explode(col("hour_array")).alias("date_time_key"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_date_time_final = df_hourly_base.select(
    # Core unique primary key tracking down to the exact hour
    col("date_time_key"),
    # Truncated date reference for standard daily rollups
    col("date_time_key").cast("date").alias("calendar_date"),
    # Numeric components for strict sorting loops
    year(col("date_time_key")).alias("calendar_year"),
    month(col("date_time_key")).alias("calendar_month_number"),
    dayofmonth(col("date_time_key")).alias("calendar_day_number"),
    hour(col("date_time_key")).alias("calendar_hour_24"),
    quarter(col("date_time_key")).alias("calendar_quarter_number"),
    # Textual attributes for clean axis charting labels
    date_format(col("date_time_key"), "MMMM").alias("month_name"),
    date_format(col("date_time_key"), "MMM").alias("month_short_name"),
    date_format(col("date_time_key"), "EEEE").alias("day_name"),
    date_format(col("date_time_key"), "E").alias("day_short_name"),
    date_format(col("date_time_key"), "yyyy-MM").alias("year_month_sort"),
    # Formatted quarter expressions (e.g., Q1, Q2)
    date_format(col("date_time_key"), "'Q'q").alias("calendar_quarter_name"),
    # Operational flags to easily isolate weekend traffic spikes
    when(dayofweek(col("date_time_key")).isin(1, 7), 1).otherwise(0).alias("is_weekend"),
    current_timestamp().alias("gold_processed_at")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_date_time_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_CATALOG}.dim.dim_date_time")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"OPTIMIZE {GOLD_CATALOG}.dim.dim_date_time")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
