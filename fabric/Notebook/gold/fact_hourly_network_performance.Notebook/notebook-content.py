# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "cec1bc9b-9520-409f-8953-d3b0ce82ba40",
# META       "default_lakehouse_name": "lh_netpulse_silver",
# META       "default_lakehouse_workspace_id": "ee55eafc-ce50-459a-8e28-ce1fedc0187f",
# META       "known_lakehouses": [
# META         {
# META           "id": "cec1bc9b-9520-409f-8953-d3b0ce82ba40"
# META         },
# META         {
# META           "id": "80dcf58c-5264-4461-8fd6-8abb278d8e85"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws
from pyspark.sql.functions import col, date_trunc, sum, avg, max, count, when, coalesce, lit, current_timestamp, round

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

SILVER_CATALOG = "lh_netpulse_silver"
GOLD_CATALOG = "lh_netpulse_gold"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_cdr_hourly = spark.read.table(f"{SILVER_CATALOG}.event.cdr") \
    .withColumn("hour", date_trunc("hour", col("event_timestamp"))) \
    .groupBy("hour", "tower_id", "sector_id", "telecom_circle") \
    .agg(
        count(when(col("session_type") == "VOICE", 1)).alias("total_voice_calls"),
        count(when((col("session_type") == "VOICE") & (col("reason_code") != "NORMAL"), 1)).alias("dropped_voice_calls"),
        count(when(col("session_type") == "DATA", 1)).alias("total_data_sessions"),
        sum(coalesce(col("data_volume_bytes"), lit(0))).alias("total_data_volume_bytes"),
        sum(coalesce(col("duration_seconds"), lit(0))).alias("total_voice_duration_seconds")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_telemetry_hourly = spark.read.table(f"{SILVER_CATALOG}.event.telemetry") \
    .withColumn("hour", date_trunc("hour", col("telemetry_timestamp"))) \
    .groupBy("hour", "tower_id", "sector_id") \
    .agg(
        avg(col("cpu_utilization_percent")).alias("avg_cpu_utilization_percent"),
        max(col("active_connections_count")).alias("peak_active_connections_count"),
        avg(col("rsrp_signal_strength_dbm")).alias("avg_rsrp_signal_strength_dbm"),
        avg(col("packet_drop_rate_percent")).alias("avg_packet_drop_rate_percent")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold_fact = df_cdr_hourly.join(df_telemetry_hourly, on=["hour", "tower_id", "sector_id"], how="full")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold_fact_final = df_gold_fact.select(
    col("hour"), col("tower_id"), col("sector_id"),
    coalesce(col("telecom_circle"), lit("UNKNOWN")).alias("telecom_circle"),
    coalesce(col("total_voice_calls"), lit(0)).alias("total_voice_calls"),
    coalesce(col("dropped_voice_calls"), lit(0)).alias("dropped_voice_calls"),
    coalesce(col("total_data_sessions"), lit(0)).alias("total_data_sessions"),
    coalesce(col("total_data_volume_bytes"), lit(0)).alias("total_data_volume_bytes"),
    coalesce(col("total_voice_duration_seconds"), lit(0)).alias("total_voice_duration_seconds"),
    round(coalesce(col("avg_cpu_utilization_percent"), lit(0.0)), 2).alias("avg_cpu_utilization_percent"),
    coalesce(col("peak_active_connections_count"), lit(0)).alias("peak_active_connections_count"),
    round(coalesce(col("avg_rsrp_signal_strength_dbm"), lit(0.0)), 1).alias("avg_rsrp_signal_strength_dbm"),
    round(coalesce(col("avg_packet_drop_rate_percent"), lit(0.0)), 3).alias("avg_packet_drop_rate_percent")
).withColumn(
    "voice_call_drop_rate_percent",
    round(when(col("total_voice_calls") > 0, (col("dropped_voice_calls") / col("total_voice_calls")) * 100.0).otherwise(0.0), 2)
).withColumn("gold_processed_at", current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold_fact_final = df_gold_fact_final.withColumn(
    "tower_sector_key", 
    concat_ws("-", col("tower_id"), col("sector_id"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold_fact_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD_CATALOG}.fact.fact_hourly_network_performance")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"OPTIMIZE {GOLD_CATALOG}.fact.fact_hourly_network_performance")

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
