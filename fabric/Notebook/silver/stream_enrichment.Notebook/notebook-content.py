# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c9aee28c-8f0d-418f-b58f-c4039c45203c",
# META       "default_lakehouse_name": "lh_netpulse_bronze",
# META       "default_lakehouse_workspace_id": "ee55eafc-ce50-459a-8e28-ce1fedc0187f",
# META       "known_lakehouses": [
# META         {
# META           "id": "c9aee28c-8f0d-418f-b58f-c4039c45203c"
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
from pyspark.sql.functions import *

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

BRONZE_CATALOG = "lh_netpulse_bronze"
SILVER_CATALOG = "lh_netpulse_silver"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

static_device_path = "Files/static/device_tac_master.csv"
df_device_tac_master = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(static_device_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_device_registry_batch = spark.read.table(f"{BRONZE_CATALOG}.inventory.device_registry")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_device_lookup_matrix = df_device_registry_batch \
    .withColumn("tac_prefix", substring(col("imei"), 1, 8)) \
    .join(df_device_tac_master, on="tac_prefix", how="inner") \
    .select(
        col("subscriber_id").alias("lookup_subscriber_id"),
        col("brand"),
        col("model"),
        col("marketing_name"),
        col("device_type")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_cdr_raw = spark.read.table(f"{BRONZE_CATALOG}.event.cdr")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_cdr_silver = df_cdr_raw \
    .withColumn("duration_seconds", col("duration_seconds").cast("int")) \
    .withColumn("data_volume_bytes", col("data_volume_bytes").cast("bigint")) \
    .withColumn("event_timestamp", to_timestamp(col("event_timestamp"))) \
    .join(df_device_lookup_matrix, col("subscriber_id") == col("lookup_subscriber_id"), how="left") \
    .drop("lookup_subscriber_id") \
    .withColumn("silver_processed_at", current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_cdr_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SILVER_CATALOG}.event.cdr")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_telemetry_raw = spark.read.table(f"{BRONZE_CATALOG}.event.telemetry")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_telemetry_silver = df_telemetry_raw \
    .withColumn("cpu_utilization_percent", col("cpu_utilization_percent").cast("double")) \
    .withColumn("active_connections_count", col("active_connections_count").cast("int")) \
    .withColumn("rsrp_signal_strength_dbm", col("rsrp_signal_strength_dbm").cast("int")) \
    .withColumn("packet_drop_rate_percent", col("packet_drop_rate_percent").cast("double")) \
    .withColumn("telemetry_timestamp", to_timestamp(col("telemetry_timestamp"))) \
    .withColumn("silver_processed_at", current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_telemetry_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SILVER_CATALOG}.event.telemetry")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"OPTIMIZE lh_netpulse_silver.event.cdr")
spark.sql(f"OPTIMIZE lh_netpulse_silver.event.telemetry")

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
