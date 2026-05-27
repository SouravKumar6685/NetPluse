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
from pyspark.sql.functions import concat_ws
from pyspark.sql.functions import col, coalesce, lit, current_timestamp, abs, hash, when

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

df_cdr_towers = spark.read.table(f"{SILVER_CATALOG}.event.cdr") \
    .select("tower_id", "sector_id", "telecom_circle") \
    .distinct()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_telemetry_towers = spark.read.table(f"{SILVER_CATALOG}.event.telemetry") \
    .select("tower_id", "sector_id") \
    .distinct()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_unified_towers = df_cdr_towers.join(
    df_telemetry_towers, 
    on=["tower_id", "sector_id"], 
    how="full"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_hashed = df_unified_towers.withColumn("tower_seed", abs(hash(col("tower_id"))))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_spatial = df_hashed.withColumn(
    "base_lat",
    when(col("telecom_circle") == "DELHI_NCR", 28.61)
    .when(col("telecom_circle") == "MUMBAI_METRO", 19.07)
    .when(col("telecom_circle") == "MAHARASHTRA", 19.75)
    .when(col("telecom_circle") == "KARNATAKA", 15.31)
    .otherwise(17.38) # AP_TELANGANA Default
).withColumn(
    "base_long",
    when(col("telecom_circle") == "DELHI_NCR", 77.20)
    .when(col("telecom_circle") == "MUMBAI_METRO", 72.87)
    .when(col("telecom_circle") == "MAHARASHTRA", 75.71)
    .when(col("telecom_circle") == "KARNATAKA", 75.71)
    .otherwise(78.48) # AP_TELANGANA Default
).withColumn(
    "latitude",
    col("base_lat") + ((col("tower_seed") % 1000) / 5000.0)
).withColumn(
    "longitude",
    col("base_long") + ((col("tower_seed") % 1337) / 5000.0)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_towers_final = df_spatial.select(
    col("tower_id"),
    col("sector_id"),
    coalesce(col("telecom_circle"), lit("UNKNOWN")).alias("telecom_circle"),
    col("latitude").cast("double"),
    col("longitude").cast("double"),
    # Vendor split logic using %
    when((col("tower_seed") % 3) == 0, "Ericsson")
    .when((col("tower_seed") % 3) == 1, "Nokia")
    .otherwise("Samsung").alias("equipment_vendor"),
    # Hardware generation split logic using %
    when((col("tower_seed") % 4) == 0, "4G Macro Cell")
    .otherwise("5G Massive MIMO").alias("hardware_generation"),
    current_timestamp().alias("gold_processed_at")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_towers_final = df_dim_towers_final.withColumn(
    "tower_sector_key", 
    concat_ws("-", col("tower_id"), col("sector_id"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_towers_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_CATALOG}.dim.dim_towers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"OPTIMIZE {GOLD_CATALOG}.dim.dim_towers")

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
