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
from pyspark.sql.functions import col, concat, lit, sha2, current_timestamp, substring

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

SECURE_SALT = "NETPULSE_PROD_CORE_SALT_2026_MX#"

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

df_subscribers_raw = spark.read.table(f"{BRONZE_CATALOG}.crm.subscribers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_subscribers_raw)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_subscribers_silver = df_subscribers_raw.withColumn(
    "phone_number", 
    concat(substring(col("phone_number"), 1, 3), lit("xxxxxx"), substring(col("phone_number"), -3, 3))
).withColumn(
    "email", 
    concat(substring(col("email"), 1, 3), lit("xxxxxx"), substring(col("email"), -10, 10))
).withColumn(
    "silver_processed_at", 
    current_timestamp()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_subscribers_silver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_subscribers_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SILVER_CATALOG}.crm.crm_subscribers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_kyc_raw = spark.read.table(f"{BRONZE_CATALOG}.crm.subscriber_kyc")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_kyc_silver = df_kyc_raw.withColumn(
    "aadhaar_number", 
    concat(substring(col("aadhaar_number"), 1, 3), lit("xxxxxx"), substring(col("aadhaar_number"), -3, 3))
).withColumn(
    "pan_card", 
    concat(substring(col("pan_card"), 1, 3), lit("xxxxxx"), substring(col("pan_card"), -3, 3))
).withColumn(
    "silver_processed_at", 
    current_timestamp()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_kyc_silver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_kyc_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SILVER_CATALOG}.crm.subscriber_kyc")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"OPTIMIZE lh_netpulse_bronze.crm.subscribers")
spark.sql(f"OPTIMIZE lh_netpulse_silver.crm.subscriber_kyc")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
