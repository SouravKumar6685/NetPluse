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
from pyspark.sql.functions import col, concat, lit, coalesce, current_timestamp

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

df_crm_silver = spark.read.table(f"{SILVER_CATALOG}.crm.crm_subscribers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_billing_silver = spark.read.table(f"{SILVER_CATALOG}.billing.accounts")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_subscribers_prep = df_crm_silver.join(
    df_billing_silver, 
    on="subscriber_id", 
    how="left"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_subscribers_final = df_dim_subscribers_prep.select(
    col("subscriber_id"),
    coalesce(col("account_id"), lit("UNASSIGNED")).alias("account_id"),
    # Concatenate name pieces into a clean single reporting vector
    concat(col("first_name"), lit(" "), col("last_name")).alias("full_name"),
    # Carry forward the structural substring masks built in the Silver layer
    col("phone_number").alias("masked_phone_number"),
    col("email").alias("masked_email"),
    coalesce(col("telecom_circle"), lit("UNKNOWN")).alias("telecom_circle"),
    # FIXED: Replaced non-existent account_status_tier with billing_type from the trace
    coalesce(col("billing_type"), lit("STANDARD")).alias("profile_tier"),
    current_timestamp().alias("gold_processed_at")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_subscribers_final = df_dim_subscribers_prep.select(
    col("subscriber_id"),
    coalesce(col("account_id"), lit("UNASSIGNED")).alias("account_id"),
    # Concatenate name pieces into a clean single reporting vector
    concat(col("first_name"), lit(" "), col("last_name")).alias("full_name"),
    # Carry forward the structural substring masks built in the Silver layer
    col("phone_number").alias("masked_phone_number"),
    col("email").alias("masked_email"),
    coalesce(col("telecom_circle"), lit("UNKNOWN")).alias("telecom_circle"),
    # FIXED: Replaced non-existent account_status_tier with billing_type from the trace
    coalesce(col("billing_type"), lit("STANDARD")).alias("profile_tier"),
    current_timestamp().alias("gold_processed_at")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_subscribers_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_CATALOG}.dim.dim_subscribers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"OPTIMIZE {GOLD_CATALOG}.dim.dim_subscribers")

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
