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
# META           "id": "c9aee28c-8f0d-418f-b58f-c4039c45203c"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, current_timestamp
from pyspark.sql.types import DecimalType

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

df_accounts_raw = spark.read.table(f"{BRONZE_CATALOG}.billing.accounts")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_accounts_silver = df_accounts_raw \
    .withColumn("account_balance_rupees", (col("account_balance_paisa") / 100.0).cast(DecimalType(12, 2))) \
    .drop("account_balance_paisa") \
    .withColumn("silver_processed_at", current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_accounts_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SILVER_CATALOG}.billing.accounts")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ledger_raw = spark.read.table(f"{BRONZE_CATALOG}.billing.payment_ledger")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ledger_silver = df_ledger_raw \
    .withColumn("amount_rupees", (col("amount_paisa") / 100.0).cast(DecimalType(12, 2))) \
    .drop("amount_paisa") \
    .withColumn("silver_processed_at", current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ledger_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SILVER_CATALOG}.billing.payment_ledger")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_allowance_raw = spark.read.table(f"{BRONZE_CATALOG}.billing.data_allowance_ledgers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_allowance_silver = df_allowance_raw \
    .withColumn("allocated_quota_bytes", col("allocated_quota_bytes").cast("bigint")) \
    .withColumn("consumed_quota_bytes", col("consumed_quota_bytes").cast("bigint")) \
    .withColumn("silver_processed_at", current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_allowance_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SILVER_CATALOG}.billing.data_allowance_ledgers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"OPTIMIZE {SILVER_CATALOG}.billing.accounts")
spark.sql(f"OPTIMIZE {SILVER_CATALOG}.billing.payment_ledger")
spark.sql(f"OPTIMIZE {SILVER_CATALOG}.billing.data_allowance_ledgers")

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
