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
from pyspark.sql.functions import col, sum, avg, count, when, coalesce, lit, current_timestamp, round

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

df_cdr_silver = spark.read.table(f"{SILVER_CATALOG}.event.cdr")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_subscriber_network = df_cdr_silver \
    .groupBy("subscriber_id") \
    .agg(
        count(when(col("session_type") == "VOICE", 1)).alias("total_voice_calls"),
        count(when((col("session_type") == "VOICE") & (col("reason_code") != "NORMAL"), 1)).alias("dropped_voice_calls"),
        count(when(col("session_type") == "DATA", 1)).alias("total_data_sessions"),
        sum(coalesce(col("data_volume_bytes"), lit(0))).alias("total_data_bytes"),
        sum(coalesce(col("duration_seconds"), lit(0))).alias("total_voice_seconds")
    ) \
    .withColumn(
        "call_drop_rate_percent",
        round(
            when(col("total_voice_calls") > 0, (col("dropped_voice_calls") / col("total_voice_calls")) * 100.0)
            .otherwise(0.0), 
            2
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ledger_silver = spark.read.table(f"{SILVER_CATALOG}.billing.payment_ledger")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_subscriber_payments = df_ledger_silver \
    .groupBy("account_id") \
    .agg(
        count(col("transaction_id")).alias("total_payment_attempts"),
        sum(col("amount_rupees")).alias("total_amount_paid_rupees"),
        avg(col("amount_rupees")).alias("avg_transaction_value_rupees")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_accounts_silver = spark.read.table(f"{SILVER_CATALOG}.billing.accounts")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_billing_profile = df_accounts_silver \
    .join(df_subscriber_payments, on="account_id", how="left")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_churn_base = df_billing_profile \
    .join(df_subscriber_network, on="subscriber_id", how="left")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_churn_signals_final = df_churn_base.select(
    col("subscriber_id"),
    col("account_id"),
    coalesce(col("account_balance_rupees"), lit(0.0)).alias("account_balance_rupees"),
    coalesce(col("total_payment_attempts"), lit(0)).alias("total_payment_attempts"),
    round(coalesce(col("total_amount_paid_rupees"), lit(0.0)), 2).alias("total_amount_paid_rupees"),
    coalesce(col("total_voice_calls"), lit(0)).alias("total_voice_calls"),
    coalesce(col("dropped_voice_calls"), lit(0)).alias("dropped_voice_calls"),
    coalesce(col("call_drop_rate_percent"), lit(0.0)).alias("call_drop_rate_percent"),
    coalesce(col("total_data_sessions"), lit(0)).alias("total_data_sessions"),
    coalesce(col("total_data_bytes"), lit(0)).alias("total_data_bytes"),
    # Flag 1: Financial risk (Outstanding debt balance)
    when(col("account_balance_rupees") < -100.0, 1).otherwise(0).alias("flag_financial_deficit"),
    # Flag 2: Poor network experience (Call drop rate exceeds acceptable threshold)
    when(col("call_drop_rate_percent") >= 5.0, 1).otherwise(0).alias("flag_severe_call_drops"),
    # Flag 3: Low engagement anomaly (Active subscriber dropping data usage completely)
    when((col("total_voice_calls") == 0) & (col("total_data_sessions") == 0), 1).otherwise(0).alias("flag_zero_utilization")
).withColumn("gold_processed_at", current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_churn_signals_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_CATALOG}.fact.fact_subscriber_churn_signals")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"OPTIMIZE {GOLD_CATALOG}.fact.fact_subscriber_churn_signals")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
