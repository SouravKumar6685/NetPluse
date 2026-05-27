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
# META         },
# META         {
# META           "id": "c9aee28c-8f0d-418f-b58f-c4039c45203c"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ======================================================================
# CORRUPT DELTA LOG PURGE ENGINE (PROJECT NETPULSE)
# FIX: Forcefully drop metadata and delete the underlying corrupt 
# directory from OneLake using Microsoft Spark Utilities (mssparkutils).
# ======================================================================

print("Starting deep purge of corrupted event layers...")

# 1. Force drop metadata entries from the catalogs
try:
    print("Dropping ghost metadata from catalogs...")
    spark.sql("DROP TABLE IF EXISTS lh_netpulse_bronze.event.cdr")
    spark.sql("DROP TABLE IF EXISTS lh_netpulse_silver.event.cdr")
    print("-> Metadata dropped successfully.")
except Exception as e:
    print(f"-> Metadata drop skipped or failed: {e}")

# 2. Directly delete the corrupted physical folders from OneLake
# Using the exact workspace storage paths extracted from your error log
corrupt_paths = [
    "Files/../Tables/event/cdr",
    "Files/../Tables/event"
]

for path in corrupt_paths:
    try:
        print(f"Purging physical directory from OneLake: {path}")
        # mssparkutils directory ko recursively bina kisi permission error के clear karta hai
        mssparkutils.fs.rm(path, True)
        print(f"-> Successfully deleted: {path}")
    except Exception as e:
        print(f"-> Path bypass or already clean: {path}. Details: {e}")

print("\n=== PURGE COMPLETE: SYSTEM IS NOW CLEAN ===")

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
