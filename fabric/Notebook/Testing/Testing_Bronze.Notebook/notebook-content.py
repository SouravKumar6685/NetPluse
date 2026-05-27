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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select 'billing.accounts' as table_name,count(*) as total_row from billing.accounts
# MAGIC union all
# MAGIC select 'billing.data_allowance_ledgers',count(*) from billing.data_allowance_ledgers
# MAGIC union all
# MAGIC SELECT 'billing.payment_ledger',count(*) from billing.payment_ledger
# MAGIC union all
# MAGIC select 'crm.subscriber_kyc', count(*) from crm.subscriber_kyc
# MAGIC union all
# MAGIC select 'crm.subscribers',count(*) from crm.subscribers
# MAGIC union all
# MAGIC select 'inventory.device_registry', count(*) from inventory.device_registry
# MAGIC union all
# MAGIC select 'inventory.sim_cards', count(*) from inventory.sim_cards
# MAGIC union all
# MAGIC select 'event.cdr' ,count(*) from event.cdr
# MAGIC union all
# MAGIC select 'event.telementry', count(*) from event.telementry;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
