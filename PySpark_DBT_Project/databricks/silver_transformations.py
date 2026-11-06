# Databricks notebook source
# MAGIC %md
# MAGIC ## **Customers**

# COMMAND ----------

import os 
import sys

sys.path.append(os.getcwd())

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Cell 2
from custom_utils import transformations

# COMMAND ----------

class transformations:

    def dedup(self, df:DataFrame,dedup_cols:List,cdc:str):
        df = df.withColumn("dedupKey",concat(*dedup_cols))
        df = df.withColumn("dedupCount",row_number()\
                .over(Window.partitionBy("dedupKey").orderBy(desc(cdc))))
        df = df.filter(col("dedupCount") == 1)
        df = df.drop("dedupKey","dedupCount")
        return df
    
    def process_timestamp(self,df):
        df = df.withColumn("process_timestamp",current_timestamp())
        return df

    # create another function to upsert
    def upsert(self,df,key_cols,table,cdc):
        merge_condition = " AND ".join([f"src.{i} = trg.{i}" for i in key_cols])
        dlt_obj = DeltaTable.forName(spark,f"pysparkdbt.silver.{table}")
        dlt_obj.alias("trg")\
            .merge(df.alias("src"), merge_condition)\
            .whenMatchedUpdateAll(condition = f"src.{cdc} >= trg.{cdc}")\
            .whenNotMatchedInsertAll()\
            .execute()

        return 1

       

# COMMAND ----------

df_cust =  spark.read.table("pysparkdbt.bronze.customers")
display(df_cust)

# COMMAND ----------

# withColumn : whenever we want to make a new column

df_cust = df_cust.withColumn("domain",split(col('email'),"@")[1])
display(df_cust)


# COMMAND ----------

df_cust = df_cust.withColumn("phone_number",regexp_replace("phone_number",r"[^0-9]",""))
display(df_cust)

# COMMAND ----------

df_cust = df_cust.withColumn("full_name",concat_ws(" ",col("first_name"),col("last_name")))
df_cust = df_cust.drop("first_name","last_name")
display(df_cust)

# COMMAND ----------

cust_obj = transformations()

cust_df_trns = cust_obj.dedup(df_cust,['customer_id'],'last_updated_timestamp')
cust_df_trns

# COMMAND ----------

display(cust_df_trns)

# COMMAND ----------

df_cust = cust_obj.process_timestamp(cust_df_trns)
display(df_cust)

# COMMAND ----------

from delta.tables import DeltaTable

if not spark.catalog.tableExists("pysparkdbt.silver.customers"):

    df_cust.write.format("delta")\
        .mode("append")\
        .saveAsTable("pysparkdbt.silver.customers")

else:

    cust_obj.upsert(df_cust,['customer_id'],'customers','last_updated_timestamp')

    


# COMMAND ----------

# MAGIC %md
# MAGIC ## DRIVERS
# MAGIC

# COMMAND ----------

df_driver = spark.read.table("pysparkdbt.bronze.drivers")
display(df_driver)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM pysparkdbt.bronze.drivers
# MAGIC

# COMMAND ----------

df_drivre = df_driver.withColumn("phone_number",regexp_replace("phone_number",r"[^0-9]",""))
display(df_driver)

# COMMAND ----------

df_driver = df_driver.withColumn("full_name",concat_ws(" ",col("first_name"),col("last_name")))
df_driver = df_driver.drop("first_name","last_name")
display(df_driver)

# COMMAND ----------

driver_obj = transformations()

# COMMAND ----------

df_driver = driver_obj.dedup(df_driver,['driver_id'],'last_updated_timestamp')


# COMMAND ----------

df_driver = driver_obj.process_timestamp(df_driver)


# COMMAND ----------

# aooly upsert
if not spark.catalog.tableExists("pysparkdbt.silver.drivers"):

    df_driver.write.format("delta")\
        .mode("append")\
        .saveAsTable("pysparkdbt.silver.drivers")

else:

    driver_obj.upsert(df_driver,['driver_id'],'drivers','last_updated_timestamp')


# COMMAND ----------

# MAGIC %md
# MAGIC ## locations

# COMMAND ----------

df_locations = spark.read.table("pysparkdbt.bronze.locations")
display(df_locations)

# COMMAND ----------

locations_obj = transformations()
df_locations = locations_obj.dedup(df_locations,['location_id'],'last_updated_timestamp')
df_locations = locations_obj.process_timestamp(df_locations)

# COMMAND ----------

if not spark.catalog.tableExists("pysparkdbt.silver.locations"):

    df_locations.write.format("delta")\
        .mode("append")\
        .saveAsTable("pysparkdbt.silver.locations")

else:

    locations_obj.upsert(df_locations,['location_id'],'locations','last_updated_timestamp')   
    

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM pysparkdbt.silver.locations

# COMMAND ----------

# MAGIC %md
# MAGIC ## payments
# MAGIC

# COMMAND ----------

df_payments = spark.read.table("pysparkdbt.bronze.payments")
display(df_payments)

# COMMAND ----------


df_payments = df_payments.withColumn(
    "online_payment_status",
    when(
        (col("payment_method") == "Card") & 
        (col("payment_status") == "Success"),
        "online-success")
    .when(
        (col("payment_method") == "Card") & 
        (col("payment_status") == "Failed"),
        "online-failed")
    .when(
        (col("payment_method") == "Card") & 
        (col("payment_status") == "Pending"),
        "online-pending")
    .otherwise("offline")
)

display(df_payments)

# COMMAND ----------

payments_obj = transformations()
df_payments = payments_obj.dedup(df_payments,['payment_id'],'last_updated_timestamp')
df_payments = payments_obj.process_timestamp(df_payments)

# COMMAND ----------

# DBTITLE 1,Cell 32
if not spark.catalog.tableExists("pysparkdbt.silver.payments"):

    df_payments.write.format("delta")\
        .mode("append")\
        .saveAsTable("pysparkdbt.silver.payments")
else:

    payments_obj.upsert(df_payments,['payment_id'],'payments','last_updated_timestamp')

   

# COMMAND ----------

# MAGIC %md
# MAGIC ## vehicles

# COMMAND ----------

df_veh = spark.read.table("pysparkdbt.bronze.vehicles")
display(df_veh)

# COMMAND ----------

# model -> upper case
df_veh = df_veh.withColumn("make",upper(col("make")))
# df_veh = df_veh.withColumn("model",upper(col("model")))

display(df_veh)

# COMMAND ----------

vehicle_obj = transformations()
df_veh = vehicle_obj.dedup(df_veh,['vehicle_id'],'last_updated_timestamp')
df_veh = vehicle_obj.process_timestamp(df_veh)

if not spark.catalog.tableExists("pysparkdbt.silver.vehicles"):

    df_veh.write.format("delta")\
        .mode("append")\
        .saveAsTable("pysparkdbt.silver.vehicles")
else:

    vehicle_obj.upsert(df_veh,['vehicle_id'],'vehicles','last_updated_timestamp')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from pysparkdbt.silver.vehicles

# COMMAND ----------

