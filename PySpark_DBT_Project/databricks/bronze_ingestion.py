# Databricks notebook source
# MAGIC %md
# MAGIC ### **SPARK STREAMING**

# COMMAND ----------

# entities = ['customers','drivers','locations','payments','trips','vehicles']

# COMMAND ----------

# %sql
# DROP TABLE IF EXISTS pysparkdbt.bronze.entity;

# COMMAND ----------

# for entity in entities:

#     df_batch = spark.read.format("csv")\
#                 .option("header", True)\
#                 .option("inferSchema", True)\
#                 .load(f"/Volumes/pysparkdbt/source/source_date/{entity}/")
    
#     schema_entity = df_batch.schema

#     df = spark.readStream.format("csv") \
#                 .option("header", True) \
#                 .schema(schema_entity) \
#                 .load(f"/Volumes/pysparkdbt/source/source_date/{entity}/")

#     df.writeStream.format("delta")\
#                 .outputMode("append")\
#                 .option("checkpointLocation",f"/Volumes/pysparkdbt/bronze/checkpoint/{entity}")\
#                 .trigger(once=True)\
#                 .toTable(f"pysparkdbt.bronze.{entity}")

# COMMAND ----------

# for entity in entities:
#     print(f"\nEntity: {entity}")

#     files = dbutils.fs.ls(
#         f"/Volumes/pysparkdbt/source/source_date/{entity}/"
#     )

#     for file in files:
#         print(file.name, file.size)

# COMMAND ----------

# for entity in entities:

#     df_batch = spark.read.format("csv") \
#         .option("header", True) \
#         .option("inferSchema", True) \
#         .load(f"/Volumes/pysparkdbt/source/source_date/{entity}/")

#     print(f"{entity}: {df_batch.count()} records")

# COMMAND ----------


# for entity in entities:

#     count = spark.table(
#         f"pysparkdbt.bronze.{entity}"
#     ).count()

#     print(f"{entity}: {count} records in Bronze")

# COMMAND ----------

entities = ['customers', 'drivers', 'locations',
            'payments', 'trips', 'vehicles']

for entity in entities:

    # Check existing Bronze records
    bronze_count = spark.table(
        f"pysparkdbt.bronze.{entity}"
    ).count()

    if bronze_count > 0:
        print(f"{entity}: Already loaded. Skipping.")
        continue

    # Read batch data to infer schema
    df_batch = spark.read.format("csv") \
        .option("header", True) \
        .option("inferSchema", True) \
        .load(f"/Volumes/pysparkdbt/source/source_date/{entity}/")

    schema_entity = df_batch.schema

    # Read streaming data
    df = spark.readStream.format("csv") \
        .option("header", True) \
        .schema(schema_entity) \
        .load(f"/Volumes/pysparkdbt/source/source_date/{entity}/")

    # Write to Bronze
    query = df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option(
            "checkpointLocation",
            f"/Volumes/pysparkdbt/bronze/checkpoint/retry_{entity}"
        ) \
        .trigger(availableNow=True) \
        .toTable(f"pysparkdbt.bronze.{entity}")

    # Wait until streaming completes
    query.awaitTermination()

    print(f"{entity}: Streaming completed!")

# COMMAND ----------

