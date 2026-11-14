# Databricks notebook source
# MAGIC %md
# MAGIC ###INCREMENTAL DATA INGESTION

# COMMAND ----------

dbutils.widgets.text("src", "")

# COMMAND ----------

src_value = dbutils.widgets.get("src")

# COMMAND ----------

df = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "csv")\
    .option("cloudFiles.schemaLocation", f"/Volumes/workspace/bronze/bronzevolume/{src_value}/checkpoints")\
    .option("cloudFiles.schemaEvolutionMode", "rescue")\
    .load(f"/Volumes/workspace/raw/rawvolume/rawdata/{src_value}/")

# COMMAND ----------


df.writeStream.format("delta")\
     .outputMode("append")\
     .option("checkpointLocation", f"/Volumes/workspace/bronze/bronzevolume/{src_value}/checkpoints")\
     .option("path", f"/Volumes/workspace/bronze/bronzevolume/{src_value}/data")\
     .trigger(once=True)\
     .start()

# COMMAND ----------

