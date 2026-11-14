# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE VOLUME workspace.raw.rawvolume
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME workspace.raw.bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME workspace.raw.silver

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME workspace.raw.gold

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/raw/rawvolume/rawdata")

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/raw/rawvolume/rawdata/bookings")
dbutils.fs.mkdirs("/Volumes/workspace/raw/rawvolume/rawdata/customers")
dbutils.fs.mkdirs("/Volumes/workspace/raw/rawvolume/rawdata/airports")
dbutils.fs.mkdirs("/Volumes/workspace/raw/rawvolume/rawdata/flights")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME workspace.bronze.bronzeVolume

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/workspace/bronze/bronzevolume/bookings/data/`