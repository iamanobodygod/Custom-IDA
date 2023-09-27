# Databricks notebook source
# dbutils.fs.mount(

#   source = "wasbs://raw@sushmitastorage.blob.core.windows.net",

#   mount_point = "/mnt/sushmitastorage/raw",

#   extra_configs = {"fs.azure.account.key.sushmitastorage.blob.core.windows.net":"k6Ujo0OcQbxngIZr5pQnleaVUA4j0gXoNYUCl3J3INhgTGtlXPpZNkxVPWtNLQXdV2K983tc4HqY+ASt9kMn0w=="})

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/sushmitastorage/

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/sushmitastorage/raw/

# COMMAND ----------

df = spark.read.json("dbfs:/mnt/sushmitastorage/raw/json/")

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df1=df.withColumn("ingestiondate",current_timestamp()).withColumn("path",input_file_name())

# COMMAND ----------

df1=df.withColumn("ingestiondate",current_timestamp()).withColumn("path",input_file_name())

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create schema if not exists json

# COMMAND ----------

df1.write.mode("overwrite").option("path","dbfs:/mnt/sushmitastorage/raw/json/sush/json").saveAsTable("json.bronzetable")

# COMMAND ----------



# COMMAND ----------


