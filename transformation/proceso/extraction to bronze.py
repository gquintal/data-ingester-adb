# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col

# COMMAND ----------

data_path = "abfss://bronze@adlscursosmartdata.dfs.core.windows.net/data"

# COMMAND ----------

# Leer datasets
df_flights = spark.read.parquet(data_path)

# COMMAND ----------

df_flights_bronze = df_flights.withColumn("ingestion_timestamp", current_timestamp())

# COMMAND ----------

df_flights_bronze.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("curso_smartdata.bronze.flights")
