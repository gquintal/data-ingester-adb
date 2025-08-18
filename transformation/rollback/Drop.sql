-- Databricks notebook source
-- MAGIC %python
-- MAGIC # Obtener info específica de un external location
-- MAGIC external_info = spark.sql("DESCRIBE EXTERNAL LOCATION ext-adls-bronze").collect()
-- MAGIC location_bronze = external_info[0]["url"]
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Obtener info específica de un external location
-- MAGIC external_info = spark.sql("DESCRIBE EXTERNAL LOCATION ext-adls-silver").collect()
-- MAGIC location_silver = external_info[0]["url"]

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Obtener info específica de un external location
-- MAGIC external_info = spark.sql("DESCRIBE EXTERNAL LOCATION ext-adls-gold").collect()
-- MAGIC location_gold = external_info[0]["url"]

-- COMMAND ----------

-- DROP TABLES
DROP TABLE IF EXISTS curso_smartdata.bronze.flights;
DROP TABLE IF EXISTS curso_smartdata.silver.flights;
DROP TABLE IF EXISTS curso_smartdata.gold.dim_airlines;
DROP TABLE IF EXISTS curso_smartdata.gold.dim_routes;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm(f"{location_bronze}/tb_flights", True)
-- MAGIC dbutils.fs.rm(f"{location_silver}/tb_flights", True)
-- MAGIC dbutils.fs.rm(f"{location_gold}/tb_dim_airlines", True)
-- MAGIC dbutils.fs.rm(f"{location_gold}/tb_dim_routes", True)
