# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, count, avg, sum, when, concat_ws, row_number, min, max, countDistinct, percentile_approx
from pyspark.sql.window import Window

# COMMAND ----------

df_flights_silver = spark.table("curso_smartdata.silver.flights")

# COMMAND ----------

# MAGIC %md
# MAGIC Metricas por Aerolinea

# COMMAND ----------

# Calcular métricas por aerolínea
dim_airlines = df_flights_silver.groupBy("Airline").agg(
    count("*").alias("total_flights"),
    avg("Price").cast("decimal(10,2)").alias("avg_price"),
    #avg("duration_hours").cast("decimal(5,2)").alias("avg_duration_hours"),
    sum(when(col("is_direct_flight"), 1).otherwise(0)).alias("direct_flights"),
    avg("total_stops").cast("decimal(3,2)").alias("avg_stops")
)

# COMMAND ----------

# Calcular porcentaje de vuelos directos
dim_airlines = dim_airlines.withColumn(
    "direct_flights_percentage",
    ((col("direct_flights") / col("total_flights")) * 100).cast("decimal(5,2)")
)

# COMMAND ----------

# Encontrar la ruta más común por aerolínea
route_counts = df_flights_silver.groupBy("Airline", "Source", "Destination") \
    .count() \
    .withColumn("route", concat_ws("-", col("Source"), col("Destination")))

window_spec = Window.partitionBy("Airline").orderBy(col("count").desc())

most_common_routes = route_counts \
    .withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") == 1) \
    .select("Airline", col("route").alias("most_common_route"))

# COMMAND ----------

# Unir con métricas principales
dim_airlines_final = dim_airlines.join(most_common_routes, "Airline", "left") \
    .withColumn("last_updated", current_timestamp()) \
    .select(
        col("Airline").alias("airline"),
        "total_flights",
        "avg_price",
        "most_common_route",
        "direct_flights_percentage",
        "avg_stops",
        "last_updated"
    )

# COMMAND ----------

dim_airlines_final.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("curso_smartdata.gold.dim_airlines")

# COMMAND ----------

# MAGIC %md
# MAGIC Metricas por Ruta

# COMMAND ----------

df_with_route_id = df_flights_silver.withColumn(
    "route_id", 
    concat_ws("-", col("Source"), col("Destination"))
)

# COMMAND ----------

# Calcular métricas por ruta
dim_routes = df_with_route_id.groupBy("route_id", "Source", "Destination").agg(
    count("*").alias("total_flights"),
    avg("Price").cast("decimal(10,2)").alias("avg_price"),
    min("Price").cast("decimal(10,2)").alias("min_price"),
    max("Price").cast("decimal(10,2)").alias("max_price"),
    #avg("duration_hours").cast("decimal(5,2)").alias("avg_duration_hours"),
    avg("total_stops").cast("decimal(3,2)").alias("avg_stops"),
    countDistinct("Airline").alias("airlines_count"),
    percentile_approx("Price", 0.5).cast("decimal(10,2)").alias("median_price")
)

# COMMAND ----------

# Agregar timestamp
dim_routes_final = dim_routes \
    .withColumn("last_updated", current_timestamp()) \
    .select(
        "route_id",
        col("Source").alias("source"),
        col("Destination").alias("destination"),
        "total_flights",
        "avg_price",
        "min_price",
        "max_price",
        "median_price",
        "avg_stops",
        "airlines_count",
        "last_updated"
    ) \
    .orderBy(col("total_flights").desc())

# COMMAND ----------

dim_routes_final.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("curso_smartdata.gold.dim_routes")
