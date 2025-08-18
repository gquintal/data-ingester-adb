# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, to_date, current_timestamp, split, regexp_extract

# COMMAND ----------

df_flights_bronze = spark.table("curso_smartdata.bronze.flights")

# COMMAND ----------

# Eliminar duplicados
df_flights_nodup = df_flights_bronze.dropDuplicates()

# COMMAND ----------

df_silver_col = df_flights_nodup.select(\
    "Airline", "Date_of_Journey", "Source", "Destination", "Route", "Dep_Time", "Arrival_Time", "Duration", "Total_Stops", "Additional_Info", "Price"\
    )

# COMMAND ----------

# castear columna fecha
df_silver = df_silver_col.withColumn("Date_of_Journey", to_date(col("Date_of_Journey"), "d/M/yyyy"))

# COMMAND ----------

# Agregar columna de duracion en minutos
df_flights_silver = df_silver.withColumn(
    "Duration_Minutes",
    (
        # Extraer horas (si existen) y convertir a minutos
        when(col("Duration").rlike("\\d+h"), 
             regexp_extract(col("Duration"), "(\\d+)h", 1).cast("int") * 60)
        .otherwise(0)
    ) + (
        # Extraer minutos (si existen)
        when(col("Duration").rlike("\\d+m"), 
             regexp_extract(col("Duration"), "(\\d+)m", 1).cast("int"))
        .otherwise(0)
    )
)

# COMMAND ----------

# Estandarizar Total_Stops
df_flights_silver = df_flights_silver.withColumn(
    "Total_Stops",
    when(col("Total_Stops") == "non-stop", 0)
    .when(col("Total_Stops") == "1 stop", 1)
    .when(col("Total_Stops") == "2 stops", 2)
    .when(col("Total_Stops") == "3 stops", 3)
    .when(col("Total_Stops") == "4 stops", 4)
    .otherwise(0)
)

# COMMAND ----------

# Identificar vuelos directos
df_flights_silver = df_flights_silver.withColumn(
    "Is_Direct_Flight",
    when(col("Total_Stops") == 0, True).otherwise(False)
)

# COMMAND ----------

df_flights_silver = df_flights_silver.withColumn("Price", col("Price").cast("decimal(10,2)"))

# COMMAND ----------

# Agregar timestamp
df_flights_silver = df_flights_silver.withColumn("ingestion_timestamp", current_timestamp())

# COMMAND ----------

df_flights_silver.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("curso_smartdata.silver.flights")
