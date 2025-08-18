-- Databricks notebook source
CREATE CATALOG IF NOT EXISTS curso_smartdata;

USE CATALOG curso_smartdata

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS bronze;
USE SCHEMA bronze;


-- COMMAND ----------

CREATE OR REPLACE TABLE curso_smartdata.bronze.flights (
    Airline STRING,
    Date_of_Journey STRING,
    Source STRING,
    Destination STRING,
    Route STRING,
    Dep_Time STRING,
    Arrival_Time STRING,
    Duration STRING,
    Total_Stops STRING,
    Additional_Info STRING,
    Price STRING,
    ingestion_timestamp TIMESTAMP
)
USING DELTA
LOCATION 'abfss://bronze@adlscursosmartdata.dfs.core.windows.net/tb_flights';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS silver;
USE SCHEMA silver;


-- COMMAND ----------

CREATE OR REPLACE TABLE curso_smartdata.silver.flights (
    Airline STRING,
    Date_of_Journey DATE,
    Source STRING,
    Destination STRING,
    Route STRING,
    Dep_Time STRING,
    Arrival_Time STRING,
    Duration STRING,
    Total_Stops INTEGER,
    Additional_Info STRING,
    Price DECIMAL(10,2),
    Duration_Minutes INTEGER,
    Is_Direct_Flight BOOLEAN,
    ingestion_timestamp TIMESTAMP
)
USING DELTA
LOCATION 'abfss://silver@adlscursosmartdata.dfs.core.windows.net/tb_flights';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS gold;
USE SCHEMA gold;

-- COMMAND ----------

CREATE OR REPLACE TABLE curso_smartdata.gold.dim_airlines (
    airline STRING,
    total_flights LONG,
    avg_price DECIMAL(10,2),
    most_common_route STRING,
    direct_flights_percentage DECIMAL(5,2),
    avg_stops DECIMAL(3,2),
    last_updated TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@adlscursosmartdata.dfs.core.windows.net/tb_dim_airlines';

-- COMMAND ----------

CREATE OR REPLACE TABLE curso_smartdata.gold.dim_routes (
    route_id STRING,
    source STRING,
    destination STRING,
    total_flights LONG,
    avg_price DECIMAL(10,2),
    min_price DECIMAL(10,2),
    max_price DECIMAL(10,2),
    median_price DECIMAL(10,2),
    avg_stops DECIMAL(3,2),
    airlines_count LONG,
    last_updated TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@adlscursosmartdata.dfs.core.windows.net/dim_routes';
