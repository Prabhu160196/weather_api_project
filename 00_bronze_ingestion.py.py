# Databricks notebook source
 # %sql
# DROP TABLE IF EXISTS weather_API.bronze.bronze_weather_api;
# drop volume if exists weather_API.storage.checkpoints;


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS weather_API;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS weather_API.configs;
# MAGIC CREATE SCHEMA IF NOT EXISTS weather_API.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS weather_API.storage;
# MAGIC CREATE SCHEMA IF NOT EXISTS weather_API.validation;
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS weather_API.configs.weather_rt;
# MAGIC CREATE VOLUME IF NOT EXISTS weather_API.storage.checkpoints;
# MAGIC CREATE VOLUME IF NOT EXISTS weather_API.validation.validation_files;
# MAGIC
# MAGIC DROP TABLE IF EXISTS weather_API.bronze.bronze_weather_api;

# COMMAND ----------

import requests
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *


# Load config
config = json.load(open("/Workspace/Users/nirendraprabhu750@gmail.com/weather-rt/config.json"))

api_key = config["api_key"]
cities = config["cities"]
base_url = config["api_base_url"]

data = []

for city in cities:
    url = f"{base_url}?q={city}&appid={api_key}&units=metric"

    start = datetime.now()
    response = requests.get(url)
    latency_ms = (datetime.now() - start).total_seconds() * 1000

    raw_json = response.text if response.status_code == 200 else None

    data.append((
        city,
        response.status_code,
        latency_ms,
        raw_json
    ))

# Define schema
schema = StructType([
    StructField("source_city", StringType(), True),
    StructField("api_status_code", IntegerType(), True),
    StructField("latency_ms", DoubleType(), True),
    StructField("raw_json", StringType(), True)
])

# Create DataFrame with proper column names
df = spark.createDataFrame(data, schema=schema)

# Add metadata columns
bronze_df = (
    df
    .withColumn("ingestion_time", F.current_timestamp())
    .withColumn("load_date", F.current_date())
    .withColumn("batch_id", F.date_format(F.current_timestamp(), "yyyyMMddHHmmss"))
    .withColumn("source", F.lit("openweather_api"))
)

# Write to Delta Bronze table
bronze_df.write.format("delta") \
    .mode("append") \
    .partitionBy("load_date") \
    .saveAsTable("weather_api.bronze.bronze_weather_api2")


# COMMAND ----------

df_bronze = spark.read.table("weather_api.bronze.bronze_weather_api2")
display(df_bronze)

# COMMAND ----------

# %sql
# drop table if exists weather_api.bronze.bronze_weather_api2