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
# MAGIC                 
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS weather_API.configs.weather_rt;

# COMMAND ----------

import requests
import json
import time
from datetime import datetime
from pyspark.sql import functions as F


# Load config
config = json.load(open("/Workspace/Users/nirendraprabhu750@gmail.com/weather-rt/config.json"))

api_key = config["api_key"]
cities = config["cities"]
base_url = config["api_base_url"]
units = config.get("units", 'metric')

def fetch_weather_for_city(cities, base_url, api_key, units='metric'):

    url = f"{base_url}?q={cities}&appid={api_key}&units={units}"
    start_time = time.time()
    call_timestamp = datetime.now()

    response = requests.get(url, timeout=10)
    latency_ms = int((time.time() - start_time) * 1000)
    return {
            'requested_city': cities,
            'status_code': response.status_code,
            'raw_response': response.text,
            'call_timestamp': call_timestamp,
            'latency_ms': latency_ms
        }

weather_api_responses = [fetch_weather_for_city(city, base_url, api_key, units) for city in cities]
# Create DataFrame with proper column names
df = spark.createDataFrame(weather_api_responses)

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
    .saveAsTable("weather_api.bronze.bronze_weather_api")


# COMMAND ----------

df_bronze = spark.read.table("weather_api.bronze.bronze_weather_api")
display(df_bronze)

# COMMAND ----------

# spark.sql("DROP TABLE IF EXISTS weather_api.bronze.bronze_weather_api")


# COMMAND ----------

