# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *

BRONZE_TABLE = "weather_API.bronze.bronze_weather_api2"
SILVER_TABLE = "weather_API.silver.silver_weather_clean"
QUARANTINE_TABLE = "weather_API.silver.silver_weather_quarantine"
VALIDATION_LOG_TABLE = "weather_API.silver.validation_log"

# COMMAND ----------

#Bronze table exists
assert spark.catalog.tableExists(BRONZE_TABLE), "Bronze table not found"

print("PASSED: Bronze table exists")

# COMMAND ----------

#Bronze has required columns
bronze_cols = spark.table(BRONZE_TABLE).columns

required_cols = [
    "source_city",
    "api_status_code",
    "latency_ms",
    "raw_json",
    "ingestion_time",
    "load_date",
    "batch_id",
    "source"
]

missing_cols = [c for c in required_cols if c not in bronze_cols]

assert len(missing_cols) == 0, f"Missing Bronze columns: {missing_cols}"

print("PASSED: Bronze required columns exist")

# COMMAND ----------

#Bronze has data
bronze_count = spark.table(BRONZE_TABLE).count()

assert bronze_count > 0, "Bronze table is empty"

print("PASSED: Bronze has data")

# COMMAND ----------

#API status code valid
invalid_status_count = (
    spark.table(BRONZE_TABLE)
    .filter(F.col("api_status_code").isNull())
    .count()
)

assert invalid_status_count == 0, "Some API status codes are NULL"

print("PASSED: API status code is valid")

# COMMAND ----------

