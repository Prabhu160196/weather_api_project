# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *

BRONZE_TABLE = "weather_API.bronze.bronze_weather_api2"
SILVER_TABLE = "weather_API.silver.silver_weather_clean"
QUARANTINE_TABLE = "weather_API.silver.silver_weather_quarantine"
VALIDATION_LOG_TABLE = "weather_API.silver.validation_log"

# COMMAND ----------

#Silver table exists
assert spark.catalog.tableExists(SILVER_TABLE), "Silver clean table not found"

print("PASSED: Silver table exists")

# COMMAND ----------

# Silver has no invalid temperature
bad_temp_count = (
    spark.table(SILVER_TABLE)
    .filter((F.col("temperature") < -50) | (F.col("temperature") > 60))
    .count()
)

assert bad_temp_count == 0, "Silver has out-of-range temperature"

print("PASSED: Silver temperature validation")

# COMMAND ----------

# Silver has no invalid humidity
bad_humidity_count = (
    spark.table(SILVER_TABLE)
    .filter((F.col("humidity") < 0) | (F.col("humidity") > 100))
    .count()
)

assert bad_humidity_count == 0, "Silver has invalid humidity"

print("PASSED: Silver humidity validation")

# COMMAND ----------

#Silver has no negative wind speed
bad_wind_count = (
    spark.table(SILVER_TABLE)
    .filter(F.col("wind_speed") < 0)
    .count()
)

assert bad_wind_count == 0, "Silver has negative wind speed"

print("PASSED: Silver wind speed validation")

# COMMAND ----------

# Quarantine table exists
assert spark.catalog.tableExists(QUARANTINE_TABLE), "Quarantine table not found"

print("PASSED: Quarantine table exists")

# COMMAND ----------

#Quarantine reason should not be null
bad_quarantine_count = (
    spark.table(QUARANTINE_TABLE)
    .filter(F.col("quarantine_reason").isNull())
    .count()
)

assert bad_quarantine_count == 0, "Quarantine table has NULL reason"

print("PASSED: Quarantine reason validation")

# COMMAND ----------

#Validation log exists
assert spark.catalog.tableExists(VALIDATION_LOG_TABLE), "Validation log table not found"

print("PASSED: Validation log table exists")

# COMMAND ----------

#Validation log has run history
log_count = spark.table(VALIDATION_LOG_TABLE).count()

assert log_count > 0, "Validation log is empty"

print("PASSED: Validation log has run records")

# COMMAND ----------

#Total rows check
latest_log = (
    spark.table(VALIDATION_LOG_TABLE)
    .orderBy(F.col("run_time").desc())
    .limit(1)
    .collect()[0]
)

assert latest_log["total_rows"] == latest_log["valid_rows"] + latest_log["quarantine_rows"], \
    "Validation count mismatch"

print("PASSED: Validation log count matches")