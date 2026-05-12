# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS weather_API.storage.silver_checkpoints;
# MAGIC create schema if not exists weather_API.audit;
# MAGIC CREATE VOLUME IF NOT EXISTS weather_API.audit.audit_vol;
# MAGIC CREATE SCHEMA IF NOT EXISTS weather_API.storage;
# MAGIC CREATE SCHEMA IF NOT EXISTS weather_API.audit;
# MAGIC CREATE SCHEMA IF NOT EXISTS weather_API.silver;

# COMMAND ----------

BRONZE_TABLE = "weather_API.bronze.bronze_weather_api"

SILVER_CLEAN_TABLE = "weather_API.silver.silver_weather_clean"
SILVER_QUARANTINE_TABLE = "weather_API.silver.silver_weather_quarantine"
AUDIT_LOG_TABLE = "weather_API.silver.audit_log"

CHECKPOINT_PATH = "/Volumes/weather_api/storage/silver_checkpoints"

# COMMAND ----------

import pyspark.sql.types as t

weather_schema = t.StructType([

    t.StructField(
        "coord",
        t.StructType([
            t.StructField("lon", t.DoubleType(), True),
            t.StructField("lat", t.DoubleType(), True)
        ]),
        True
    ),

    t.StructField(
        "weather",
        t.ArrayType(
            t.StructType([
                t.StructField("id", t.IntegerType(), True),
                t.StructField("main", t.StringType(), True),
                t.StructField("description", t.StringType(), True),
                t.StructField("icon", t.StringType(), True)
            ])
        ),
        True
    ),

    t.StructField("base", t.StringType(), True),

    t.StructField(
        "main",
        t.StructType([
            t.StructField("temp", t.DoubleType(), True),
            t.StructField("feels_like", t.DoubleType(), True),
            t.StructField("temp_min", t.DoubleType(), True),
            t.StructField("temp_max", t.DoubleType(), True),
            t.StructField("pressure", t.IntegerType(), True),
            t.StructField("humidity", t.IntegerType(), True),
            t.StructField("sea_level", t.IntegerType(), True),
            t.StructField("grnd_level", t.IntegerType(), True)
        ]),
        True
    ),

    t.StructField("visibility", t.IntegerType(), True),

    t.StructField(
        "wind",
        t.StructType([
            t.StructField("speed", t.DoubleType(), True),
            t.StructField("deg", t.IntegerType(), True),
            t.StructField("gust", t.DoubleType(), True)
        ]),
        True
    ),

    t.StructField(
        "rain",
        t.StructType([
            t.StructField("1h", t.DoubleType(), True),
            t.StructField("3h", t.DoubleType(), True)
        ]),
        True
    ),

    t.StructField(
        "clouds",
        t.StructType([
            t.StructField("all", t.IntegerType(), True)
        ]),
        True
    ),

    t.StructField("dt", t.LongType(), True),

    t.StructField(
        "sys",
        t.StructType([
            t.StructField("type", t.IntegerType(), True),
            t.StructField("id", t.IntegerType(), True),
            t.StructField("country", t.StringType(), True),
            t.StructField("sunrise", t.LongType(), True),
            t.StructField("sunset", t.LongType(), True)
        ]),
        True
    ),

    t.StructField("timezone", t.IntegerType(), True),
    t.StructField("id", t.IntegerType(), True),
    t.StructField("name", t.StringType(), True),
    t.StructField("cod", t.IntegerType(), True)

])

# COMMAND ----------

# DBTITLE 1,Cell 5
# FOREACH BATCH FUNCTION
# =========================
def process_silver_batch(batch_df, batch_id):

    if batch_df.count() == 0:
        return
    
    # Parse JSON
    parsed_df = (
        batch_df
        .withColumn("parsed", F.from_json(F.col("raw_response"), weather_schema))
        .withColumn("ingestion_time", F.current_timestamp())
    )
   
    extracted_df = (
    parsed_df
    .withColumn("city", F.col("parsed.name"))
    .withColumn("temperature", F.col("parsed.main.temp"))
    .withColumn("feels_like", F.col("parsed.main.feels_like"))
    .withColumn("humidity", F.col("parsed.main.humidity"))
    .withColumn("pressure", F.col("parsed.main.pressure"))
    .withColumn("wind_speed", F.col("parsed.wind.speed"))
    .withColumn("weather_condition", F.expr("parsed.weather[0].main"))
    .withColumn("weather_description", F.expr("parsed.weather[0].description"))

    .withColumn(
        "event_time",
        F.from_unixtime(F.col("parsed.dt")).cast("timestamp")
    )
    .withColumn(
        "rain_fall",
        F.coalesce(
            F.col("parsed.rain").getField("1h"),
            F.col("parsed.rain").getField("3h"),
            F.lit(0.0)
        )
    )
    # =========================
    # DERIVED COLUMNS
    # =========================
    # event_date
    .withColumn(
        "event_date",
        F.col("event_time").cast("date")
    )
    # event_hour
    .withColumn(
        "event_hour",
        F.hour(F.col("event_time"))
    )
    # temperature_band
    .withColumn(
        "temperature_band",
        F.when(F.col("temperature") < 15, "Cold")
         .when(
             (F.col("temperature") >= 15) &
             (F.col("temperature") <= 30),
             "Normal"
         )
         .when(
             (F.col("temperature") > 30) &
             (F.col("temperature") <= 40),
             "Hot"
         )
         .otherwise("Extreme")
    )
    # weather_severity_score
    .withColumn(
        "weather_severity_score",
        (
            F.when(F.col("temperature") > 40, 3).otherwise(0)
            +
            F.when(F.col("rain_fall") > 30, 2).otherwise(0)
            +
            F.when(F.col("wind_speed") > 40, 2).otherwise(0)
        ).cast("int")
    )
    # sunrise timestamp
    .withColumn(
        "sunrise_time",
        F.from_unixtime(F.col("parsed.sys.sunrise")).cast("timestamp")
    )
    # sunset timestamp
    .withColumn(
        "sunset_time",
        F.from_unixtime(F.col("parsed.sys.sunset")).cast("timestamp")
    )

    # is_daytime
    .withColumn(
        "is_daytime",
        F.when(
            F.col("sunrise_time").isNull() |
            F.col("sunset_time").isNull(),
            F.lit(None)
        ).otherwise(
            (
                (F.col("event_time") >= F.col("sunrise_time")) &
                (F.col("event_time") <= F.col("sunset_time"))
            )
        )
    )
    # source_system
    .withColumn(
        "source_system",
        F.lit("openweathermap")
    )
    # ingestion_time
    .withColumn(
        "ingestion_time",
        F.current_timestamp()
    )
)
    checked_df = (
        extracted_df
        .withColumn(
            "quarantine_reason",
            F.when(F.col("status_code") != 200, F.lit("api_failed"))
             .when(F.col("raw_response").isNull(), F.lit("raw_json_missing"))
             .when(F.col("parsed").isNull(), F.lit("json_parse_failed"))
             .when(F.col("city").isNull(), F.lit("city_missing"))
             .when(F.col("temperature").isNull(), F.lit("temperature_missing"))
             .when((F.col("temperature") < -50) | (F.col("temperature") > 60), F.lit("out_of_range_temperature"))
             .when(F.col("humidity").isNull(), F.lit("humidity_missing"))
             .when((F.col("humidity") < 0) | (F.col("humidity") > 100), F.lit("out_of_range_humidity"))
             .when(F.col("wind_speed") < 0, F.lit("out_of_range_wind_speed"))
             .when(F.col("event_time").isNull(), F.lit("event_time_missing"))
             .when(F.col("rain_fall").isNull(), F.lit("rain_fall_missing"))
             .otherwise(F.lit(None))
        )
    )
    quarantine_df = (
        checked_df
        .filter(F.col("quarantine_reason").isNotNull())
        .select(
            "requested_city",
            "status_code",
            "latency_ms",
            "raw_response",
            "ingestion_time",
            "quarantine_reason"
        )
    )
    clean_df = (
        checked_df
        .filter(F.col("quarantine_reason").isNull())
        .drop("quarantine_reason")
    )
    # Data is appended into the table
    clean_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(SILVER_CLEAN_TABLE)
            
    quarantine_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(SILVER_QUARANTINE_TABLE)

    # Validation log
    total_rows = checked_df.count()
    valid_rows = clean_df.count()
    quarantine_rows = quarantine_df.count()

    log_df = spark.createDataFrame(
        [(total_rows, valid_rows, quarantine_rows)],
        ["total_rows", "valid_rows", "quarantine_rows"]
    ).withColumn("run_time", F.current_timestamp()) \
        .select("run_time", "total_rows", "valid_rows", "quarantine_rows")

    log_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(AUDIT_LOG_TABLE)

# COMMAND ----------

bronze_stream_df = (
    spark.readStream
    .format("delta")
    .table(BRONZE_TABLE)
)

# COMMAND ----------

# DBTITLE 1,Cell 7
# Drop and recreate tables with correct schema
spark.sql(f"DROP TABLE IF EXISTS {SILVER_QUARANTINE_TABLE}")
spark.sql(f"DROP TABLE IF EXISTS {AUDIT_LOG_TABLE}")

# Ensure target tables exist before streaming starts

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_QUARANTINE_TABLE} (
    requested_city STRING,
    status_code BIGINT,
    latency_ms BIGINT,
    raw_response STRING,
    ingestion_time TIMESTAMP,
    quarantine_reason STRING
)
USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {AUDIT_LOG_TABLE} (
    run_time TIMESTAMP,
    total_rows BIGINT,
    valid_rows BIGINT,
    quarantine_rows BIGINT
)
USING DELTA
""")

query = (
    bronze_stream_df.writeStream
    .format("delta")
    .foreachBatch(process_silver_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

# COMMAND ----------

display(spark.table(SILVER_CLEAN_TABLE))
display(spark.table("weather_API.silver.silver_weather_quarantine"))
display(spark.table("weather_API.silver.audit_log"))

# COMMAND ----------

import sys

# Point to your workspace tests root
TESTS_ROOT = "/Workspace/Users/nirendraprabhu750@gmail.com/weather-rt/test_files"
sys.path.insert(0, TESTS_ROOT)

# Run the test runner
runner_path = TESTS_ROOT + "/run_unit_tests.py"
with open(runner_path, "r", encoding="utf-8") as f:
    exec(f.read(), globals())

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# dbutils.fs.rm(CHECKPOINT_PATH, True)
# spark.sql(f"DROP TABLE IF EXISTS {SILVER_CLEAN_TABLE}")
# spark.sql(f"DROP TABLE IF EXISTS {SILVER_QUARANTINE_TABLE}")
# spark.sql(f"DROP TABLE IF EXISTS {AUDIT_LOG_TABLE}")

# COMMAND ----------

